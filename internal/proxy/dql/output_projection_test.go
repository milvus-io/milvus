// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"sort"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func projectionTestSchema(dynamic bool) *schemaInfo {
	fields := []*schemapb.FieldSchema{
		{Name: "id", FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{Name: "title", FieldID: 101, DataType: schemapb.DataType_VarChar},
		{Name: "price", FieldID: 102, DataType: schemapb.DataType_Double},
		{Name: "vec", FieldID: 103, DataType: schemapb.DataType_FloatVector},
		{Name: "count", FieldID: 104, DataType: schemapb.DataType_Int64},
		{Name: "as", FieldID: 105, DataType: schemapb.DataType_Int64},
	}
	if dynamic {
		fields = append(fields, &schemapb.FieldSchema{Name: "$meta", FieldID: 106, DataType: schemapb.DataType_JSON, IsDynamic: true})
	}
	return mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name:               "projection_test",
		EnableDynamicField: dynamic,
		Fields:             fields,
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			Name: "st", FieldID: 110,
			Fields: []*schemapb.FieldSchema{
				{Name: "st[a]", FieldID: 111, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
			},
		}},
	})
}

func sorted(in []string) []string {
	out := append([]string{}, in...)
	sort.Strings(out)
	return out
}

func TestBindOutputFields_Aliases(t *testing.T) {
	schema := projectionTestSchema(true)
	for _, tc := range []struct {
		name       string
		in         []string
		search     bool
		stored     []string
		outputs    []string
		dynamic    []string
		aliases    []outputAlias
		keepSource []string
	}{
		{"plain", []string{"title", "price"}, false,
			[]string{"price", "title"}, []string{"price", "title"}, nil, nil, []string{"price", "title"}},
		{"alias", []string{"title AS t"}, false,
			[]string{"title"}, []string{"t"}, nil, []outputAlias{{"title", "t"}}, nil},
		{"alias and plain", []string{"title AS t", "title"}, false,
			[]string{"title"}, []string{"t", "title"}, nil, []outputAlias{{"title", "t"}}, []string{"title"}},
		{"two aliases of one field", []string{"title AS t1", "title as t2"}, false,
			[]string{"title"}, []string{"t1", "t2"}, nil, []outputAlias{{"title", "t1"}, {"title", "t2"}}, nil},
		{"same alias item twice", []string{"title AS t", " title  AS  t "}, false,
			[]string{"title"}, []string{"t"}, nil, []outputAlias{{"title", "t"}}, nil},
		{"alias equals a field that is not returned", []string{"title AS price"}, false,
			[]string{"title"}, []string{"price"}, nil, []outputAlias{{"title", "price"}}, nil},
		{"swap", []string{"title AS price", "price AS title"}, false,
			[]string{"price", "title"}, []string{"price", "title"}, nil, []outputAlias{{"title", "price"}, {"price", "title"}}, nil},
		{"keyword names", []string{"as AS as2", "count AS as"}, false,
			[]string{"as", "count"}, []string{"as", "as2"}, nil, []outputAlias{{"as", "as2"}, {"count", "as"}}, nil},
		{"vector alias", []string{"vec AS embedding"}, false,
			[]string{"vec"}, []string{"embedding"}, nil, []outputAlias{{"vec", "embedding"}}, nil},
		{"dynamic key forms share one output name", []string{"color", `$meta["color"]`}, false,
			[]string{"$meta"}, []string{"color"}, []string{"color"}, nil, nil},
		{"explicit dynamic key is named by the key", []string{`$meta["color"]`}, false,
			[]string{"$meta"}, []string{"color"}, []string{"color"}, nil, nil},
		{"search drops pk from stored list", []string{"id", "title AS t"}, true,
			[]string{"title"}, []string{"t"}, nil, []outputAlias{{"title", "t"}}, []string{"id"}},
		{"search pk alias", []string{"id AS pk"}, true,
			nil, []string{"pk"}, nil, []outputAlias{{"id", "pk"}}, nil},
		{"query pk alias", []string{"id AS pk"}, false,
			[]string{"id"}, []string{"pk"}, nil, []outputAlias{{"id", "pk"}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := bindOutputFields(tc.in, schema, tc.search)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.stored, b.resultFieldNames)
			require.ElementsMatch(t, tc.outputs, b.userOutputFields)
			require.ElementsMatch(t, tc.dynamic, b.userDynamicFields)
			require.Equal(t, tc.aliases, b.projection.aliases)
			kept := make([]string, 0)
			for name := range b.projection.keepSource {
				kept = append(kept, name)
			}
			want := tc.keepSource
			if !tc.search {
				// A Query always returns the primary key under its own name.
				want = append([]string{"id"}, want...)
			}
			require.Equal(t, sorted(want), sorted(kept))
		})
	}
}

func TestBindOutputFields_Star(t *testing.T) {
	schema := projectionTestSchema(true)
	b, err := bindOutputFields([]string{"*", "title AS t"}, schema, false)
	require.NoError(t, err)
	require.Contains(t, b.userOutputFields, "t")
	require.Contains(t, b.userOutputFields, "title")
	require.True(t, b.projection.wholeDynamic)
	require.True(t, b.projection.keepSource["title"])
	require.Empty(t, b.userDynamicFields)

	// "*" returns "price" under its own name, so the alias is not unique.
	for _, in := range [][]string{{"*", "title AS price"}, {"title AS price", "*"}} {
		_, err = bindOutputFields(in, schema, false)
		require.ErrorContains(t, err, "duplicate output field name price")
	}

	b, err = bindOutputFields([]string{"$meta", "title AS t"}, schema, false)
	require.NoError(t, err)
	require.True(t, b.projection.wholeDynamic)

	b, err = bindOutputFields([]string{"color", "title AS t"}, schema, false)
	require.NoError(t, err)
	require.False(t, b.projection.wholeDynamic)
}

func TestBindOutputFields_Errors(t *testing.T) {
	dyn := projectionTestSchema(true)
	static := projectionTestSchema(false)
	for _, tc := range []struct {
		name   string
		schema *schemaInfo
		in     []string
		search bool
		want   string
	}{
		{"two fields one alias", dyn, []string{"title AS x", "price AS x"}, false, "duplicate output field name x"},
		{"alias equals returned field", dyn, []string{"title", "price AS title"}, false, "duplicate output field name title"},
		{"alias equals returned field, reversed", dyn, []string{"price AS title", "title"}, false, "duplicate output field name title"},
		{"alias equals explicit dynamic key", dyn, []string{"title AS c", "c"}, false, "duplicate output field name c"},
		{"alias equals explicit dynamic key, path form", dyn, []string{`$meta["c"]`, "title AS c"}, false, "duplicate output field name c"},
		{"alias equals aggregate name", dyn, []string{"count(*)", "price AS x", "sum(price) AS x"}, false, "duplicate output field name x"},
		{"alias is pk, query", dyn, []string{"title AS id"}, false, "primary key field name"},
		{"alias is pk, search", dyn, []string{"title AS id"}, true, "primary key field name"},
		{"search alias distance", dyn, []string{"title AS distance"}, true, "reserved in search results"},
		{"search alias score", dyn, []string{"price AS score"}, true, "reserved in search results"},
		{"dynamic key alias", dyn, []string{"color AS c"}, false, "dynamic field key color cannot take an alias"},
		{"dynamic key alias, same name", dyn, []string{"color AS color"}, false, "cannot take an alias"},
		{"dynamic path alias", dyn, []string{`$meta["color"] AS c`}, false, "dynamic field key color cannot take an alias"},
		{"dynamic field alias", dyn, []string{"$meta AS m"}, false, "the dynamic field $meta cannot take an alias"},
		{"struct array alias", dyn, []string{"st AS s"}, false, "struct array field st cannot take an alias"},
		{"struct sub-field alias", dyn, []string{"st[a] AS s"}, false, "struct sub-field st[a] cannot take an alias"},
		{"unknown field with alias, static schema", static, []string{"colr AS c"}, false, "field colr not exist"},
		{"unknown field, static schema", static, []string{"colr"}, false, "field colr not exist"},
		{"missing AS", dyn, []string{"title t"}, false, "parse output field name failed: title t"},
		{"missing AS, static schema", static, []string{"title t"}, false, "field title t not exist"},
		{"unparsable name", dyn, []string{"%"}, false, "parse output field name failed: %"},
		{"empty name", dyn, []string{""}, false, "parse output field name failed: "},
		{"empty name, static schema", static, []string{""}, false, "not exist"},
		{"expression", dyn, []string{"price * 0.8 AS d"}, false, "expressions in output fields are not supported yet"},
		{"score in query", dyn, []string{"$score"}, false, "only with a ranker"},
		{"distance in query", dyn, []string{"$distance AS d"}, false, "not available in a query"},
		{"sub-request column in query", dyn, []string{"$rank(0)"}, false, "only available in hybrid search"},
		{"virtual column name taken by an alias", dyn, []string{"$score AS s", "title AS s"}, true, "duplicate output field name s"},
		{"alias equal to a virtual column's default name is impossible", dyn, []string{"title AS $score"}, true, "parse output field name failed"},
		{"nested dynamic path", dyn, []string{`$meta["a"]["b"]`}, false, "parse output field name failed"},
		{"aggregate on missing field", dyn, []string{"sum(nope) AS s"}, false, "target field nope for aggregation:sum does not exist"},
		{"sum star", dyn, []string{"sum(*)"}, false, "only count(*) is allowed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := bindOutputFields(tc.in, tc.schema, tc.search)
			require.ErrorContains(t, err, tc.want)
		})
	}
	// In a Query the alias names "distance" and "score" are ordinary names.
	_, err := bindOutputFields([]string{"title AS distance", "price AS score"}, dyn, false)
	require.NoError(t, err)
}

func TestBindOutputFields_Aggregates(t *testing.T) {
	schema := projectionTestSchema(true)
	b, err := bindOutputFields([]string{"count(*) AS n", "SUM( price ) AS total", "avg(price)", "title"}, schema, false)
	require.NoError(t, err)
	names := make([]string, 0)
	for _, a := range b.aggregates {
		names = append(names, a.OriginalName())
	}
	// avg contributes a sum and a count under one name.
	require.Equal(t, []string{"count(*)", "SUM( price )", "avg(price)", "avg(price)"}, names)
	require.ElementsMatch(t, []string{"n", "total", "avg(price)", "title"}, b.userOutputFields)
	require.Equal(t, []outputAlias{{"count(*)", "n"}, {"SUM( price )", "total"}}, b.projection.aliases)
	require.Equal(t, []string{"count(*)", "SUM( price )", "avg(price)", "title"}, b.sourceOutputFields)

	// A field named like an aggregate function stays a field.
	b, err = bindOutputFields([]string{"count", "count(count) AS c"}, schema, false)
	require.NoError(t, err)
	require.Len(t, b.aggregates, 1)
	require.ElementsMatch(t, []string{"count"}, b.resultFieldNames)

	// Without aliases the request's own list is handed on unchanged.
	in := []string{" count(*) ", "title"}
	b, err = bindOutputFields(in, schema, false)
	require.NoError(t, err)
	require.Equal(t, in, b.sourceOutputFields)

	// The same aggregate with and without an alias is computed once.
	b, err = bindOutputFields([]string{"count(*)", "count(*) AS n"}, schema, false)
	require.NoError(t, err)
	require.Equal(t, []string{"count(*)"}, b.sourceOutputFields)
}

func TestResolveOrderByAliases(t *testing.T) {
	schema := projectionTestSchema(true)
	b, err := bindOutputFields([]string{"title AS t", "price AS title", "count(*) AS n"}, schema, false)
	require.NoError(t, err)

	got, err := resolveOrderByAliases([]string{"t:desc", " title ", "price:asc", "other"}, b.projection)
	require.NoError(t, err)
	// "title" is an alias of price here, and the alias wins over the field.
	require.Equal(t, []string{"title:desc", "price", "price:asc", "other"}, got)

	// Alias lookup is case-sensitive.
	got, err = resolveOrderByAliases([]string{"T:desc"}, b.projection)
	require.NoError(t, err)
	require.Equal(t, []string{"T:desc"}, got)

	_, err = resolveOrderByAliases([]string{"n:desc"}, b.projection)
	require.ErrorContains(t, err, "ORDER BY on aggregate expression")

	in := []string{"t:desc"}
	got, err = resolveOrderByAliases(in, nil)
	require.NoError(t, err)
	require.Equal(t, in, got)
}

func TestParseOrderByFields_Alias(t *testing.T) {
	schema := projectionTestSchema(true)
	b, err := bindOutputFields([]string{"price AS p"}, schema, true)
	require.NoError(t, err)
	params := []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "p:desc"}}

	fields, err := parseOrderByFields(params, schema.CollectionSchema, b.projection)
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, int64(102), fields[0].FieldID)
	require.False(t, fields[0].Ascending)

	plain, err := parseOrderByFields([]*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "price:desc"}}, schema.CollectionSchema)
	require.NoError(t, err)
	require.Equal(t, plain, fields)
}

func stringColumn(name string, id int64, values ...string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_VarChar, FieldName: name, FieldId: id,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}},
		}},
	}
}

func jsonColumn(rows ...string) *schemapb.FieldData {
	data := make([][]byte, len(rows))
	for i, row := range rows {
		data[i] = []byte(row)
	}
	return &schemapb.FieldData{
		Type: schemapb.DataType_JSON, FieldName: "$meta", FieldId: 106, IsDynamic: true,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}},
		}},
	}
}

func columnNames(columns []*schemapb.FieldData) []string {
	names := make([]string, len(columns))
	for i, c := range columns {
		names[i] = c.GetFieldName()
	}
	return names
}

func TestOutputProjectionApply(t *testing.T) {
	schema := projectionTestSchema(true)
	apply := func(t *testing.T, items []string, columns ...*schemapb.FieldData) []*schemapb.FieldData {
		b, err := bindOutputFields(items, schema, false)
		require.NoError(t, err)
		return b.projection.apply(columns)
	}

	t.Run("no alias returns the input", func(t *testing.T) {
		in := []*schemapb.FieldData{stringColumn("title", 101, "a")}
		out := apply(t, []string{"title"}, in...)
		require.Equal(t, in, out)
	})

	t.Run("rename", func(t *testing.T) {
		out := apply(t, []string{"title AS t"}, stringColumn("id", 100, "1"), stringColumn("title", 101, "a", "b"))
		require.Equal(t, []string{"id", "t"}, columnNames(out))
		require.Equal(t, []string{"a", "b"}, out[1].GetScalars().GetStringData().GetData())
		require.Equal(t, int64(101), out[1].GetFieldId())
		require.Equal(t, schemapb.DataType_VarChar, out[1].GetType())
	})

	t.Run("copy when the source is also returned", func(t *testing.T) {
		out := apply(t, []string{"title", "title AS t1", "title AS t2"}, stringColumn("title", 101, "a"))
		require.Equal(t, []string{"title", "t1", "t2"}, columnNames(out))
		for _, c := range out {
			require.Equal(t, []string{"a"}, c.GetScalars().GetStringData().GetData())
		}
	})

	t.Run("swap", func(t *testing.T) {
		out := apply(t, []string{"title AS as", "as AS title"},
			stringColumn("title", 101, "T"), stringColumn("as", 105, "A"))
		require.Equal(t, []string{"as", "title"}, columnNames(out))
		require.Equal(t, []string{"T"}, out[0].GetScalars().GetStringData().GetData())
		require.Equal(t, []string{"A"}, out[1].GetScalars().GetStringData().GetData())
	})

	t.Run("valid data is carried", func(t *testing.T) {
		in := stringColumn("title", 101, "a", "")
		in.ValidData = []bool{true, false}
		out := apply(t, []string{"title AS t"}, in)
		require.Equal(t, []bool{true, false}, out[0].GetValidData())
	})

	t.Run("dynamic keys equal to an alias are removed", func(t *testing.T) {
		meta := jsonColumn(
			`{"c": 1, "color": "red"}`,
			`{"color":"blue"}`,
			`{"x":{"c":2},"c":[1,2],"d":"c"}`,
			`{"c":3,"k":"v"}`,
			`{}`,
			`{"note":"say \"c\" twice"}`,
			`{"c":1}`,
		)
		out := apply(t, []string{"title AS c", "*"}, stringColumn("title", 101, "a", "b", "c", "d", "e", "f", "g"), meta)
		require.Equal(t, []string{"title", "c", "$meta"}, columnNames(out))
		rows := out[2].GetScalars().GetJsonData().GetData()
		require.Equal(t, `{"color":"red"}`, string(rows[0]))
		require.Equal(t, `{"color":"blue"}`, string(rows[1]))
		require.Equal(t, `{"x":{"c":2},"d":"c"}`, string(rows[2]))
		require.Equal(t, `{"k":"v"}`, string(rows[3]))
		require.Equal(t, `{}`, string(rows[4]))
		require.Equal(t, `{"note":"say \"c\" twice"}`, string(rows[5]))
		require.Equal(t, `{}`, string(rows[6]))
	})

	t.Run("selected dynamic keys are left alone", func(t *testing.T) {
		meta := jsonColumn(`{"color":"red"}`)
		out := apply(t, []string{"title AS t", "color"}, stringColumn("title", 101, "a"), meta)
		require.Equal(t, `{"color":"red"}`, string(out[1].GetScalars().GetJsonData().GetData()[0]))
	})

	t.Run("rows untouched when no key matches", func(t *testing.T) {
		row := `{ "color" : "red" , "n" : 1 }`
		out := apply(t, []string{"title AS t", "$meta"}, stringColumn("title", 101, "a"), jsonColumn(row))
		require.Equal(t, row, string(out[1].GetScalars().GetJsonData().GetData()[0]))
	})
}

func TestBindOutputFields_SearchScore(t *testing.T) {
	schema := projectionTestSchema(true)
	b, err := bindOutputFields([]string{"$score", "title", "$score AS s", " $score ", "$score AS s", "$score AS score"}, schema, true)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"title"}, b.resultFieldNames)
	require.ElementsMatch(t, []string{"$score", "title", "s", "score"}, b.userOutputFields)
	require.Len(t, b.projection.virtualItems, 3)
	for i, want := range []struct {
		id   int64
		name string
	}{{-1000, "$score"}, {-1001, "s"}, {-1002, "score"}} {
		item := b.projection.virtualItems[i]
		require.Equal(t, want.id, item.id)
		require.Equal(t, want.name, item.name)
		require.Equal(t, schemapb.DataType_Float, item.dataType)
		require.True(t, item.evaluatedAtProxy(true))
		require.Same(t, item, b.projection.virtualItemByID(want.id))
	}
	require.Nil(t, b.projection.virtualItemByID(-1003))
	require.False(t, b.projection.empty())

	result := &schemapb.SearchResultData{
		Scores:     []float32{0.9, 0.5, 0.1},
		FieldsData: []*schemapb.FieldData{stringColumn("title", 101, "a", "b", "c")},
	}
	b.projection.appendSearchScoreColumns(result)
	out := b.projection.apply(result.FieldsData)
	require.Equal(t, []string{"title", "$score", "s", "score"}, columnNames(out))
	for _, c := range out[1:] {
		require.Equal(t, schemapb.DataType_Float, c.GetType())
		require.Equal(t, []float32{0.9, 0.5, 0.1}, c.GetScalars().GetFloatData().GetData())
	}
	require.Equal(t, int64(-1000), out[1].GetFieldId())
	// The columns hold copies: a later rewrite of Scores does not change them.
	result.Scores[0] = 0
	require.Equal(t, float32(0.9), out[1].GetScalars().GetFloatData().GetData()[0])

	// A virtual column's output name is removed from the whole dynamic field.
	b, err = bindOutputFields([]string{"*", "$score AS s"}, schema, true)
	require.NoError(t, err)
	meta := jsonColumn(`{"s":1,"k":2}`, `{"$score":3}`)
	out = b.projection.apply([]*schemapb.FieldData{meta})
	rows := out[0].GetScalars().GetJsonData().GetData()
	require.Equal(t, `{"k":2}`, string(rows[0]))
	require.Equal(t, `{"$score":3}`, string(rows[1]))

	b, err = bindOutputFields([]string{"*", "$score"}, schema, true)
	require.NoError(t, err)
	out = b.projection.apply([]*schemapb.FieldData{jsonColumn(`{"$score":3,"k":2}`)})
	require.Equal(t, `{"k":2}`, string(out[0].GetScalars().GetJsonData().GetData()[0]))
}

func longColumn(id int64, values ...int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldId: id,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
		}},
	}
}

func intIDs(ids ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
}

func TestBindOutputFields_PlacementColumns(t *testing.T) {
	schema := projectionTestSchema(true)
	for _, search := range []bool{false, true} {
		b, err := bindOutputFields([]string{"$seg_id", "title", "$seg_offset AS off", "$seg_id AS sid"}, schema, search)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"$seg_id", "title", "off", "sid"}, b.userOutputFields)
		items := b.projection.planItems(search)
		require.Len(t, items, 3)
		require.Equal(t, int64(-1000), items[0].GetOutputId())
		require.Equal(t, planpb.VirtualColumn_VirtualColumnSegmentID, items[0].GetVirtualColumn())
		require.Equal(t, int64(-1001), items[1].GetOutputId())
		require.Equal(t, planpb.VirtualColumn_VirtualColumnSegmentOffset, items[1].GetVirtualColumn())
		require.Equal(t, int64(-1002), items[2].GetOutputId())
		require.Equal(t, schemapb.DataType_Int64, items[2].GetDataType())
		require.Equal(t, []int64{-1000, -1001, -1002}, b.projection.queryNodeItemIDs(search))
	}

	// $score of a Search is computed at the proxy and is not sent in the plan,
	// but it still takes an output ID in output_fields order.
	b, err := bindOutputFields([]string{"$score", "$seg_id"}, schema, true)
	require.NoError(t, err)
	items := b.projection.planItems(true)
	require.Len(t, items, 1)
	require.Equal(t, int64(-1001), items[0].GetOutputId())
}

func TestCompleteQueryNodeColumns(t *testing.T) {
	schema := projectionTestSchema(true)
	b, err := bindOutputFields([]string{"$seg_id AS sid", "$seg_offset"}, schema, false)
	require.NoError(t, err)

	out, err := b.projection.completeQueryNodeColumns([]*schemapb.FieldData{
		longColumn(100, 1, 2), longColumn(-1001, 7, 8), longColumn(-1000, 5, 5),
	}, false, -1)
	require.NoError(t, err)
	require.Equal(t, []string{"", "$seg_offset", "sid"}, columnNames(out))
	require.Equal(t, schemapb.DataType_Int64, out[2].GetType())

	// Rows without the column: an error, never a filled-in value.
	_, err = b.projection.completeQueryNodeColumns([]*schemapb.FieldData{longColumn(100, 1, 2), longColumn(-1000, 5, 5)}, false, -1)
	require.ErrorContains(t, err, "virtual column $seg_offset is missing from the result")
	_, err = b.projection.completeQueryNodeColumns(nil, true, 3)
	require.ErrorContains(t, err, "is missing from the result")

	// No rows: the columns are added empty.
	out, err = b.projection.completeQueryNodeColumns([]*schemapb.FieldData{longColumn(100)}, false, -1)
	require.NoError(t, err)
	require.Equal(t, []string{"", "sid", "$seg_offset"}, columnNames(out))
	out, err = b.projection.completeQueryNodeColumns(nil, true, 0)
	require.NoError(t, err)
	require.Len(t, out, 2)
}

func TestVirtualColumnStash(t *testing.T) {
	carry := func(result *schemapb.SearchResultData, reduced []*milvuspb.SearchResults) error {
		stash := newVirtualColumnStash([]int64{-1000}, false)
		stash.capture(reduced, nil)
		// A later pipeline node may replace fields_data of the reduced result.
		for _, r := range reduced {
			saved := r.Results.FieldsData
			r.Results.FieldsData = nil
			defer func() { r.Results.FieldsData = saved }()
		}
		return stash.carry(result)
	}

	// Two queries; the reduced result holds 3 and 2 rows, the final result
	// keeps a reordered subset.
	reduced := []*milvuspb.SearchResults{{Results: &schemapb.SearchResultData{
		Ids:        intIDs(10, 11, 12, 11, 13),
		Topks:      []int64{3, 2},
		FieldsData: []*schemapb.FieldData{longColumn(-1000, 100, 110, 120, 211, 213)},
	}}}
	result := &schemapb.SearchResultData{
		Ids:        intIDs(12, 10, 13, 11),
		Topks:      []int64{2, 2},
		FieldsData: []*schemapb.FieldData{stringColumn("title", 101, "a", "b", "c", "d")},
	}
	require.NoError(t, carry(result, reduced))
	require.Len(t, result.FieldsData, 2)
	// pk 11 appears in both queries with different values.
	require.Equal(t, []int64{120, 100, 213, 211}, result.FieldsData[1].GetScalars().GetLongData().GetData())

	// Already present: nothing is added.
	require.NoError(t, carry(result, reduced))
	require.Len(t, result.FieldsData, 2)

	// A row that is not in the reduced result.
	bad := &schemapb.SearchResultData{Ids: intIDs(99), Topks: []int64{1}}
	require.ErrorContains(t, carry(bad, reduced), "is not in the reduced search result")

	// Hybrid: two sub-results; a row is taken from the first one that has it.
	hybrid := []*milvuspb.SearchResults{
		{Results: &schemapb.SearchResultData{Ids: intIDs(1, 2), Topks: []int64{2}, FieldsData: []*schemapb.FieldData{longColumn(-1000, 7, 8)}}},
		{Results: &schemapb.SearchResultData{Ids: intIDs(2, 3), Topks: []int64{2}, FieldsData: []*schemapb.FieldData{longColumn(-1000, 8, 9)}}},
	}
	fused := &schemapb.SearchResultData{Ids: intIDs(3, 2, 1), Topks: []int64{3}}
	require.NoError(t, carry(fused, hybrid))
	require.Equal(t, []int64{9, 8, 7}, fused.FieldsData[0].GetScalars().GetLongData().GetData())

	// Element-level rows are keyed by (primary key, element index).
	elemReduced := []*milvuspb.SearchResults{{Results: &schemapb.SearchResultData{
		Ids: intIDs(1, 1, 2), Topks: []int64{3}, ElementIndices: &schemapb.LongArray{Data: []int64{0, 4, 1}},
		FieldsData: []*schemapb.FieldData{longColumn(-1000, 50, 54, 61)},
	}}}
	elem := &schemapb.SearchResultData{Ids: intIDs(1, 2, 1), Topks: []int64{3}, ElementIndices: &schemapb.LongArray{Data: []int64{4, 1, 0}}}
	require.NoError(t, carry(elem, elemReduced))
	require.Equal(t, []int64{54, 61, 50}, elem.FieldsData[0].GetScalars().GetLongData().GetData())

	// A row-level final result over element-level reduced results takes the
	// first element of each primary key.
	collapsed := &schemapb.SearchResultData{Ids: intIDs(2, 1), Topks: []int64{2}}
	require.NoError(t, carry(collapsed, elemReduced))
	require.Equal(t, []int64{61, 50}, collapsed.FieldsData[0].GetScalars().GetLongData().GetData())

	// Float columns.
	floatReduced := []*milvuspb.SearchResults{{Results: &schemapb.SearchResultData{
		Ids: intIDs(1, 2), Topks: []int64{2},
		FieldsData: []*schemapb.FieldData{{
			Type: schemapb.DataType_Float, FieldId: -1000,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{0.25, 0.5}}},
			}},
		}},
	}}}
	floats := &schemapb.SearchResultData{Ids: intIDs(2, 1), Topks: []int64{2}}
	require.NoError(t, carry(floats, floatReduced))
	require.Equal(t, []float32{0.5, 0.25}, floats.FieldsData[0].GetScalars().GetFloatData().GetData())

	// An empty result, and a request without virtual columns, are no-ops.
	require.NoError(t, carry(&schemapb.SearchResultData{}, reduced))
	var none *virtualColumnStash
	none.capture(reduced, nil)
	require.NoError(t, none.carry(result))
	require.Nil(t, newVirtualColumnStash(nil, false))
	require.NotNil(t, newVirtualColumnStash(nil, true))
}

// A final result that still has the captured rows in the captured order is
// matched by position; the row index is built only when the rows differ.
func TestVirtualColumnStashPositionalMatch(t *testing.T) {
	strIDs := func(ids ...string) *schemapb.IDs {
		return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids}}}
	}
	newStash := func(reduced *schemapb.SearchResultData) *virtualColumnStash {
		stash := newVirtualColumnStash([]int64{-1000, -1001}, false)
		stash.capture([]*milvuspb.SearchResults{{Results: reduced}}, nil)
		return stash
	}
	columns := func() []*schemapb.FieldData {
		return []*schemapb.FieldData{longColumn(-1000, 100, 110, 120, 211, 213), floatColumn(-1001, 0.1, 0.2, 0.3, 0.4, 0.5)}
	}
	longs := func(result *schemapb.SearchResultData, i int) []int64 {
		return result.FieldsData[i].GetScalars().GetLongData().GetData()
	}
	floats := func(result *schemapb.SearchResultData, i int) []float32 {
		return result.FieldsData[i].GetScalars().GetFloatData().GetData()
	}

	// Same rows, same order: taken by position, no row index.
	reduced := &schemapb.SearchResultData{Ids: intIDs(10, 11, 12, 11, 13), Topks: []int64{3, 2}, FieldsData: columns()}
	stash := newStash(reduced)
	result := &schemapb.SearchResultData{Ids: intIDs(10, 11, 12, 11, 13), Topks: []int64{3, 2}}
	require.NoError(t, stash.carry(result))
	require.Equal(t, []int64{100, 110, 120, 211, 213}, longs(result, 0))
	require.Equal(t, []float32{0.1, 0.2, 0.3, 0.4, 0.5}, floats(result, 1))
	require.Nil(t, stash.subResults[0].rows)
	// The carried columns are copies.
	longs(result, 0)[0] = -1
	require.Equal(t, int64(100), reduced.FieldsData[0].GetScalars().GetLongData().GetData()[0])

	// The same ids split differently between the queries are different rows.
	regrouped := &schemapb.SearchResultData{Ids: intIDs(10, 11, 12, 11, 13), Topks: []int64{2, 3}}
	require.ErrorContains(t, stash.carry(regrouped), "is not in the reduced search result")
	require.NotNil(t, stash.subResults[0].rows)

	// A pipeline node that reorders the reduced result in place after the
	// capture does not change what was captured.
	inPlace := &schemapb.SearchResultData{Ids: intIDs(1, 2, 3), Topks: []int64{3}, FieldsData: []*schemapb.FieldData{longColumn(-1000, 7, 8, 9)}}
	stash = newStash(inPlace)
	inPlace.Ids.GetIntId().Data[0], inPlace.Ids.GetIntId().Data[2] = 3, 1
	inPlace.FieldsData = nil
	require.NoError(t, stash.carry(inPlace))
	require.Equal(t, []int64{9, 8, 7}, longs(inPlace, 0))

	// A truncated result (a prefix) is not the same rows; it is matched by key.
	stash = newStash(&schemapb.SearchResultData{Ids: intIDs(1, 2, 3), Topks: []int64{3}, FieldsData: []*schemapb.FieldData{longColumn(-1000, 7, 8, 9)}})
	prefix := &schemapb.SearchResultData{Ids: intIDs(1, 2), Topks: []int64{2}}
	require.NoError(t, stash.carry(prefix))
	require.Equal(t, []int64{7, 8}, longs(prefix, 0))
	require.NotNil(t, stash.subResults[0].rows)

	// String primary keys.
	stash = newStash(&schemapb.SearchResultData{Ids: strIDs("a", "b"), Topks: []int64{2}, FieldsData: []*schemapb.FieldData{longColumn(-1000, 1, 2)}})
	same := &schemapb.SearchResultData{Ids: strIDs("a", "b"), Topks: []int64{2}}
	require.NoError(t, stash.carry(same))
	require.Equal(t, []int64{1, 2}, longs(same, 0))
	require.Nil(t, stash.subResults[0].rows)
	swapped := &schemapb.SearchResultData{Ids: strIDs("b", "a"), Topks: []int64{2}}
	require.NoError(t, stash.carry(swapped))
	require.Equal(t, []int64{2, 1}, longs(swapped, 0))
	// Int64 ids never match captured string ids.
	require.False(t, stash.subResults[0].sameRows(&schemapb.SearchResultData{Ids: intIDs(1, 2), Topks: []int64{2}}))

	// Element-level rows: the element indices are part of the row.
	elemReduced := &schemapb.SearchResultData{
		Ids: intIDs(1, 1, 2), Topks: []int64{3}, ElementIndices: &schemapb.LongArray{Data: []int64{0, 4, 1}},
		FieldsData: []*schemapb.FieldData{longColumn(-1000, 50, 54, 61)},
	}
	stash = newStash(elemReduced)
	elemSame := &schemapb.SearchResultData{Ids: intIDs(1, 1, 2), Topks: []int64{3}, ElementIndices: &schemapb.LongArray{Data: []int64{0, 4, 1}}}
	require.NoError(t, stash.carry(elemSame))
	require.Equal(t, []int64{50, 54, 61}, longs(elemSame, 0))
	require.Nil(t, stash.subResults[0].rows)
	elemOther := &schemapb.SearchResultData{Ids: intIDs(1, 1, 2), Topks: []int64{3}, ElementIndices: &schemapb.LongArray{Data: []int64{4, 0, 1}}}
	require.NoError(t, stash.carry(elemOther))
	require.Equal(t, []int64{54, 50, 61}, longs(elemOther, 0))
	// A row-level result with the same ids is not the same rows.
	require.False(t, stash.subResults[0].sameRows(&schemapb.SearchResultData{Ids: intIDs(1, 1, 2), Topks: []int64{3}}))

	// A column shorter than the result is an error, not a panic.
	short := &schemapb.SearchResultData{Ids: intIDs(1, 2, 3), Topks: []int64{3}, FieldsData: []*schemapb.FieldData{longColumn(-1000, 7, 8)}}
	stash = newStash(short)
	require.ErrorContains(t, stash.carry(&schemapb.SearchResultData{Ids: intIDs(1, 2, 3), Topks: []int64{3}}), "holds 2 values")
}

// Where a Search takes each virtual column from.
func TestConfigureSearchSources(t *testing.T) {
	schema := projectionTestSchema(true)
	bind := func() *outputProjection {
		b, err := bindOutputFields([]string{"$seg_id", "$seg_id AS sid", "$seg_offset", "$distance", "$score", "$distance AS d"}, schema, true)
		require.NoError(t, err)
		return b.projection
	}
	planColumns := func(p *outputProjection) []planpb.VirtualColumn {
		return lo.Map(p.planItems(true), func(item *planpb.ProjectionItem, _ int) planpb.VirtualColumn { return item.GetVirtualColumn() })
	}
	segID, segOffset, distance := planpb.VirtualColumn_VirtualColumnSegmentID, planpb.VirtualColumn_VirtualColumnSegmentOffset, planpb.VirtualColumn_VirtualColumnDistance

	// No requery, scores rewritten: everything but $score travels with the rows.
	p := bind()
	p.configureSearch(false, false)
	require.Equal(t, []planpb.VirtualColumn{segID, segID, segOffset, distance, distance}, planColumns(p))
	require.Empty(t, p.requeryOutputFields())
	require.Len(t, p.searchRowItemIDs(), 5)

	// Requery: the placement columns leave the Search plan, once per column.
	p = bind()
	p.configureSearch(true, false)
	require.Equal(t, []planpb.VirtualColumn{distance, distance}, planColumns(p))
	require.Equal(t, []string{"$seg_id", "$seg_offset"}, p.requeryOutputFields())
	require.Len(t, p.searchRowItemIDs(), 2)
	// They stay among the columns the end of the pipeline keeps and checks.
	require.Len(t, p.queryNodeItemIDs(true), 5)

	// Scores are distances: $distance leaves the plan too.
	p = bind()
	p.configureSearch(true, true)
	require.Empty(t, planColumns(p))
	require.Empty(t, p.searchRowItemIDs())
	p = bind()
	p.configureSearch(false, true)
	require.Equal(t, []planpb.VirtualColumn{segID, segID, segOffset}, planColumns(p))

	// The requery result: one column per virtual column becomes one column
	// per item, under the item's ID; stored columns pass through.
	p = bind()
	p.configureSearch(true, true)
	requery := []*schemapb.FieldData{
		stringColumn("title", 101, "a", "b"),
		{Type: schemapb.DataType_Int64, FieldName: "$seg_id", FieldId: -1000, Field: longColumn(0, 7, 8).Field},
		{Type: schemapb.DataType_Int64, FieldName: "$seg_offset", FieldId: -1001, Field: longColumn(0, 3, 4).Field},
	}
	adopted := p.adoptRequeryColumns(requery)
	require.Len(t, adopted, 4)
	require.Equal(t, "title", adopted[0].GetFieldName())
	byID := lo.SliceToMap(adopted[1:], func(fd *schemapb.FieldData) (int64, []int64) {
		return fd.GetFieldId(), fd.GetScalars().GetLongData().GetData()
	})
	require.Equal(t, map[int64][]int64{-1000: {7, 8}, -1001: {7, 8}, -1002: {3, 4}}, byID)
	// The final step names them and finds none missing.
	scores := &schemapb.SearchResultData{Scores: []float32{0.5, 0.25}, FieldsData: adopted}
	var err error
	scores.FieldsData, err = p.completeQueryNodeColumns(scores.FieldsData, true, 2)
	require.NoError(t, err)
	p.appendSearchScoreColumns(scores)
	values := map[string]any{}
	for _, fd := range scores.FieldsData {
		if fd.GetScalars().GetFloatData() != nil {
			values[fd.GetFieldName()] = fd.GetScalars().GetFloatData().GetData()
		}
	}
	require.Equal(t, map[string]any{"$distance": []float32{0.5, 0.25}, "$score": []float32{0.5, 0.25}, "d": []float32{0.5, 0.25}}, values)

	// A request without requery columns leaves the fields untouched.
	p = bind()
	p.configureSearch(false, false)
	require.Equal(t, requery, p.adoptRequeryColumns(requery))
	var none *outputProjection
	none.configureSearch(true, true)
	require.Equal(t, requery, none.adoptRequeryColumns(requery))
}

func floatColumn(id int64, values ...float32) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_Float, FieldId: id,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: values}},
		}},
	}
}

func TestSubRequestColumns(t *testing.T) {
	schema := projectionTestSchema(true)
	items := []string{"$score(vec)", "$score(1) AS s1", "$distance(0) AS d0", "$distance(1)", "$rank(vec)", "$rank(1) AS r1", "$score"}
	b, err := bindOutputFields(items, schema, true)
	require.NoError(t, err)
	p := b.projection
	require.True(t, p.hasSubRequestColumns())
	require.True(t, p.hiddenDistance)
	require.Equal(t, int64(-1007), p.hiddenDistanceID())
	require.ElementsMatch(t, []string{"$score(vec)", "s1", "d0", "$distance(1)", "$rank(vec)", "r1", "$score"}, b.userOutputFields)

	// Only the hidden distance is sent to the QueryNodes.
	planItems := p.planItems(true)
	require.Len(t, planItems, 1)
	require.Equal(t, int64(-1007), planItems[0].GetOutputId())
	require.Equal(t, planpb.VirtualColumn_VirtualColumnDistance, planItems[0].GetVirtualColumn())
	require.Equal(t, []int64{-1007}, p.queryNodeItemIDs(true))

	targets := []subRequestTarget{{annsField: "vec"}, {annsField: "vec2"}}
	require.NoError(t, p.resolveSubRequests(targets))
	subs := make([]int, 0)
	for _, v := range p.virtualItems {
		subs = append(subs, v.subRequest)
	}
	require.Equal(t, []int{0, 1, 0, 1, 0, 1, 0}, subs)

	// Sub-request 0 returns pks 1,2,3 for query 0 and 3,4 for query 1; its
	// scores were rewritten on the QueryNode, so the hidden column holds the
	// raw distance. Sub-request 1 returns 2,9 and 4.
	reduced := []*milvuspb.SearchResults{
		{Results: &schemapb.SearchResultData{
			Ids: intIDs(1, 2, 3, 3, 4), Topks: []int64{3, 2}, Scores: []float32{10, 20, 30, 31, 41},
			FieldsData: []*schemapb.FieldData{floatColumn(-1007, 1.5, 2.5, 3.5, 3.25, 4.25)},
		}},
		{Results: &schemapb.SearchResultData{
			Ids: intIDs(2, 9, 4), Topks: []int64{2, 1}, Scores: []float32{0.2, 0.9, 0.4},
			FieldsData: []*schemapb.FieldData{floatColumn(-1007, 0.2, 0.9, 0.4)},
		}},
	}
	stash := newVirtualColumnStash(p.queryNodeItemIDs(true), true)
	stash.capture(reduced, []searchScoreInfo{{MetricType: "L2", Rewritten: true}, {MetricType: "COSINE"}})

	result := &schemapb.SearchResultData{Ids: intIDs(2, 3, 9, 4, 3), Topks: []int64{3, 2}, Scores: []float32{5, 4, 3, 2, 1}}
	require.NoError(t, p.appendSubRequestColumns(result, stash))
	p.appendSearchScoreColumns(result)
	cols := map[string]*schemapb.FieldData{}
	for _, fd := range result.FieldsData {
		cols[fd.GetFieldName()] = fd
	}
	floats := func(name string) ([]float32, []bool) {
		return cols[name].GetScalars().GetFloatData().GetData(), cols[name].GetValidData()
	}
	longs := func(name string) ([]int64, []bool) {
		return cols[name].GetScalars().GetLongData().GetData(), cols[name].GetValidData()
	}

	// rows: (q0,2) (q0,3) (q0,9) (q1,4) (q1,3)
	v, ok := floats("$score(vec)")
	require.Equal(t, []bool{true, true, false, true, true}, ok)
	require.Equal(t, []float32{20, 30, 0, 41, 31}, v)
	v, ok = floats("s1")
	require.Equal(t, []bool{true, false, true, true, false}, ok)
	require.Equal(t, []float32{0.2, 0, 0.9, 0.4, 0}, v)
	// $distance(0): sub-request 0 was rewritten, so the hidden column is read.
	v, ok = floats("d0")
	require.Equal(t, []bool{true, true, false, true, true}, ok)
	require.Equal(t, []float32{2.5, 3.5, 0, 4.25, 3.25}, v)
	// $distance(1): not rewritten, the sub-request scores are the distance.
	v, ok = floats("$distance(1)")
	require.Equal(t, []bool{true, false, true, true, false}, ok)
	require.Equal(t, []float32{0.2, 0, 0.9, 0.4, 0}, v)
	r, ok := longs("$rank(vec)")
	require.Equal(t, []bool{true, true, false, true, true}, ok)
	require.Equal(t, []int64{2, 3, 0, 2, 1}, r)
	r, ok = longs("r1")
	require.Equal(t, []bool{true, false, true, true, false}, ok)
	require.Equal(t, []int64{1, 0, 2, 1, 0}, r)
	require.Equal(t, schemapb.DataType_Int64, cols["r1"].GetType())
	v, _ = floats("$score")
	require.Equal(t, []float32{5, 4, 3, 2, 1}, v)

	// The hidden column is dropped from the output and is not reported missing.
	out, err := p.completeQueryNodeColumns([]*schemapb.FieldData{floatColumn(-1007, 1, 2)}, true, 2)
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestResolveSubRequests(t *testing.T) {
	schema := projectionTestSchema(true)
	bind := func(items ...string) *outputProjection {
		b, err := bindOutputFields(items, schema, true)
		require.NoError(t, err)
		return b.projection
	}
	two := []subRequestTarget{{annsField: "vec"}, {annsField: "vec2"}}
	same := []subRequestTarget{{annsField: "vec"}, {annsField: "vec"}}
	bm25 := []subRequestTarget{{annsField: "vec"}, {annsField: "sparse", isBM25: true}}

	require.ErrorContains(t, bind("$score(2)").resolveSubRequests(two), "the request has 2 sub-requests")
	require.ErrorContains(t, bind("$rank(title)").resolveSubRequests(two), "no sub-request searches field title")
	require.ErrorContains(t, bind("$score(vec)").resolveSubRequests(same), "more than one sub-request searches field vec")
	require.NoError(t, bind("$score(1)", "$rank(0)").resolveSubRequests(same))
	require.ErrorContains(t, bind("$distance(sparse)").resolveSubRequests(bm25), "is a full-text search")
	require.ErrorContains(t, bind("$distance(1)").resolveSubRequests(bm25), "is a full-text search")
	require.NoError(t, bind("$score(sparse)", "$rank(1)", "$distance(vec)").resolveSubRequests(bm25))

	// The aliases "distance" and "score" are not allowed on argument forms.
	_, err := bindOutputFields([]string{"$distance(0) AS distance"}, schema, true)
	require.ErrorContains(t, err, "reserved in search results")
	_, err = bindOutputFields([]string{"$score(vec) AS score"}, schema, true)
	require.ErrorContains(t, err, "reserved in search results")

	// One vector field: $distance(x) means $distance when x is that field.
	p := bind("$distance(vec) AS d", "$distance(0)")
	require.True(t, p.hiddenDistance)
	require.NoError(t, p.resolveSingleSearchArguments("vec"))
	require.False(t, p.hiddenDistance)
	require.False(t, p.hasSubRequestColumns())
	require.Len(t, p.planItems(true), 2)
	require.ErrorContains(t, bind("$distance(vec2)").resolveSingleSearchArguments("vec"), "the request searches field vec only")
	require.ErrorContains(t, bind("$distance(1)").resolveSingleSearchArguments("vec"), "the request searches field vec only")
	require.ErrorContains(t, bind("$score(vec)").resolveSingleSearchArguments("vec"), "only available in hybrid search")
	require.ErrorContains(t, bind("$rank(0)").resolveSingleSearchArguments("vec"), "only available in hybrid search")
}

// Requests without aliases and virtual columns are never rejected by the
// uniqueness rule: a stored field and a same-named dynamic key in the
// explicit form were accepted before and return the stored field.
func TestBindOutputFields_PlainItemsSharingAName(t *testing.T) {
	schema := projectionTestSchema(true)
	for _, in := range [][]string{
		{"title", `$meta["title"]`},
		{`$meta["title"]`, "title"},
		{"*", `$meta["price"]`},
		{`$meta["price"]`, "*"},
	} {
		b, err := bindOutputFields(in, schema, false)
		require.NoError(t, err, in)
		require.True(t, b.projection.empty())
	}
	b, err := bindOutputFields([]string{"title", `$meta["title"]`}, schema, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"title"}, b.userOutputFields)
	require.ElementsMatch(t, []string{"title", "$meta"}, b.resultFieldNames)
	require.ElementsMatch(t, []string{"title"}, b.userDynamicFields)

	// With an alias on either side the name is chosen by the request, and the
	// clash is an error in both orders.
	for _, in := range [][]string{
		{"price AS title", `$meta["title"]`},
		{`$meta["title"]`, "price AS title"},
		{"title", `$meta["title"]`, "price AS title"},
	} {
		_, err := bindOutputFields(in, schema, false)
		require.ErrorContains(t, err, "duplicate output field name title", in)
	}
}
