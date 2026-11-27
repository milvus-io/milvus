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

// End-to-end coverage of element-level null on struct sub-fields: element-nullable scalar,
// vector and Array<T> sub-fields, omitted sub-fields, element-level filters and search offsets,
// and adding / dropping a struct sub-field.
package testcases

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	elemNullPrefix = "struct_elem_null"
	elemNullDim    = 4
	elemNullRows   = 6
	// elemNullNullRow is the row whose whole struct value is null.
	elemNullNullRow = 5
)

// elemNullStructSchema: score is not nullable; lang, vec and tags hold null elements; the leaves
// of tags are nullable too.
func elemNullStructSchema() *entity.StructSchema {
	return entity.NewStructSchema().
		WithField(entity.NewField().WithName("score").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("lang").WithDataType(entity.FieldTypeVarChar).WithMaxLength(16).WithNullable(true)).
		WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(elemNullDim).WithNullable(true)).
		WithField(entity.NewField().WithName("tags").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).
			WithMaxCapacity(3).WithMaxLength(16).WithNullable(true).WithElementNullable(true))
}

func elemNullSchema(collName string) *entity.Schema {
	return entity.NewSchema().WithName(collName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(elemNullDim)).
		WithField(entity.NewField().WithName("clips").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(4).WithNullable(true).WithStructSchema(elemNullStructSchema()))
}

func elemNullStr(s string) *string {
	return &s
}

func elemNullVec(seed int) []float32 {
	v := make([]float32, elemNullDim)
	for i := range v {
		v[i] = float32((seed+i)%7 + 1)
	}
	return v
}

// elemNullClipsRow builds struct row i: two elements. Even rows have a null lang at element 1
// and tags [["a", null], null]; odd rows have a null vec at element 0 and tags [["b"], ["c"]].
func elemNullClipsRow(i int) map[string]any {
	if i == elemNullNullRow {
		return nil
	}
	row := map[string]any{"score": []int64{int64(i * 10), int64(i*10 + 1)}}
	if i%2 == 0 {
		row["lang"] = []*string{elemNullStr("en"), nil}
		row["vec"] = [][]float32{elemNullVec(i * 2), elemNullVec(i*2 + 1)}
		row["tags"] = [][]*string{{elemNullStr("a"), nil}, nil}
	} else {
		row["lang"] = []*string{elemNullStr("en"), elemNullStr("fr")}
		row["vec"] = [][]float32{nil, elemNullVec(i*2 + 1)}
		row["tags"] = [][]*string{{elemNullStr("b")}, {elemNullStr("c")}}
	}
	return row
}

func elemNullCreate(t *testing.T, ctx CtxT, mc MC, suffix string) string {
	collName := common.GenRandomString(elemNullPrefix+"_"+suffix, 6)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, elemNullSchema(collName)).WithConsistencyLevel(entity.ClStrong)), true)
	return collName
}

func elemNullInsert(t *testing.T, ctx CtxT, mc MC, collName string, from, to int) {
	ids := make([]int64, 0, to-from)
	vectors := make([][]float32, 0, to-from)
	clips := make([]map[string]any, 0, to-from)
	for i := from; i < to; i++ {
		ids = append(ids, int64(i))
		vectors = append(vectors, elemNullVec(100+i))
		clips = append(clips, elemNullClipsRow(i))
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", ids).
		WithFloatVectorColumn("vector", elemNullDim, vectors).
		WithStructArrayColumn("clips", elemNullStructSchema(), clips))
	common.CheckErr(t, err, true)
}

func elemNullIndexAndLoad(t *testing.T, ctx CtxT, mc MC, collName string) {
	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "clips[vec]", index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)
}

func elemNullQueryClips(t *testing.T, ctx CtxT, mc MC, collName, filter string) (map[int64]map[string]any, client.ResultSet) {
	rs, err := mc.Query(ctx, client.NewQueryOption(collName).WithFilter(filter).
		WithOutputFields("id", "clips").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	idCol := rs.GetColumn("id")
	clipsCol := rs.GetColumn("clips")
	require.NotNil(t, idCol)
	require.NotNil(t, clipsCol)
	rows := make(map[int64]map[string]any, rs.ResultCount)
	for i := 0; i < rs.ResultCount; i++ {
		id, err := idCol.GetAsInt64(i)
		require.NoError(t, err)
		v, err := clipsCol.Get(i)
		require.NoError(t, err)
		if v == nil {
			rows[id] = nil
			continue
		}
		rows[id] = v.(map[string]any)
	}
	return rows, rs
}

// elemNullCheckRow compares a read-back struct row with what elemNullClipsRow wrote.
func elemNullCheckRow(t *testing.T, i int, got map[string]any) {
	want := elemNullClipsRow(i)
	if want == nil {
		require.Nil(t, got, "row %d", i)
		return
	}
	require.NotNil(t, got, "row %d", i)
	require.Equal(t, want["score"], got["score"], "row %d score", i)
	require.Equal(t, want["lang"], got["lang"], "row %d lang", i)
	require.Equal(t, want["tags"], got["tags"], "row %d tags", i)
	gotVec := got["vec"].([]entity.FloatVector)
	wantVec := want["vec"].([][]float32)
	require.Len(t, gotVec, len(wantVec), "row %d vec", i)
	for j := range wantVec {
		if wantVec[j] == nil {
			require.Nil(t, gotVec[j], "row %d vec %d", i, j)
		} else {
			require.Equal(t, entity.FloatVector(wantVec[j]), gotVec[j], "row %d vec %d", i, j)
		}
	}
}

func TestStructElementNullDescribe(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName := elemNullCreate(t, ctx, mc, "describe")

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	var clips *entity.Field
	for _, field := range coll.Schema.Fields {
		if field.Name == "clips" {
			clips = field
		}
	}
	require.NotNil(t, clips)
	require.True(t, clips.Nullable)
	require.Equal(t, "4", clips.TypeParams[entity.TypeParamMaxCapacity])
	subs := map[string]*entity.Field{}
	for _, sub := range clips.StructSchema.Fields {
		subs[sub.Name] = sub
	}
	require.False(t, subs["score"].Nullable)
	require.Equal(t, entity.FieldTypeVarChar, subs["lang"].DataType)
	require.True(t, subs["lang"].Nullable)
	require.Equal(t, entity.FieldTypeFloatVector, subs["vec"].DataType)
	require.True(t, subs["vec"].Nullable)
	tags := subs["tags"]
	require.Equal(t, entity.FieldTypeArray, tags.DataType)
	require.Equal(t, entity.FieldTypeVarChar, tags.ElementType)
	require.True(t, tags.Nullable)
	require.True(t, tags.ElementNullable)
	require.Equal(t, "3", tags.TypeParams[entity.TypeParamMaxCapacity])
	require.Equal(t, "16", tags.TypeParams[entity.TypeParamMaxLength])
	// the described schema is accepted as is
	require.NoError(t, coll.Schema.Validate())
}

func TestStructElementNullInsertQuery(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName := elemNullCreate(t, ctx, mc, "query")

	// rows [0, 4) sealed, [4, 6) growing
	elemNullInsert(t, ctx, mc, collName, 0, 4)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	elemNullInsert(t, ctx, mc, collName, 4, elemNullRows)

	// row-based rows omitting the element-nullable sub-fields: each gets null elements
	rows := []any{
		map[string]any{"id": int64(100), "vector": elemNullVec(1), "clips": map[string]any{"score": []int64{7, 8, 9}}},
		map[string]any{"id": int64(101), "vector": elemNullVec(2), "clips": map[string]any{
			"score": []int64{1},
			"tags":  [][]string{{}},
		}},
	}
	_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(collName, rows...))
	common.CheckErr(t, err, true)
	elemNullIndexAndLoad(t, ctx, mc, collName)

	got, rs := elemNullQueryClips(t, ctx, mc, collName, "id < 100")
	require.Len(t, got, elemNullRows)
	for i := 0; i < elemNullRows; i++ {
		elemNullCheckRow(t, i, got[int64(i)])
	}
	// the sub-columns expose element validity directly
	idCol := rs.GetColumn("id")
	for _, sub := range rs.GetColumn("clips").(column.StructArrayColumn).Fields() {
		if sub.Name() != "lang" {
			continue
		}
		lang := sub.(column.ElementNullableColumn)
		require.True(t, lang.ElementNullable())
		for i := 0; i < rs.ResultCount; i++ {
			id, err := idCol.GetAsInt64(i)
			require.NoError(t, err)
			if id == elemNullNullRow {
				continue
			}
			isNull, err := lang.ElementIsNull(i, 1)
			require.NoError(t, err)
			require.Equal(t, id%2 == 0, isNull, "row %d", id)
		}
	}

	got, _ = elemNullQueryClips(t, ctx, mc, collName, "id >= 100")
	require.Equal(t, []*string{nil, nil, nil}, got[100]["lang"])
	require.Equal(t, []entity.FloatVector{nil, nil, nil}, got[100]["vec"])
	require.Equal(t, [][]*string{nil, nil, nil}, got[100]["tags"])
	require.Equal(t, [][]*string{{}}, got[101]["tags"], "an empty inner array is not null")

	// element-level query: one offset list per row
	got, rs = elemNullQueryClips(t, ctx, mc, collName, `id < 100 && element_filter(clips, $[lang] IS NULL)`)
	require.ElementsMatch(t, []int64{0, 2, 4}, elemNullKeys(got))
	require.Len(t, rs.QueryElementOffsets, rs.ResultCount)
	for _, offsets := range rs.QueryElementOffsets {
		require.Equal(t, []int64{1}, offsets)
	}

	got, rs = elemNullQueryClips(t, ctx, mc, collName, `id < 100 && element_filter(clips, $[vec] IS NULL)`)
	require.ElementsMatch(t, []int64{1, 3}, elemNullKeys(got))
	for _, offsets := range rs.QueryElementOffsets {
		require.Equal(t, []int64{0}, offsets)
	}

	got, _ = elemNullQueryClips(t, ctx, mc, collName, `element_filter(clips, $[lang] == "fr")`)
	require.ElementsMatch(t, []int64{1, 3}, elemNullKeys(got))

	// upsert row 1 with an element null in lang and read it back
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{1}).
		WithFloatVectorColumn("vector", elemNullDim, [][]float32{elemNullVec(1)}).
		WithStructArrayColumn("clips", elemNullStructSchema(), []map[string]any{{
			"score": []int64{11},
			"lang":  []*string{nil},
		}}))
	common.CheckErr(t, err, true)
	got, _ = elemNullQueryClips(t, ctx, mc, collName, "id == 1")
	require.Equal(t, []int64{11}, got[1]["score"])
	require.Equal(t, []*string{nil}, got[1]["lang"])
	require.Equal(t, []entity.FloatVector{nil}, got[1]["vec"])
}

func TestStructElementNullElementSearch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName := elemNullCreate(t, ctx, mc, "search")
	elemNullInsert(t, ctx, mc, collName, 0, elemNullRows)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	elemNullIndexAndLoad(t, ctx, mc, collName)

	checkOffsets := func(rs client.ResultSet) {
		require.NoError(t, rs.Err)
		require.Greater(t, rs.ResultCount, 0)
		require.Len(t, rs.ElementOffsets, rs.ResultCount)
		idCol := rs.GetColumn("id")
		for i := 0; i < rs.ResultCount; i++ {
			id, err := idCol.GetAsInt64(i)
			require.NoError(t, err)
			require.NotEqual(t, int64(elemNullNullRow), id, "a null struct row has no element")
			offset := rs.ElementOffsets[i]
			require.GreaterOrEqual(t, offset, int64(0))
			require.Less(t, offset, int64(2))
			if id%2 == 1 {
				require.Equal(t, int64(1), offset, "element 0 of row %d is a null vector", id)
			}
		}
	}

	query := entity.FloatVector(elemNullVec(3))
	rs, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{query}).
		WithANNSField("clips[vec]").
		WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Len(t, rs, 1)
	checkOffsets(rs[0])

	rs, err = mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{query}).
		WithANNSField("clips[vec]").
		WithFilter(`element_filter(clips, $[lang] IS NOT NULL && $[score] >= 0)`).
		WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	checkOffsets(rs[0])
	for i := 0; i < rs[0].ResultCount; i++ {
		id, _ := rs[0].GetColumn("id").GetAsInt64(i)
		if id%2 == 0 {
			// lang of element 1 is null in even rows, so only element 0 matches
			require.Equal(t, int64(0), rs[0].ElementOffsets[i], "row %d", id)
		}
	}

	hybrid, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(collName, 5,
		client.NewAnnRequest("vector", 5, entity.FloatVector(elemNullVec(4))),
		client.NewAnnRequest("clips[vec]", 5, query),
	).WithReranker(client.NewRRFReranker()).WithOutputFields("id", "clips").WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Len(t, hybrid, 1)
	require.NoError(t, hybrid[0].Err)
	require.Greater(t, hybrid[0].ResultCount, 0)
	idCol := hybrid[0].GetColumn("id")
	clipsCol := hybrid[0].GetColumn("clips")
	for i := 0; i < hybrid[0].ResultCount; i++ {
		id, err := idCol.GetAsInt64(i)
		require.NoError(t, err)
		v, err := clipsCol.Get(i)
		require.NoError(t, err)
		if v == nil {
			elemNullCheckRow(t, int(id), nil)
			continue
		}
		elemNullCheckRow(t, int(id), v.(map[string]any))
	}
}

func TestStructElementNullAddDropSubField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName := elemNullCreate(t, ctx, mc, "add_drop")
	elemNullInsert(t, ctx, mc, collName, 0, elemNullRows)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	elemNullIndexAndLoad(t, ctx, mc, collName)

	// a sub-field to add must be nullable
	err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(collName,
		entity.NewField().WithName("clips[note]").WithDataType(entity.FieldTypeVarChar).WithMaxLength(16)))
	common.CheckErr(t, err, false, "nullable")

	err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(collName,
		entity.NewField().WithName("clips[note]").WithDataType(entity.FieldTypeVarChar).WithMaxLength(16).WithNullable(true)))
	common.CheckErr(t, err, true)
	err = mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(collName,
		entity.NewField().WithName("clips[marks]").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt32).
			WithMaxCapacity(2).WithNullable(true).WithElementNullable(true)))
	common.CheckErr(t, err, true)

	coll, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	structSchema := elemNullFindStruct(t, coll.Schema, "clips")
	subs := map[string]*entity.Field{}
	for _, sub := range structSchema.Fields {
		subs[sub.Name] = sub
	}
	require.Contains(t, subs, "note")
	require.True(t, subs["note"].Nullable)
	require.Contains(t, subs, "marks")
	require.Equal(t, entity.FieldTypeInt32, subs["marks"].ElementType)
	require.True(t, subs["marks"].ElementNullable)

	// rows written before the sub-fields existed read null elements
	got, _ := elemNullQueryClips(t, ctx, mc, collName, "id == 0")
	require.Equal(t, []*string{nil, nil}, got[0]["note"])
	require.Equal(t, [][]*int32{nil, nil}, got[0]["marks"])

	one := int32(1)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{200}).
		WithFloatVectorColumn("vector", elemNullDim, [][]float32{elemNullVec(9)}).
		WithStructArrayColumn("clips", structSchema, []map[string]any{{
			"score": []int64{1, 2},
			"note":  []*string{elemNullStr("hi"), nil},
			"marks": [][]*int32{{&one, nil}, nil},
		}}))
	common.CheckErr(t, err, true)
	got, _ = elemNullQueryClips(t, ctx, mc, collName, "id == 200")
	require.Equal(t, []*string{elemNullStr("hi"), nil}, got[200]["note"])
	require.Equal(t, [][]*int32{{&one, nil}, nil}, got[200]["marks"])
	require.Equal(t, []*string{nil, nil}, got[200]["lang"])

	got, _ = elemNullQueryClips(t, ctx, mc, collName, `element_filter(clips, $[note] == "hi")`)
	require.ElementsMatch(t, []int64{200}, elemNullKeys(got))

	// drop the sub-field; reads and writes go on without it
	common.CheckErr(t, mc.DropCollectionField(ctx, client.NewDropCollectionFieldOption(collName, "clips[note]")), true)
	coll, err = mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(collName))
	common.CheckErr(t, err, true)
	structSchema = elemNullFindStruct(t, coll.Schema, "clips")
	for _, sub := range structSchema.Fields {
		require.NotEqual(t, "note", sub.Name)
	}

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{201}).
		WithFloatVectorColumn("vector", elemNullDim, [][]float32{elemNullVec(10)}).
		WithStructArrayColumn("clips", structSchema, []map[string]any{{"score": []int64{5}}}))
	common.CheckErr(t, err, true)
	got, _ = elemNullQueryClips(t, ctx, mc, collName, "id in [200, 201]")
	require.Len(t, got, 2)
	require.NotContains(t, got[200], "note")
	require.Equal(t, []int64{5}, got[201]["score"])
	require.Equal(t, [][]*int32{nil}, got[201]["marks"])
}

func elemNullFindStruct(t *testing.T, schema *entity.Schema, name string) *entity.StructSchema {
	for _, field := range schema.Fields {
		if field.Name == name {
			require.NotNil(t, field.StructSchema)
			return field.StructSchema
		}
	}
	require.FailNow(t, "struct field not found", name)
	return nil
}

func elemNullKeys(rows map[int64]map[string]any) []int64 {
	keys := make([]int64, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	return keys
}
