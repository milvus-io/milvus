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

package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
)

func outputFieldsTestSchema() *entity.Schema {
	return entity.NewSchema().WithName("products").WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("title").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
}

func TestResolveOutputFields(t *testing.T) {
	schema := outputFieldsTestSchema()
	for _, tc := range []struct {
		name      string
		requested []string
		served    []string
		want      []string
	}{
		{"old server reports nothing", []string{"title AS name"}, nil, []string{"title AS name"}},
		{"plain fields keep the requested list", []string{"title", "id"}, []string{"title", "id"}, []string{"title", "id"}},
		{"dynamic key keeps the requested list", []string{"color"}, []string{"color"}, []string{"color"}},
		{"schema field missing from the served list", []string{"title", "vec"}, []string{"title"}, []string{"title", "vec"}},
		{"wildcard keeps the requested list", []string{"*"}, []string{"id", "title", "vec", "$meta"}, []string{"*"}},
		{"virtual column named as requested", []string{"$score", "title"}, []string{"title", "$score"}, []string{"$score", "title"}},
		{"alias", []string{"title AS name", "$score"}, []string{"name", "$score"}, []string{"name", "$score"}},
		{"alias with spaces around the item", []string{"  title  as  name "}, []string{"name"}, []string{"name"}},
		{"explicit dynamic key", []string{`$meta["color"]`}, []string{"color"}, []string{"color"}},
		{"alias with wildcard", []string{"*", "title AS name"}, []string{"id", "title", "vec", "$meta", "name"}, []string{"id", "title", "vec", "$meta", "name", "*"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, resolveOutputFields(schema, tc.requested, tc.served))
		})
	}
}

// A Search whose output_fields has an alias and virtual columns: the columns
// come back under the served names and are all returned.
func TestHandleSearchResultWithAliasAndVirtualColumns(t *testing.T) {
	c := &Client{}
	str := func(name string, id int64, values ...string) *schemapb.FieldData {
		return &schemapb.FieldData{Type: schemapb.DataType_VarChar, FieldName: name, FieldId: id, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}}}
	}
	flt := func(name string, id int64, valid []bool, values ...float32) *schemapb.FieldData {
		return &schemapb.FieldData{Type: schemapb.DataType_Float, FieldName: name, FieldId: id, ValidData: valid, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: values}}}}}
	}
	lng := func(name string, id int64, values ...int64) *schemapb.FieldData {
		return &schemapb.FieldData{Type: schemapb.DataType_Int64, FieldName: name, FieldId: id, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}}}}
	}
	resp := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		NumQueries: 1, TopK: 2, Topks: []int64{2}, Scores: []float32{0.1, 0.2},
		Ids:          &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		OutputFields: []string{"name", "$score", "$seg_id", "d1"},
		FieldsData: []*schemapb.FieldData{
			str("name", 101, "red mug", "blue mug"),
			flt("$score", -1000, nil, 0.1, 0.2),
			lng("$seg_id", -1001, 7, 8),
			flt("d1", -1002, []bool{true, false}, 0.5, 0),
		},
	}}
	sets, err := c.handleSearchResult(outputFieldsTestSchema(), []string{"title AS name", "$score", "$seg_id", "$distance(1) AS d1"}, 1, resp)
	require.NoError(t, err)
	require.Len(t, sets, 1)
	rs := sets[0]
	require.NoError(t, rs.Err)
	require.Equal(t, 2, rs.ResultCount)
	for _, name := range []string{"name", "$score", "$seg_id", "d1"} {
		require.NotNil(t, rs.GetColumn(name), name)
	}
	require.Nil(t, rs.GetColumn("title"))
	v, err := rs.GetColumn("name").GetAsString(1)
	require.NoError(t, err)
	require.Equal(t, "blue mug", v)
	seg, err := rs.GetColumn("$seg_id").GetAsInt64(0)
	require.NoError(t, err)
	require.Equal(t, int64(7), seg)
	null, err := rs.GetColumn("d1").IsNull(1)
	require.NoError(t, err)
	require.True(t, null)
	null, err = rs.GetColumn("d1").IsNull(0)
	require.NoError(t, err)
	require.False(t, null)
}
