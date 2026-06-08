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

package segments

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestValidateExternalMaterializedFields(t *testing.T) {
	schema := externalMaterializationTestSchema(true)
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    1,
		ManifestPath: "manifest",
		BinlogPaths: []*datapb.FieldBinlog{
			{ChildFields: []int64{100, 101}},
		},
		DataVersion:    3,
		StorageVersion: 3,
	}

	assert.NoError(t, ValidateExternalMaterializedFields(schema, []*querypb.SegmentLoadInfo{loadInfo}, []int64{100, 101}))

	err := ValidateExternalMaterializedFields(schema, []*querypb.SegmentLoadInfo{loadInfo}, []int64{102})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "external field score is not materialized")
	assert.Contains(t, err.Error(), "run RefreshExternalCollection")
}

func TestValidateExternalMaterializedFieldsSkipsNonExternalFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "external_materialization_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "id"},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Int64},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    1,
		ManifestPath: "manifest",
		BinlogPaths: []*datapb.FieldBinlog{
			{ChildFields: []int64{100}},
		},
	}

	assert.NoError(t, ValidateExternalMaterializedFields(schema, []*querypb.SegmentLoadInfo{loadInfo}, []int64{1, 101}))
}

func TestValidateExternalMaterializedFieldsChecksFunctionOutput(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "external_materialization_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    1,
		ManifestPath: "manifest",
		BinlogPaths: []*datapb.FieldBinlog{
			{ChildFields: []int64{100}},
		},
	}

	err := ValidateExternalMaterializedFields(schema, []*querypb.SegmentLoadInfo{loadInfo}, []int64{101})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "external field sparse is not materialized")

	loadInfo.Bm25Logs = []*datapb.FieldBinlog{
		{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 1}}},
	}
	assert.NoError(t, ValidateExternalMaterializedFields(schema, []*querypb.SegmentLoadInfo{loadInfo}, []int64{101}))
}

func TestValidateExternalMaterializedFieldsChecksUnknownRequestedField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "external_materialization_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    1,
		ManifestPath: "manifest",
		BinlogPaths: []*datapb.FieldBinlog{
			{ChildFields: []int64{100}},
		},
	}

	err := ValidateExternalMaterializedFields(schema, []*querypb.SegmentLoadInfo{loadInfo}, []int64{101})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "external field 101 is not materialized")
}

func TestValidateExternalMaterializedFieldsSkipsInternalCollection(t *testing.T) {
	schema := externalMaterializationTestSchema(false)
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    1,
		ManifestPath: "manifest",
		BinlogPaths: []*datapb.FieldBinlog{
			{ChildFields: []int64{100, 101}},
		},
	}

	assert.NoError(t, ValidateExternalMaterializedFields(schema, []*querypb.SegmentLoadInfo{loadInfo}, []int64{102}))
}

func TestRequiredFieldIDs(t *testing.T) {
	retrieveReq := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			OutputFieldsId:  []int64{101, 100, 101},
			GroupByFieldIds: []int64{102},
			Aggregates: []*planpb.Aggregate{
				{FieldId: 0},
				{FieldId: 103},
			},
			OrderByFields: []*planpb.OrderByField{
				{FieldId: 104},
			},
		},
	}

	assert.ElementsMatch(t, []int64{100, 101, 102, 103, 104}, RetrieveRequiredFieldIDs(retrieveReq))
	assert.Nil(t, RetrieveRequiredFieldIDs(nil))
	assert.Nil(t, RetrieveRequiredFieldIDs(&querypb.QueryRequest{}))

	searchReq := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			OutputFieldsId:  []int64{103},
			GroupByFieldId:  104,
			FieldId:         0,
			GroupByFieldIds: []int64{105},
			SubReqs: []*internalpb.SubSearchRequest{
				{FieldId: 106, GroupByFieldId: 107},
			},
		},
	}
	assert.Empty(t, SearchRequiredFieldIDs(nil, nil))
	assert.ElementsMatch(t,
		[]int64{102, 103, 104, 105, 106, 107},
		SearchRequiredFieldIDs(searchRequestWithFieldID(t, 102), searchReq),
	)

	assert.Empty(t, LoadInfosFromSegments([]Segment{nil}))
}

func TestValidateExternalMaterializedFieldsForCollectionSkipsEmptyFields(t *testing.T) {
	assert.NoError(t, validateExternalMaterializedFieldsForCollection(nil, 100, nil, nil))
}

func TestAllowUnmaterializedExternalFieldAccessSkipsQueryNodeGate(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QueryNodeCfg.ExternalCollectionAllowUnmaterializedFieldAccess.Key
	original := paramtable.Get().QueryNodeCfg.ExternalCollectionAllowUnmaterializedFieldAccess.GetValue()
	defer paramtable.Get().Save(key, original)

	paramtable.Get().Save(key, "false")
	assert.False(t, AllowUnmaterializedExternalFieldAccess())
	assert.Error(t, validateExternalMaterializedFieldsForCollection(nil, 100, nil, []int64{102}))

	paramtable.Get().Save(key, "true")
	assert.True(t, AllowUnmaterializedExternalFieldAccess())
	assert.NoError(t, validateExternalMaterializedFieldsForCollection(nil, 100, nil, []int64{102}))
}

func externalMaterializationTestSchema(external bool) *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 102, Name: "score", DataType: schemapb.DataType_Double},
	}
	if external {
		for _, field := range fields {
			field.ExternalField = field.Name
		}
	}

	return &schemapb.CollectionSchema{
		Name:   "external_materialization_test",
		Fields: fields,
	}
}

func searchRequestWithFieldID(t *testing.T, fieldID int64) *SearchRequest {
	t.Helper()

	searchReq := &SearchRequest{}
	value := reflect.ValueOf(searchReq).Elem().FieldByName("searchFieldID")
	require.True(t, value.IsValid())
	require.True(t, value.CanAddr())
	require.Equal(t, reflect.Int64, value.Kind())
	reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Elem().SetInt(fieldID)
	return searchReq
}
