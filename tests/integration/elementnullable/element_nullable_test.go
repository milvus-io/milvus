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

package elementnullable

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	structName = "items"
	intsName   = "ints"
	nestedName = "nested_ints"
	textName   = "nested_text"
	nullVec    = "nullable_vectors"
	plainVec   = "vectors"
	dim        = 8
)

type ElementNullableSuite struct {
	integration.MiniClusterSuite
}

func (s *ElementNullableSuite) SetupSuite() {
	s.WithMilvusConfig("mq.type", "woodpecker")
	s.MiniClusterSuite.SetupSuite()
}

func TestElementNullable(t *testing.T) {
	suite.Run(t, new(ElementNullableSuite))
}

func kv(key, value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: key, Value: value}
}

func nestedType(leaf schemapb.DataType, innerNullable, leafNullable, rowNullable bool) *schemapb.TypeSchema {
	leafNode := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: leaf}, Nullable: leafNullable}
	if leaf == schemapb.DataType_VarChar {
		leafNode.TypeParams = []*commonpb.KeyValuePair{kv(common.MaxLengthKey, "64")}
	}
	inner := &schemapb.TypeSchema{
		Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: leafNode},
		Nullable:   innerNullable,
		TypeParams: []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8")},
	}
	return &schemapb.TypeSchema{
		Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: inner},
		Nullable:   rowNullable,
		TypeParams: []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8")},
	}
}

func collectionSchema(name string) *schemapb.CollectionSchema {
	arrayParams := []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8")}
	vectorParams := []*commonpb.KeyValuePair{kv(common.DimKey, "8"), kv(common.MaxCapacityKey, "8")}
	return &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "score", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "legacy", DataType: schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64, TypeParams: arrayParams},
			{FieldID: 103, Name: "embedding", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{kv(common.DimKey, "8")}},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 104, Name: structName, Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 105, Name: intsName, DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64, Nullable: true, ElementNullable: true, TypeParams: arrayParams},
				{FieldID: 106, Name: nestedName, DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: true,
					TypeParams: arrayParams, TypeSchema: nestedType(schemapb.DataType_Int64, true, true, true)},
				{FieldID: 107, Name: textName, DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Array, Nullable: true,
					TypeParams: arrayParams, TypeSchema: nestedType(schemapb.DataType_VarChar, false, false, true)},
				{FieldID: 108, Name: nullVec, DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true, TypeParams: vectorParams},
				{FieldID: 109, Name: plainVec, DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: vectorParams},
			},
		}},
	}
}

func scalarField(name string, id int64, rows ...*schemapb.ScalarField) *schemapb.FieldData {
	return &schemapb.FieldData{FieldName: name, FieldId: id, Type: schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: rows}},
		}}}
}

func longRow(values []int64, valid ...bool) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
		LongData: &schemapb.LongArray{Data: values}}, ValidData: valid}
}

func stringRow(values ...string) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{
		StringData: &schemapb.StringArray{Data: values}}}
}

func arrayRow(elementType schemapb.DataType, valid []bool, children ...*schemapb.ScalarField) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
		ArrayData: &schemapb.ArrayArray{ElementType: elementType, Data: children}}, ValidData: valid}
}

func vectorRow(values []float32, valid ...bool) *schemapb.VectorField {
	return &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{
		FloatVector: &schemapb.FloatArray{Data: values}}, ValidData: valid}
}

func vectorField(name string, id int64, rows ...*schemapb.VectorField) *schemapb.FieldData {
	return &schemapb.FieldData{FieldName: name, FieldId: id, Type: schemapb.DataType_ArrayOfVector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim,
			Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
				Dim: dim, ElementType: schemapb.DataType_FloatVector, Data: rows,
			}},
		}}}
}

func vector(seed float32) []float32 {
	return []float32{seed, seed + 1, seed + 2, seed + 3, seed + 4, seed + 5, seed + 6, seed + 7}
}

func insertFields() []*schemapb.FieldData {
	validRows := []bool{true, true, false}
	children := []*schemapb.FieldData{
		scalarField(intsName, 105, longRow([]int64{11}, true, false), longRow(nil)),
		scalarField(nestedName, 106,
			arrayRow(schemapb.DataType_Int64, []bool{true, false}, longRow([]int64{21, 23}, true, false, true)),
			arrayRow(schemapb.DataType_Int64, nil)),
		scalarField(textName, 107,
			arrayRow(schemapb.DataType_VarChar, nil, stringRow("alpha"), stringRow("beta")),
			arrayRow(schemapb.DataType_VarChar, nil)),
		vectorField(nullVec, 108, vectorRow(vector(1), true, false), vectorRow(nil)),
		vectorField(plainVec, 109, vectorRow(append(vector(2), vector(3)...)), vectorRow(nil)),
	}
	for _, child := range children {
		typeutil.SetFieldDataValidData(child, validRows)
	}
	return []*schemapb.FieldData{
		{FieldName: "pk", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}},
			}}},
		{FieldName: "score", FieldId: 101, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}}},
			}}},
		scalarField("legacy", 102, longRow([]int64{1}), longRow(nil), longRow([]int64{3})),
		{FieldName: "embedding", FieldId: 103, Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
					Data: append(append(vector(1), vector(2)...), vector(3)...),
				}},
			}}},
		{FieldName: structName, FieldId: 104, Type: schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}}},
	}
}

func (s *ElementNullableSuite) create(ctx context.Context, schema *schemapb.CollectionSchema) *commonpb.Status {
	data, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: schema.GetName(), Schema: data, ShardsNum: 1,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(err)
	return status
}

func (s *ElementNullableSuite) TestSchemaRejections() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, tc := range []struct {
		name, want string
		change     func(*schemapb.CollectionSchema)
	}{
		{"top_level_nullable_element", "element_nullable is only supported", func(schema *schemapb.CollectionSchema) {
			schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 110, Name: "bad_array",
				DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
				ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8")}})
		}},
		{"top_level_nested", "nested array can only be in a struct array field", func(schema *schemapb.CollectionSchema) {
			schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 110, Name: "bad_array",
				DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
				TypeParams: []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8")},
				TypeSchema: nestedType(schemapb.DataType_Int64, false, false, false)})
		}},
		{"inner_nullability_mismatch", "must match element_nullable", func(schema *schemapb.CollectionSchema) {
			schema.StructArrayFields[0].Fields[1].TypeSchema.GetArrayElement().Nullable = false
		}},
		{"nested_vector_leaf", "element type FloatVector is not supported", func(schema *schemapb.CollectionSchema) {
			schema.StructArrayFields[0].Fields[1].TypeSchema.GetArrayElement().GetArrayElement().Kind =
				&schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_FloatVector}
		}},
	} {
		s.Run(tc.name, func() {
			schema := collectionSchema("w7i_bad_" + funcutil.RandomString(8))
			tc.change(schema)
			err := merr.Error(s.create(ctx, schema))
			s.Require().Error(err)
			s.Contains(err.Error(), tc.want)
		})
	}
}

func (s *ElementNullableSuite) insert(ctx context.Context, name string, fields []*schemapb.FieldData) error {
	result, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name, FieldsData: fields, NumRows: 3,
		HashKeys: integration.GenerateHashKeys(3),
	})
	return merr.CheckRPCCall(result, err)
}

func (s *ElementNullableSuite) TestInsertValidationAndImport() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	name := "w7i_validate_" + funcutil.RandomString(8)
	s.Require().NoError(merr.Error(s.create(ctx, collectionSchema(name))))

	for _, tc := range []struct {
		name, want string
		change     func([]*schemapb.FieldData)
	}{
		{"scalar_compact_mismatch", "compact payload", func(fields []*schemapb.FieldData) {
			fields[4].GetStructArrays().Fields[0].GetScalars().GetArrayData().Data[0].ValidData = []bool{false, false}
		}},
		{"inner_compact_mismatch", "compact payload", func(fields []*schemapb.FieldData) {
			fields[4].GetStructArrays().Fields[1].GetScalars().GetArrayData().Data[0].ValidData = []bool{true, true}
		}},
		{"leaf_compact_mismatch", "compact payload", func(fields []*schemapb.FieldData) {
			fields[4].GetStructArrays().Fields[1].GetScalars().GetArrayData().Data[0].GetArrayData().Data[0].ValidData = []bool{true, true, true}
		}},
		{"nonnullable_inner_mask", "does not support element valid_data", func(fields []*schemapb.FieldData) {
			fields[4].GetStructArrays().Fields[2].GetScalars().GetArrayData().Data[0].ValidData = []bool{true, true}
		}},
		{"nonnullable_leaf_mask", "not element nullable", func(fields []*schemapb.FieldData) {
			fields[4].GetStructArrays().Fields[2].GetScalars().GetArrayData().Data[0].GetArrayData().Data[0].ValidData = []bool{true}
		}},
		{"vector_compact_mismatch", "valid", func(fields []*schemapb.FieldData) {
			fields[4].GetStructArrays().Fields[3].GetVectors().GetVectorArray().Data[0].ValidData = []bool{true, true}
		}},
		{"nonnullable_vector_mask", "not element nullable", func(fields []*schemapb.FieldData) {
			fields[4].GetStructArrays().Fields[4].GetVectors().GetVectorArray().Data[0].ValidData = []bool{true, true}
		}},
	} {
		s.Run(tc.name, func() {
			fields := insertFields()
			tc.change(fields)
			err := s.insert(ctx, name, fields)
			s.Require().Error(err)
			s.Contains(err.Error(), tc.want)
		})
	}

	resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: name,
		Files:          []*internalpb.ImportFile{{Paths: []string{"unused.json"}}},
	})
	s.Require().NoError(err)
	err = merr.Error(resp.GetStatus())
	s.Require().Error(err)
	s.Contains(err.Error(), "bulk import of element-nullable or nested Array field items.ints is not supported yet")
}

func (s *ElementNullableSuite) queryRow(ctx context.Context, name string, pk int64) map[string]*schemapb.FieldData {
	resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: name, Expr: fmt.Sprintf("pk == %d", pk),
		OutputFields: []string{"pk", "score", "legacy", "embedding", structName},
	})
	s.Require().NoError(merr.CheckRPCCall(resp, err))
	fields := make(map[string]*schemapb.FieldData)
	for _, field := range resp.GetFieldsData() {
		fields[field.GetFieldName()] = field
	}
	s.Require().Contains(fields, structName)
	return fields
}

func (s *ElementNullableSuite) assertRows(ctx context.Context, name string) {
	for _, pk := range []int64{1, 2, 3} {
		fields := s.queryRow(ctx, name, pk)
		s.Equal([]int64{pk}, fields["pk"].GetScalars().GetLongData().GetData())
		s.Equal([]int64{pk * 10}, fields["score"].GetScalars().GetLongData().GetData())
		s.Equal(vector(float32(pk)), fields["embedding"].GetVectors().GetFloatVector().GetData())
		legacy := fields["legacy"].GetScalars().GetArrayData().GetData()
		s.Require().Len(legacy, 1)
		if pk == 2 {
			s.Empty(legacy[0].GetLongData().GetData())
		} else {
			s.Equal([]int64{pk}, legacy[0].GetLongData().GetData())
		}
		children := make(map[string]*schemapb.FieldData)
		for _, child := range fields[structName].GetStructArrays().GetFields() {
			children[child.GetFieldName()] = child
		}
		s.Require().Len(children, 5)
		for childName, child := range children {
			s.Equal([]bool{pk != 3}, child.GetValidData(), childName+" FieldData.valid_data")
			if child.GetScalars() != nil {
				s.Equal([]bool{pk != 3}, child.GetScalars().GetValidData(), childName+" Scalars.valid_data")
			} else {
				s.Equal([]bool{pk != 3}, child.GetVectors().GetValidData(), childName+" Vectors.valid_data")
			}
		}
		if pk == 3 {
			continue
		}
		intRow := children[intsName].GetScalars().GetArrayData().GetData()[0]
		intNested := children[nestedName].GetScalars().GetArrayData().GetData()[0]
		textNested := children[textName].GetScalars().GetArrayData().GetData()[0]
		nullVector := children[nullVec].GetVectors().GetVectorArray().GetData()[0]
		plainVector := children[plainVec].GetVectors().GetVectorArray().GetData()[0]
		if pk == 2 {
			s.Empty(intRow.GetLongData().GetData())
			s.Empty(intNested.GetArrayData().GetData())
			s.Empty(textNested.GetArrayData().GetData())
			s.Empty(nullVector.GetFloatVector().GetData())
			s.Empty(plainVector.GetFloatVector().GetData())
			continue
		}
		s.Equal([]bool{true, false}, intRow.GetValidData())
		s.Equal([]int64{11, 0}, intRow.GetLongData().GetData())
		s.Equal([]bool{true, false}, intNested.GetValidData())
		s.Require().Len(intNested.GetArrayData().GetData(), 2)
		leaf := intNested.GetArrayData().GetData()[0]
		s.Equal([]bool{true, false, true}, leaf.GetValidData())
		s.Equal([]int64{21, 0, 23}, leaf.GetLongData().GetData())
		s.Empty(intNested.GetArrayData().GetData()[1].GetLongData().GetData())
		s.Empty(textNested.GetValidData())
		s.Require().Len(textNested.GetArrayData().GetData(), 2)
		s.Equal([]string{"alpha"}, textNested.GetArrayData().GetData()[0].GetStringData().GetData())
		s.Equal([]string{"beta"}, textNested.GetArrayData().GetData()[1].GetStringData().GetData())
		s.Equal([]bool{true, false}, nullVector.GetValidData())
		s.Equal(vector(1), nullVector.GetFloatVector().GetData())
		s.Empty(plainVector.GetValidData())
		s.Equal(append(vector(2), vector(3)...), plainVector.GetFloatVector().GetData())
	}
}

func (s *ElementNullableSuite) TestGrowingSealedSearchAndExpressions() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	name := "w7i_data_" + funcutil.RandomString(8)
	s.Require().NoError(merr.Error(s.create(ctx, collectionSchema(name))))
	plainName := typeutil.ConcatStructFieldName(structName, plainVec)
	nullName := typeutil.ConcatStructFieldName(structName, nullVec)

	for _, field := range []string{"embedding", plainName} {
		metricType := metric.L2
		indexName := "idx_embedding"
		if field == plainName {
			metricType = metric.MaxSim
			indexName = "idx_plain"
		}
		status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: name, FieldName: field, IndexName: indexName,
			ExtraParams: integration.ConstructIndexParam(dim, integration.IndexHNSW, metricType),
		})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForIndexBuilt(ctx, name, field)
	}
	for _, metricType := range []string{metric.L2, metric.MaxSim} {
		status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: name, FieldName: nullName,
			ExtraParams: integration.ConstructIndexParam(dim, integration.IndexHNSW, metricType),
		})
		s.Require().NoError(err)
		err = merr.Error(status)
		s.Require().Error(err)
		if metricType == metric.MaxSim {
			s.Contains(err.Error(), "only support element-level metrics")
		} else {
			s.Contains(err.Error(), "indexing element-nullable vector array field "+nullName+" is not supported yet")
		}
	}

	status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
	s.Require().NoError(s.insert(ctx, name, insertFields()))
	s.assertRows(ctx, name)

	search := integration.ConstructEmbeddingListSearchRequest("", name, "pk == 1", plainName,
		schemapb.DataType_FloatVector, nil, metric.MaxSim,
		integration.GetSearchParams(integration.IndexHNSW, metric.MaxSim), 1, dim, 1, -1)
	result, err := s.Cluster.MilvusClient.Search(ctx, search)
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.Require().Len(result.GetResults().GetIds().GetIntId().GetData(), 1)

	for _, embeddingList := range []bool{false, true} {
		var request *milvuspb.SearchRequest
		if embeddingList {
			request = integration.ConstructEmbeddingListSearchRequest("", name, "", nullName,
				schemapb.DataType_FloatVector, nil, metric.MaxSim,
				integration.GetSearchParams(integration.IndexHNSW, metric.MaxSim), 1, dim, 1, -1)
		} else {
			request = integration.ConstructElementLevelSearchRequest("", name, "", nullName,
				schemapb.DataType_FloatVector, nil, metric.L2,
				integration.GetSearchParams(integration.IndexHNSW, metric.L2), 1, dim, 1, -1)
		}
		result, err := s.Cluster.MilvusClient.Search(ctx, request)
		s.Require().NoError(err)
		err = merr.Error(result.GetStatus())
		s.Require().Error(err)
		if embeddingList {
			s.Contains(err.Error(), "embedding-list search is not supported for element-nullable vector array field "+nullName)
		} else {
			s.Contains(err.Error(), "search on element-nullable vector array field "+nullName+" is not supported yet")
		}
	}

	for _, expr := range []string{
		"element_filter(items, $[ints] > 0)",
		"MATCH_ANY(items, $[ints] > 0)",
		"array_contains(items[ints], 11)",
	} {
		result, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			CollectionName: name, Expr: expr, OutputFields: []string{"pk"},
		})
		s.Require().NoError(err)
		err = merr.Error(result.GetStatus())
		s.Require().Error(err)
		s.Contains(err.Error(), "not supported")
	}

	resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: name, Expr: "array_length(items[nested_text]) > 0", OutputFields: []string{"pk"},
	})
	s.Require().NoError(merr.CheckRPCCall(resp, err))
	s.Require().Equal([]int64{1}, resp.GetFieldsData()[0].GetScalars().GetLongData().GetData())

	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(flush.GetStatus(), err))
	segmentIDs := flush.GetFlushCollSegIDs()[name].GetData()
	s.Require().NotEmpty(segmentIDs)
	s.WaitForFlush(ctx, segmentIDs, flush.GetCollFlushTs()[name], "", name)
	status, err = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
	s.assertRows(ctx, name)

	// Two flushed segments give manual mix compaction real input segments.
	second := insertFields()
	second[0].GetScalars().GetLongData().Data = []int64{4, 5, 6}
	s.Require().NoError(s.insert(ctx, name, second))
	flush, err = s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(flush.GetStatus(), err))
	segmentIDs = flush.GetFlushCollSegIDs()[name].GetData()
	s.Require().NotEmpty(segmentIDs)
	s.WaitForFlush(ctx, segmentIDs, flush.GetCollFlushTs()[name], "", name)

	describe, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(describe.GetStatus(), err))
	compact, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionID: describe.GetCollectionID(), MajorCompaction: true,
	})
	s.Require().NoError(merr.CheckRPCCall(compact.GetStatus(), err))
	s.Require().Eventually(func() bool {
		state, err := s.Cluster.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{
			CompactionID: compact.GetCompactionID(),
		})
		return err == nil && merr.Ok(state.GetStatus()) && state.GetState() == commonpb.CompactionState_Completed
	}, 5*time.Minute, 2*time.Second)
	s.assertRows(ctx, name)
}
