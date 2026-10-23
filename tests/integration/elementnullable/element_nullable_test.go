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
	"encoding/json"
	"fmt"
	"path"
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
			{
				FieldID: 102, Name: "legacy", DataType: schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64, TypeParams: arrayParams,
			},
			{
				FieldID: 103, Name: "embedding", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{kv(common.DimKey, "8")},
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 104, Name: structName, Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: 105, Name: intsName, DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64, Nullable: true, ElementNullable: true, TypeParams: arrayParams,
				},
				{
					FieldID: 106, Name: nestedName, DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: true,
					TypeParams: arrayParams, TypeSchema: nestedType(schemapb.DataType_Int64, true, true, true),
				},
				{
					FieldID: 107, Name: textName, DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Array, Nullable: true,
					TypeParams: arrayParams, TypeSchema: nestedType(schemapb.DataType_VarChar, false, false, true),
				},
				{
					FieldID: 108, Name: nullVec, DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true, TypeParams: vectorParams,
				},
				{
					FieldID: 109, Name: plainVec, DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: vectorParams,
				},
			},
		}},
	}
}

func scalarField(name string, id int64, rows ...*schemapb.ScalarField) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: name, FieldId: id, Type: schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: rows}},
		}},
	}
}

func longRow(values []int64, valid ...bool) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
		LongData: &schemapb.LongArray{Data: values},
	}, ValidData: valid}
}

func stringRow(values ...string) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{
		StringData: &schemapb.StringArray{Data: values},
	}}
}

func arrayRow(elementType schemapb.DataType, valid []bool, children ...*schemapb.ScalarField) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
		ArrayData: &schemapb.ArrayArray{ElementType: elementType, Data: children},
	}, ValidData: valid}
}

func vectorRow(values []float32, valid ...bool) *schemapb.VectorField {
	return &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{
		FloatVector: &schemapb.FloatArray{Data: values},
	}, ValidData: valid}
}

func vectorField(name string, id int64, rows ...*schemapb.VectorField) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: name, FieldId: id, Type: schemapb.DataType_ArrayOfVector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
				Dim: dim, ElementType: schemapb.DataType_FloatVector, Data: rows,
			}},
		}},
	}
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
		{
			FieldName: "pk", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}},
			}},
		},
		{
			FieldName: "score", FieldId: 101, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}}},
			}},
		},
		scalarField("legacy", 102, longRow([]int64{1}), longRow(nil), longRow([]int64{3})),
		{
			FieldName: "embedding", FieldId: 103, Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
					Data: append(append(vector(1), vector(2)...), vector(3)...),
				}},
			}},
		},
		{
			FieldName: structName, FieldId: 104, Type: schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}},
		},
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
			schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
				FieldID: 110, Name: "bad_array",
				DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
				ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8")},
			})
		}},
		{"top_level_nested", "nested array can only be in a struct array field", func(schema *schemapb.CollectionSchema) {
			schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
				FieldID: 110, Name: "bad_array",
				DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
				TypeParams: []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8")},
				TypeSchema: nestedType(schemapb.DataType_Int64, false, false, false),
			})
		}},
		{"inner_nullability_mismatch", "must match element_nullable", func(schema *schemapb.CollectionSchema) {
			schema.StructArrayFields[0].Fields[1].TypeSchema.GetArrayElement().Nullable = false
		}},
		{"nested_vector_leaf", "element type FloatVector is not supported", func(schema *schemapb.CollectionSchema) {
			schema.StructArrayFields[0].Fields[1].TypeSchema.GetArrayElement().GetArrayElement().Kind = &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_FloatVector}
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

	// Binlog (backup) imports cannot fill element-nullable or nested Array
	// sub-fields, and NumPy files cannot carry a struct field.
	resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: name, PartitionName: "_default",
		Files:   []*internalpb.ImportFile{{Paths: []string{"backup/insert_log/", "backup/delta_log/"}}},
		Options: []*commonpb.KeyValuePair{kv("backup", "true")},
	})
	s.Require().NoError(err)
	err = merr.Error(resp.GetStatus())
	s.Require().ErrorIs(err, merr.ErrParameterInvalid)
	s.Contains(err.Error(), "binlog import (backup restore) into a collection with element-nullable or nested Array field items.ints is not supported")

	resp, err = s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: name,
		Files:          []*internalpb.ImportFile{{Paths: []string{"numpy/pk.npy", "numpy/embedding.npy"}}},
	})
	s.Require().NoError(err)
	err = merr.Error(resp.GetStatus())
	s.Require().ErrorIs(err, merr.ErrImportFailed)
	s.Contains(err.Error(), "numpy import does not support struct field 'items'")

	// A JSON import of the rows of insertFields reads back like the insert.
	s.importJSON(ctx, name, importRows())
	s.indexAndLoad(ctx, name)
	s.assertRows(ctx, name)
}

// importRows are the rows of insertFields in the JSON import format: a struct
// value is a list of element objects, a null element value is null, and a
// nested Array value is one inner array (null for a null inner array).
func importRows() []map[string]any {
	return []map[string]any{
		{
			"pk": 1, "score": 10, "legacy": []int64{1}, "embedding": vector(1),
			structName: []map[string]any{
				{intsName: 11, nestedName: []any{21, nil, 23}, textName: []string{"alpha"}, nullVec: vector(1), plainVec: vector(2)},
				{intsName: nil, nestedName: nil, textName: []string{"beta"}, nullVec: nil, plainVec: vector(3)},
			},
		},
		{"pk": 2, "score": 20, "legacy": []int64{}, "embedding": vector(2), structName: []map[string]any{}},
		{"pk": 3, "score": 30, "legacy": []int64{3}, "embedding": vector(3), structName: nil},
	}
}

func (s *ElementNullableSuite) importJSON(ctx context.Context, name string, rows []map[string]any) {
	data, err := json.Marshal(rows)
	s.Require().NoError(err)
	filePath := path.Join(s.Cluster.RootPath(), "json", name+"_"+funcutil.RandomString(8)+".json")
	s.Require().NoError(s.Cluster.ChunkManager.Write(ctx, filePath, data))
	resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: name, Files: []*internalpb.ImportFile{{Paths: []string{filePath}}},
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	for {
		progress, err := s.Cluster.ProxyClient.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{JobID: resp.GetJobID()})
		s.Require().NoError(merr.CheckRPCCall(progress.GetStatus(), err))
		s.Require().NotEqual(internalpb.ImportJobState_Failed, progress.GetState(), progress.GetReason())
		if progress.GetState() == internalpb.ImportJobState_Completed {
			return
		}
		select {
		case <-ctx.Done():
			s.FailNow("import did not complete before the deadline")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// indexAndLoad indexes every vector field, including the element-nullable
// vector sub-field that must be indexed before load, and loads the collection.
func (s *ElementNullableSuite) indexAndLoad(ctx context.Context, name string) {
	plainName := typeutil.ConcatStructFieldName(structName, plainVec)
	nullName := typeutil.ConcatStructFieldName(structName, nullVec)
	for _, field := range []string{"embedding", plainName, nullName} {
		metricType := metric.L2
		if field == plainName {
			metricType = metric.MaxSim
		}
		status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: name, FieldName: field, IndexName: "idx_" + funcutil.RandomString(4),
			ExtraParams: integration.ConstructIndexParam(dim, integration.IndexHNSW, metricType),
		})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForIndexBuilt(ctx, name, field)
	}
	status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
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
	s.assertRowsWithScore(ctx, name, func(pk int64) int64 { return pk * 10 })
}

func (s *ElementNullableSuite) assertRowsWithScore(ctx context.Context, name string, score func(pk int64) int64) {
	for _, pk := range []int64{1, 2, 3} {
		fields := s.queryRow(ctx, name, pk)
		s.Equal([]int64{pk}, fields["pk"].GetScalars().GetLongData().GetData())
		s.Equal([]int64{score(pk)}, fields["score"].GetScalars().GetLongData().GetData())
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
	status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: nullName,
		ExtraParams: integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.MaxSim),
	})
	s.Require().NoError(err)
	err = merr.Error(status)
	s.Require().Error(err)
	s.Contains(err.Error(), "only support element-level metrics")

	// The element-nullable vector array must be indexed before it can be loaded.
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	err = merr.Error(status)
	s.Require().Error(err)
	s.Contains(err.Error(), "there is no vector index on field")

	status, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: nullName, IndexName: "idx_null",
		ExtraParams: integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.L2),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuilt(ctx, name, nullName)

	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
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
		if !embeddingList {
			s.Require().NoError(merr.CheckRPCCall(result, err))
			continue
		}
		s.Require().NoError(err)
		err = merr.Error(result.GetStatus())
		s.Require().Error(err)
		s.Contains(err.Error(), "embedding-list search is not supported for element-nullable vector array field "+nullName)
	}

	// Element-level predicates on element-nullable sub-fields: the null
	// element of pk 1 is UNKNOWN, its first element matches.
	for _, expr := range []string{
		"element_filter(items, $[ints] > 0)",
		"MATCH_ANY(items, $[ints] > 0)",
	} {
		resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			CollectionName: name, Expr: expr, OutputFields: []string{"pk"},
		})
		s.Require().NoError(merr.CheckRPCCall(resp, err), expr)
		s.Require().NotEmpty(resp.GetFieldsData(), expr)
		s.Equal([]int64{1}, resp.GetFieldsData()[0].GetScalars().GetLongData().GetData(), expr)
	}

	// Row-level filters: ints is [11, NULL], [] and a NULL struct row. A null
	// element matches nothing in array_contains and a subscript on it is
	// UNKNOWN, so NOT selects neither the null element nor the null row.
	for expr, want := range map[string][]int64{
		"array_contains(items[ints], 11)":           {1},
		"not array_contains(items[ints], 11)":       {2},
		"array_contains(items[ints], 0)":            nil,
		"array_length(items[ints]) == 2":            {1},
		"items[1][ints] == 0":                       nil,
		"not (items[1][ints] == 0)":                 nil,
		"items[0][ints] == 11":                      {1},
		"items[ints] != [11]":                       {1, 2},
		"array_length(items[0][nested_ints]) == 3":  {1},
		"array_contains(items[0][nested_ints], 23)": {1},
		"array_contains(items[0][nested_ints], 0)":  nil,
		"items[1][nested_ints] == []":               nil,
	} {
		resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			CollectionName: name, Expr: expr, OutputFields: []string{"pk"},
		})
		s.Require().NoError(merr.CheckRPCCall(resp, err), expr)
		var got []int64
		if fields := resp.GetFieldsData(); len(fields) > 0 {
			got = fields[0].GetScalars().GetLongData().GetData()
		}
		s.ElementsMatch(want, got, expr)
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

func (s *ElementNullableSuite) flushAndReload(ctx context.Context, name string) {
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(flush.GetStatus(), err))
	segmentIDs := flush.GetFlushCollSegIDs()[name].GetData()
	s.Require().NotEmpty(segmentIDs)
	s.WaitForFlush(ctx, segmentIDs, flush.GetCollFlushTs()[name], "", name)
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
}

// partialUpsertScore updates only score. The struct field is omitted, so
// partial upsert keeps the old struct value read back by Query.
func (s *ElementNullableSuite) partialUpsertScore(ctx context.Context, name string, pks []int64, score func(pk int64) int64) {
	scores := make([]int64, len(pks))
	for i, pk := range pks {
		scores[i] = score(pk)
	}
	result, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{
		CollectionName: name, NumRows: uint32(len(pks)), PartialUpdate: true,
		HashKeys: integration.GenerateHashKeys(len(pks)),
		FieldsData: []*schemapb.FieldData{
			{
				FieldName: "pk", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
				}},
			},
			{
				FieldName: "score", FieldId: 101, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: scores}},
				}},
			},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.Equal(int64(len(pks)), result.GetUpsertCnt())
}

// partialUpsertWithNewRow updates score, legacy and embedding of pks, which
// may include rows that do not exist yet. legacy and embedding get the values
// the rows were inserted with, and the struct field is omitted: existing rows
// keep their struct and new rows get a null struct.
func (s *ElementNullableSuite) partialUpsertWithNewRow(ctx context.Context, name string, pks []int64, score func(pk int64) int64) {
	scores := make([]int64, len(pks))
	legacy := make([]*schemapb.ScalarField, len(pks))
	var embeddings []float32
	for i, pk := range pks {
		scores[i] = score(pk)
		if pk == 2 {
			legacy[i] = longRow(nil)
		} else {
			legacy[i] = longRow([]int64{pk})
		}
		embeddings = append(embeddings, vector(float32(pk))...)
	}
	result, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{
		CollectionName: name, NumRows: uint32(len(pks)), PartialUpdate: true,
		HashKeys: integration.GenerateHashKeys(len(pks)),
		FieldsData: []*schemapb.FieldData{
			{
				FieldName: "pk", FieldId: 100, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
				}},
			},
			{
				FieldName: "score", FieldId: 101, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: scores}},
				}},
			},
			scalarField("legacy", 102, legacy...),
			{
				FieldName: "embedding", FieldId: 103, Type: schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  dim,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: embeddings}},
				}},
			},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.Equal(int64(len(pks)), result.GetUpsertCnt())
}

// assertNullStructRow checks a row written by partialUpsertWithNewRow that did
// not exist before.
func (s *ElementNullableSuite) assertNullStructRow(ctx context.Context, name string, pk int64, score int64) {
	fields := s.queryRow(ctx, name, pk)
	s.Equal([]int64{pk}, fields["pk"].GetScalars().GetLongData().GetData())
	s.Equal([]int64{score}, fields["score"].GetScalars().GetLongData().GetData())
	s.Equal(vector(float32(pk)), fields["embedding"].GetVectors().GetFloatVector().GetData())
	legacy := fields["legacy"].GetScalars().GetArrayData().GetData()
	s.Require().Len(legacy, 1)
	s.Equal([]int64{pk}, legacy[0].GetLongData().GetData())
	children := fields[structName].GetStructArrays().GetFields()
	s.Require().Len(children, 5)
	for _, child := range children {
		s.Equal([]bool{false}, child.GetValidData(), child.GetFieldName()+" FieldData.valid_data")
	}
}

func (s *ElementNullableSuite) TestPartialUpsertKeepsOmittedStruct() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	name := "w7i_partial_" + funcutil.RandomString(8)
	s.Require().NoError(merr.Error(s.create(ctx, collectionSchema(name))))
	plainName := typeutil.ConcatStructFieldName(structName, plainVec)
	nullName := typeutil.ConcatStructFieldName(structName, nullVec)
	// The element-nullable vector array must be indexed before it can be loaded.
	for _, field := range []string{"embedding", plainName, nullName} {
		metricType := metric.L2
		if field == plainName {
			metricType = metric.MaxSim
		}
		status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: name, FieldName: field, IndexName: "idx_" + funcutil.RandomString(4),
			ExtraParams: integration.ConstructIndexParam(dim, integration.IndexHNSW, metricType),
		})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForIndexBuilt(ctx, name, field)
	}
	status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
	s.Require().NoError(s.insert(ctx, name, insertFields()))

	// The old rows are read from a growing segment. The request order differs
	// from the insert order, and pk 3 has a null struct.
	first := func(pk int64) int64 { return pk*10 + 1 }
	s.partialUpsertScore(ctx, name, []int64{3, 1, 2}, first)
	s.assertRowsWithScore(ctx, name, first)

	// The old rows are read from a sealed segment.
	s.flushAndReload(ctx, name)
	s.assertRowsWithScore(ctx, name, first)
	second := func(pk int64) int64 { return pk*10 + 2 }
	s.partialUpsertScore(ctx, name, []int64{2, 3, 1}, second)
	s.assertRowsWithScore(ctx, name, second)
	s.flushAndReload(ctx, name)
	s.assertRowsWithScore(ctx, name, second)

	// One batch holds pk 3 with a null struct, pk 1 with null elements and
	// null leaves, and pk 4, which does not exist yet and gets a null struct.
	third := func(pk int64) int64 {
		if pk == 2 {
			return second(pk)
		}
		return pk*10 + 3
	}
	s.partialUpsertWithNewRow(ctx, name, []int64{3, 4, 1}, third)
	s.assertRowsWithScore(ctx, name, third)
	s.assertNullStructRow(ctx, name, 4, third(4))
	s.flushAndReload(ctx, name)
	s.assertRowsWithScore(ctx, name, third)
	s.assertNullStructRow(ctx, name, 4, third(4))
}
