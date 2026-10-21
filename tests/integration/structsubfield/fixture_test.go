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

package structsubfield

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
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const structName = "S"
const dim = 8

type StructSubFieldSuite struct {
	integration.MiniClusterSuite
}

func (s *StructSubFieldSuite) SetupSuite() {
	s.WithMilvusConfig("mq.type", "woodpecker")
	s.MiniClusterSuite.SetupSuite()
}

func TestStructSubField(t *testing.T) {
	suite.Run(t, new(StructSubFieldSuite))
}

func collectionName(prefix string) string {
	return "bt_" + prefix + "_" + funcutil.RandomString(8)
}

func param(key, value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: key, Value: value}
}

func scalarSchema(name string, elementType schemapb.DataType, elementNullable bool) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		Name: name, DataType: schemapb.DataType_Array, ElementType: elementType,
		ElementNullable: elementNullable, TypeParams: []*commonpb.KeyValuePair{param(common.MaxCapacityKey, "8")},
	}
}

func vectorSchema(name string, elementNullable bool) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		Name: name, DataType: schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector, ElementNullable: elementNullable,
		TypeParams: []*commonpb.KeyValuePair{param(common.DimKey, "8"), param(common.MaxCapacityKey, "8")},
	}
}

func nestedSchema(name string) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		Name: name, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
		ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{param(common.MaxCapacityKey, "8")},
		TypeSchema: &schemapb.TypeSchema{
			Nullable:   true,
			TypeParams: []*commonpb.KeyValuePair{param(common.MaxCapacityKey, "8")},
			Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
				Nullable: true, TypeParams: []*commonpb.KeyValuePair{param(common.MaxCapacityKey, "8")},
				Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
					Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64},
				}},
			}},
		},
	}
}

func baseSchema(name string, withVector bool, parentNullable bool, children ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}
	if withVector {
		fields = append(fields, &schemapb.FieldSchema{FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{param(common.DimKey, "8")}})
	}
	schema := &schemapb.CollectionSchema{Name: name, Fields: fields}
	if len(children) > 0 {
		for i, child := range children {
			child.FieldID = int64(103 + i)
			child.Nullable = parentNullable
		}
		schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 102, Name: structName, Nullable: parentNullable, Fields: children}}
	}
	return schema
}

func (s *StructSubFieldSuite) create(ctx context.Context, schema *schemapb.CollectionSchema) {
	data, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: schema.GetName(), Schema: data, ShardsNum: 1,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
}

func (s *StructSubFieldSuite) describe(ctx context.Context, name string) *milvuspb.DescribeCollectionResponse {
	resp, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	return resp
}

func (s *StructSubFieldSuite) describeInternal(ctx context.Context, name string) *milvuspb.DescribeCollectionResponse {
	resp, err := s.Cluster.MixCoordClient.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection}, CollectionName: name,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	return resp
}

func (s *StructSubFieldSuite) waitVersion(ctx context.Context, name string, version int32) *milvuspb.DescribeCollectionResponse {
	var result *milvuspb.DescribeCollectionResponse
	s.Require().Eventually(func() bool {
		resp, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
		if err != nil || !merr.Ok(resp.GetStatus()) || resp.GetSchema().GetVersion() < version {
			return false
		}
		result = resp
		return true
	}, 30*time.Second, 100*time.Millisecond)
	return result
}

func (s *StructSubFieldSuite) addStruct(ctx context.Context, name string, parentNullable bool) {
	before := s.describe(ctx, name).GetSchema().GetVersion()
	children := []*schemapb.FieldSchema{vectorSchema("vec", false), scalarSchema("ints", schemapb.DataType_Int64, true)}
	for _, child := range children {
		child.Nullable = parentNullable
	}
	status, err := s.Cluster.MilvusClient.AddCollectionStructField(ctx, &milvuspb.AddCollectionStructFieldRequest{
		CollectionName:         name,
		StructArrayFieldSchema: &schemapb.StructArrayFieldSchema{Name: structName, Nullable: parentNullable, Fields: children},
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.waitVersion(ctx, name, before+1)
}

func (s *StructSubFieldSuite) alterAdd(ctx context.Context, name, path, indexName string, field *schemapb.FieldSchema, fn *schemapb.FunctionSchema) error {
	addRequest := &milvuspb.AlterCollectionSchemaRequest_AddRequest{
		FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{StructPath: path, FieldSchema: field, IndexName: indexName}},
	}
	if fn != nil {
		addRequest.FuncSchema = []*schemapb.FunctionSchema{fn}
	}
	request := &milvuspb.AlterCollectionSchemaRequest{
		CollectionName: name,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
			AddRequest: addRequest,
		}},
	}
	resp, err := s.Cluster.MilvusClient.AlterCollectionSchema(ctx, request)
	return merr.CheckRPCCall(resp.GetAlterStatus(), err)
}

func (s *StructSubFieldSuite) add(ctx context.Context, name string, field *schemapb.FieldSchema) {
	before := s.describe(ctx, name).GetSchema().GetVersion()
	s.Require().NoError(s.alterAdd(ctx, name, structName, "", field, nil))
	s.waitVersion(ctx, name, before+1)
}

func (s *StructSubFieldSuite) alterDrop(ctx context.Context, name, storedName string) error {
	resp, err := s.Cluster.MilvusClient.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		CollectionName: name,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
			DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{
				Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{FieldName: storedName},
			},
		}},
	})
	return merr.CheckRPCCall(resp.GetAlterStatus(), err)
}

func (s *StructSubFieldSuite) drop(ctx context.Context, name, storedName string) {
	before := s.describe(ctx, name).GetSchema().GetVersion()
	s.Require().NoError(s.alterDrop(ctx, name, storedName))
	s.waitVersion(ctx, name, before+1)
}

func values(seed float32) []float32 {
	return []float32{seed, seed + 1, seed + 2, seed + 3, seed + 4, seed + 5, seed + 6, seed + 7}
}

func longRow(data []int64, valid ...bool) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}, ValidData: valid}
}

func stringRow(data []string, valid ...bool) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}}, ValidData: valid}
}

func scalarChild(name string, row *schemapb.ScalarField, rowValid bool) *schemapb.FieldData {
	child := &schemapb.FieldData{FieldName: name, Type: schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: []*schemapb.ScalarField{row}}},
		}}}
	typeutil.SetFieldDataValidData(child, []bool{rowValid})
	return child
}

func vectorChild(name string, row *schemapb.VectorField, rowValid bool) *schemapb.FieldData {
	child := &schemapb.FieldData{FieldName: name, Type: schemapb.DataType_ArrayOfVector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim,
			Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
				Dim: dim, ElementType: schemapb.DataType_FloatVector, Data: []*schemapb.VectorField{row},
			}},
		}}}
	typeutil.SetFieldDataValidData(child, []bool{rowValid})
	return child
}

func baseChildren() []*schemapb.FieldData {
	return []*schemapb.FieldData{
		vectorChild("vec", &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: append(values(1), values(2)...)}},
		}, true),
		scalarChild("ints", longRow([]int64{11}, true, false), true),
	}
}

func baseChildrenWithoutRowValidity() []*schemapb.FieldData {
	children := baseChildren()
	for _, child := range children {
		typeutil.SetFieldDataValidData(child, nil)
	}
	return children
}

func (s *StructSubFieldSuite) insert(ctx context.Context, name string, pk int64, withEmbedding bool, children ...*schemapb.FieldData) error {
	fields := []*schemapb.FieldData{{FieldName: "pk", FieldId: 100, Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{pk}}},
		}}}}
	if withEmbedding {
		fields = append(fields, &schemapb.FieldData{FieldName: "embedding", FieldId: 101, Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: values(float32(pk))}},
			}}})
	}
	if children != nil {
		fields = append(fields, &schemapb.FieldData{FieldName: structName, FieldId: 102, Type: schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}}})
	}
	resp, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name, FieldsData: fields, NumRows: 1, HashKeys: integration.GenerateHashKeys(1),
	})
	return merr.CheckRPCCall(resp, err)
}

func (s *StructSubFieldSuite) indexAndLoad(ctx context.Context, name string, loadFields ...string) {
	status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: "embedding", IndexName: "idx_embedding",
		ExtraParams: integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.L2),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuilt(ctx, name, "embedding")
	for _, parent := range s.describe(ctx, name).GetSchema().GetStructArrayFields() {
		for _, child := range parent.GetFields() {
			if child.GetName() != "vec" && child.GetName() != "S[vec]" {
				continue
			}
			status, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
				CollectionName: name, FieldName: "S[vec]", IndexName: "idx_struct_vec",
				ExtraParams: integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.MaxSim),
			})
			s.Require().NoError(merr.CheckRPCCall(status, err))
			s.WaitForIndexBuilt(ctx, name, "S[vec]")
		}
	}
	s.load(ctx, name, loadFields...)
}

func (s *StructSubFieldSuite) load(ctx context.Context, name string, fields ...string) {
	status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: name, LoadFields: fields,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
}

func (s *StructSubFieldSuite) release(ctx context.Context, name string) {
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
}

func (s *StructSubFieldSuite) flush(ctx context.Context, name string) {
	resp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	ids := resp.GetFlushCollSegIDs()[name].GetData()
	s.Require().NotEmpty(ids)
	s.WaitForFlush(ctx, ids, resp.GetCollFlushTs()[name], "", name)
}

func (s *StructSubFieldSuite) compact(ctx context.Context, name string) {
	id := s.describe(ctx, name).GetCollectionID()
	resp, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionID: id, MajorCompaction: true,
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	s.Require().Eventually(func() bool {
		state, err := s.Cluster.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{CompactionID: resp.GetCompactionID()})
		return err == nil && merr.Ok(state.GetStatus()) && state.GetState() == commonpb.CompactionState_Completed
	}, 5*time.Minute, 2*time.Second)
}

func (s *StructSubFieldSuite) restartQueryNode(ctx context.Context, name string) {
	s.Cluster.StopAllQueryNode()
	s.Cluster.AddQueryNode()
	s.WaitForLoad(ctx, name)
}

func (s *StructSubFieldSuite) query(ctx context.Context, name string, pk int64, output ...string) *milvuspb.QueryResults {
	resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: name, Expr: fmt.Sprintf("pk == %d", pk), OutputFields: output,
	})
	s.Require().NoError(merr.CheckRPCCall(resp, err))
	s.Require().NotEmpty(resp.GetFieldsData())
	return resp
}

func (s *StructSubFieldSuite) child(ctx context.Context, name string, pk int64, childName string) *schemapb.FieldData {
	resp := s.query(ctx, name, pk, structName)
	for _, field := range resp.GetFieldsData() {
		if field.GetFieldName() != structName {
			continue
		}
		for _, child := range field.GetStructArrays().GetFields() {
			if child.GetFieldName() == childName || child.GetFieldName() == structName+"["+childName+"]" {
				return child
			}
		}
	}
	s.Require().FailNow("struct child missing", childName)
	return nil
}

func (s *StructSubFieldSuite) assertBackfilled(ctx context.Context, name string, pk int64, childName string, count int, rowValid bool, implicitRowValidity ...bool) {
	child := s.child(ctx, name, pk, childName)
	checkRowValidity := func(actual []bool) {
		if len(implicitRowValidity) > 0 && implicitRowValidity[0] && rowValid && len(actual) == 0 {
			return // A non-nullable parent can omit its all-true row bitmap.
		}
		s.Require().Equal([]bool{rowValid}, actual)
	}
	checkRowValidity(child.GetValidData())
	if child.GetScalars() != nil {
		checkRowValidity(child.GetScalars().GetValidData())
		if rowValid {
			rows := child.GetScalars().GetArrayData().GetData()
			s.Require().Len(rows, 1)
			if count == 0 {
				s.Require().Empty(rows[0].GetValidData())
			} else {
				s.Require().Equal(make([]bool, count), rows[0].GetValidData())
			}
		} else {
			rows := child.GetScalars().GetArrayData().GetData()
			s.Require().Len(rows, 1)
			s.Require().Empty(rows[0].GetValidData())
			s.Require().Empty(rows[0].GetLongData().GetData())
			s.Require().Empty(rows[0].GetStringData().GetData())
			s.Require().Empty(rows[0].GetArrayData().GetData())
		}
	} else {
		checkRowValidity(child.GetVectors().GetValidData())
		if rowValid {
			rows := child.GetVectors().GetVectorArray().GetData()
			s.Require().Len(rows, 1)
			if count == 0 {
				s.Require().Empty(rows[0].GetValidData())
			} else {
				s.Require().Equal(make([]bool, count), rows[0].GetValidData())
			}
		} else {
			rows := child.GetVectors().GetVectorArray().GetData()
			s.Require().Len(rows, 1)
			s.Require().Empty(rows[0].GetValidData())
			s.Require().Empty(rows[0].GetFloatVector().GetData())
		}
	}
}
