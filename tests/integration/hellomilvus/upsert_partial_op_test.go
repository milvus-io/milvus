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

package hellomilvus

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

// Array partial-update integration tests.
//
// These exercise ARRAY_APPEND / ARRAY_REMOVE semantics end-to-end through
// the proxy, validating that:
//  - a non-REPLACE op on FieldData implicitly promotes the request to
//    partial_update=true without the caller setting it explicitly,
//  - the merged array matches the expected element order / multiplicity,
//  - oversized APPEND payloads are rejected via max_capacity check.
//
// The base collection schema: (id int64 PK, vector float-vec dim=4,
// tags Array<Int64> max_capacity=16). "vector" is required because Milvus
// requires at least one vector field per collection.

const (
	partialOpDim           = 4
	partialOpArrayCapacity = 16
	partialOpTagsField     = "tags"
)

func (s *HelloMilvusSuite) TestArrayPartialOp_Append() {
	s.runArrayPartialOpScenario("append", schemapb.FieldPartialUpdateOp_ARRAY_APPEND,
		// seed row 1 → [1,2]; append [3,4] → [1,2,3,4]
		[][]int64{{1, 2}, {10, 20}},
		[][]int64{{3, 4}, {30}},
		[][]int64{{1, 2, 3, 4}, {10, 20, 30}},
		false,
	)
}

func (s *HelloMilvusSuite) TestArrayPartialOp_Remove() {
	s.runArrayPartialOpScenario("remove", schemapb.FieldPartialUpdateOp_ARRAY_REMOVE,
		[][]int64{{1, 2, 3, 2, 1}, {10, 20, 30}},
		// request payload = values to delete
		[][]int64{{2}, {40}}, // 40 not in base → no-op for row 1
		[][]int64{{1, 3, 1}, {10, 20, 30}},
		false,
	)
}

func (s *HelloMilvusSuite) TestArrayPartialOp_AppendExceedsCapacity() {
	// base len 15 + append len 5 > max_capacity 16 → must be rejected.
	payload := make([]int64, 5)
	for i := range payload {
		payload[i] = int64(100 + i)
	}
	base := make([]int64, 15)
	for i := range base {
		base[i] = int64(i)
	}
	s.runArrayPartialOpScenario("append-overflow",
		schemapb.FieldPartialUpdateOp_ARRAY_APPEND,
		[][]int64{base},
		[][]int64{payload},
		nil,
		true, // expectError
	)
}

// runArrayPartialOpScenario drives a full insert → op-upsert → query flow and
// asserts the per-row array state. When expectError is true, the op-upsert is
// expected to fail; in that case the expected-result slice is ignored.
func (s *HelloMilvusSuite) runArrayPartialOpScenario(
	label string,
	op schemapb.FieldPartialUpdateOp_OpType,
	seed [][]int64,
	opPayload [][]int64,
	expected [][]int64,
	expectError bool,
) {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := fmt.Sprintf("TestArrayPartialOp_%s_", label)
	collectionName := prefix + funcutil.GenRandomStr()

	schema := s.buildArrayPartialOpSchema(collectionName)
	marshaled, err := proto.Marshal(schema)
	s.NoError(err)

	createStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaled,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.True(merr.Ok(createStatus))

	pkCol, vectorCol, tagsCol := partialOpInitialColumns(seed)

	insertResp, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkCol, vectorCol, tagsCol},
		HashKeys:       integration.GenerateHashKeys(len(seed)),
		NumRows:        uint32(len(seed)),
	})
	s.NoError(err)
	s.True(merr.Ok(insertResp.GetStatus()))

	// Build upsert payload. The op is attached at the UpsertRequest level
	// (not on FieldData) to keep FieldData a pure data carrier.
	opPkCol := partialOpPKColumn(len(opPayload))
	opTagsCol := partialOpArrayColumn(partialOpTagsField, opPayload)

	upsertReq := &milvuspb.UpsertRequest{
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{opPkCol, opTagsCol},
		HashKeys:       integration.GenerateHashKeys(len(opPayload)),
		NumRows:        uint32(len(opPayload)),
		// partial_update intentionally left false — server should auto-promote.
	}
	if op != schemapb.FieldPartialUpdateOp_REPLACE {
		upsertReq.FieldOps = []*schemapb.FieldPartialUpdateOp{
			{FieldName: partialOpTagsField, Op: op},
		}
	}
	upsertResp, err := c.MilvusClient.Upsert(ctx, upsertReq)
	s.NoError(err)
	if expectError {
		s.False(merr.Ok(upsertResp.GetStatus()),
			"upsert should fail when ARRAY_APPEND payload would exceed max_capacity")
		return
	}
	s.True(merr.Ok(upsertResp.GetStatus()))

	// Flush + load so the merged state becomes queryable.
	_, err = c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{collectionName}})
	s.NoError(err)

	_, err = c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: collectionName})
	s.NoError(err)
	s.WaitForLoad(ctx, collectionName)

	queryResp, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: collectionName,
		Expr:           "id >= 0",
		OutputFields:   []string{"id", partialOpTagsField},
	})
	s.NoError(err)
	s.True(merr.Ok(queryResp.GetStatus()))

	var ids []int64
	for _, fd := range queryResp.GetFieldsData() {
		if fd.GetFieldName() == integration.Int64Field {
			ids = fd.GetScalars().GetLongData().GetData()
		}
	}
	gotTags := make(map[int64][]int64, len(ids))
	for _, fd := range queryResp.GetFieldsData() {
		if fd.GetFieldName() != partialOpTagsField {
			continue
		}
		rows := fd.GetScalars().GetArrayData().GetData()
		s.Require().Len(rows, len(ids))
		for i, row := range rows {
			gotTags[ids[i]] = row.GetLongData().GetData()
		}
	}

	for pk, want := range partialOpExpectedByPK(expected) {
		s.ElementsMatchf(want, gotTags[pk], "row %d", pk)
	}
}

func (s *HelloMilvusSuite) buildArrayPartialOpSchema(name string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        name,
		Description: "array partial op integration test",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         integration.Int64Field,
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     integration.FloatVecField,
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: fmt.Sprintf("%d", partialOpDim)},
				},
			},
			{
				FieldID:     102,
				Name:        partialOpTagsField,
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxCapacityKey, Value: fmt.Sprintf("%d", partialOpArrayCapacity)},
				},
			},
		},
	}
}

func partialOpPKColumn(numRows int) *schemapb.FieldData {
	pks := make([]int64, numRows)
	for i := range pks {
		pks[i] = int64(i)
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: integration.Int64Field,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
		}},
	}
}

func partialOpVectorColumn(numRows, dim int) *schemapb.FieldData {
	data := make([]float32, numRows*dim)
	for i := range data {
		data[i] = float32(i%7) / 10.0
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: integration.FloatVecField,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}},
		}},
	}
}

func partialOpArrayColumn(name string, rows [][]int64) *schemapb.FieldData {
	rowFields := make([]*schemapb.ScalarField, len(rows))
	for i, row := range rows {
		rowFields[i] = &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: row}},
		}
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				Data:        rowFields,
				ElementType: schemapb.DataType_Int64,
			}},
		}},
	}
}

func partialOpInitialColumns(seed [][]int64) (pk, vec, tags *schemapb.FieldData) {
	return partialOpPKColumn(len(seed)),
		partialOpVectorColumn(len(seed), partialOpDim),
		partialOpArrayColumn(partialOpTagsField, seed)
}

func partialOpExpectedByPK(rows [][]int64) map[int64][]int64 {
	out := make(map[int64][]int64, len(rows))
	for i, row := range rows {
		out[int64(i)] = row
	}
	return out
}
