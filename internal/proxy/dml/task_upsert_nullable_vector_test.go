// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dml

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Each valid row has a distinct value so reordering and compact offsets are checked.
func nullableUpsertVector(dataType schemapb.DataType, values []int64, valid []bool) *schemapb.FieldData {
	vector := &schemapb.VectorField{Dim: 8, ValidData: valid}
	floats := make([]float32, 0, len(values)*8)
	bytes := make([]byte, 0, len(values)*8)
	binary := make([]byte, 0, len(values))
	sparse := make([][]byte, 0, len(values))
	for i, value := range values {
		if len(valid) > 0 && !valid[i] {
			continue
		}
		for range 8 {
			floats = append(floats, float32(value))
			bytes = append(bytes, byte(value))
		}
		binary = append(binary, byte(value))
		sparse = append(sparse, typeutil.CreateSparseFloatRow([]uint32{7}, []float32{float32(value)}))
	}
	switch dataType {
	case schemapb.DataType_FloatVector:
		vector.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: floats}}
	case schemapb.DataType_BinaryVector:
		vector.Data = &schemapb.VectorField_BinaryVector{BinaryVector: binary}
	case schemapb.DataType_Float16Vector:
		vector.Data = &schemapb.VectorField_Float16Vector{Float16Vector: typeutil.Float32ArrayToFloat16Bytes(floats)}
	case schemapb.DataType_BFloat16Vector:
		vector.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: typeutil.Float32ArrayToBFloat16Bytes(floats)}
	case schemapb.DataType_Int8Vector:
		vector.Data = &schemapb.VectorField_Int8Vector{Int8Vector: bytes}
	case schemapb.DataType_SparseFloatVector:
		vector.Dim = 0
		vector.Data = &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: sparse}}
	}
	return &schemapb.FieldData{FieldName: "vector", FieldId: 101, Type: dataType, Field: &schemapb.FieldData_Vectors{Vectors: vector}}
}

func TestUpsertPreExecuteNullableVectorValidity(t *testing.T) {
	for _, dataType := range []schemapb.DataType{
		schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector,
		schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector, schemapb.DataType_SparseFloatVector,
	} {
		for _, existing := range [][]int64{{30, 20, 10}, {30, 10}, {}} {
			for _, bitmap := range []struct {
				name    string
				valid   []bool
				legacy  bool
				invalid bool
			}{
				{name: "omitted"},
				{name: "all_valid", valid: []bool{true, true, true}},
				{name: "existing_null", valid: []bool{false, true, true}},
				{name: "mixed_nulls", valid: []bool{true, false, false}},
				{name: "all_null", valid: []bool{false, false, false}},
				{name: "legacy_nulls", valid: []bool{false, true, true}, legacy: true},
				{name: "missing_vector_row", invalid: true},
				{name: "short_bitmap", invalid: true},
				{name: "conflicting_bitmaps", invalid: true},
				{name: "num_rows_mismatch", invalid: true},
			} {
				for _, implicit := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/existing=%d/%s/implicit=%t", dataType, len(existing), bitmap.name, implicit), func(t *testing.T) {
						task := createTestUpdateTask()
						schema := &schemapb.CollectionSchema{
							Name: task.req.CollectionName,
							Fields: []*schemapb.FieldSchema{
								{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
								{FieldID: 101, Name: "vector", DataType: dataType, Nullable: true,
									TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
								{FieldID: 102, Name: "array", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
									TypeParams: []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "8"}}},
							},
						}
						task.schema = mustNewSchemaInfo(schema)
						arrayField := func(ids []int64) *schemapb.FieldData {
							rows := make([]*schemapb.ScalarField, len(ids))
							for i, id := range ids {
								rows[i] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{id}}}}
							}
							return &schemapb.FieldData{FieldName: "array", FieldId: 102, Type: schemapb.DataType_Array,
								Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
									ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64, Data: rows},
								}}}}
						}
						ids := []int64{10, 20, 30}
						requestVector := nullableUpsertVector(dataType, ids, bitmap.valid)
						if bitmap.legacy {
							requestVector.ValidData = bitmap.valid
							requestVector.GetVectors().ValidData = nil
						}
						switch bitmap.name {
						case "missing_vector_row":
							requestVector = nullableUpsertVector(dataType, ids[:2], nil)
						case "short_bitmap":
							requestVector.GetVectors().ValidData = []bool{true, true}
						case "conflicting_bitmaps":
							requestVector.ValidData = []bool{true, true, true}
							requestVector.GetVectors().ValidData = []bool{false, true, true}
						case "num_rows_mismatch":
							task.req.NumRows = 2
						}
						task.req.FieldsData = []*schemapb.FieldData{partialUpdateCASPKFieldData(ids), requestVector, arrayField(ids)}
						task.req.PartialUpdate = !implicit
						if implicit {
							task.req.FieldOps = []*schemapb.FieldPartialUpdateOp{{FieldName: "array", Op: schemapb.FieldPartialUpdateOp_ARRAY_APPEND}}
						}
						existingValid := make([]bool, len(existing))
						existingValues := make([]int64, len(existing))
						for i, id := range existing {
							existingValid[i] = i%2 == 0
							existingValues[i] = id + 50
						}
						queryResult := &milvuspb.QueryResults{Status: merr.Success(), FieldsData: []*schemapb.FieldData{
							partialUpdateCASPKFieldData(existing), nullableUpsertVector(dataType, existingValues, existingValid), arrayField(existingValues),
						}}
						setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels)
						oldWAL := streaming.WAL()
						streaming.SetWALForTest(newPartialUpdateCASTestWAL(t, 9))
						t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })
						patch := func(target any, results ...any) {
							m := mockey.Mock(target).Return(results...).Build()
							t.Cleanup(func() { m.UnPatch() })
						}
						patch((*metacache.MetaCache).GetCollectionID, int64(1001), nil)
						patch((*metacache.MetaCache).GetCollectionInfo, &collectionInfo{Schema: task.schema}, nil)
						patch((*metacache.MetaCache).GetPartitionID, int64(1002), nil)
						allocation := mockey.Mock(common.AllocAutoID).To(func(_ func(uint32) (int64, int64, error), rowNum uint32, _ uint64) (int64, int64, error) {
							return 1000, 1000 + int64(rowNum), nil
						}).Build()
						t.Cleanup(func() { allocation.UnPatch() })
						patch(retrieveByPKs, queryResult, segcore.StorageCost{}, nil)

						// Exercise the input boundary, partial merge, and final insert validation.
						err := task.PreExecute(context.Background())
						if bitmap.invalid {
							require.ErrorIs(t, err, merr.ErrParameterInvalid)
							return
						}
						require.NoError(t, err)
						require.True(t, task.req.GetPartialUpdate())
						var expectedIDs, expectedDeletes []int64
						var expectedValid []bool
						for _, wantExisting := range []bool{true, false} {
							for i, id := range ids {
								found := false
								for _, oldID := range existing {
									found = found || oldID == id
								}
								if found != wantExisting {
									continue
								}
								expectedIDs = append(expectedIDs, id)
								if found {
									expectedDeletes = append(expectedDeletes, id)
								}
								expectedValid = append(expectedValid, len(bitmap.valid) == 0 || bitmap.valid[i])
							}
						}
						require.Equal(t, expectedIDs, task.result.GetIDs().GetIntId().GetData())
						require.Equal(t, expectedDeletes, task.deletePKs.GetIntId().GetData())
						var merged *schemapb.FieldData
						for _, field := range task.upsertMsg.InsertMsg.GetFieldsData() {
							if field.GetFieldName() == "vector" {
								merged = field
							}
						}
						require.NotNil(t, merged)
						expected := nullableUpsertVector(dataType, expectedIDs, expectedValid)
						require.True(t, proto.Equal(expected.GetVectors(), merged.GetVectors()), "expected %v, got %v", expected.GetVectors(), merged.GetVectors())
						require.Empty(t, merged.GetValidData())
						require.EqualValues(t, 3, task.result.InsertCnt)
						require.EqualValues(t, len(existing), task.result.DeleteCnt)
					})
				}
			}
		}
	}
}
