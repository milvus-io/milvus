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

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/testutils"
)

func (s *MiniClusterSuite) WaitForFlush(ctx context.Context, segIDs []int64, flushTs uint64, dbName, collectionName string) {
	flushed := func() bool {
		resp, err := s.Cluster.Proxy.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
			SegmentIDs:     segIDs,
			FlushTs:        flushTs,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		if err != nil {
			return false
		}
		return resp.GetFlushed()
	}
	for !flushed() {
		select {
		case <-ctx.Done():
			s.FailNow("failed to wait for flush until ctx done")
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func NewInt64FieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: GenerateInt64Array(numRows, 0),
					},
				},
			},
		},
	}
}

func NewInt64FieldDataWithStart(fieldName string, numRows int, start int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: GenerateInt64Array(numRows, start),
					},
				},
			},
		},
	}
}

func NewInt64SameFieldData(fieldName string, numRows int, value int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: GenerateSameInt64Array(numRows, value),
					},
				},
			},
		},
	}
}

func NewVarCharSameFieldData(fieldName string, numRows int, value string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_String,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: GenerateSameStringArray(numRows, value),
					},
				},
			},
		},
	}
}

func NewStringFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: GenerateStringArray(numRows),
					},
				},
			},
		},
	}
}

func NewFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: GenerateFloatVectors(numRows, dim),
					},
				},
			},
		},
	}
}

func NewFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Float16Vector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_Float16Vector{
					Float16Vector: GenerateFloat16Vectors(numRows, dim),
				},
			},
		},
	}
}

// func NewBFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
// 	return &schemapb.FieldData{
// 		Type:      schemapb.DataType_BFloat16Vector,
// 		FieldName: fieldName,
// 		Field: &schemapb.FieldData_Vectors{
// 			Vectors: &schemapb.VectorField{
// 				Dim: int64(dim),
// 				Data: &schemapb.VectorField_Bfloat16Vector{
// 					Bfloat16Vector: GenerateBFloat16Vectors(numRows, dim),
// 				},
// 			},
// 		},
// 	}
// }

func NewBinaryVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_BinaryVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: GenerateBinaryVectors(numRows, dim),
				},
			},
		},
	}
}

func NewSparseFloatVectorFieldData(fieldName string, numRows int) *schemapb.FieldData {
	sparseVecs := GenerateSparseFloatArray(numRows)
	return &schemapb.FieldData{
		Type:      schemapb.DataType_SparseFloatVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: sparseVecs.Dim,
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: sparseVecs,
				},
			},
		},
	}
}

func GenerateInt64Array(numRows int, start int64) []int64 {
	ret := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = int64(i) + start
	}
	return ret
}

func GenerateSameInt64Array(numRows int, value int64) []int64 {
	ret := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = value
	}
	return ret
}

func GenerateSameStringArray(numRows int, value string) []string {
	ret := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = value
	}
	return ret
}

func GenerateStringArray(numRows int) []string {
	ret := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = fmt.Sprintf("%d", i)
	}
	return ret
}

func GenerateFloatVectors(numRows, dim int) []float32 {
	total := numRows * dim
	ret := make([]float32, 0, total)
	for i := 0; i < total; i++ {
		ret = append(ret, rand.Float32())
	}
	return ret
}

func GenerateBinaryVectors(numRows, dim int) []byte {
	total := (numRows * dim) / 8
	ret := make([]byte, total)
	_, err := rand.Read(ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func GenerateFloat16Vectors(numRows, dim int) []byte {
	total := numRows * dim * 2
	ret := make([]byte, total)
	_, err := rand.Read(ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func GenerateSparseFloatArray(numRows int) *schemapb.SparseFloatArray {
	return testutils.GenerateSparseFloatVectors(numRows)
}

// func GenerateBFloat16Vectors(numRows, dim int) []byte {
// 	total := numRows * dim * 2
// 	ret := make([]byte, total)
// 	_, err := rand.Read(ret)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return ret
// }

func GenerateHashKeys(numRows int) []uint32 {
	ret := make([]uint32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Uint32())
	}
	return ret
}
