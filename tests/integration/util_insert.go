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
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
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

func NewInt64FieldDataNullableWithStart(fieldName string, numRows, start int) *schemapb.FieldData {
	validData, num := GenerateBoolArray(numRows)
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: GenerateInt64Array(num, int64(start)),
					},
				},
			},
		},
		ValidData: validData,
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

func NewVarCharFieldData(fieldName string, numRows int, nullable bool) *schemapb.FieldData {
	numValid := numRows
	if nullable {
		numValid = numRows / 2
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_String,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: testutils.GenerateStringArray(numValid),
						// Data: testutils.GenerateStringArray(numRows),
					},
				},
			},
		},
		ValidData: testutils.GenerateBoolArray(numRows),
	}
}

func NewStringFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return testutils.NewStringFieldData(fieldName, numRows)
}

func NewFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewFloatVectorFieldData(fieldName, numRows, dim)
}

func NewFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewFloat16VectorFieldData(fieldName, numRows, dim)
}

func NewBFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewBFloat16VectorFieldData(fieldName, numRows, dim)
}

func NewBinaryVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewBinaryVectorFieldData(fieldName, numRows, dim)
}

func NewSparseFloatVectorFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return testutils.NewSparseFloatVectorFieldData(fieldName, numRows)
}

func NewInt8VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewInt8VectorFieldData(fieldName, numRows, dim)
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

func GenerateBoolArray(numRows int) ([]bool, int) {
	var num int
	ret := make([]bool, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = i%2 == 0
		if ret[i] {
			num++
		}
	}
	return ret, num
}

func GenerateSameStringArray(numRows int, value string) []string {
	ret := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = value
	}
	return ret
}

func GenerateSparseFloatArray(numRows int) *schemapb.SparseFloatArray {
	return testutils.GenerateSparseFloatVectors(numRows)
}

func GenerateHashKeys(numRows int) []uint32 {
	return testutils.GenerateHashKeys(numRows)
}
