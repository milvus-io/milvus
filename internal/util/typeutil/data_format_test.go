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

package typeutil

import (
	"encoding/binary"
	"testing"

	"github.com/milvus-io/milvus/internal/common"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/schemapb"
)

func TestTransferColumnBasedDataToRowBasedData(t *testing.T) {
	fieldSchema := []*schemapb.FieldSchema{
		{
			FieldID:  100,
			Name:     "bool_field",
			DataType: schemapb.DataType_Bool,
		},
		{
			FieldID:  101,
			Name:     "int8_field",
			DataType: schemapb.DataType_Int8,
		},
		{
			FieldID:  102,
			Name:     "int16_field",
			DataType: schemapb.DataType_Int16,
		},
		{
			FieldID:  103,
			Name:     "int32_field",
			DataType: schemapb.DataType_Int32,
		},
		{
			FieldID:  104,
			Name:     "int64_field",
			DataType: schemapb.DataType_Int64,
		},
		{
			FieldID:  105,
			Name:     "float32_field",
			DataType: schemapb.DataType_Float,
		},
		{
			FieldID:  106,
			Name:     "float64_field",
			DataType: schemapb.DataType_Double,
		},
		{
			FieldID:  107,
			Name:     "float_vector_field",
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "1",
				},
			},
		},
		{
			FieldID:  108,
			Name:     "binary_vector_field",
			DataType: schemapb.DataType_BinaryVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "8",
				},
			},
		},
	}

	columns := []*schemapb.FieldData{
		{
			FieldId: 100,
			Type:    schemapb.DataType_Bool,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: []bool{true, false, true},
						},
					},
				},
			},
		},
		{
			FieldId: 101,
			Type:    schemapb.DataType_Int8,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{0, 0xf, 0x1f},
						},
					},
				},
			},
		},
		{
			FieldId: 102,
			Type:    schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{0, 0xff, 0x1fff},
						},
					},
				},
			},
		},
		{
			FieldId: 103,
			Type:    schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{0, 0xffff, 0x1fffffff},
						},
					},
				},
			},
		},
		{
			FieldId: 104,
			Type:    schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{0, 0xffffffff, 0x1fffffffffffffff},
						},
					},
				},
			},
		},
		{
			FieldId: 105,
			Type:    schemapb.DataType_Float,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: []float32{0, 0, 0},
						},
					},
				},
			},
		},
		{
			FieldId: 106,
			Type:    schemapb.DataType_Double,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{0, 0, 0},
						},
					},
				},
			},
		},
		{
			FieldId: 107,
			Type:    schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 1,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: []float32{0, 0, 0},
						},
					},
				},
			},
		},
		{
			FieldId: 108,
			Type:    schemapb.DataType_BinaryVector,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 8,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: []byte{1, 2, 3},
					},
				},
			},
		},
	}
	rows, err := TransferColumnBasedDataToRowBasedData(&schemapb.CollectionSchema{Fields: fieldSchema}, columns)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(rows))
	if common.Endian == binary.LittleEndian {
		// low byte in high address

		assert.ElementsMatch(t,
			[]byte{
				1,    // true
				0,    // 0
				0, 0, // 0
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				// b + 1, // "1"
				1,          // 1
				0, 0, 0, 0, // 0
			},
			rows[0].Value)
		assert.ElementsMatch(t,
			[]byte{
				0,       // false
				0xf,     // 0xf
				0, 0xff, // 0xff
				0, 0, 0xff, 0xff, // 0xffff
				0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // 0xffffffff
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				// b + 2, // "2"
				2,          // 2
				0, 0, 0, 0, // 0
			},
			rows[1].Value)
		assert.ElementsMatch(t,
			[]byte{
				1,          // false
				0x1f,       // 0x1f
				0xff, 0x1f, // 0x1fff
				0xff, 0xff, 0xff, 0x1f, // 0x1fffffff
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1f, // 0x1fffffffffffffff
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				// b + 3, // "3"
				3,          // 3
				0, 0, 0, 0, // 0
			},
			rows[2].Value)
	}
}
