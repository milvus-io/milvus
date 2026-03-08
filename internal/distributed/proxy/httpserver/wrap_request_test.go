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

package httpserver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
)

func TestFieldData_AsSchemapb(t *testing.T) {
	t.Run("varchar_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_VarChar,
			Field: []byte(`["a", "b", "c"]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("varchar_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_VarChar,
			Field: []byte("[1, 2, 3]"),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("bool_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Bool,
			Field: []byte("[true, true, false]"),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("bool_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Bool,
			Field: []byte("[1, 2, 3]"),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})

	t.Run("int8_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int8,
			Field: []byte("[1, 2, 3]"),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("int8_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int8,
			Field: []byte(`["a", "b", "c"]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("int32_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int32,
			Field: []byte("[1, 2, 3]"),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("int32_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int32,
			Field: []byte(`["a", "b", "c"]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("int64_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int64,
			Field: []byte("[1, 2, 3]"),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("int64_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int64,
			Field: []byte(`["a", "b", "c"]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("float_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Float,
			Field: []byte(`[1.1, 2.1, 3.1]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("float_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Float,
			Field: []byte(`["a", "b", "c"]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("double_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Double,
			Field: []byte(`[1.1, 2.1, 3.1]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("double_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Double,
			Field: []byte(`["a", "b", "c"]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("string_not_support", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_String,
			Field: []byte(`["a", "b", "c"]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})

	// vectors
	testcases := []struct {
		name     string
		dataType schemapb.DataType
	}{
		{
			"float", schemapb.DataType_FloatVector,
		},
		{
			"float16", schemapb.DataType_Float16Vector,
		},
		{
			"bfloat16", schemapb.DataType_BFloat16Vector,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name+"vector_ok", func(t *testing.T) {
			fieldData := FieldData{
				Type: tc.dataType,
				Field: []byte(`[
					[1.1, 2.2, 3.1],
					[1.1, 2.2, 3.1],
					[1.1, 2.2, 3.1]
				]`),
			}
			raw, _ := json.Marshal(fieldData)
			json.Unmarshal(raw, &fieldData)
			_, err := fieldData.AsSchemapb()
			assert.NoError(t, err)
		})
		t.Run(tc.name+"vector_empty_error", func(t *testing.T) {
			fieldData := FieldData{
				Type:  tc.dataType,
				Field: []byte(""),
			}
			raw, _ := json.Marshal(fieldData)
			json.Unmarshal(raw, &fieldData)
			_, err := fieldData.AsSchemapb()
			assert.Error(t, err)
		})
		t.Run(tc.name+"vector_dim=0_error", func(t *testing.T) {
			fieldData := FieldData{
				Type:  tc.dataType,
				Field: []byte(`[]`),
			}
			raw, _ := json.Marshal(fieldData)
			json.Unmarshal(raw, &fieldData)
			_, err := fieldData.AsSchemapb()
			assert.Error(t, err)
		})
		t.Run(tc.name+"vector_vectorTypeError_error", func(t *testing.T) {
			fieldData := FieldData{
				Type:  tc.dataType,
				Field: []byte(`["1"]`),
			}
			raw, _ := json.Marshal(fieldData)
			json.Unmarshal(raw, &fieldData)
			_, err := fieldData.AsSchemapb()
			assert.Error(t, err)
		})
		t.Run(tc.name+"vector_error", func(t *testing.T) {
			fieldData := FieldData{
				Type:  tc.dataType,
				Field: []byte(`["a", "b", "c"]`),
			}
			raw, _ := json.Marshal(fieldData)
			json.Unmarshal(raw, &fieldData)
			_, err := fieldData.AsSchemapb()
			assert.Error(t, err)
		})
	}

	t.Run("sparsefloatvector_ok_1", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_SparseFloatVector,
			Field: []byte(`[
				{"1": 0.1, "2": 0.2},
				{"3": 0.1, "5": 0.2},
				{"4": 0.1, "6": 0.2}
			]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})

	t.Run("sparsefloatvector_ok_2", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_SparseFloatVector,
			Field: []byte(`[
				{"indices": [1, 2], "values": [0.1, 0.2]},
				{"indices": [3, 5], "values": [0.1, 0.2]},
				{"indices": [4, 6], "values": [0.1, 0.2]}
			]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})

	t.Run("sparsefloatvector_ok_3", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_SparseFloatVector,
			Field: []byte(`[
				{"indices": [1, 2], "values": [0.1, 0.2]},
				{"3": 0.1, "5": 0.2},
				{"indices": [4, 6], "values": [0.1, 0.2]}
			]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})

	t.Run("sparsefloatvector_empty_err", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_SparseFloatVector,
			Field: []byte(`[]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})

	t.Run("sparsefloatvector_invalid_json_err", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_SparseFloatVector,
			Field: []byte(`[
				{"3": 0.1, : 0.2}
			]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})

	t.Run("sparsefloatvector_invalid_row_1_err", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_SparseFloatVector,
			Field: []byte(`[
				{"indices": [1, 2], "values": [-0.1, 0.2]},
			]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})

	t.Run("sparsefloatvector_invalid_row_2_err", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_SparseFloatVector,
			Field: []byte(`[
				{"indices": [1, -2], "values": [0.1, 0.2]},
			]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})

	t.Run("int8vector_ok_1", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_Int8Vector,
			Field: []byte(`[
				[1, 2, 3, 4],
				[-11, -52, 37, 121],
				[-128, -35, 31, 127]
			]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("int8vector_ok_1", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_Int8Vector,
			Field: []byte(`[
				[-200, 141]
			]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("int8vector_empty_err", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int8Vector,
			Field: []byte(""),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("int8vector_dim0_err", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int8Vector,
			Field: []byte(`[]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("int8vector_datatype_err", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int8Vector,
			Field: []byte(`['a', 'b', 'c']`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
}

func Test_vector2Bytes(t *testing.T) {
	ret := vector2Bytes([][]float32{{1.1, 1.2}})
	assert.NotEmpty(t, ret)
}

func Test_binaryVector2Bytes(t *testing.T) {
	ret := binaryVector2Bytes([][]byte{
		[]byte("somebytes"),
	})
	assert.NotEmpty(t, ret)
}

func TestVectorsArray_AsPbVectorArray(t *testing.T) {
	dim := int64(1)
	t.Run("vector_ok", func(t *testing.T) {
		vector := []float32{1, 2}
		v := VectorsArray{
			Dim:     dim,
			Vectors: vector,
		}
		ret := v.AsPbVectorArray()
		da, ok := ret.Array.(*milvuspb.VectorsArray_DataArray)
		assert.True(t, ok)
		assert.Equal(t, dim, da.DataArray.Dim)
		assert.Equal(t, vector, da.DataArray.GetFloatVector().Data)
	})
	t.Run("binary_vector_ok", func(t *testing.T) {
		bv := []byte("somebytes")
		v := VectorsArray{
			// IDs: ,
			Dim:           dim,
			BinaryVectors: bv,
		}
		ret := v.AsPbVectorArray()
		da, ok := ret.Array.(*milvuspb.VectorsArray_DataArray)
		assert.True(t, ok)
		assert.Equal(t, dim, da.DataArray.Dim)
		assert.Equal(t, bv, da.DataArray.GetBinaryVector())
	})
	t.Run("ids_ok", func(t *testing.T) {
		ids := []int64{1, 2, 3}
		cn := "collection"
		paritions := []string{"p1", "p2"}
		field := "field"
		v := VectorsArray{
			IDs: &VectorIDs{
				CollectionName: cn,
				PartitionNames: paritions,
				FieldName:      field,
				IDArray:        ids,
			},
		}
		ret := v.AsPbVectorArray()
		ia, ok := ret.Array.(*milvuspb.VectorsArray_IdArray)
		assert.True(t, ok)
		assert.Equal(t, cn, ia.IdArray.CollectionName)
		assert.Equal(t, paritions, ia.IdArray.PartitionNames)
		assert.Equal(t, field, ia.IdArray.FieldName)
		ints, ok := ia.IdArray.IdArray.IdField.(*schemapb.IDs_IntId)
		assert.True(t, ok)
		assert.Equal(t, ids, ints.IntId.Data)
	})
}
