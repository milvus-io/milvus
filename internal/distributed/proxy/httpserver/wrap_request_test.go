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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	t.Run("text_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Text,
			Field: []byte(`["a", "b", "c"]`),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		result, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
		assert.Equal(t, schemapb.DataType_Text, result.GetType())
		assert.Equal(t, []string{"a", "b", "c"}, result.GetScalars().GetStringData().GetData())
		assert.Empty(t, typeutil.GetFieldDataValidData(result))
	})
	t.Run("text_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Text,
			Field: []byte("[1, 2, 3]"),
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("text_nullable", func(t *testing.T) {
		fieldData := FieldData{
			Type:      schemapb.DataType_Text,
			FieldName: "nullable_text",
			Field:     []byte(`["a", null, ""]`),
		}
		raw, err := json.Marshal(fieldData)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &fieldData))

		result, err := fieldData.AsSchemapb()
		require.NoError(t, err)
		assert.Equal(t, schemapb.DataType_Text, result.GetType())
		assert.Equal(t, []string{"a", ""}, result.GetScalars().GetStringData().GetData())
		assert.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(result))
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
	ret := vector2Bytes([]FloatVectorQuery{{1.1, 1.2}})
	assert.NotEmpty(t, ret)
}

func Test_binaryVector2Bytes(t *testing.T) {
	ret := binaryVector2Bytes([]Base64VectorQuery{
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

// The low-level /api/v1 API decoded straight into []bool, []float32 and
// friends, so a null element silently became the zero value -- false, 0, an
// empty string -- and was stored, searched or measured as a value the caller
// never sent.
func TestLowLevelAPIRejectsNull(t *testing.T) {
	t.Run("insert field payloads", func(t *testing.T) {
		for name, tt := range map[string]struct {
			dtype schemapb.DataType
			field string
		}{
			"bool null":          {schemapb.DataType_Bool, `[null]`},
			"varchar null":       {schemapb.DataType_VarChar, `[null]`},
			"int32 null":         {schemapb.DataType_Int32, `[null]`},
			"int64 null":         {schemapb.DataType_Int64, `[null]`},
			"float null":         {schemapb.DataType_Float, `[null]`},
			"double null":        {schemapb.DataType_Double, `[null]`},
			"float vector null":  {schemapb.DataType_FloatVector, `[[0.1, null]]`},
			"fp16 vector null":   {schemapb.DataType_Float16Vector, `[[0.1, null]]`},
			"bf16 vector null":   {schemapb.DataType_BFloat16Vector, `[[0.1, null]]`},
			"int8 vector null":   {schemapb.DataType_Int8Vector, `[[1, null]]`},
			"sparse value null":  {schemapb.DataType_SparseFloatVector, `[{"1": null}]`},
			"whole payload null": {schemapb.DataType_Float, `null`},
		} {
			t.Run(name, func(t *testing.T) {
				fieldData := FieldData{Type: tt.dtype, FieldName: "f", Field: []byte(tt.field)}
				_, err := fieldData.AsSchemapb()
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "null")
			})
		}

		// the same payloads without the null still convert
		for name, tt := range map[string]struct {
			dtype schemapb.DataType
			field string
		}{
			"bool":         {schemapb.DataType_Bool, `[true]`},
			"float vector": {schemapb.DataType_FloatVector, `[[0.1, 0.2]]`},
		} {
			t.Run(name+" without null", func(t *testing.T) {
				fieldData := FieldData{Type: tt.dtype, FieldName: "f", Field: []byte(tt.field)}
				_, err := fieldData.AsSchemapb()
				assert.NoError(t, err)
			})
		}
	})

	// A scalar column can be nullable, so refusing the null there is an
	// improvement on storing a zero rather than the only possible answer:
	// compatibilityMode brings the zero back. A vector has no per-element
	// validity to restore, so it stays refused either way.
	t.Run("compatibility mode restores the zero for scalars only", func(t *testing.T) {
		paramtable.Init()
		key := paramtable.Get().HTTPCfg.CompatibilityMode.Key
		paramtable.Get().Save(key, "true")
		defer paramtable.Get().Reset(key)

		// bool and float are the ones the decoder silently zeroed; it already
		// refused a null for an integer or a string element
		boolean := FieldData{Type: schemapb.DataType_Bool, FieldName: "f", Field: []byte(`[true, null]`)}
		converted, err := boolean.AsSchemapb()
		require.NoError(t, err)
		assert.Equal(t, []bool{true, false}, converted.GetScalars().GetBoolData().GetData())

		double := FieldData{Type: schemapb.DataType_Double, FieldName: "d", Field: []byte(`[1.5, null]`)}
		converted, err = double.AsSchemapb()
		require.NoError(t, err)
		assert.Equal(t, []float64{1.5, 0}, converted.GetScalars().GetDoubleData().GetData())

		vector := FieldData{Type: schemapb.DataType_FloatVector, FieldName: "v", Field: []byte(`[[0.1, null]]`)}
		_, err = vector.AsSchemapb()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "null")
	})

	t.Run("search vectors", func(t *testing.T) {
		var req SearchRequest
		err := json.Unmarshal([]byte(`{"vectors": [[0.1, null]]}`), &req)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "null")

		assert.NoError(t, json.Unmarshal([]byte(`{"vectors": [[0.1, 0.2]]}`), &req))
	})

	// A whole-value null used to decode to a non-nil empty slice, which slipped
	// past the callers' required-field checks: {"vector": null} passed the v1
	// Vector == nil test with a vector that held nothing.
	t.Run("whole-value null is refused, not emptied", func(t *testing.T) {
		var vec FloatVectorQuery
		err := json.Unmarshal([]byte(`null`), &vec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "null")

		var ids Int64ListQuery
		err = json.Unmarshal([]byte(`null`), &ids)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "null")

		// an empty list is a different thing and still decodes
		require.NoError(t, json.Unmarshal([]byte(`[]`), &vec))
		require.NotNil(t, vec)
		require.NoError(t, json.Unmarshal([]byte(`[]`), &ids))
		require.NotNil(t, ids)
	})

	t.Run("distance ids", func(t *testing.T) {
		var v VectorsArray
		err := json.Unmarshal([]byte(`{"ids": {"collection_name": "c", "field_name": "f", "id_array": [1, null, 3]}}`), &v)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "null")

		require.NoError(t, json.Unmarshal(
			[]byte(`{"ids": {"collection_name": "c", "field_name": "f", "id_array": [1, 3]}}`), &v))
	})

	t.Run("binary search vectors", func(t *testing.T) {
		var req SearchRequest
		err := json.Unmarshal([]byte(`{"binary_vectors": [null]}`), &req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "null")

		require.NoError(t, json.Unmarshal([]byte(`{"binary_vectors": ["AQI="]}`), &req))
	})

	t.Run("distance vectors", func(t *testing.T) {
		var v VectorsArray
		err := json.Unmarshal([]byte(`{"dim": 2, "vectors": [0.1, null]}`), &v)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "null")

		assert.NoError(t, json.Unmarshal([]byte(`{"dim": 2, "vectors": [0.1, 0.2]}`), &v))
	})
}
