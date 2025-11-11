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
	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestAssembleStructArrayField(t *testing.T) {
	t.Run("nil input data", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[field1]", ElementType: schemapb.DataType_Int32},
			},
		}
		result := gjson.Parse("null")
		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nil input json data")
	})

	t.Run("empty array input data", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[field1]", ElementType: schemapb.DataType_Int32},
			},
		}
		result := gjson.Parse("[]")
		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty array input json data")
	})

	t.Run("valid scalar types", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[bool_field]", ElementType: schemapb.DataType_Bool},
				{Name: "test_struct[int8_field]", ElementType: schemapb.DataType_Int8},
				{Name: "test_struct[int16_field]", ElementType: schemapb.DataType_Int16},
				{Name: "test_struct[int32_field]", ElementType: schemapb.DataType_Int32},
				{Name: "test_struct[int64_field]", ElementType: schemapb.DataType_Int64},
				{Name: "test_struct[float_field]", ElementType: schemapb.DataType_Float},
				{Name: "test_struct[double_field]", ElementType: schemapb.DataType_Double},
				{Name: "test_struct[varchar_field]", ElementType: schemapb.DataType_VarChar},
			},
		}
		jsonData := `[
			{"bool_field": true, "int8_field": 1, "int16_field": 100, "int32_field": 1000, "int64_field": 10000, "float_field": 1.5, "double_field": 2.5, "varchar_field": "hello"},
			{"bool_field": false, "int8_field": 2, "int16_field": 200, "int32_field": 2000, "int64_field": 20000, "float_field": 2.5, "double_field": 3.5, "varchar_field": "world"}
		]`
		result := gjson.Parse(jsonData)

		structFieldDatas, err := assembleStructArrayField(result, structArrayField)
		assert.NoError(t, err)
		assert.NotNil(t, structFieldDatas)

		// Verify bool field
		boolData := structFieldDatas["bool_field"].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_BoolData).BoolData.Data
		assert.Equal(t, []bool{true, false}, boolData)

		// Verify int32 field
		int32Data := structFieldDatas["int32_field"].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_IntData).IntData.Data
		assert.Equal(t, []int32{1000, 2000}, int32Data)

		// Verify int64 field
		int64Data := structFieldDatas["int64_field"].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_LongData).LongData.Data
		assert.Equal(t, []int64{10000, 20000}, int64Data)

		// Verify varchar field
		varcharData := structFieldDatas["varchar_field"].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_StringData).StringData.Data
		assert.Equal(t, []string{"hello", "world"}, varcharData)
	})

	t.Run("valid float vector type", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[vector_field]",
					ElementType: schemapb.DataType_FloatVector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				},
			},
		}
		jsonData := `[
			{"vector_field": [0.1, 0.2, 0.3, 0.4]},
			{"vector_field": [0.5, 0.6, 0.7, 0.8]}
		]`
		result := gjson.Parse(jsonData)

		structFieldDatas, err := assembleStructArrayField(result, structArrayField)
		assert.NoError(t, err)
		assert.NotNil(t, structFieldDatas)

		vectorData := structFieldDatas["vector_field"].(*schemapb.VectorField).Data.(*schemapb.VectorField_FloatVector).FloatVector.Data
		assert.Equal(t, 8, len(vectorData))
		assert.InDelta(t, 0.1, vectorData[0], 0.0001)
		assert.InDelta(t, 0.5, vectorData[4], 0.0001)
	})

	t.Run("valid binary vector type", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[binary_vector]",
					ElementType: schemapb.DataType_BinaryVector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}},
				},
			},
		}
		jsonData := `[
			{"binary_vector": [255]},
			{"binary_vector": [128]}
		]`
		result := gjson.Parse(jsonData)

		structFieldDatas, err := assembleStructArrayField(result, structArrayField)
		assert.NoError(t, err)
		assert.NotNil(t, structFieldDatas)

		binaryData := structFieldDatas["binary_vector"].(*schemapb.VectorField).Data.(*schemapb.VectorField_BinaryVector).BinaryVector
		assert.Equal(t, 2, len(binaryData))
		assert.Equal(t, byte(255), binaryData[0])
		assert.Equal(t, byte(128), binaryData[1])
	})

	t.Run("valid int8 vector type", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[int8_vector]",
					ElementType: schemapb.DataType_Int8Vector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				},
			},
		}
		jsonData := `[
			{"int8_vector": [1, 2, 3, 4]},
			{"int8_vector": [5, 6, 7, 8]}
		]`
		result := gjson.Parse(jsonData)

		structFieldDatas, err := assembleStructArrayField(result, structArrayField)
		assert.NoError(t, err)
		assert.NotNil(t, structFieldDatas)

		int8Data := structFieldDatas["int8_vector"].(*schemapb.VectorField).Data.(*schemapb.VectorField_Int8Vector).Int8Vector
		assert.Equal(t, 8, len(int8Data))
	})

	t.Run("unsupported element type", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[unsupported]", ElementType: schemapb.DataType_JSON},
			},
		}
		jsonData := `[{"unsupported": {}}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported element type")
	})

	t.Run("invalid bool value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[bool_field]", ElementType: schemapb.DataType_Bool},
			},
		}
		jsonData := `[{"bool_field": "not_a_bool"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid bool value")
	})

	t.Run("invalid int value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[int32_field]", ElementType: schemapb.DataType_Int32},
			},
		}
		jsonData := `[{"int32_field": "not_a_number"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid int32 value")
	})

	t.Run("invalid float vector value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[vector_field]",
					ElementType: schemapb.DataType_FloatVector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				},
			},
		}
		jsonData := `[{"vector_field": "not_a_vector"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid float vector value")
	})

	t.Run("valid float16 vector type", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[float16_vector]",
					ElementType: schemapb.DataType_Float16Vector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				},
			},
		}
		jsonData := `[{"float16_vector": [0.1, 0.2, 0.3, 0.4]}]`
		result := gjson.Parse(jsonData)

		structFieldDatas, err := assembleStructArrayField(result, structArrayField)
		assert.NoError(t, err)
		assert.NotNil(t, structFieldDatas)

		float16Data := structFieldDatas["float16_vector"].(*schemapb.VectorField).Data.(*schemapb.VectorField_Float16Vector).Float16Vector
		assert.Equal(t, 8, len(float16Data)) // 4 floats * 2 bytes each
	})

	t.Run("valid bfloat16 vector type", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[bfloat16_vector]",
					ElementType: schemapb.DataType_BFloat16Vector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				},
			},
		}
		jsonData := `[{"bfloat16_vector": [0.1, 0.2, 0.3, 0.4]}]`
		result := gjson.Parse(jsonData)

		structFieldDatas, err := assembleStructArrayField(result, structArrayField)
		assert.NoError(t, err)
		assert.NotNil(t, structFieldDatas)

		bfloat16Data := structFieldDatas["bfloat16_vector"].(*schemapb.VectorField).Data.(*schemapb.VectorField_Bfloat16Vector).Bfloat16Vector
		assert.Equal(t, 8, len(bfloat16Data)) // 4 floats * 2 bytes each
	})

	t.Run("valid sparse float vector type", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[sparse_vector]",
					ElementType: schemapb.DataType_SparseFloatVector,
				},
			},
		}
		jsonData := `[{"sparse_vector": {"1": 0.5, "10": 0.3}}]`
		result := gjson.Parse(jsonData)

		structFieldDatas, err := assembleStructArrayField(result, structArrayField)
		assert.NoError(t, err)
		assert.NotNil(t, structFieldDatas)

		sparseData := structFieldDatas["sparse_vector"].(*schemapb.VectorField).Data.(*schemapb.VectorField_SparseFloatVector).SparseFloatVector.Contents
		assert.Equal(t, 1, len(sparseData))
	})

	t.Run("invalid binary vector value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[binary_vector]",
					ElementType: schemapb.DataType_BinaryVector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}},
				},
			},
		}
		jsonData := `[{"binary_vector": "not_a_vector"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid binary vector value")
	})

	t.Run("invalid float16 vector value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[float16_vector]",
					ElementType: schemapb.DataType_Float16Vector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				},
			},
		}
		jsonData := `[{"float16_vector": "not_a_vector"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid float16 vector value")
	})

	t.Run("invalid bfloat16 vector value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[bfloat16_vector]",
					ElementType: schemapb.DataType_BFloat16Vector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				},
			},
		}
		jsonData := `[{"bfloat16_vector": "not_a_vector"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid bfloat16 vector value")
	})

	t.Run("invalid sparse float vector value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[sparse_vector]",
					ElementType: schemapb.DataType_SparseFloatVector,
				},
			},
		}
		jsonData := `[{"sparse_vector": "not_a_vector"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid sparse float vector value")
	})

	t.Run("invalid int8 vector value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "test_struct[int8_vector]",
					ElementType: schemapb.DataType_Int8Vector,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				},
			},
		}
		jsonData := `[{"int8_vector": "not_a_vector"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid int8 vector value")
	})

	t.Run("invalid int8 value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[int8_field]", ElementType: schemapb.DataType_Int8},
			},
		}
		jsonData := `[{"int8_field": "not_a_number"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid int8 value")
	})

	t.Run("invalid int16 value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[int16_field]", ElementType: schemapb.DataType_Int16},
			},
		}
		jsonData := `[{"int16_field": "not_a_number"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid int16 value")
	})

	t.Run("invalid int64 value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[int64_field]", ElementType: schemapb.DataType_Int64},
			},
		}
		jsonData := `[{"int64_field": "not_a_number"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid int64 value")
	})

	t.Run("invalid float value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[float_field]", ElementType: schemapb.DataType_Float},
			},
		}
		jsonData := `[{"float_field": "not_a_number"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid float value")
	})

	t.Run("invalid double value", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[double_field]", ElementType: schemapb.DataType_Double},
			},
		}
		jsonData := `[{"double_field": "not_a_number"}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid double value")
	})

	t.Run("invalid field name format in schema with multiple brackets", func(t *testing.T) {
		// Field name with multiple brackets triggers error
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[field[nested]]", ElementType: schemapb.DataType_Int32},
			},
		}
		jsonData := `[{"field": 123}]`
		result := gjson.Parse(jsonData)

		_, err := assembleStructArrayField(result, structArrayField)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid struct field name")
	})

	t.Run("varchar value conversion", func(t *testing.T) {
		structArrayField := &schemapb.StructArrayFieldSchema{
			Name: "test_struct",
			Fields: []*schemapb.FieldSchema{
				{Name: "test_struct[varchar_field]", ElementType: schemapb.DataType_VarChar},
			},
		}
		jsonData := `[{"varchar_field": "test string"}]`
		result := gjson.Parse(jsonData)

		structFieldDatas, err := assembleStructArrayField(result, structArrayField)
		assert.NoError(t, err)
		assert.NotNil(t, structFieldDatas)

		varcharData := structFieldDatas["varchar_field"].(*schemapb.ScalarField).Data.(*schemapb.ScalarField_StringData).StringData.Data
		assert.Equal(t, []string{"test string"}, varcharData)
	})
}
