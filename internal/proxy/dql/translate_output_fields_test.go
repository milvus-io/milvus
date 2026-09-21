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

package dql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestTranslateOutputFields(t *testing.T) {
	const (
		idFieldName                = "id"
		tsFieldName                = "timestamp"
		floatVectorFieldName       = "float_vector"
		binaryVectorFieldName      = "binary_vector"
		float16VectorFieldName     = "float16_vector"
		bfloat16VectorFieldName    = "bfloat16_vector"
		sparseFloatVectorFieldName = "sparse_float_vector"
	)
	var outputFields []string
	var userOutputFields []string
	var userDynamicFields []string
	var requestedPK bool
	var err error

	collSchema := &schemapb.CollectionSchema{
		Name:        "TestTranslateOutputFields",
		Description: "TestTranslateOutputFields",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{Name: idFieldName, FieldID: 0, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: tsFieldName, FieldID: 1, DataType: schemapb.DataType_Int64},
			{Name: floatVectorFieldName, FieldID: 100, DataType: schemapb.DataType_FloatVector},
			{Name: binaryVectorFieldName, FieldID: 101, DataType: schemapb.DataType_BinaryVector},
			{Name: float16VectorFieldName, FieldID: 102, DataType: schemapb.DataType_Float16Vector},
			{Name: bfloat16VectorFieldName, FieldID: 103, DataType: schemapb.DataType_BFloat16Vector},
			{Name: sparseFloatVectorFieldName, FieldID: 104, DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:             "bm25",
				Type:             schemapb.FunctionType_BM25,
				OutputFieldNames: []string{sparseFloatVectorFieldName},
				// omit other fields for brevity
			},
		},
	}
	schema := mustNewSchemaInfo(collSchema)

	// Test empty output fields
	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, outputFields)
	assert.ElementsMatch(t, []string{}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.False(t, requestedPK)

	// Test single field
	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{idFieldName}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test multiple fields
	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test with vector field
	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test without id field
	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{tsFieldName, floatVectorFieldName}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.False(t, requestedPK)

	// Test wildcard - should not include function output fields
	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{"*"}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test wildcard with spaces
	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{" * "}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	_, _, _, _, _, err = translateOutputFields([]string{"*", sparseFloatVectorFieldName}, schema, false)
	assert.Error(t, err)
	_, _, _, _, _, err = translateOutputFields([]string{sparseFloatVectorFieldName}, schema, false)
	assert.Error(t, err)
	_, _, _, _, _, err = translateOutputFields([]string{sparseFloatVectorFieldName}, schema, true)
	assert.Error(t, err)

	// Test with removePkField=true
	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{}, schema, true)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, outputFields)
	assert.ElementsMatch(t, []string{}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.False(t, requestedPK)

	// if removePkField is true, pk field should be removed from output fields

	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{"*"}, schema, true)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{tsFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{"*"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{"*", tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{"*", floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, outputFields)
	assert.ElementsMatch(t, []string{tsFieldName, floatVectorFieldName, binaryVectorFieldName, float16VectorFieldName, bfloat16VectorFieldName}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.True(t, requestedPK)

	// Test non-existent field, dynamic field not enabled
	_, _, _, _, _, err = translateOutputFields([]string{"A"}, schema, true)
	assert.Error(t, err)

	t.Run("enable dynamic schema", func(t *testing.T) {
		collSchema := &schemapb.CollectionSchema{
			Name:               "TestTranslateOutputFields",
			Description:        "TestTranslateOutputFields",
			AutoID:             false,
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{Name: idFieldName, FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{Name: tsFieldName, FieldID: 2, DataType: schemapb.DataType_Int64},
				{Name: floatVectorFieldName, FieldID: 100, DataType: schemapb.DataType_FloatVector},
				{Name: binaryVectorFieldName, FieldID: 101, DataType: schemapb.DataType_BinaryVector},
				{Name: common.MetaFieldName, FieldID: 102, DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}
		schema := mustNewSchemaInfo(collSchema)

		outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{"A", idFieldName}, schema, true)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{common.MetaFieldName}, outputFields)
		assert.ElementsMatch(t, []string{"A"}, userOutputFields)
		assert.ElementsMatch(t, []string{"A"}, userDynamicFields)
		assert.True(t, requestedPK)

		outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{"$meta[\"A\"]", idFieldName}, schema, true)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{common.MetaFieldName}, outputFields)
		assert.ElementsMatch(t, []string{"$meta[\"A\"]"}, userOutputFields)
		assert.ElementsMatch(t, []string{"A"}, userDynamicFields)
		assert.True(t, requestedPK)

		// Test invalid dynamic field expressions
		_, _, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, `$meta["A"]["B"]`}, schema, true)
		assert.Error(t, err)

		_, _, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "$meta[\"\"]"}, schema, true)
		assert.Error(t, err)

		_, _, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "$meta[]"}, schema, true)
		assert.Error(t, err)

		_, _, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "$meta["}, schema, true)
		assert.Error(t, err)

		_, _, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "[]"}, schema, true)
		assert.Error(t, err)

		_, _, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, "A > 1"}, schema, true)
		assert.Error(t, err)

		_, _, _, _, _, err = translateOutputFields([]string{idFieldName, floatVectorFieldName, ""}, schema, true)
		assert.Error(t, err)
	})
}

func TestTranslateOutputFields_StructArrayField(t *testing.T) {
	const (
		idFieldName                = "id"
		tsFieldName                = "timestamp"
		floatVectorFieldName       = "float_vector"
		binaryVectorFieldName      = "binary_vector"
		float16VectorFieldName     = "float16_vector"
		bfloat16VectorFieldName    = "bfloat16_vector"
		sparseFloatVectorFieldName = "sparse_float_vector"
	)
	var outputFields []string
	var userOutputFields []string
	var userDynamicFields []string
	var requestedPK bool
	var err error

	collSchema := &schemapb.CollectionSchema{
		Name:        "TestTranslateOutputFields",
		Description: "TestTranslateOutputFields",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{Name: idFieldName, FieldID: 0, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: tsFieldName, FieldID: 1, DataType: schemapb.DataType_Int64},
			{Name: floatVectorFieldName, FieldID: 100, DataType: schemapb.DataType_FloatVector},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID: 101,
				Name:    "struct_array_field",
				Fields: []*schemapb.FieldSchema{
					{Name: "sub_field", FieldID: 102, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
					{Name: "sub_vector_field", FieldID: 103, DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector},
				},
			},
		},
	}
	schema := mustNewSchemaInfo(collSchema)

	// Test struct array field
	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{"sub_vector_field"}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"sub_vector_field"}, outputFields)
	assert.ElementsMatch(t, []string{"sub_vector_field"}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.False(t, requestedPK)

	outputFields, userOutputFields, userDynamicFields, _, requestedPK, err = translateOutputFields([]string{"struct_array_field", "sub_field"}, schema, false)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"sub_vector_field", "sub_field"}, outputFields)
	assert.ElementsMatch(t, []string{"sub_vector_field", "sub_field"}, userOutputFields)
	assert.ElementsMatch(t, []string{}, userDynamicFields)
	assert.False(t, requestedPK)
}
func Test_MaxQueryResultWindow(t *testing.T) {
	paramtable.Init()
	assert.Nil(t, validateMaxQueryResultWindow(0, 16384, false))
	assert.Nil(t, validateMaxQueryResultWindow(0, 1, false))
	assert.Error(t, validateMaxQueryResultWindow(0, 16385, false))
	assert.Error(t, validateMaxQueryResultWindow(0, 0, false))
	assert.Error(t, validateMaxQueryResultWindow(1, 0, false))

	paramtable.Get().Save(paramtable.Get().QuotaConfig.LargeMaxQueryResultWindow.Key, "1000000")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.LargeMaxQueryResultWindow.Key)
	assert.Nil(t, validateMaxQueryResultWindow(0, 16385, true))
	assert.Nil(t, validateMaxQueryResultWindow(0, 1000000, true))
	assert.Error(t, validateMaxQueryResultWindow(0, 1000001, true))
}

func Test_reconstructStructFieldData(t *testing.T) {
	t.Run("count(*) query - should return early", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "count(*)",
				FieldId:   0,
				Type:      schemapb.DataType_Int64,
			},
		}
		outputFields := []string{"count(*)"}

		schema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "test_struct[sub_field]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		originalFieldsData := make([]*schemapb.FieldData, len(fieldsData))
		copy(originalFieldsData, fieldsData)
		originalOutputFields := make([]string, len(outputFields))
		copy(originalOutputFields, outputFields)

		resultFieldsData, resultOutputFields := reconstructStructFieldData(fieldsData, outputFields, schema)

		// Should not modify anything for count(*) query
		assert.Equal(t, originalFieldsData, resultFieldsData)
		assert.Equal(t, originalOutputFields, resultOutputFields)
	})

	t.Run("group by with count(*) should preserve aggregate field", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "id",
				FieldId:   100,
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{1, 2}},
						},
					},
				},
			},
			{
				FieldName: "count(*)",
				FieldId:   0,
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{3, 4}},
						},
					},
				},
			},
		}
		outputFields := []string{"id", "count(*)"}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "id",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "test_struct[sub_field]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldData(fieldsData, outputFields, schema)

		assert.Len(t, resultFieldsData, 2)
		assert.Equal(t, "id", resultFieldsData[0].FieldName)
		assert.Equal(t, int64(100), resultFieldsData[0].FieldId)
		assert.Equal(t, "count(*)", resultFieldsData[1].FieldName)
		assert.Equal(t, int64(0), resultFieldsData[1].FieldId)
		assert.Equal(t, []string{"id", "count(*)"}, resultOutputFields)
		assert.Equal(t, []int64{3, 4}, resultFieldsData[1].GetScalars().GetLongData().GetData())
	})

	t.Run("struct field query - should reconstruct struct field", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "test_struct[sub_field]",
				FieldId:   1021, // Use the correct field ID that matches the schema
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								ElementType: schemapb.DataType_Int32,
								Data: []*schemapb.ScalarField{
									{
										Data: &schemapb.ScalarField_IntData{
											IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		outputFields := []string{"test_struct[sub_field]"}

		schema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "test_struct[sub_field]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldData(fieldsData, outputFields, schema)

		// Should reconstruct the struct field with the restored field name
		assert.Len(t, resultFieldsData, 1)
		assert.Equal(t, "test_struct", resultFieldsData[0].FieldName)
		assert.Equal(t, int64(102), resultFieldsData[0].FieldId)
		assert.Equal(t, schemapb.DataType_ArrayOfStruct, resultFieldsData[0].Type)

		// Check that the sub-field name has been restored
		structArrayField := resultFieldsData[0].GetStructArrays()
		assert.NotNil(t, structArrayField)
		assert.Len(t, structArrayField.Fields, 1)
		assert.Equal(t, "sub_field", structArrayField.Fields[0].FieldName) // Name should be restored

		assert.Equal(t, []string{"test_struct"}, resultOutputFields)
	})

	t.Run("no struct array fields - should return early", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{
			{
				FieldName: "field1",
				FieldId:   100,
				Type:      schemapb.DataType_Int64,
			},
			{
				FieldName: "field2",
				FieldId:   101,
				Type:      schemapb.DataType_VarChar,
			},
		}
		outputFields := []string{"field1", "field2"}

		schema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{},
		}

		originalFieldsData := make([]*schemapb.FieldData, len(fieldsData))
		copy(originalFieldsData, fieldsData)
		originalOutputFields := make([]string, len(outputFields))
		copy(originalOutputFields, outputFields)

		resultFieldsData, resultOutputFields := reconstructStructFieldData(fieldsData, outputFields, schema)

		// Should not modify anything when no struct array fields
		assert.Equal(t, originalFieldsData, resultFieldsData)
		assert.Equal(t, originalOutputFields, resultOutputFields)
	})

	t.Run("reconstruct single struct field", func(t *testing.T) {
		// Create mock data with transformed field names (as they would be internally)
		subField1Data := &schemapb.FieldData{
			FieldName: "test_struct[sub_int_array]",
			FieldId:   1021,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_Int32,
							Data: []*schemapb.ScalarField{
								{
									Data: &schemapb.ScalarField_IntData{
										IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
									},
								},
							},
						},
					},
				},
			},
		}

		subField2Data := &schemapb.FieldData{
			FieldName: "test_struct[sub_text_array]",
			FieldId:   1022,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_VarChar,
							Data: []*schemapb.ScalarField{
								{
									Data: &schemapb.ScalarField_StringData{
										StringData: &schemapb.StringArray{Data: []string{"hello", "world"}},
									},
								},
							},
						},
					},
				},
			},
		}

		fieldsData := []*schemapb.FieldData{subField1Data, subField2Data}
		outputFields := []string{"test_struct[sub_int_array]", "test_struct[sub_text_array]"}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "test_struct[sub_int_array]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
						{
							FieldID:     1022,
							Name:        "test_struct[sub_text_array]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_VarChar,
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldData(fieldsData, outputFields, schema)

		// Check result
		assert.Len(t, resultFieldsData, 1, "Should only have one reconstructed struct field")
		assert.Len(t, resultOutputFields, 1, "Output fields should only have one")

		structField := resultFieldsData[0]
		assert.Equal(t, "test_struct", structField.FieldName)
		assert.Equal(t, int64(102), structField.FieldId)
		assert.Equal(t, schemapb.DataType_ArrayOfStruct, structField.Type)
		assert.Equal(t, "test_struct", resultOutputFields[0])

		// Check fields inside struct
		structArrays := structField.GetStructArrays()
		assert.NotNil(t, structArrays)
		assert.Len(t, structArrays.Fields, 2, "Struct should contain 2 sub fields")

		// Check sub fields
		var foundIntField, foundTextField bool
		for _, field := range structArrays.Fields {
			switch field.FieldId {
			case 1021:
				assert.Equal(t, "sub_int_array", field.FieldName)
				assert.Equal(t, schemapb.DataType_Array, field.Type)
				foundIntField = true
			case 1022:
				assert.Equal(t, "sub_text_array", field.FieldName)
				assert.Equal(t, schemapb.DataType_Array, field.Type)
				foundTextField = true
			}
		}
		assert.True(t, foundIntField, "Should find int array field")
		assert.True(t, foundTextField, "Should find text array field")
	})

	t.Run("mixed regular and struct fields", func(t *testing.T) {
		// Create regular field data
		regularField := &schemapb.FieldData{
			FieldName: "regular_field",
			FieldId:   100,
			Type:      schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
					},
				},
			},
		}

		// Create struct sub field data with transformed name
		subFieldData := &schemapb.FieldData{
			FieldName: "test_struct[sub_field]",
			FieldId:   1021,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_Int32,
							Data: []*schemapb.ScalarField{
								{
									Data: &schemapb.ScalarField_IntData{
										IntData: &schemapb.IntArray{Data: []int32{10, 20}},
									},
								},
							},
						},
					},
				},
			},
		}

		fieldsData := []*schemapb.FieldData{regularField, subFieldData}
		outputFields := []string{"regular_field", "test_struct[sub_field]"}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "regular_field",
					DataType: schemapb.DataType_Int64,
				},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "test_struct",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "test_struct[sub_field]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldData(fieldsData, outputFields, schema)

		// Check result: should have 2 fields (1 regular + 1 reconstructed struct)
		assert.Len(t, resultFieldsData, 2)
		assert.Len(t, resultOutputFields, 2)

		// Check regular and struct fields both exist
		var foundRegularField, foundStructField bool
		for i, field := range resultFieldsData {
			switch field.FieldId {
			case 100:
				assert.Equal(t, "regular_field", field.FieldName)
				assert.Equal(t, schemapb.DataType_Int64, field.Type)
				assert.Equal(t, "regular_field", resultOutputFields[i])
				foundRegularField = true
			case 102:
				assert.Equal(t, "test_struct", field.FieldName)
				assert.Equal(t, schemapb.DataType_ArrayOfStruct, field.Type)
				assert.Equal(t, "test_struct", resultOutputFields[i])
				foundStructField = true
			}
		}
		assert.True(t, foundRegularField, "Should find regular field")
		assert.True(t, foundStructField, "Should find reconstructed struct field")
	})

	t.Run("multiple struct fields", func(t *testing.T) {
		// Create sub field for first struct
		struct1SubField := &schemapb.FieldData{
			FieldName: "struct1[struct1_sub]",
			FieldId:   1021,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_Int32,
							Data:        []*schemapb.ScalarField{},
						},
					},
				},
			},
		}

		// Create sub fields for second struct
		struct2SubField1 := &schemapb.FieldData{
			FieldName: "struct2[struct2_sub1]",
			FieldId:   1031,
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							ElementType: schemapb.DataType_Int32,
							Data:        []*schemapb.ScalarField{},
						},
					},
				},
			},
		}

		struct2SubField2 := &schemapb.FieldData{
			FieldName: "struct2[struct2_sub2]",
			FieldId:   1032,
			Type:      schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: []string{"test"}},
					},
				},
			},
		}

		fieldsData := []*schemapb.FieldData{struct1SubField, struct2SubField1, struct2SubField2}
		outputFields := []string{"struct1[struct1_sub]", "struct2[struct2_sub1]", "struct2[struct2_sub2]"}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 102,
					Name:    "struct1",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1021,
							Name:        "struct1[struct1_sub]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
				{
					FieldID: 103,
					Name:    "struct2",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:     1031,
							Name:        "struct2[struct2_sub1]",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
						{
							FieldID:  1032,
							Name:     "struct2[struct2_sub2]",
							DataType: schemapb.DataType_VarChar,
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldData(fieldsData, outputFields, schema)

		// Check result: should have 2 struct fields
		assert.Len(t, resultFieldsData, 2)
		assert.Len(t, resultOutputFields, 2)

		// Check both struct fields
		var foundStruct1, foundStruct2 bool
		for _, field := range resultFieldsData {
			switch field.FieldId {
			case 102:
				assert.Equal(t, "struct1", field.FieldName)
				assert.Equal(t, schemapb.DataType_ArrayOfStruct, field.Type)
				foundStruct1 = true
				structArrays := field.GetStructArrays()
				assert.NotNil(t, structArrays)
				assert.Len(t, structArrays.Fields, 1)
			case 103:
				assert.Equal(t, "struct2", field.FieldName)
				assert.Equal(t, schemapb.DataType_ArrayOfStruct, field.Type)
				foundStruct2 = true
				structArrays := field.GetStructArrays()
				assert.NotNil(t, structArrays)
				assert.Len(t, structArrays.Fields, 2)
			}
		}
		assert.True(t, foundStruct1, "Should find struct1")
		assert.True(t, foundStruct2, "Should find struct2")
	})

	t.Run("partial struct fields query - only return queried fields", func(t *testing.T) {
		// Create a struct with 3 fields, but only query 2 of them
		// This tests that we only return what the user requested

		// Create mock data for only 2 out of 3 struct fields
		clipStrData := &schemapb.FieldData{
			FieldName: "clip[str]",
			FieldId:   2001,
			Type:      schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: []string{"text1", "text2"}},
					},
				},
			},
		}

		clipIntData := &schemapb.FieldData{
			FieldName: "clip[int]",
			FieldId:   2002,
			Type:      schemapb.DataType_Int32,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{Data: []int32{100, 200}},
					},
				},
			},
		}

		// Note: clip[embedding] is NOT included in query results
		fieldsData := []*schemapb.FieldData{clipStrData, clipIntData}
		outputFields := []string{"clip[str]", "clip[int]"}

		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 200,
					Name:    "clip",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:  2001,
							Name:     "clip[str]",
							DataType: schemapb.DataType_VarChar,
						},
						{
							FieldID:  2002,
							Name:     "clip[int]",
							DataType: schemapb.DataType_Int32,
						},
						{
							FieldID:  2003,
							Name:     "clip[embedding]",
							DataType: schemapb.DataType_FloatVector,
							TypeParams: []*commonpb.KeyValuePair{
								{Key: "dim", Value: "128"},
							},
						},
					},
				},
			},
		}

		resultFieldsData, resultOutputFields := reconstructStructFieldData(fieldsData, outputFields, schema)

		// Check result
		assert.Len(t, resultFieldsData, 1, "Should have one reconstructed struct field")
		assert.Len(t, resultOutputFields, 1, "Output fields should have one")

		structField := resultFieldsData[0]
		assert.Equal(t, "clip", structField.FieldName)
		assert.Equal(t, int64(200), structField.FieldId)
		assert.Equal(t, schemapb.DataType_ArrayOfStruct, structField.Type)
		assert.Equal(t, "clip", resultOutputFields[0])

		// Check that struct only contains the 2 queried fields, NOT the embedding field
		structArrays := structField.GetStructArrays()
		assert.NotNil(t, structArrays)
		assert.Len(t, structArrays.Fields, 2, "Struct should only contain 2 queried fields, not 3")

		// Verify the field names have been restored to original names
		var foundStr, foundInt bool
		for _, field := range structArrays.Fields {
			switch field.FieldId {
			case 2001:
				assert.Equal(t, "str", field.FieldName, "Field name should be restored to original")
				assert.Equal(t, schemapb.DataType_VarChar, field.Type)
				foundStr = true
			case 2002:
				assert.Equal(t, "int", field.FieldName, "Field name should be restored to original")
				assert.Equal(t, schemapb.DataType_Int32, field.Type)
				foundInt = true
			case 2003:
				assert.Fail(t, "Should not include embedding field as it was not queried")
			}
		}
		assert.True(t, foundStr, "Should find str field")
		assert.True(t, foundInt, "Should find int field")
	})
}

func TestResolveTimezone(t *testing.T) {
	ctx := context.Background()
	colInfoWithTz := &collectionInfo{
		Properties: []*commonpb.KeyValuePair{
			{Key: common.TimezoneKey, Value: "America/New_York"},
		},
	}
	colInfoWithoutTz := &collectionInfo{}

	t.Run("request timezone wins over collection timezone", func(t *testing.T) {
		params := []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: "Asia/Shanghai"}}
		tz, err := resolveTimezone(ctx, params, colInfoWithTz)
		assert.NoError(t, err)
		assert.Equal(t, "Asia/Shanghai", tz)
	})

	t.Run("invalid request timezone is rejected as ParameterInvalid", func(t *testing.T) {
		params := []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: "Not/AZone"}}
		_, err := resolveTimezone(ctx, params, colInfoWithTz)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("absent request timezone falls back to collection timezone", func(t *testing.T) {
		tz, err := resolveTimezone(ctx, nil, colInfoWithTz)
		assert.NoError(t, err)
		assert.Equal(t, "America/New_York", tz)
	})

	t.Run("empty request timezone is treated as unspecified", func(t *testing.T) {
		params := []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: ""}}
		tz, err := resolveTimezone(ctx, params, colInfoWithTz)
		assert.NoError(t, err)
		assert.Equal(t, "America/New_York", tz)
	})

	t.Run("no request or collection timezone defaults to UTC", func(t *testing.T) {
		tz, err := resolveTimezone(ctx, nil, colInfoWithoutTz)
		assert.NoError(t, err)
		assert.Equal(t, common.DefaultTimezone, tz)
	})
}
