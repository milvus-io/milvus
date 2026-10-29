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

package ddl

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func testArrayTypeSchema(element *schemapb.TypeSchema, params ...*commonpb.KeyValuePair) *schemapb.TypeSchema {
	return &schemapb.TypeSchema{
		Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: element},
		TypeParams: params,
	}
}

func TestValidateFieldName(t *testing.T) {
	assert.Nil(t, validateFieldName("abc"))
	assert.Nil(t, validateFieldName("_123abc"))
	assert.Nil(t, validateFieldName("abc123_"))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"abc$",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
		"True",
		"null",
		"Null",
		"NULL",
		"nUlL",
		"NuLL",
		"array_contains",
		"json_contains_any",
		"ARRAY_LENGTH",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, validateFieldName(name))
	}
}

func TestValidateDimension(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "1",
			},
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))
	fieldSchema = &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "2",
			},
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: paramtable.Get().ProxyCfg.MaxDimension.GetValue(),
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))

	// invalid dim
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "-1",
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(int(paramtable.Get().ProxyCfg.MaxDimension.GetAsInt32() + 1)),
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))

	fieldSchema.DataType = schemapb.DataType_BinaryVector
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "8",
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(paramtable.Get().ProxyCfg.MaxDimension.GetAsInt()),
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "9",
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))

	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "262145",
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))

	fieldSchema.DataType = schemapb.DataType_Int8Vector
	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "200",
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))

	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "201",
		},
	}
	assert.Nil(t, validateDimension(fieldSchema))

	fieldSchema.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(int(paramtable.Get().ProxyCfg.MaxDimension.GetAsInt32() + 1)),
		},
	}
	assert.NotNil(t, validateDimension(fieldSchema))
}

func TestValidateFieldAllowsAnalyzerParamsWithoutEnableAnalyzer(t *testing.T) {
	field := &schemapb.FieldSchema{
		Name:     "text_field",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "100"},
			{Key: common.AnalyzerParamKey, Value: `{"tokenizer":"standard"}`},
		},
	}
	schema := &schemapb.CollectionSchema{Name: "test_collection"}

	err := ValidateField(field, schema)
	require.NoError(t, err)
}

func TestValidateFieldNestedArray(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "test_collection"}
	nestedField := func(element *schemapb.TypeSchema) *schemapb.FieldSchema {
		rootParams := []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}}
		return &schemapb.FieldSchema{
			Name:        "nested_array",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array,
			TypeParams:  rootParams,
			TypeSchema:  testArrayTypeSchema(element, rootParams...),
		}
	}
	leafType := func(dataType schemapb.DataType, params ...*commonpb.KeyValuePair) *schemapb.TypeSchema {
		return &schemapb.TypeSchema{
			Kind:       &schemapb.TypeSchema_LeafType{LeafType: dataType},
			TypeParams: params,
		}
	}

	t.Run("top-level nested array rejected", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		err := ValidateField(field, schema)
		require.ErrorContains(t, err, "nested array can only be in a struct array field")
	})

	t.Run("array of array in struct field supported", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		assert.NoError(t, ValidateFieldsInStruct(field, schema))
	})

	t.Run("root capacity mirror is optional", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		field.TypeParams = nil
		assert.NoError(t, ValidateFieldsInStruct(field, schema))
	})

	t.Run("root capacity mirror must match", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		field.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "50"}}
		err := ValidateFieldsInStruct(field, schema)
		require.ErrorContains(t, err, "must match type_schema root capacity")
	})

	t.Run("root type schema capacity is required", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		field.TypeSchema.TypeParams = nil
		err := ValidateFieldsInStruct(field, schema)
		require.ErrorContains(t, err, "type param(max_capacity) should be specified")
	})

	t.Run("nested type schema capacity is required", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		field.TypeSchema.GetArrayElement().TypeParams = nil
		err := ValidateFieldsInStruct(field, schema)
		require.ErrorContains(t, err, "type param(max_capacity) should be specified")
	})

	t.Run("root type schema capacity observes configured limit", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		field.TypeParams = nil
		field.TypeSchema.TypeParams[0].Value = strconv.FormatInt(
			paramtable.Get().ProxyCfg.MaxArrayCapacity.GetAsInt64()+1, 10)
		err := ValidateFieldsInStruct(field, schema)
		require.ErrorContains(t, err, "maximum capacity")
	})

	t.Run("nested type schema capacity observes configured limit", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{
				Key: common.MaxCapacityKey,
				Value: strconv.FormatInt(
					paramtable.Get().ProxyCfg.MaxArrayCapacity.GetAsInt64()+1, 10),
			},
		))
		err := ValidateFieldsInStruct(field, schema)
		require.ErrorContains(t, err, "maximum capacity")
	})

	t.Run("nullable nested array element is rejected", func(t *testing.T) {
		child := testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		)
		child.Nullable = true
		field := nestedField(child)
		err := ValidateFieldsInStruct(field, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nullable nested array elements are not supported")
	})

	t.Run("legacy element nullable on nested array is rejected", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		field.ElementNullable = true

		err := ValidateField(field, schema)
		require.ErrorContains(t, err, "element_nullable is not supported for nested Array")
	})

	t.Run("valid array of varchar array", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_VarChar,
				&commonpb.KeyValuePair{Key: common.MaxLengthKey, Value: "64"}),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		assert.NoError(t, ValidateFieldsInStruct(field, schema))
	})

	t.Run("nested varchar array missing max length", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_VarChar),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max_length")
	})

	t.Run("missing type schema", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "nested_array",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array,
			TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "element type Array is not supported")
		assert.Contains(t, err.Error(), "must specify type_schema")
	})

	t.Run("type schema element type mismatch", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		field.ElementType = schemapb.DataType_Int64
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must specify data_type Array and element_type Array")
	})

	t.Run("deeper nested array rejected", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			testArrayTypeSchema(
				leafType(schemapb.DataType_Int64),
				&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "5"},
			),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		err := ValidateFieldsInStruct(field, schema)
		require.ErrorContains(t, err, "supports exactly one nested array level")
	})

	t.Run("nested array in struct array field supported", func(t *testing.T) {
		field := nestedField(testArrayTypeSchema(
			leafType(schemapb.DataType_Int64),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		))
		require.NoError(t, ValidateStructArrayField(
			&schemapb.StructArrayFieldSchema{
				Name:   "struct_array",
				Fields: []*schemapb.FieldSchema{field},
			},
			schema,
		))
	})
}

func TestValidateFieldNestedArrayOfVector(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "test_collection"}

	nestedField := func() *schemapb.FieldSchema {
		return &schemapb.FieldSchema{
			Name:        "nested_array_of_vector",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array,
			TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
			TypeSchema: &schemapb.TypeSchema{
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
				Kind: &schemapb.TypeSchema_ArrayElement{
					ArrayElement: &schemapb.TypeSchema{
						TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "10"}},
						Kind: &schemapb.TypeSchema_ArrayElement{
							ArrayElement: &schemapb.TypeSchema{
								Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_ArrayOfVector},
							},
						},
					},
				},
			},
		}
	}

	t.Run("top-level nested array of vector rejected", func(t *testing.T) {
		err := ValidateField(nestedField(), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "element type ArrayOfVector is not supported")
	})

	t.Run("plain struct field array of vector still supported", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "array_of_vector",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "100"},
				{Key: common.DimKey, Value: "4"},
			},
		}
		assert.NoError(t, ValidateFieldsInStruct(field, schema))
	})

	t.Run("plain top-level array of vector still rejected", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "array_of_vector",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "100"},
				{Key: common.DimKey, Value: "4"},
			},
		}
		err := ValidateField(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "array of vector can only be in the struct array field")
	})

	t.Run("old-style nested array of vector rejected", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "nested_array_of_vector",
			DataType:    schemapb.DataType_ArrayOfVector,
			TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
			ElementType: schemapb.DataType_ArrayOfVector,
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nested ArrayOfVector is not supported")
	})
}

func Test_validateMaxCapacityPerRow(t *testing.T) {
	paramtable.Init()

	t.Run("normal case", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "100",
				},
				{
					Key:   common.MaxCapacityKey,
					Value: "10",
				},
			},
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.NoError(t, err)
	})

	t.Run("no max capacity", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.Error(t, err)
	})

	t.Run("max capacity not int", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxCapacityKey,
					Value: "six",
				},
			},
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.Error(t, err)
	})

	t.Run("max capacity exceed max", func(t *testing.T) {
		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxCapacityKey,
					Value: "4097",
				},
			},
		}

		err := validateMaxCapacityPerRow("collection", arrayField)
		assert.Error(t, err)
	})

	t.Run("custom max capacity", func(t *testing.T) {
		paramtable.Init()
		err := paramtable.Get().Save(paramtable.Get().ProxyCfg.MaxArrayCapacity.Key, "5000")
		assert.NoError(t, err)
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.MaxArrayCapacity.Key)

		arrayField := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxCapacityKey,
					Value: "5000",
				},
			},
		}

		err = validateMaxCapacityPerRow("collection", arrayField)
		assert.NoError(t, err)
	})
}

func Test_validateMaxLengthPerRow(t *testing.T) {
	paramtable.Init()

	t.Run("normal case", func(t *testing.T) {
		err := validateMaxLengthPerRow("collection", "field", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "100",
			},
		})
		assert.NoError(t, err)
	})

	t.Run("max length not int", func(t *testing.T) {
		err := validateMaxLengthPerRow("collection", "arr", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "()",
			},
		})
		// A malformed max_length must surface as an invalid-parameter error (1100)
		// that names the field and the parameter, not a raw strconv error (65535).
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "max_length")
		assert.Contains(t, err.Error(), "arr")
	})

	t.Run("no max length", func(t *testing.T) {
		err := validateMaxLengthPerRow("collection", "field", schemapb.DataType_VarChar, nil)
		assert.Error(t, err)
	})
}

func TestValidateFieldsInStruct(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	t.Run("valid array field", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "valid_array",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int32,
			TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.NoError(t, err)
	})

	t.Run("valid array of vector field", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "valid_array_vector",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
				{Key: common.MaxCapacityKey, Value: "100"},
			},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.NoError(t, err)
	})

	t.Run("invalid field name", func(t *testing.T) {
		testCases := []struct {
			name     string
			expected string
		}{
			{"", "field name should not be empty"},
			{"123abc", "The first character of a field name must be an underscore or letter"},
			{"abc-def", "Field name can only contain numbers, letters, and underscores"},
		}

		for _, tc := range testCases {
			field := &schemapb.FieldSchema{
				Name:        tc.name,
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
			}
			err := ValidateFieldsInStruct(field, schema)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expected)
		}
	})

	t.Run("invalid data type", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:     "invalid_type",
			DataType: schemapb.DataType_Int32, // Not array or array of vector
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fields in StructArrayField can only be array or array of struct")
	})

	t.Run("JSON not supported in struct", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "json_field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_JSON,
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not supported")
	})

	t.Run("old-style nested element_type not supported", func(t *testing.T) {
		testCases := []struct {
			elementType schemapb.DataType
			expected    string
		}{
			{schemapb.DataType_ArrayOfStruct, "nested ArrayOfStruct is not supported"},
			{schemapb.DataType_ArrayOfVector, "nested ArrayOfVector is not supported"},
			{schemapb.DataType_Array, "nested array field nested_array must specify type_schema"},
		}

		for _, tc := range testCases {
			field := &schemapb.FieldSchema{
				Name:        "nested_array",
				DataType:    schemapb.DataType_Array,
				ElementType: tc.elementType,
			}
			err := ValidateFieldsInStruct(field, schema)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expected)
		}
	})

	t.Run("array field with vector element type", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "array_with_vector",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_FloatVector,
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "element type FloatVector is not supported")
	})

	t.Run("array of vector field with non-vector element type", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "array_vector_with_scalar",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_Int32,
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only fixed dimension vector types are supported")
	})

	t.Run("array of vector missing dimension", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "array_vector_no_dim",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams:  []*commonpb.KeyValuePair{}, // No dimension specified
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "dimension is not defined in field")
	})

	t.Run("array of vector with invalid dimension", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "array_vector_invalid_dim",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "not_a_number"},
			},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
	})

	t.Run("varchar array without max_length", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "varchar_array_no_max_length",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			TypeParams:  []*commonpb.KeyValuePair{}, // No max_length specified
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "type param(max_length) should be specified")
	})

	t.Run("varchar array with valid max_length", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "varchar_array_valid",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "100"},
				{Key: common.MaxCapacityKey, Value: "100"},
			},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.NoError(t, err)
	})

	t.Run("varchar array with invalid max_length", func(t *testing.T) {
		// Test with max_length exceeding limit
		field := &schemapb.FieldSchema{
			Name:        "varchar_array_invalid_length",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "99999999"}, // Exceeds limit
			},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "the maximum length specified for the field")
	})

	t.Run("nullable field in struct is allowed", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			Name:        "nullable_field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int32,
			Nullable:    true,
			TypeParams:  []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "100"}},
		}
		err := ValidateFieldsInStruct(field, schema)
		assert.NoError(t, err)
	})

	// t.Run("sparse float vector in array of vector", func(t *testing.T) {
	// 	// Note: ArrayOfVector with sparse vector element type still requires dimension
	// 	// because validateDimension checks the field's DataType (ArrayOfVector), not ElementType
	// 	field := &schemapb.FieldSchema{
	// 		Name:        "sparse_vector_array",
	// 		DataType:    schemapb.DataType_ArrayOfVector,
	// 		ElementType: schemapb.DataType_SparseFloatVector,
	// 		TypeParams:  []*commonpb.KeyValuePair{},
	// 	}
	// 	err := ValidateFieldsInStruct(field, schema)
	// 	assert.Error(t, err)
	// 	assert.Contains(t, err.Error(), "dimension is not defined")
	// })

	t.Run("array with various scalar element types", func(t *testing.T) {
		validScalarTypes := []schemapb.DataType{
			schemapb.DataType_Bool,
			schemapb.DataType_Int8,
			schemapb.DataType_Int16,
			schemapb.DataType_Int32,
			schemapb.DataType_Int64,
			schemapb.DataType_Float,
			schemapb.DataType_Double,
		}

		for _, dt := range validScalarTypes {
			field := &schemapb.FieldSchema{
				Name:        "array_" + dt.String(),
				DataType:    schemapb.DataType_Array,
				ElementType: dt,
				TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
			}
			err := ValidateFieldsInStruct(field, schema)
			assert.NoError(t, err)
		}
	})

	t.Run("array of vector with various vector types", func(t *testing.T) {
		validVectorTypes := []schemapb.DataType{
			schemapb.DataType_FloatVector,
			// schemapb.DataType_BinaryVector,
			// schemapb.DataType_Float16Vector,
			// schemapb.DataType_BFloat16Vector,
			// Note: SparseFloatVector is excluded because validateDimension checks
			// the field's DataType (ArrayOfVector), not ElementType, so it still requires dimension
		}

		for _, vt := range validVectorTypes {
			field := &schemapb.FieldSchema{
				Name:        "vector_array_" + vt.String(),
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: vt,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
					{Key: common.MaxCapacityKey, Value: "100"},
				},
			}
			err := ValidateFieldsInStruct(field, schema)
			assert.NoError(t, err)
		}
	})
}

func TestValidateStructArrayField_MaxCapacity(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	t.Run("missing max_capacity is rejected", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name: "my_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "sub_a",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
				},
			},
		}

		err := ValidateStructArrayField(structField, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "type param(max_capacity) should be specified")
		assert.Contains(t, err.Error(), "sub_a")
	})

	t.Run("different max_capacity values in same struct are rejected", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name: "my_struct",
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "sub_a",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
					TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
				},
				{
					Name:        "sub_b",
					DataType:    schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "128"},
						{Key: common.MaxCapacityKey, Value: "200"},
					},
				},
			},
		}

		err := ValidateStructArrayField(structField, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "same max_capacity")
		assert.Contains(t, err.Error(), "my_struct")
		assert.Contains(t, err.Error(), "sub_b")
	})
}

func TestValidateStructArrayField_NullablePropagation(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}

	t.Run("nullable struct propagates to sub-fields", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name:     "my_struct",
			Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "sub_a",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "100"}},
				},
				{
					Name:        "sub_b",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_VarChar,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "100"}, {Key: "max_length", Value: "256"}},
				},
			},
		}

		err := ValidateStructArrayField(structField, schema)
		require.NoError(t, err)

		for _, subField := range structField.Fields {
			assert.True(t, subField.GetNullable(), "sub-field %s should be nullable after propagation", subField.Name)
		}
	})

	t.Run("non-nullable struct rejects nullable sub-fields", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name:     "my_struct",
			Nullable: false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "sub_a",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
					Nullable:    true,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "100"}},
				},
			},
		}

		err := ValidateStructArrayField(structField, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nullable individually")
	})

	t.Run("non-nullable struct with non-nullable sub-fields passes", func(t *testing.T) {
		structField := &schemapb.StructArrayFieldSchema{
			Name:     "my_struct",
			Nullable: false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:        "sub_a",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
					TypeParams:  []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "100"}},
				},
			},
		}

		err := ValidateStructArrayField(structField, schema)
		assert.NoError(t, err)
	})
}
