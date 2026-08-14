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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestValidateArrayElementType(t *testing.T) {
	for _, dataType := range []schemapb.DataType{
		schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar,
	} {
		require.NoError(t, ValidateArrayElementType(dataType))
	}

	for _, dataType := range []schemapb.DataType{
		schemapb.DataType_String,
		schemapb.DataType_Text,
		schemapb.DataType_Timestamptz,
		schemapb.DataType_JSON,
		schemapb.DataType_FloatVector,
	} {
		require.Error(t, ValidateArrayElementType(dataType))
	}
}

func TestValidateFieldTypeSchema(t *testing.T) {
	leaf := func(dataType schemapb.DataType) *schemapb.TypeSchema {
		return &schemapb.TypeSchema{
			Kind: &schemapb.TypeSchema_LeafType{LeafType: dataType},
		}
	}
	array := func(element *schemapb.TypeSchema) *schemapb.TypeSchema {
		return &schemapb.TypeSchema{
			Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: element},
		}
	}

	t.Run("legacy scalar", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
		})
		require.NoError(t, err)
	})

	t.Run("legacy array", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int32,
		})
		require.NoError(t, err)
	})

	t.Run("legacy validation is delegated", func(t *testing.T) {
		require.NoError(t, ValidateFieldTypeSchema(&schemapb.FieldSchema{}))
	})

	t.Run("nested array", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array,
			TypeSchema:  array(array(leaf(schemapb.DataType_Int32))),
		})
		require.NoError(t, err)
	})

	t.Run("scalar type schema is rejected", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:       "field",
			DataType:   schemapb.DataType_Int64,
			TypeSchema: leaf(schemapb.DataType_Int64),
		})
		require.ErrorContains(t, err, "only supported for nested array")
	})

	t.Run("single-level array type schema is rejected", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:        "field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int32,
			TypeSchema:  array(leaf(schemapb.DataType_Int32)),
		})
		require.ErrorContains(t, err, "only supported for nested array")
	})

	t.Run("nested array requires type schema", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:        "field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array,
		})
		require.ErrorContains(t, err, "element type Array is not supported")
		require.ErrorContains(t, err, "must specify type_schema")
	})

	t.Run("nested array requires array compatibility fields", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:       "field",
			TypeSchema: array(array(leaf(schemapb.DataType_Int32))),
		})
		require.ErrorContains(t, err, "must specify data_type Array and element_type Array")
	})

	t.Run("array leaf must use array element", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:       "field",
			TypeSchema: array(array(leaf(schemapb.DataType_Array))),
		})
		require.ErrorContains(t, err, "must use array_element")
	})

	t.Run("deeply nested kind is required", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:       "field",
			TypeSchema: array(array(&schemapb.TypeSchema{})),
		})
		require.ErrorContains(t, err, "kind should be specified")
	})

	t.Run("missing kind", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:       "field",
			TypeSchema: &schemapb.TypeSchema{},
		})
		require.ErrorContains(t, err, "kind should be specified")
	})

	t.Run("missing array element", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name: "field",
			TypeSchema: &schemapb.TypeSchema{
				Kind: &schemapb.TypeSchema_ArrayElement{},
			},
		})
		require.ErrorContains(t, err, "array_element should be specified")
	})

	t.Run("invalid leaf type", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:       "field",
			TypeSchema: array(array(leaf(schemapb.DataType(999)))),
		})
		require.ErrorContains(t, err, "leaf_type 999 is not valid")
	})

	t.Run("unsupported array leaf type", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:        "field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array,
			TypeSchema:  array(array(leaf(schemapb.DataType_FloatVector))),
		})
		require.ErrorContains(t, err, "element type FloatVector is not supported")
	})

	t.Run("duplicate type schema params are rejected at every level", func(t *testing.T) {
		for _, duplicateAt := range []func(*schemapb.TypeSchema){
			func(typeSchema *schemapb.TypeSchema) {
				typeSchema.TypeParams = []*commonpb.KeyValuePair{
					{Key: "max_capacity", Value: "10"},
					{Key: "max_capacity", Value: "20"},
				}
			},
			func(typeSchema *schemapb.TypeSchema) {
				typeSchema.GetArrayElement().TypeParams = []*commonpb.KeyValuePair{
					{Key: "max_capacity", Value: "10"},
					{Key: "max_capacity", Value: "20"},
				}
			},
		} {
			root := array(array(leaf(schemapb.DataType_Int32)))
			duplicateAt(root)
			err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
				Name:        "field",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Array,
				TypeSchema:  root,
			})
			require.ErrorContains(t, err, "duplicated type_schema param key")
		}
	})
}

func TestIsNestedArrayTypeSchema(t *testing.T) {
	leaf := func(dataType schemapb.DataType) *schemapb.TypeSchema {
		return &schemapb.TypeSchema{
			Kind: &schemapb.TypeSchema_LeafType{LeafType: dataType},
		}
	}
	array := func(element *schemapb.TypeSchema) *schemapb.TypeSchema {
		return &schemapb.TypeSchema{
			Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: element},
		}
	}

	require.False(t, IsNestedArrayTypeSchema(nil))
	require.False(t, IsNestedArrayTypeSchema(leaf(schemapb.DataType_Int64)))
	require.False(t, IsNestedArrayTypeSchema(array(leaf(schemapb.DataType_Int64))))
	require.True(t, IsNestedArrayTypeSchema(array(array(leaf(schemapb.DataType_Int64)))))
	require.True(t, IsNestedArrayTypeSchema(array(array(array(leaf(schemapb.DataType_Int64))))))
}
