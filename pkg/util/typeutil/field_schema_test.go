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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

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

	t.Run("unset", func(t *testing.T) {
		require.NoError(t, ValidateFieldTypeSchema(&schemapb.FieldSchema{
			DataType: schemapb.DataType_Int64,
		}))
	})

	t.Run("matching leaf", func(t *testing.T) {
		require.NoError(t, ValidateFieldTypeSchema(&schemapb.FieldSchema{
			DataType:   schemapb.DataType_Int64,
			TypeSchema: leaf(schemapb.DataType_Int64),
		}))
	})

	t.Run("matching array", func(t *testing.T) {
		require.NoError(t, ValidateFieldTypeSchema(&schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int32,
			TypeSchema:  array(leaf(schemapb.DataType_Int32)),
		}))
	})

	t.Run("matching recursive array", func(t *testing.T) {
		require.NoError(t, ValidateFieldTypeSchema(&schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array,
			TypeSchema:  array(array(leaf(schemapb.DataType_Int32))),
		}))
	})

	t.Run("data type mismatch", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:       "field",
			DataType:   schemapb.DataType_Array,
			TypeSchema: leaf(schemapb.DataType_Int64),
		})
		require.ErrorContains(t, err, "does not match data_type")
	})

	t.Run("element type mismatch", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:        "field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int32,
			TypeSchema:  array(array(leaf(schemapb.DataType_Int32))),
		})
		require.ErrorContains(t, err, "does not match element_type")
	})

	t.Run("leaf field element type mismatch", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:        "field",
			DataType:    schemapb.DataType_Int64,
			ElementType: schemapb.DataType_Int32,
			TypeSchema:  leaf(schemapb.DataType_Int64),
		})
		require.ErrorContains(t, err, "does not match element_type")
	})

	t.Run("root array must use array element", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:        "field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int32,
			TypeSchema:  leaf(schemapb.DataType_Array),
		})
		require.ErrorContains(t, err, "must use array_element")
	})

	t.Run("nested array must use array element", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:        "field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array,
			TypeSchema:  array(leaf(schemapb.DataType_Array)),
		})
		require.ErrorContains(t, err, "must use array_element")
	})

	t.Run("deeply nested array must use array element", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:        "field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array,
			TypeSchema:  array(array(leaf(schemapb.DataType_Array))),
		})
		require.ErrorContains(t, err, "must use array_element")
	})

	t.Run("deeply nested kind is required", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:        "field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array,
			TypeSchema:  array(array(&schemapb.TypeSchema{})),
		})
		require.ErrorContains(t, err, "kind should be specified")
	})

	t.Run("missing kind", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:       "field",
			DataType:   schemapb.DataType_Int64,
			TypeSchema: &schemapb.TypeSchema{},
		})
		require.ErrorContains(t, err, "kind should be specified")
	})

	t.Run("missing array element", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:        "field",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeSchema: &schemapb.TypeSchema{
				Kind: &schemapb.TypeSchema_ArrayElement{},
			},
		})
		require.ErrorContains(t, err, "array_element should be specified")
	})

	t.Run("invalid leaf type", func(t *testing.T) {
		err := ValidateFieldTypeSchema(&schemapb.FieldSchema{
			Name:       "field",
			DataType:   schemapb.DataType(999),
			TypeSchema: leaf(schemapb.DataType(999)),
		})
		require.ErrorContains(t, err, "leaf_type 999 is not valid")
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
