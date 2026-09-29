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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func testArrayTypeSchema(element *schemapb.TypeSchema, params ...*commonpb.KeyValuePair) *schemapb.TypeSchema {
	return &schemapb.TypeSchema{
		Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: element},
		TypeParams: params,
	}
}

func TestValidateFieldNestedArrayElementNullable(t *testing.T) {
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
	innerArray := func() *schemapb.TypeSchema {
		return testArrayTypeSchema(
			&schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}},
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"},
		)
	}

	t.Run("nullable nested array element matches element_nullable", func(t *testing.T) {
		child := innerArray()
		child.Nullable = true
		field := nestedField(child)
		field.ElementNullable = true
		require.NoError(t, ValidateFieldsInStruct(field, schema))
	})

	t.Run("nullable nested array element without element_nullable is rejected", func(t *testing.T) {
		child := innerArray()
		child.Nullable = true
		field := nestedField(child)
		require.ErrorContains(t, ValidateFieldsInStruct(field, schema),
			"inner nullable=true must match element_nullable=false")
	})

	t.Run("nested element nullable at top level is rejected", func(t *testing.T) {
		field := nestedField(innerArray())
		field.ElementNullable = true
		require.ErrorContains(t, ValidateField(field, schema),
			"element_nullable is only supported for Array and ArrayOfVector sub-fields")
	})
}

func TestValidateStructArrayFieldNestedNullable(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "test_collection"}
	for _, rowNullable := range []bool{false, true} {
		for _, elementNullable := range []bool{false, true} {
			for _, leafNullable := range []bool{false, true} {
				field := &schemapb.FieldSchema{
					Name: "nested", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
					ElementNullable: elementNullable,
					TypeSchema: testArrayTypeSchema(
						testArrayTypeSchema(&schemapb.TypeSchema{
							Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}, Nullable: leafNullable,
						}, &commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"}),
						&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "100"}),
				}
				field.TypeSchema.GetArrayElement().Nullable = elementNullable
				structField := &schemapb.StructArrayFieldSchema{Name: "items", Nullable: rowNullable, Fields: []*schemapb.FieldSchema{field}}
				require.NoError(t, ValidateStructArrayField(structField, schema))
				require.Equal(t, rowNullable, field.GetNullable())
				require.Equal(t, rowNullable, field.GetTypeSchema().GetNullable())
				require.Equal(t, elementNullable, field.GetTypeSchema().GetArrayElement().GetNullable())
				require.Equal(t, leafNullable, field.GetTypeSchema().GetArrayElement().GetArrayElement().GetNullable())
			}
		}
	}

	field := &schemapb.FieldSchema{
		Name: "nested", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
		ElementNullable: true,
		TypeSchema: testArrayTypeSchema(testArrayTypeSchema(
			&schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}},
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "10"}),
			&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "100"}),
	}
	err := ValidateStructArrayField(&schemapb.StructArrayFieldSchema{Name: "items", Fields: []*schemapb.FieldSchema{field}}, schema)
	require.ErrorContains(t, err, "inner nullable=false must match element_nullable=true")

	single := &schemapb.FieldSchema{
		Name: "single", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
		ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
	}
	require.NoError(t, ValidateStructArrayField(&schemapb.StructArrayFieldSchema{Name: "items", Nullable: true, Fields: []*schemapb.FieldSchema{single}}, schema))
	require.True(t, single.GetNullable())
	require.ErrorContains(t, ValidateField(single, schema), "element_nullable is only supported")
}
