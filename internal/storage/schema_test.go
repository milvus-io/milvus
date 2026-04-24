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

package storage

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func TestConvertArrowSchema(t *testing.T) {
	fieldSchemas := []*schemapb.FieldSchema{
		{FieldID: 1, Name: "field0", DataType: schemapb.DataType_Bool},
		{FieldID: 2, Name: "field1", DataType: schemapb.DataType_Int8},
		{FieldID: 3, Name: "field2", DataType: schemapb.DataType_Int16},
		{FieldID: 4, Name: "field3", DataType: schemapb.DataType_Int32},
		{FieldID: 5, Name: "field4", DataType: schemapb.DataType_Int64},
		{FieldID: 6, Name: "field5", DataType: schemapb.DataType_Float},
		{FieldID: 7, Name: "field6", DataType: schemapb.DataType_Double},
		{FieldID: 8, Name: "field7", DataType: schemapb.DataType_String},
		{FieldID: 9, Name: "field8", DataType: schemapb.DataType_VarChar},
		{FieldID: 10, Name: "field9", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 11, Name: "field10", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 12, Name: "field11", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		{FieldID: 13, Name: "field12", DataType: schemapb.DataType_JSON},
		{FieldID: 14, Name: "field13", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 15, Name: "field14", DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 16, Name: "field15", DataType: schemapb.DataType_Int8Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 17, Name: "field16", DataType: schemapb.DataType_BinaryVector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 18, Name: "field17", DataType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 19, Name: "field18", DataType: schemapb.DataType_Float16Vector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 20, Name: "field19", DataType: schemapb.DataType_BFloat16Vector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 21, Name: "field20", DataType: schemapb.DataType_Int8Vector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 22, Name: "field21", DataType: schemapb.DataType_SparseFloatVector, Nullable: true},
	}

	StructArrayFieldSchemas := []*schemapb.StructArrayFieldSchema{
		{FieldID: 23, Name: "struct_field0", Fields: []*schemapb.FieldSchema{
			{FieldID: 24, Name: "field22", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
			{FieldID: 25, Name: "field23", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float},
		}},
	}

	schema := &schemapb.CollectionSchema{
		Fields:            fieldSchemas,
		StructArrayFields: StructArrayFieldSchemas,
	}
	arrowSchema, err := ConvertToArrowSchema(schema, false)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldSchemas)+len(StructArrayFieldSchemas[0].Fields), len(arrowSchema.Fields()))

	for i, field := range arrowSchema.Fields() {
		if i >= 16 && i <= 20 {
			dimVal, ok := field.Metadata.GetValue("dim")
			assert.True(t, ok, "nullable vector field should have dim metadata")
			assert.Equal(t, "128", dimVal)
		}
	}
}

func TestConvertArrowSchemaWithoutDim(t *testing.T) {
	fieldSchemas := []*schemapb.FieldSchema{
		{FieldID: 1, Name: "field0", DataType: schemapb.DataType_Bool},
		{FieldID: 2, Name: "field1", DataType: schemapb.DataType_Int8},
		{FieldID: 3, Name: "field2", DataType: schemapb.DataType_Int16},
		{FieldID: 4, Name: "field3", DataType: schemapb.DataType_Int32},
		{FieldID: 5, Name: "field4", DataType: schemapb.DataType_Int64},
		{FieldID: 6, Name: "field5", DataType: schemapb.DataType_Float},
		{FieldID: 7, Name: "field6", DataType: schemapb.DataType_Double},
		{FieldID: 8, Name: "field7", DataType: schemapb.DataType_String},
		{FieldID: 9, Name: "field8", DataType: schemapb.DataType_VarChar},
		{FieldID: 10, Name: "field9", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 11, Name: "field10", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 12, Name: "field11", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		{FieldID: 13, Name: "field12", DataType: schemapb.DataType_JSON},
		{FieldID: 14, Name: "field13", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{}},
		{FieldID: 15, Name: "field14", DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{}},
		{FieldID: 16, Name: "field15", DataType: schemapb.DataType_Int8Vector, TypeParams: []*commonpb.KeyValuePair{}},
	}

	schema := &schemapb.CollectionSchema{
		Fields: fieldSchemas,
	}
	_, err := ConvertToArrowSchema(schema, false)
	assert.Error(t, err)
}

func TestFilterRowIDFromSchema(t *testing.T) {
	t.Run("removes RowID field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: common.RowIDField, Name: "RowID", DataType: schemapb.DataType_Int64},
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			},
		}
		filtered := FilterRowIDFromSchema(schema)
		assert.Len(t, filtered.Fields, 2)
		for _, f := range filtered.Fields {
			assert.NotEqual(t, common.RowIDField, f.FieldID)
		}
	})

	t.Run("no RowID field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{
					FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}},
				},
			},
		}
		filtered := FilterRowIDFromSchema(schema)
		assert.Len(t, filtered.Fields, 2)
	})

	t.Run("deep copy correctness", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: common.RowIDField, Name: "RowID", DataType: schemapb.DataType_Int64},
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
			},
		}
		filtered := FilterRowIDFromSchema(schema)
		// mutate output
		filtered.Fields[0].Name = "MUTATED"
		// original unchanged
		assert.Equal(t, "pk", schema.Fields[1].Name)
	})

	t.Run("empty schema", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{}}
		filtered := FilterRowIDFromSchema(schema)
		assert.Len(t, filtered.Fields, 0)
	})
}

func TestOverrideTextFieldsToBinary(t *testing.T) {
	t.Run("TEXT fields converted to binary", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "content", DataType: schemapb.DataType_Text},
			},
		}
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
			{Name: "content", Type: arrow.BinaryTypes.String},
		}, nil)

		result := overrideTextFieldsToBinary(schema, arrowSchema)
		assert.Equal(t, arrow.BinaryTypes.Binary, result.Field(1).Type)
		// non-TEXT field unchanged
		assert.Equal(t, arrow.PrimitiveTypes.Int64, result.Field(0).Type)
	})

	t.Run("no TEXT fields returns same pointer", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "name", DataType: schemapb.DataType_VarChar},
			},
		}
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		}, nil)

		result := overrideTextFieldsToBinary(schema, arrowSchema)
		assert.True(t, result == arrowSchema) // same pointer
	})

	t.Run("mixed types with multiple TEXT", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "t1", DataType: schemapb.DataType_Text},
				{FieldID: 102, Name: "name", DataType: schemapb.DataType_VarChar},
				{FieldID: 103, Name: "t2", DataType: schemapb.DataType_Text},
			},
		}
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
			{Name: "t1", Type: arrow.BinaryTypes.String},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "t2", Type: arrow.BinaryTypes.String},
		}, nil)

		result := overrideTextFieldsToBinary(schema, arrowSchema)
		assert.Equal(t, arrow.PrimitiveTypes.Int64, result.Field(0).Type)
		assert.Equal(t, arrow.BinaryTypes.Binary, result.Field(1).Type) // TEXT → binary
		assert.Equal(t, arrow.BinaryTypes.String, result.Field(2).Type) // VarChar unchanged
		assert.Equal(t, arrow.BinaryTypes.Binary, result.Field(3).Type) // TEXT → binary
	})

	t.Run("arrow schema shorter than proto fields", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "content", DataType: schemapb.DataType_Text},
				{FieldID: 102, Name: "extra", DataType: schemapb.DataType_Text},
			},
		}
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
			{Name: "content", Type: arrow.BinaryTypes.String},
		}, nil)

		// should not panic even though proto has more fields
		result := overrideTextFieldsToBinary(schema, arrowSchema)
		assert.Equal(t, 2, result.NumFields())
		assert.Equal(t, arrow.BinaryTypes.Binary, result.Field(1).Type)
	})
}
