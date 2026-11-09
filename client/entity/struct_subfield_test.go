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

package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func elementNullStructSchema() *Schema {
	return NewSchema().WithName("coll").
		WithField(NewField().WithName("id").WithDataType(FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(NewField().WithName("clips").WithDataType(FieldTypeArray).WithElementType(FieldTypeStruct).
			WithMaxCapacity(10).WithNullable(true).
			WithStructSchema(NewStructSchema().
				WithField(NewField().WithName("lang").WithDataType(FieldTypeVarChar).WithMaxLength(16).WithNullable(true)).
				WithField(NewField().WithName("vec").WithDataType(FieldTypeFloatVector).WithDim(4).WithNullable(true)).
				WithField(NewField().WithName("score").WithDataType(FieldTypeInt64)).
				WithField(NewField().WithName("tags").WithDataType(FieldTypeArray).WithElementType(FieldTypeVarChar).
					WithMaxCapacity(8).WithMaxLength(32).WithNullable(true).WithElementNullable(true))))
}

func kv(params []*commonpb.KeyValuePair) map[string]string {
	return KvPairsMap(params)
}

func TestStructSubFieldProto(t *testing.T) {
	schema := elementNullStructSchema()
	require.NoError(t, schema.Validate())

	structProto := schema.ProtoMessage().GetStructArrayFields()[0]
	require.True(t, structProto.GetNullable())
	subs := map[string]*schemapb.FieldSchema{}
	for _, sub := range structProto.GetFields() {
		subs[sub.GetName()] = sub
		assert.True(t, sub.GetNullable(), "sub-field %s follows the struct row nullable", sub.GetName())
	}

	lang := subs["lang"]
	assert.Equal(t, schemapb.DataType_Array, lang.GetDataType())
	assert.Equal(t, schemapb.DataType_VarChar, lang.GetElementType())
	assert.True(t, lang.GetElementNullable())
	assert.Nil(t, lang.GetTypeSchema())
	assert.Equal(t, "10", kv(lang.GetTypeParams())[TypeParamMaxCapacity])

	vec := subs["vec"]
	assert.Equal(t, schemapb.DataType_ArrayOfVector, vec.GetDataType())
	assert.Equal(t, schemapb.DataType_FloatVector, vec.GetElementType())
	assert.True(t, vec.GetElementNullable())

	score := subs["score"]
	assert.False(t, score.GetElementNullable())

	tags := subs["tags"]
	assert.Equal(t, schemapb.DataType_Array, tags.GetDataType())
	assert.Equal(t, schemapb.DataType_Array, tags.GetElementType())
	assert.True(t, tags.GetElementNullable())
	// the inner max_capacity and the leaf max_length live in type_schema only
	assert.NotContains(t, kv(tags.GetTypeParams()), TypeParamMaxCapacity)
	assert.NotContains(t, kv(tags.GetTypeParams()), TypeParamMaxLength)
	root := tags.GetTypeSchema()
	require.NotNil(t, root)
	assert.True(t, root.GetNullable())
	assert.Equal(t, "10", kv(root.GetTypeParams())[TypeParamMaxCapacity])
	inner := root.GetArrayElement()
	require.NotNil(t, inner)
	assert.True(t, inner.GetNullable())
	assert.Equal(t, "8", kv(inner.GetTypeParams())[TypeParamMaxCapacity])
	leaf := inner.GetArrayElement()
	require.NotNil(t, leaf)
	assert.Equal(t, schemapb.DataType_VarChar, leaf.GetLeafType())
	assert.True(t, leaf.GetNullable())
	assert.Equal(t, "32", kv(leaf.GetTypeParams())[TypeParamMaxLength])
}

func TestStructSubFieldProtoWithoutStructCapacity(t *testing.T) {
	sub := NewField().WithName("clips[tags]").WithDataType(FieldTypeArray).WithElementType(FieldTypeInt64).
		WithMaxCapacity(4).WithNullable(true)
	p := StructSubFieldProto(sub, false, "")
	assert.Equal(t, "clips[tags]", p.GetName())
	assert.False(t, p.GetNullable())
	assert.Empty(t, p.GetTypeSchema().GetTypeParams())
	assert.Equal(t, "4", kv(p.GetTypeSchema().GetArrayElement().GetTypeParams())[TypeParamMaxCapacity])
	assert.False(t, p.GetTypeSchema().GetArrayElement().GetArrayElement().GetNullable())

	scalar := NewField().WithName("lang").WithDataType(FieldTypeVarChar).WithMaxLength(8).WithNullable(true)
	p = StructSubFieldProto(scalar, false, "")
	assert.NotContains(t, kv(p.GetTypeParams()), TypeParamMaxCapacity)
	assert.True(t, p.GetElementNullable())
}

func TestStructCapacityFromSubField(t *testing.T) {
	// a struct without its own max_capacity takes it from a scalar sub-field
	field := NewField().WithName("clips").WithDataType(FieldTypeArray).WithElementType(FieldTypeStruct).
		WithStructSchema(NewStructSchema().
			WithField(NewField().WithName("score").WithDataType(FieldTypeInt64).WithMaxCapacity(6)).
			WithField(NewField().WithName("tags").WithDataType(FieldTypeArray).WithElementType(FieldTypeInt64).WithMaxCapacity(3)))
	structProto := NewSchema().WithField(field).ProtoMessage().GetStructArrayFields()[0]
	tags := structProto.GetFields()[1]
	assert.Equal(t, "6", kv(tags.GetTypeSchema().GetTypeParams())[TypeParamMaxCapacity])
	assert.Equal(t, "3", kv(tags.GetTypeSchema().GetArrayElement().GetTypeParams())[TypeParamMaxCapacity])
}

func TestStructSubFieldRoundTrip(t *testing.T) {
	schema := elementNullStructSchema()
	restored := NewSchema().ReadProto(schema.ProtoMessage())
	require.NoError(t, restored.Validate())

	clips := restored.Fields[1]
	assert.True(t, clips.Nullable)
	assert.Equal(t, "10", clips.TypeParams[TypeParamMaxCapacity])
	subs := map[string]*Field{}
	for _, sub := range clips.StructSchema.Fields {
		subs[sub.Name] = sub
	}

	assert.Equal(t, FieldTypeVarChar, subs["lang"].DataType)
	assert.True(t, subs["lang"].Nullable)
	assert.False(t, subs["lang"].ElementNullable)
	assert.Equal(t, FieldTypeFloatVector, subs["vec"].DataType)
	assert.True(t, subs["vec"].Nullable)
	assert.False(t, subs["score"].Nullable)

	tags := subs["tags"]
	assert.Equal(t, FieldTypeArray, tags.DataType)
	assert.Equal(t, FieldTypeVarChar, tags.ElementType)
	assert.True(t, tags.Nullable)
	assert.True(t, tags.ElementNullable)
	assert.Equal(t, "8", tags.TypeParams[TypeParamMaxCapacity])
	assert.Equal(t, "32", tags.TypeParams[TypeParamMaxLength])

	// converting the restored schema again yields the same wire schema
	again := restored.ProtoMessage().GetStructArrayFields()[0]
	for i, sub := range again.GetFields() {
		orig := schema.ProtoMessage().GetStructArrayFields()[0].GetFields()[i]
		assert.Equal(t, orig.GetElementNullable(), sub.GetElementNullable(), sub.GetName())
		assert.Equal(t, orig.GetTypeSchema().String(), sub.GetTypeSchema().String(), sub.GetName())
	}
}

func TestStructSubFieldFromProtoWithoutElementNullable(t *testing.T) {
	// sub-fields described without element_nullable are plain sub-fields
	p := &schemapb.StructArrayFieldSchema{
		Name:     "clips",
		Nullable: true,
		Fields: []*schemapb.FieldSchema{{
			Name:        "score",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			Nullable:    true,
			TypeParams:  []*commonpb.KeyValuePair{{Key: TypeParamMaxCapacity, Value: "5"}},
		}},
	}
	field := structArrayFieldFromProto(p)
	require.Len(t, field.StructSchema.Fields, 1)
	sub := field.StructSchema.Fields[0]
	assert.Equal(t, FieldTypeInt64, sub.DataType)
	assert.False(t, sub.Nullable)
	assert.Equal(t, "5", field.TypeParams[TypeParamMaxCapacity])
	assert.NoError(t, field.StructSchema.Validate(field.Name))
}

func TestStructSchemaValidateElementNull(t *testing.T) {
	valid := NewStructSchema().
		WithField(NewField().WithName("a").WithDataType(FieldTypeInt64).WithNullable(true)).
		WithField(NewField().WithName("b").WithDataType(FieldTypeArray).WithElementType(FieldTypeInt32).WithElementNullable(true))
	assert.NoError(t, valid.Validate("s"))

	for name, sub := range map[string]*Field{
		"nested struct": NewField().WithName("x").WithDataType(FieldTypeArray).WithElementType(FieldTypeStruct),
		"nested array":  NewField().WithName("x").WithDataType(FieldTypeArray).WithElementType(FieldTypeArray),
		"vector leaf":   NewField().WithName("x").WithDataType(FieldTypeArray).WithElementType(FieldTypeFloatVector),
		"json leaf":     NewField().WithName("x").WithDataType(FieldTypeArray).WithElementType(FieldTypeJSON),
		"element nullable scalar": NewField().WithName("x").WithDataType(FieldTypeInt64).
			WithElementNullable(true),
	} {
		assert.Error(t, NewStructSchema().WithField(sub).Validate("s"), name)
	}
}
