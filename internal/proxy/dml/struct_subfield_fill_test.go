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

package dml

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/fieldvalidator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func structSubFieldFillTestSchema() *schemapb.CollectionSchema {
	capacity := []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "8"}}
	return &schemapb.CollectionSchema{
		Name: "test_collection",
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 100, Name: "s", Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 101, Name: "s[ref]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, Nullable: true, TypeParams: capacity},
				{FieldID: 102, Name: "s[scalar]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, Nullable: true, ElementNullable: true, TypeParams: capacity},
				{FieldID: 103, Name: "s[vector]", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "8"}, {Key: common.DimKey, Value: "2"}}},
				{
					FieldID: 104, Name: "s[nested]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: true, TypeParams: capacity,
					TypeSchema: &schemapb.TypeSchema{Nullable: true, TypeParams: capacity, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
						Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "4"}}, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
							Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int32},
						}},
					}}},
				},
			},
		}},
	}
}

func structSubFieldFillTestMsg(children ...*schemapb.FieldData) *msgstream.InsertMsg {
	return &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{
		CollectionName: "test_collection", NumRows: 3,
		FieldsData: []*schemapb.FieldData{{
			FieldName: "s", Type: schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}},
		}},
	}}
}

func TestFillOmittedStructSubFieldsScalarVectorNested(t *testing.T) {
	schema := structSubFieldFillTestSchema()
	reference := structElementCountTestScalarArray("ref", []int32{1, 2}, []int32{})
	typeutil.SetFieldDataValidData(reference, []bool{true, false, true})
	msg := structSubFieldFillTestMsg(reference)
	require.NoError(t, checkAndFlattenStructFieldData(schema, msg))
	require.Len(t, msg.GetFieldsData(), 4)
	byName := make(map[string]*schemapb.FieldData)
	for _, field := range msg.GetFieldsData() {
		byName[field.GetFieldName()] = field
	}
	for _, name := range []string{"s[scalar]", "s[vector]", "s[nested]"} {
		field := byName[name]
		require.NotNil(t, field)
		assert.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(field))
	}
	scalarRows := byName["s[scalar]"].GetScalars().GetArrayData().GetData()
	require.Len(t, scalarRows, 2)
	assert.Equal(t, []bool{false, false}, scalarRows[0].GetValidData())
	assert.Empty(t, scalarRows[0].GetLongData().GetData())
	assert.Empty(t, scalarRows[1].GetValidData())
	assert.NotNil(t, scalarRows[1].GetLongData())

	vectorRows := byName["s[vector]"].GetVectors().GetVectorArray().GetData()
	require.Len(t, vectorRows, 2)
	assert.Equal(t, int64(2), vectorRows[0].GetDim())
	assert.Equal(t, []bool{false, false}, vectorRows[0].GetValidData())
	assert.Empty(t, vectorRows[0].GetFloatVector().GetData())
	assert.Empty(t, vectorRows[1].GetValidData())

	nestedRows := byName["s[nested]"].GetScalars().GetArrayData().GetData()
	require.Len(t, nestedRows, 2)
	assert.Equal(t, schemapb.DataType_Int32, nestedRows[0].GetArrayData().GetElementType())
	assert.Equal(t, []bool{false, false}, nestedRows[0].GetValidData())
	assert.Empty(t, nestedRows[0].GetArrayData().GetData())
	assert.Empty(t, nestedRows[1].GetValidData())

	schemaInfo := mustNewSchemaInfo(schema)
	require.NoError(t, fillFieldPropertiesOnly(msg.FieldsData, schemaInfo))
	require.NoError(t, fieldvalidator.NewValidateUtil().Validate(msg.FieldsData, schemaInfo.SchemaHelper, 3))
	assert.Len(t, byName["s[scalar]"].GetScalars().GetArrayData().GetData()[0].GetLongData().GetData(), 2)
	nestedDense := byName["s[nested]"].GetScalars().GetArrayData().GetData()[0].GetArrayData().GetData()
	require.Len(t, nestedDense, 2)
	for _, inner := range nestedDense {
		assert.NotNil(t, inner.GetIntData())
		assert.Empty(t, inner.GetIntData().GetData())
	}
	// VectorArray keeps compact vector bytes; valid_data carries the two null elements.
	assert.Empty(t, byName["s[vector]"].GetVectors().GetVectorArray().GetData()[0].GetFloatVector().GetData())
	assert.Equal(t, []bool{false, false}, byName["s[vector]"].GetVectors().GetVectorArray().GetData()[0].GetValidData())
}

func TestFillOmittedStructSubFieldsRequiredAndStale(t *testing.T) {
	schema := structSubFieldFillTestSchema()
	schema.StructArrayFields[0].Fields[1].ElementNullable = false
	reference := structElementCountTestScalarArray("ref", []int32{1}, []int32{}, []int32{})
	msg := structSubFieldFillTestMsg(reference)
	require.ErrorContains(t, checkAndFlattenStructFieldData(schema, msg), "sub-field \"s[scalar]\" of struct field \"s\" is required")

	schema.StructArrayFields[0].Fields[1].ElementNullable = true
	for _, child := range []*schemapb.FieldData{
		structElementCountTestScalarArray("s[removed]", []int32{1}, []int32{}, []int32{}),
		{FieldName: "s[removed]", Type: schemapb.DataType_Array},
	} {
		msg = structSubFieldFillTestMsg(child)
		require.ErrorContains(t, checkAndFlattenStructFieldData(schema, msg), "sub-field \"s[removed]\" does not exist in struct field \"s\"")
	}
}

func TestFillOmittedStructSubFieldsAllNullStruct(t *testing.T) {
	schema := structSubFieldFillTestSchema()
	msg := structSubFieldFillTestMsg()
	require.NoError(t, checkAndFlattenStructFieldData(schema, msg))
	assert.Empty(t, msg.GetFieldsData())
	require.NoError(t, checkFieldsDataBySchema(t.Context(), typeutil.GetAllFieldSchemas(schema), schema, msg, true))
	require.Len(t, msg.GetFieldsData(), 4)
	for _, child := range msg.GetFieldsData() {
		assert.Equal(t, []bool{false, false, false}, typeutil.GetFieldDataValidData(child))
	}
}
