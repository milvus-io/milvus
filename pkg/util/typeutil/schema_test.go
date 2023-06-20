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
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
)

func TestSchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name:        "testColl",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "field_int8",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     2,
			},
			{
				FieldID:      101,
				Name:         "field_int16",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     3,
			},
			{
				FieldID:      102,
				Name:         "field_int32",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     4,
			},
			{
				FieldID:      103,
				Name:         "field_int64",
				IsPrimaryKey: true,
				Description:  "",
				DataType:     5,
			},
			{
				FieldID:      104,
				Name:         "field_float",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     10,
			},
			{
				FieldID:      105,
				Name:         "field_double",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     11,
			},
			{
				FieldID:      106,
				Name:         "field_string",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     21,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "125",
					},
				},
			},
			{
				FieldID:      107,
				Name:         "field_float_vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     101,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "nlist",
						Value: "128",
					},
				},
			},
			{
				FieldID:      108,
				Name:         "field_binary_vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     100,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
			{
				FieldID:     109,
				Name:        "field_array",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
			},
			{
				FieldID:  110,
				Name:     "field_json",
				DataType: schemapb.DataType_JSON,
			},
		},
	}

	t.Run("EstimateSizePerRecord", func(t *testing.T) {
		size, err := EstimateSizePerRecord(schema)
		assert.Equal(t, 680+DynamicFieldMaxLength*2, size)
		assert.NoError(t, err)
	})

	t.Run("SchemaHelper", func(t *testing.T) {
		_, err := CreateSchemaHelper(nil)
		assert.Error(t, err)

		helper, err := CreateSchemaHelper(schema)
		assert.NoError(t, err)

		field, err := helper.GetPrimaryKeyField()
		assert.NoError(t, err)
		assert.Equal(t, "field_int64", field.Name)

		field1, err := helper.GetFieldFromName("field_int8")
		assert.NoError(t, err)
		assert.Equal(t, "field_int8", field1.Name)

		field2, err := helper.GetFieldFromID(102)
		assert.NoError(t, err)
		assert.Equal(t, "field_int32", field2.Name)

		dim, err := helper.GetVectorDimFromID(107)
		assert.NoError(t, err)
		assert.Equal(t, 128, dim)
		dim1, err := helper.GetVectorDimFromID(108)
		assert.NoError(t, err)
		assert.Equal(t, 128, dim1)
		_, err = helper.GetVectorDimFromID(103)
		assert.Error(t, err)
	})

	t.Run("Type", func(t *testing.T) {
		assert.False(t, IsVectorType(schemapb.DataType_Bool))
		assert.False(t, IsVectorType(schemapb.DataType_Int8))
		assert.False(t, IsVectorType(schemapb.DataType_Int16))
		assert.False(t, IsVectorType(schemapb.DataType_Int32))
		assert.False(t, IsVectorType(schemapb.DataType_Int64))
		assert.False(t, IsVectorType(schemapb.DataType_Float))
		assert.False(t, IsVectorType(schemapb.DataType_Double))
		assert.False(t, IsVectorType(schemapb.DataType_String))
		assert.True(t, IsVectorType(schemapb.DataType_BinaryVector))
		assert.True(t, IsVectorType(schemapb.DataType_FloatVector))

		assert.False(t, IsIntegerType(schemapb.DataType_Bool))
		assert.True(t, IsIntegerType(schemapb.DataType_Int8))
		assert.True(t, IsIntegerType(schemapb.DataType_Int16))
		assert.True(t, IsIntegerType(schemapb.DataType_Int32))
		assert.True(t, IsIntegerType(schemapb.DataType_Int64))
		assert.False(t, IsIntegerType(schemapb.DataType_Float))
		assert.False(t, IsIntegerType(schemapb.DataType_Double))
		assert.False(t, IsIntegerType(schemapb.DataType_String))
		assert.False(t, IsIntegerType(schemapb.DataType_BinaryVector))
		assert.False(t, IsIntegerType(schemapb.DataType_FloatVector))

		assert.False(t, IsFloatingType(schemapb.DataType_Bool))
		assert.False(t, IsFloatingType(schemapb.DataType_Int8))
		assert.False(t, IsFloatingType(schemapb.DataType_Int16))
		assert.False(t, IsFloatingType(schemapb.DataType_Int32))
		assert.False(t, IsFloatingType(schemapb.DataType_Int64))
		assert.True(t, IsFloatingType(schemapb.DataType_Float))
		assert.True(t, IsFloatingType(schemapb.DataType_Double))
		assert.False(t, IsFloatingType(schemapb.DataType_String))
		assert.False(t, IsFloatingType(schemapb.DataType_BinaryVector))
		assert.False(t, IsFloatingType(schemapb.DataType_FloatVector))
	})
}

func TestSchema_GetVectorFieldSchema(t *testing.T) {

	schemaNormal := &schemapb.CollectionSchema{
		Name:        "testColl",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "field_int64",
				IsPrimaryKey: true,
				Description:  "",
				DataType:     5,
			},
			{
				FieldID:      107,
				Name:         "field_float_vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     101,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
		},
	}

	t.Run("GetVectorFieldSchema", func(t *testing.T) {
		fieldSchema, err := GetVectorFieldSchema(schemaNormal)
		assert.Equal(t, "field_float_vector", fieldSchema.Name)
		assert.NoError(t, err)
	})

	schemaInvalid := &schemapb.CollectionSchema{
		Name:        "testColl",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "field_int64",
				IsPrimaryKey: true,
				Description:  "",
				DataType:     5,
			},
		},
	}

	t.Run("GetVectorFieldSchemaInvalid", func(t *testing.T) {
		_, err := GetVectorFieldSchema(schemaInvalid)
		assert.Error(t, err)
	})

}

func TestSchema_invalid(t *testing.T) {
	t.Run("Duplicate field name", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     2,
				},
				{
					FieldID:      101,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     3,
				},
			},
		}
		_, err := CreateSchemaHelper(schema)
		assert.Error(t, err)
		assert.EqualError(t, err, "duplicated fieldName: field_int8")
	})
	t.Run("Duplicate field id", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     2,
				},
				{
					FieldID:      100,
					Name:         "field_int16",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     3,
				},
			},
		}
		_, err := CreateSchemaHelper(schema)
		assert.Error(t, err)
		assert.EqualError(t, err, "duplicated fieldID: 100")
	})
	t.Run("Duplicated primary key", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int8",
					IsPrimaryKey: true,
					Description:  "",
					DataType:     2,
				},
				{
					FieldID:      101,
					Name:         "field_int16",
					IsPrimaryKey: true,
					Description:  "",
					DataType:     3,
				},
			},
		}
		_, err := CreateSchemaHelper(schema)
		assert.Error(t, err)
		assert.EqualError(t, err, "primary key is not unique")
	})
	t.Run("field not exist", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     2,
				},
			},
		}
		helper, err := CreateSchemaHelper(schema)
		assert.NoError(t, err)

		_, err = helper.GetPrimaryKeyField()
		assert.Error(t, err)
		assert.EqualError(t, err, "failed to get primary key field: no primary in schema")

		_, err = helper.GetFieldFromName("none")
		assert.Error(t, err)
		assert.EqualError(t, err, "failed to get field schema by name: fieldName(none) not found")

		_, err = helper.GetFieldFromID(101)
		assert.Error(t, err)
		assert.EqualError(t, err, "fieldID(101) not found")
	})
	t.Run("vector dim not exist", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "testColl",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      103,
					Name:         "field_int64",
					IsPrimaryKey: true,
					Description:  "",
					DataType:     5,
				},
				{
					FieldID:      107,
					Name:         "field_float_vector",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     101,
				},
			},
		}
		helper, err := CreateSchemaHelper(schema)
		assert.NoError(t, err)

		_, err = helper.GetVectorDimFromID(100)
		assert.Error(t, err)

		_, err = helper.GetVectorDimFromID(103)
		assert.Error(t, err)

		_, err = helper.GetVectorDimFromID(107)
		assert.Error(t, err)
	})
}

func genFieldData(fieldName string, fieldID int64, fieldType schemapb.DataType, fieldValue interface{}, dim int64) *schemapb.FieldData {
	var fieldData *schemapb.FieldData
	switch fieldType {
	case schemapb.DataType_Bool:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Bool,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: fieldValue.([]bool),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int8:
		data := []int32{}
		for _, v := range fieldValue.([]int8) {
			data = append(data, int32(v))
		}
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int8,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: data,
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int16:
		data := []int32{}
		for _, v := range fieldValue.([]int16) {
			data = append(data, int32(v))
		}
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int16,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: data,
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int32:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int32,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: fieldValue.([]int32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Int64:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: fieldValue.([]int64),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Float:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Float,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: fieldValue.([]float32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Double:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Double,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: fieldValue.([]float64),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_String:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_String,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: fieldValue.([]string),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_VarChar:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: fieldValue.([]string),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_BinaryVector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_BinaryVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: fieldValue.([]byte),
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_FloatVector:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_FloatVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: fieldValue.([]float32),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	case schemapb.DataType_Array:
		data := fieldValue.([][]int32)
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Array,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							Data:        []*schemapb.ScalarField{},
							ElementType: schemapb.DataType_Int32,
						},
					},
				},
			},
		}

		for _, list := range data {
			arrayList := fieldData.GetScalars().GetArrayData()
			arrayList.Data = append(arrayList.Data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: list,
					},
				},
			})
		}

	case schemapb.DataType_JSON:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_JSON,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: fieldValue.([][]byte),
						},
					},
				},
			},
			FieldId: fieldID,
		}
	default:
		log.Error("not supported field type", zap.String("field type", fieldType.String()))
	}

	return fieldData
}

func TestAppendFieldData(t *testing.T) {
	const (
		Dim                   = 8
		BoolFieldName         = "BoolField"
		Int32FieldName        = "Int32Field"
		Int64FieldName        = "Int64Field"
		FloatFieldName        = "FloatField"
		DoubleFieldName       = "DoubleField"
		BinaryVectorFieldName = "BinaryVectorField"
		FloatVectorFieldName  = "FloatVectorField"
		BoolFieldID           = common.StartOfUserFieldID + 1
		Int32FieldID          = common.StartOfUserFieldID + 2
		Int64FieldID          = common.StartOfUserFieldID + 3
		FloatFieldID          = common.StartOfUserFieldID + 4
		DoubleFieldID         = common.StartOfUserFieldID + 5
		BinaryVectorFieldID   = common.StartOfUserFieldID + 6
		FloatVectorFieldID    = common.StartOfUserFieldID + 7
	)
	BoolArray := []bool{true, false}
	Int32Array := []int32{1, 2}
	Int64Array := []int64{11, 22}
	FloatArray := []float32{1.0, 2.0}
	DoubleArray := []float64{11.0, 22.0}
	BinaryVector := []byte{0x12, 0x34}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}

	result := make([]*schemapb.FieldData, 7)
	var fieldDataArray1 []*schemapb.FieldData
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BoolFieldName, BoolFieldID, schemapb.DataType_Bool, BoolArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int32FieldName, Int32FieldID, schemapb.DataType_Int32, Int32Array[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatFieldName, FloatFieldID, schemapb.DataType_Float, FloatArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(DoubleFieldName, DoubleFieldID, schemapb.DataType_Double, DoubleArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BinaryVectorFieldName, BinaryVectorFieldID, schemapb.DataType_BinaryVector, BinaryVector[0:Dim/8], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:Dim], Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BoolFieldName, BoolFieldID, schemapb.DataType_Bool, BoolArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int32FieldName, Int32FieldID, schemapb.DataType_Int32, Int32Array[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatFieldName, FloatFieldID, schemapb.DataType_Float, FloatArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(DoubleFieldName, DoubleFieldID, schemapb.DataType_Double, DoubleArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BinaryVectorFieldName, BinaryVectorFieldID, schemapb.DataType_BinaryVector, BinaryVector[Dim/8:2*Dim/8], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[Dim:2*Dim], Dim))

	AppendFieldData(result, fieldDataArray1, 0)
	AppendFieldData(result, fieldDataArray2, 0)

	assert.Equal(t, BoolArray, result[0].GetScalars().GetBoolData().Data)
	assert.Equal(t, Int32Array, result[1].GetScalars().GetIntData().Data)
	assert.Equal(t, Int64Array, result[2].GetScalars().GetLongData().Data)
	assert.Equal(t, FloatArray, result[3].GetScalars().GetFloatData().Data)
	assert.Equal(t, DoubleArray, result[4].GetScalars().GetDoubleData().Data)
	assert.Equal(t, BinaryVector, result[5].GetVectors().Data.(*schemapb.VectorField_BinaryVector).BinaryVector)
	assert.Equal(t, FloatVector, result[6].GetVectors().GetFloatVector().Data)
}

func TestDeleteFieldData(t *testing.T) {
	const (
		Dim                   = 8
		BoolFieldName         = "BoolField"
		Int32FieldName        = "Int32Field"
		Int64FieldName        = "Int64Field"
		FloatFieldName        = "FloatField"
		DoubleFieldName       = "DoubleField"
		JSONFieldName         = "JSONField"
		BinaryVectorFieldName = "BinaryVectorField"
		FloatVectorFieldName  = "FloatVectorField"
	)

	const (
		BoolFieldID = common.StartOfUserFieldID + iota
		Int32FieldID
		Int64FieldID
		FloatFieldID
		DoubleFieldID
		JSONFieldID
		BinaryVectorFieldID
		FloatVectorFieldID
	)
	BoolArray := []bool{true, false}
	Int32Array := []int32{1, 2}
	Int64Array := []int64{11, 22}
	FloatArray := []float32{1.0, 2.0}
	DoubleArray := []float64{11.0, 22.0}
	JSONArray := [][]byte{[]byte("{\"hello\":0}"), []byte("{\"key\":1}")}
	BinaryVector := []byte{0x12, 0x34}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}

	result1 := make([]*schemapb.FieldData, 8)
	result2 := make([]*schemapb.FieldData, 8)
	var fieldDataArray1 []*schemapb.FieldData
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BoolFieldName, BoolFieldID, schemapb.DataType_Bool, BoolArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int32FieldName, Int32FieldID, schemapb.DataType_Int32, Int32Array[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatFieldName, FloatFieldID, schemapb.DataType_Float, FloatArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(DoubleFieldName, DoubleFieldID, schemapb.DataType_Double, DoubleArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(JSONFieldName, JSONFieldID, schemapb.DataType_JSON, JSONArray[0:1], 1))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(BinaryVectorFieldName, BinaryVectorFieldID, schemapb.DataType_BinaryVector, BinaryVector[0:Dim/8], Dim))
	fieldDataArray1 = append(fieldDataArray1, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[0:Dim], Dim))

	var fieldDataArray2 []*schemapb.FieldData
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BoolFieldName, BoolFieldID, schemapb.DataType_Bool, BoolArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int32FieldName, Int32FieldID, schemapb.DataType_Int32, Int32Array[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(Int64FieldName, Int64FieldID, schemapb.DataType_Int64, Int64Array[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatFieldName, FloatFieldID, schemapb.DataType_Float, FloatArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(DoubleFieldName, DoubleFieldID, schemapb.DataType_Double, DoubleArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(JSONFieldName, JSONFieldID, schemapb.DataType_JSON, JSONArray[1:2], 1))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(BinaryVectorFieldName, BinaryVectorFieldID, schemapb.DataType_BinaryVector, BinaryVector[Dim/8:2*Dim/8], Dim))
	fieldDataArray2 = append(fieldDataArray2, genFieldData(FloatVectorFieldName, FloatVectorFieldID, schemapb.DataType_FloatVector, FloatVector[Dim:2*Dim], Dim))

	AppendFieldData(result1, fieldDataArray1, 0)
	AppendFieldData(result1, fieldDataArray2, 0)
	DeleteFieldData(result1)
	assert.Equal(t, BoolArray[0:1], result1[BoolFieldID-common.StartOfUserFieldID].GetScalars().GetBoolData().Data)
	assert.Equal(t, Int32Array[0:1], result1[Int32FieldID-common.StartOfUserFieldID].GetScalars().GetIntData().Data)
	assert.Equal(t, Int64Array[0:1], result1[Int64FieldID-common.StartOfUserFieldID].GetScalars().GetLongData().Data)
	assert.Equal(t, FloatArray[0:1], result1[FloatFieldID-common.StartOfUserFieldID].GetScalars().GetFloatData().Data)
	assert.Equal(t, DoubleArray[0:1], result1[DoubleFieldID-common.StartOfUserFieldID].GetScalars().GetDoubleData().Data)
	assert.Equal(t, JSONArray[0:1], result1[JSONFieldID-common.StartOfUserFieldID].GetScalars().GetJsonData().Data)
	assert.Equal(t, BinaryVector[0:Dim/8], result1[BinaryVectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_BinaryVector).BinaryVector)
	assert.Equal(t, FloatVector[0:Dim], result1[FloatVectorFieldID-common.StartOfUserFieldID].GetVectors().GetFloatVector().Data)

	AppendFieldData(result2, fieldDataArray2, 0)
	AppendFieldData(result2, fieldDataArray1, 0)
	DeleteFieldData(result2)
	assert.Equal(t, BoolArray[1:2], result2[BoolFieldID-common.StartOfUserFieldID].GetScalars().GetBoolData().Data)
	assert.Equal(t, Int32Array[1:2], result2[Int32FieldID-common.StartOfUserFieldID].GetScalars().GetIntData().Data)
	assert.Equal(t, Int64Array[1:2], result2[Int64FieldID-common.StartOfUserFieldID].GetScalars().GetLongData().Data)
	assert.Equal(t, FloatArray[1:2], result2[FloatFieldID-common.StartOfUserFieldID].GetScalars().GetFloatData().Data)
	assert.Equal(t, DoubleArray[1:2], result2[DoubleFieldID-common.StartOfUserFieldID].GetScalars().GetDoubleData().Data)
	assert.Equal(t, JSONArray[1:2], result2[JSONFieldID-common.StartOfUserFieldID].GetScalars().GetJsonData().Data)
	assert.Equal(t, BinaryVector[Dim/8:2*Dim/8], result2[BinaryVectorFieldID-common.StartOfUserFieldID].GetVectors().Data.(*schemapb.VectorField_BinaryVector).BinaryVector)
	assert.Equal(t, FloatVector[Dim:2*Dim], result2[FloatVectorFieldID-common.StartOfUserFieldID].GetVectors().GetFloatVector().Data)
}

func TestGetPrimaryFieldSchema(t *testing.T) {
	int64Field := &schemapb.FieldSchema{
		FieldID:  1,
		Name:     "int64Field",
		DataType: schemapb.DataType_Int64,
	}

	floatField := &schemapb.FieldSchema{
		FieldID:  2,
		Name:     "floatField",
		DataType: schemapb.DataType_Float,
	}

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{int64Field, floatField},
	}

	// no primary field error
	_, err := GetPrimaryFieldSchema(schema)
	assert.Error(t, err)

	int64Field.IsPrimaryKey = true
	primaryField, err := GetPrimaryFieldSchema(schema)
	assert.NoError(t, err)
	assert.Equal(t, schemapb.DataType_Int64, primaryField.DataType)
}

func TestGetPK(t *testing.T) {
	type args struct {
		data *schemapb.IDs
		idx  int64
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			args: args{
				data: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
				idx: 5, // > len(data)
			},
			want: nil,
		},
		{
			args: args{
				data: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
				idx: 1,
			},
			want: int64(2),
		},
		{
			args: args{
				data: &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{
						StrId: &schemapb.StringArray{
							Data: []string{"1", "2", "3"},
						},
					},
				},
				idx: 1,
			},
			want: "2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetPK(tt.args.data, tt.args.idx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetPK(%v, %v) = %v, want %v", tt.args.data, tt.args.idx, got, tt.want)
			}
		})
	}
}

func TestAppendPKs(t *testing.T) {
	intPks := &schemapb.IDs{}
	AppendPKs(intPks, int64(1))
	assert.ElementsMatch(t, []int64{1}, intPks.GetIntId().GetData())
	AppendPKs(intPks, int64(2))
	assert.ElementsMatch(t, []int64{1, 2}, intPks.GetIntId().GetData())

	strPks := &schemapb.IDs{}
	AppendPKs(strPks, "1")
	assert.ElementsMatch(t, []string{"1"}, strPks.GetStrId().GetData())
	AppendPKs(strPks, "2")
	assert.ElementsMatch(t, []string{"1", "2"}, strPks.GetStrId().GetData())
}

func TestSwapPK(t *testing.T) {
	intPks := &schemapb.IDs{}
	AppendPKs(intPks, int64(1))
	AppendPKs(intPks, int64(2))
	AppendPKs(intPks, int64(3))
	require.Equal(t, []int64{1, 2, 3}, intPks.GetIntId().GetData())

	SwapPK(intPks, 0, 1)
	assert.Equal(t, []int64{2, 1, 3}, intPks.GetIntId().GetData())
	SwapPK(intPks, 0, 1)
	assert.Equal(t, []int64{1, 2, 3}, intPks.GetIntId().GetData())
	SwapPK(intPks, 0, 2)
	assert.Equal(t, []int64{3, 2, 1}, intPks.GetIntId().GetData())
	SwapPK(intPks, 0, 2)
	assert.Equal(t, []int64{1, 2, 3}, intPks.GetIntId().GetData())
	SwapPK(intPks, 1, 2)
	assert.Equal(t, []int64{1, 3, 2}, intPks.GetIntId().GetData())
	SwapPK(intPks, 1, 2)
	assert.Equal(t, []int64{1, 2, 3}, intPks.GetIntId().GetData())

	strPks := &schemapb.IDs{}
	AppendPKs(strPks, "1")
	AppendPKs(strPks, "2")
	AppendPKs(strPks, "3")

	require.Equal(t, []string{"1", "2", "3"}, strPks.GetStrId().GetData())

	SwapPK(strPks, 0, 1)
	assert.Equal(t, []string{"2", "1", "3"}, strPks.GetStrId().GetData())
	SwapPK(strPks, 0, 1)
	assert.Equal(t, []string{"1", "2", "3"}, strPks.GetStrId().GetData())
	SwapPK(strPks, 0, 2)
	assert.Equal(t, []string{"3", "2", "1"}, strPks.GetStrId().GetData())
	SwapPK(strPks, 0, 2)
	assert.Equal(t, []string{"1", "2", "3"}, strPks.GetStrId().GetData())
	SwapPK(strPks, 1, 2)
	assert.Equal(t, []string{"1", "3", "2"}, strPks.GetStrId().GetData())
	SwapPK(strPks, 1, 2)
	assert.Equal(t, []string{"1", "2", "3"}, strPks.GetStrId().GetData())
}

func TestComparePk(t *testing.T) {
	intPks := &schemapb.IDs{}
	AppendPKs(intPks, int64(1))
	AppendPKs(intPks, int64(2))
	AppendPKs(intPks, int64(3))
	require.Equal(t, []int64{1, 2, 3}, intPks.GetIntId().GetData())

	less := ComparePKInSlice(intPks, 0, 1)
	assert.True(t, less)
	less = ComparePKInSlice(intPks, 0, 2)
	assert.True(t, less)
	less = ComparePKInSlice(intPks, 1, 2)
	assert.True(t, less)

	less = ComparePKInSlice(intPks, 1, 0)
	assert.False(t, less)
	less = ComparePKInSlice(intPks, 2, 0)
	assert.False(t, less)
	less = ComparePKInSlice(intPks, 2, 1)
	assert.False(t, less)

	strPks := &schemapb.IDs{}
	AppendPKs(strPks, "1")
	AppendPKs(strPks, "2")
	AppendPKs(strPks, "3")

	require.Equal(t, []string{"1", "2", "3"}, strPks.GetStrId().GetData())

	less = ComparePKInSlice(strPks, 0, 1)
	assert.True(t, less)
	less = ComparePKInSlice(strPks, 0, 2)
	assert.True(t, less)
	less = ComparePKInSlice(strPks, 1, 2)
	assert.True(t, less)

	less = ComparePKInSlice(strPks, 1, 0)
	assert.False(t, less)
	less = ComparePKInSlice(strPks, 2, 0)
	assert.False(t, less)
	less = ComparePKInSlice(strPks, 2, 1)
	assert.False(t, less)
}

func TestCalcColumnSize(t *testing.T) {
	fieldValues := map[int64]any{
		100: []int8{0, 1},
		101: []int16{0, 1},
		102: []int32{0, 1},
		103: []int64{0, 1},
		104: []float32{0, 1},
		105: []float64{0, 1},
		106: []string{"0", "1"},
		107: []float32{0, 1, 2, 3},
		109: [][]int32{{1, 2, 3}, {4, 5, 6}},
		110: [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)},
	}
	schema := &schemapb.CollectionSchema{
		Name:        "testColl",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "field_int8",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int8,
			},
			{
				FieldID:      101,
				Name:         "field_int16",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_Int16,
			},
			{
				FieldID:      102,
				Name:         "field_int32",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_Int32,
			},
			{
				FieldID:      103,
				Name:         "field_int64",
				IsPrimaryKey: true,
				Description:  "",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      104,
				Name:         "field_float",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_Float,
			},
			{
				FieldID:      105,
				Name:         "field_double",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_Double,
			},
			{
				FieldID:      106,
				Name:         "field_string",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "125",
					},
				},
			},
			{
				FieldID:     109,
				Name:        "field_array",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
			},
			{
				FieldID:  110,
				Name:     "field_json",
				DataType: schemapb.DataType_JSON,
			},
		},
	}

	for _, field := range schema.GetFields() {
		values := fieldValues[field.GetFieldID()]
		fieldData := genFieldData(field.GetName(), field.GetFieldID(), field.GetDataType(), values, 0)
		size := CalcColumnSize(fieldData)
		expected := 0
		switch field.GetDataType() {
		case schemapb.DataType_VarChar:
			data := values.([]string)
			for _, v := range data {
				expected += len(v)
			}
		case schemapb.DataType_Array:
			data := values.([][]int32)
			for _, v := range data {
				expected += binary.Size(v)
			}
		case schemapb.DataType_JSON:
			data := values.([][]byte)
			for _, v := range data {
				expected += len(v)
			}

		default:
			expected = binary.Size(fieldValues[field.GetFieldID()])
		}

		assert.Equal(t, expected, size, field.GetName())
	}
}

func TestGetDataAndGetDataSize(t *testing.T) {
	const (
		Dim       = 8
		fieldName = "field-0"
		fieldID   = 0
	)

	BoolArray := []bool{true, false}
	Int8Array := []int8{1, 2}
	Int16Array := []int16{3, 4}
	Int32Array := []int32{5, 6}
	Int64Array := []int64{11, 22}
	FloatArray := []float32{1.0, 2.0}
	DoubleArray := []float64{11.0, 22.0}
	VarCharArray := []string{"a", "b"}
	StringArray := []string{"c", "d"}
	BinaryVector := []byte{0x12, 0x34}
	FloatVector := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0}

	boolData := genFieldData(fieldName, fieldID, schemapb.DataType_Bool, BoolArray, 1)
	int8Data := genFieldData(fieldName, fieldID, schemapb.DataType_Int8, Int8Array, 1)
	int16Data := genFieldData(fieldName, fieldID, schemapb.DataType_Int16, Int16Array, 1)
	int32Data := genFieldData(fieldName, fieldID, schemapb.DataType_Int32, Int32Array, 1)
	int64Data := genFieldData(fieldName, fieldID, schemapb.DataType_Int64, Int64Array, 1)
	floatData := genFieldData(fieldName, fieldID, schemapb.DataType_Float, FloatArray, 1)
	doubleData := genFieldData(fieldName, fieldID, schemapb.DataType_Double, DoubleArray, 1)
	varCharData := genFieldData(fieldName, fieldID, schemapb.DataType_VarChar, VarCharArray, 1)
	stringData := genFieldData(fieldName, fieldID, schemapb.DataType_String, StringArray, 1)
	binVecData := genFieldData(fieldName, fieldID, schemapb.DataType_BinaryVector, BinaryVector, Dim)
	floatVecData := genFieldData(fieldName, fieldID, schemapb.DataType_FloatVector, FloatVector, Dim)
	invalidData := &schemapb.FieldData{
		Type: schemapb.DataType_None,
	}

	t.Run("test GetDataSize", func(t *testing.T) {
		boolDataRes := GetDataSize(boolData)
		int8DataRes := GetDataSize(int8Data)
		int16DataRes := GetDataSize(int16Data)
		int32DataRes := GetDataSize(int32Data)
		int64DataRes := GetDataSize(int64Data)
		floatDataRes := GetDataSize(floatData)
		doubleDataRes := GetDataSize(doubleData)
		varCharDataRes := GetDataSize(varCharData)
		stringDataRes := GetDataSize(stringData)
		binVecDataRes := GetDataSize(binVecData)
		floatVecDataRes := GetDataSize(floatVecData)
		invalidDataRes := GetDataSize(invalidData)

		assert.Equal(t, 2, boolDataRes)
		assert.Equal(t, 2, int8DataRes)
		assert.Equal(t, 2, int16DataRes)
		assert.Equal(t, 2, int32DataRes)
		assert.Equal(t, 2, int64DataRes)
		assert.Equal(t, 2, floatDataRes)
		assert.Equal(t, 2, doubleDataRes)
		assert.Equal(t, 2, varCharDataRes)
		assert.Equal(t, 2, stringDataRes)
		assert.Equal(t, 2*Dim/8, binVecDataRes)
		assert.Equal(t, 2*Dim, floatVecDataRes)
		assert.Equal(t, 0, invalidDataRes)
	})

	t.Run("test GetData", func(t *testing.T) {
		boolDataRes := GetData(boolData, 0)
		int8DataRes := GetData(int8Data, 0)
		int16DataRes := GetData(int16Data, 0)
		int32DataRes := GetData(int32Data, 0)
		int64DataRes := GetData(int64Data, 0)
		floatDataRes := GetData(floatData, 0)
		doubleDataRes := GetData(doubleData, 0)
		varCharDataRes := GetData(varCharData, 0)
		stringDataRes := GetData(stringData, 0)
		binVecDataRes := GetData(binVecData, 0)
		floatVecDataRes := GetData(floatVecData, 0)
		invalidDataRes := GetData(invalidData, 0)

		assert.Equal(t, BoolArray[0], boolDataRes)
		assert.Equal(t, int32(Int8Array[0]), int8DataRes)
		assert.Equal(t, int32(Int16Array[0]), int16DataRes)
		assert.Equal(t, Int32Array[0], int32DataRes)
		assert.Equal(t, Int64Array[0], int64DataRes)
		assert.Equal(t, FloatArray[0], floatDataRes)
		assert.Equal(t, DoubleArray[0], doubleDataRes)
		assert.Equal(t, VarCharArray[0], varCharDataRes)
		assert.Equal(t, StringArray[0], stringDataRes)
		assert.ElementsMatch(t, BinaryVector[:Dim/8], binVecDataRes)
		assert.ElementsMatch(t, FloatVector[:Dim], floatVecDataRes)
		assert.Nil(t, invalidDataRes)
	})
}

func TestMergeFieldData(t *testing.T) {
	dstFields := []*schemapb.FieldData{
		genFieldData("int64", 100, schemapb.DataType_Int64, []int64{1, 2, 3}, 1),
		genFieldData("vector", 101, schemapb.DataType_FloatVector, []float32{1, 2, 3}, 1),
		genFieldData("json", 102, schemapb.DataType_JSON, [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)}, 1),
	}

	srcFields := []*schemapb.FieldData{
		genFieldData("int64", 100, schemapb.DataType_Int64, []int64{4, 5, 6}, 1),
		genFieldData("vector", 101, schemapb.DataType_FloatVector, []float32{4, 5, 6}, 1),
		genFieldData("json", 102, schemapb.DataType_JSON, [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)}, 1),
	}

	err := MergeFieldData(dstFields, srcFields)
	assert.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6}, dstFields[0].GetScalars().GetLongData().Data)
	assert.Equal(t, []float32{1, 2, 3, 4, 5, 6}, dstFields[1].GetVectors().GetFloatVector().Data)
	assert.Equal(t, [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`), []byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)},
		dstFields[2].GetScalars().GetJsonData().Data)

	emptyField := &schemapb.FieldData{
		Type: schemapb.DataType_None,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: nil,
			},
		},
	}

	err = MergeFieldData([]*schemapb.FieldData{emptyField}, []*schemapb.FieldData{emptyField})
	assert.Error(t, err)
}
