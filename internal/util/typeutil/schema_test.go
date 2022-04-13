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
	"reflect"
	"testing"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
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
						Key:   "max_length_per_row",
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
						Key:   "dim",
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
						Key:   "dim",
						Value: "128",
					},
				},
			},
		},
	}

	t.Run("EstimateSizePerRecord", func(t *testing.T) {
		size, err := EstimateSizePerRecord(schema)
		assert.Equal(t, 680, size)
		assert.Nil(t, err)
	})

	t.Run("SchemaHelper", func(t *testing.T) {
		_, err := CreateSchemaHelper(nil)
		assert.NotNil(t, err)

		helper, err := CreateSchemaHelper(schema)
		assert.Nil(t, err)

		field, err := helper.GetPrimaryKeyField()
		assert.Nil(t, err)
		assert.Equal(t, "field_int64", field.Name)

		field1, err := helper.GetFieldFromName("field_int8")
		assert.Nil(t, err)
		assert.Equal(t, "field_int8", field1.Name)

		field2, err := helper.GetFieldFromID(102)
		assert.Nil(t, err)
		assert.Equal(t, "field_int32", field2.Name)

		dim, err := helper.GetVectorDimFromID(107)
		assert.Nil(t, err)
		assert.Equal(t, 128, dim)
		dim1, err := helper.GetVectorDimFromID(108)
		assert.Nil(t, err)
		assert.Equal(t, 128, dim1)
		_, err = helper.GetVectorDimFromID(103)
		assert.NotNil(t, err)
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
		assert.NotNil(t, err)
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
		assert.NotNil(t, err)
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
		assert.NotNil(t, err)
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
		assert.Nil(t, err)

		_, err = helper.GetPrimaryKeyField()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "failed to get primary key field: no primary in schema")

		_, err = helper.GetFieldFromName("none")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "failed to get field schema by name: fieldName(none) not found")

		_, err = helper.GetFieldFromID(101)
		assert.NotNil(t, err)
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
		assert.Nil(t, err)

		_, err = helper.GetVectorDimFromID(100)
		assert.NotNil(t, err)

		_, err = helper.GetVectorDimFromID(103)
		assert.NotNil(t, err)

		_, err = helper.GetVectorDimFromID(107)
		assert.NotNil(t, err)
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
	assert.Nil(t, err)
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

func TestIsPKInvalid(t *testing.T) {
	type args struct {
		pk interface{}
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				pk: float32(1.0),
			},
			want: true,
		},
		{
			args: args{
				pk: InvalidIntPK,
			},
			want: true,
		},
		{
			args: args{
				pk: InvalidStrPK,
			},
			want: true,
		},
		{
			args: args{
				pk: int64(1),
			},
			want: false,
		},
		{
			args: args{
				pk: "1",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPKInvalid(tt.args.pk); got != tt.want {
				t.Errorf("IsPKInvalid(%v) = %v, want %v", tt.args.pk, got, tt.want)
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
