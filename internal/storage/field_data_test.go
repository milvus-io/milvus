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
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestArrayFieldDataSuite(t *testing.T) {
	suite.Run(t, new(ArrayFieldDataSuite))
}

func (s *InsertDataSuite) TestMemorySize() {
	s.Equal(s.iDataEmpty.Data[RowIDField].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[TimestampField].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[BoolField].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[Int8Field].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[Int16Field].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[Int32Field].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[Int64Field].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[FloatField].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[DoubleField].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[StringField].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[ArrayField].GetMemorySize(), 2)
	// +9 bytes: Nullable(1) + L2PMapping.GetMemorySize()(8)
	s.Equal(s.iDataEmpty.Data[BinaryVectorField].GetMemorySize(), 4+9)
	s.Equal(s.iDataEmpty.Data[FloatVectorField].GetMemorySize(), 4+9)
	s.Equal(s.iDataEmpty.Data[Float16VectorField].GetMemorySize(), 4+9)
	s.Equal(s.iDataEmpty.Data[BFloat16VectorField].GetMemorySize(), 4+9)
	s.Equal(s.iDataEmpty.Data[SparseFloatVectorField].GetMemorySize(), 0+9)
	s.Equal(s.iDataEmpty.Data[Int8VectorField].GetMemorySize(), 4+9)
	s.Equal(s.iDataEmpty.Data[StructSubInt32Field].GetMemorySize(), 2)
	// +2 bytes: Nullable and ElementNullable flags (VectorArrayFieldData has no L2PMapping under Plan B)
	s.Equal(s.iDataEmpty.Data[StructSubFloatVectorField].GetMemorySize(), 0+2)

	s.Equal(s.iDataOneRow.Data[RowIDField].GetMemorySize(), 9)
	s.Equal(s.iDataOneRow.Data[TimestampField].GetMemorySize(), 9)
	s.Equal(s.iDataOneRow.Data[BoolField].GetMemorySize(), 2)
	s.Equal(s.iDataOneRow.Data[Int8Field].GetMemorySize(), 2)
	s.Equal(s.iDataOneRow.Data[Int16Field].GetMemorySize(), 3)
	s.Equal(s.iDataOneRow.Data[Int32Field].GetMemorySize(), 5)
	s.Equal(s.iDataOneRow.Data[Int64Field].GetMemorySize(), 9)
	s.Equal(s.iDataOneRow.Data[FloatField].GetMemorySize(), 5)
	s.Equal(s.iDataOneRow.Data[DoubleField].GetMemorySize(), 9)
	s.Equal(s.iDataOneRow.Data[StringField].GetMemorySize(), 20)
	s.Equal(s.iDataOneRow.Data[JSONField].GetMemorySize(), len([]byte(`{"batch":1}`))+16+1)
	s.Equal(s.iDataOneRow.Data[ArrayField].GetMemorySize(), 3*4+2)
	// +9 bytes: Nullable(1) + L2PMapping.GetMemorySize()(8)
	s.Equal(s.iDataOneRow.Data[BinaryVectorField].GetMemorySize(), 5+9)
	s.Equal(s.iDataOneRow.Data[FloatVectorField].GetMemorySize(), 20+9)
	s.Equal(s.iDataOneRow.Data[Float16VectorField].GetMemorySize(), 12+9)
	s.Equal(s.iDataOneRow.Data[BFloat16VectorField].GetMemorySize(), 12+9)
	s.Equal(s.iDataOneRow.Data[SparseFloatVectorField].GetMemorySize(), 28+9)
	s.Equal(s.iDataOneRow.Data[Int8VectorField].GetMemorySize(), 8+9)
	s.Equal(s.iDataOneRow.Data[StructSubInt32Field].GetMemorySize(), 3*4+2)
	s.Equal(s.iDataOneRow.Data[StructSubFloatVectorField].GetMemorySize(), 3*4*2+4+2)

	s.Equal(s.iDataTwoRows.Data[RowIDField].GetMemorySize(), 17)
	s.Equal(s.iDataTwoRows.Data[TimestampField].GetMemorySize(), 17)
	s.Equal(s.iDataTwoRows.Data[BoolField].GetMemorySize(), 3)
	s.Equal(s.iDataTwoRows.Data[Int8Field].GetMemorySize(), 3)
	s.Equal(s.iDataTwoRows.Data[Int16Field].GetMemorySize(), 5)
	s.Equal(s.iDataTwoRows.Data[Int32Field].GetMemorySize(), 9)
	s.Equal(s.iDataTwoRows.Data[Int64Field].GetMemorySize(), 17)
	s.Equal(s.iDataTwoRows.Data[FloatField].GetMemorySize(), 9)
	s.Equal(s.iDataTwoRows.Data[DoubleField].GetMemorySize(), 17)
	s.Equal(s.iDataTwoRows.Data[StringField].GetMemorySize(), 39)
	s.Equal(s.iDataTwoRows.Data[ArrayField].GetMemorySize(), 26)
	// +9 bytes: Nullable(1) + L2PMapping.GetMemorySize()(8)
	s.Equal(s.iDataTwoRows.Data[BinaryVectorField].GetMemorySize(), 6+9)
	s.Equal(s.iDataTwoRows.Data[FloatVectorField].GetMemorySize(), 36+9)
	s.Equal(s.iDataTwoRows.Data[Float16VectorField].GetMemorySize(), 20+9)
	s.Equal(s.iDataTwoRows.Data[BFloat16VectorField].GetMemorySize(), 20+9)
	s.Equal(s.iDataTwoRows.Data[SparseFloatVectorField].GetMemorySize(), 54+9)
	s.Equal(s.iDataTwoRows.Data[Int8VectorField].GetMemorySize(), 12+9)
	s.Equal(s.iDataTwoRows.Data[StructSubInt32Field].GetMemorySize(), 3*4+2*4+2)
	s.Equal(s.iDataTwoRows.Data[StructSubFloatVectorField].GetMemorySize(), 3*4*2+4+2*4*2+4+2)
}

func (s *InsertDataSuite) TestGetRowSize() {
	s.Equal(s.iDataOneRow.Data[RowIDField].GetRowSize(0), 8)
	s.Equal(s.iDataOneRow.Data[TimestampField].GetRowSize(0), 8)
	s.Equal(s.iDataOneRow.Data[BoolField].GetRowSize(0), 1)
	s.Equal(s.iDataOneRow.Data[Int8Field].GetRowSize(0), 1)
	s.Equal(s.iDataOneRow.Data[Int16Field].GetRowSize(0), 2)
	s.Equal(s.iDataOneRow.Data[Int32Field].GetRowSize(0), 4)
	s.Equal(s.iDataOneRow.Data[Int64Field].GetRowSize(0), 8)
	s.Equal(s.iDataOneRow.Data[FloatField].GetRowSize(0), 4)
	s.Equal(s.iDataOneRow.Data[DoubleField].GetRowSize(0), 8)
	s.Equal(s.iDataOneRow.Data[StringField].GetRowSize(0), 19)
	s.Equal(s.iDataOneRow.Data[JSONField].GetRowSize(0), len([]byte(`{"batch":1}`))+16)
	s.Equal(s.iDataOneRow.Data[ArrayField].GetRowSize(0), 3*4)
	s.Equal(s.iDataOneRow.Data[BinaryVectorField].GetRowSize(0), 1)
	s.Equal(s.iDataOneRow.Data[FloatVectorField].GetRowSize(0), 16)
	s.Equal(s.iDataOneRow.Data[Float16VectorField].GetRowSize(0), 8)
	s.Equal(s.iDataOneRow.Data[BFloat16VectorField].GetRowSize(0), 8)
	s.Equal(s.iDataOneRow.Data[SparseFloatVectorField].GetRowSize(0), 24)
	s.Equal(s.iDataOneRow.Data[Int8VectorField].GetRowSize(0), 4)
	s.Equal(s.iDataOneRow.Data[StructSubInt32Field].GetRowSize(0), 3*4)
	s.Equal(s.iDataOneRow.Data[StructSubFloatVectorField].GetRowSize(0), 3*4*2+4)
}

func GetFields(schema *schemapb.CollectionSchema) []*schemapb.FieldSchema {
	ret := make([]*schemapb.FieldSchema, 0, 100)
	ret = append(ret, schema.GetFields()...)
	for _, structField := range schema.GetStructArrayFields() {
		ret = append(ret, structField.GetFields()...)
	}
	return ret
}

func (s *InsertDataSuite) TestGetDataType() {
	for _, field := range GetFields(s.schema) {
		fieldData, ok := s.iDataOneRow.Data[field.GetFieldID()]
		s.True(ok)
		s.Equal(field.GetDataType(), fieldData.GetDataType())
	}
}

func (s *InsertDataSuite) TestGetNullable() {
	for _, field := range GetFields(s.schema) {
		fieldData, ok := s.iDataOneRow.Data[field.GetFieldID()]
		s.True(ok)
		s.Equal(field.GetNullable(), fieldData.GetNullable())
	}
}

type ArrayFieldDataSuite struct {
	suite.Suite
}

func (s *ArrayFieldDataSuite) TestArrayFieldData() {
	fieldID2Type := map[int64]schemapb.DataType{
		ArrayField + 1: schemapb.DataType_Bool,
		ArrayField + 2: schemapb.DataType_Int8,
		ArrayField + 3: schemapb.DataType_Int16,
		ArrayField + 4: schemapb.DataType_Int32,
		ArrayField + 5: schemapb.DataType_Int64,
		ArrayField + 6: schemapb.DataType_Float,
		ArrayField + 7: schemapb.DataType_Double,
		ArrayField + 8: schemapb.DataType_VarChar,
	}

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  RowIDField,
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  TimestampField,
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:      Int64Field,
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}

	for fieldID, elementType := range fieldID2Type {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:     fieldID,
			DataType:    schemapb.DataType_Array,
			ElementType: elementType,
		})
	}

	insertData, err := NewInsertData(schema)
	s.NoError(err)

	s.Equal(0, insertData.GetRowNum())
	s.Equal(19, insertData.GetMemorySize())
	s.True(insertData.IsEmpty())

	fieldIDToData := map[int64]interface{}{
		RowIDField:     int64(1),
		TimestampField: int64(2),
		Int64Field:     int64(3),
		ArrayField + 1: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{Data: []bool{true, false}},
			},
		},
		ArrayField + 2: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{0, 0}},
			},
		},
		ArrayField + 3: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{1, 1}},
			},
		},
		ArrayField + 4: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{2, 2}},
			},
		},
		ArrayField + 5: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: []int64{3, 3}},
			},
		},
		ArrayField + 6: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{Data: []float32{4, 4}},
			},
		},
		ArrayField + 7: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{Data: []float64{5, 5}},
			},
		},
		ArrayField + 8: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{Data: []string{"6", "6"}},
			},
		},
	}

	err = insertData.Append(fieldIDToData)
	s.NoError(err)
	s.Equal(1, insertData.GetRowNum())
	s.Equal(134, insertData.GetMemorySize())
	s.False(insertData.IsEmpty())
	s.Equal(115, insertData.GetRowSize(0))
}

func makeFloatVec(dim int, vals ...float32) *schemapb.VectorField {
	return &schemapb.VectorField{
		Dim: int64(dim),
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: vals},
		},
	}
}

func TestNullableVectorAppendRowsRejectsNonCompactData(t *testing.T) {
	validData := []bool{true, false, true}
	sparseRows := &SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Dim: 8,
			Contents: [][]byte{
				typeutil.CreateSparseFloatRow([]uint32{1}, []float32{1.0}),
				typeutil.CreateSparseFloatRow([]uint32{2}, []float32{2.0}),
				typeutil.CreateSparseFloatRow([]uint32{3}, []float32{3.0}),
			},
		},
	}

	tests := []struct {
		name      string
		fieldData FieldData
		rows      any
	}{
		{
			name:      "binary vector",
			fieldData: &BinaryVectorFieldData{Dim: 8, Nullable: true},
			rows:      []byte{0x01, 0x02, 0x03},
		},
		{
			name:      "float vector",
			fieldData: &FloatVectorFieldData{Dim: 2, Nullable: true},
			rows:      []float32{1, 2, 3, 4, 5, 6},
		},
		{
			name:      "float16 vector",
			fieldData: &Float16VectorFieldData{Dim: 2, Nullable: true},
			rows:      []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:      "bfloat16 vector",
			fieldData: &BFloat16VectorFieldData{Dim: 2, Nullable: true},
			rows:      []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:      "sparse vector",
			fieldData: &SparseFloatVectorFieldData{Nullable: true},
			rows:      sparseRows,
		},
		{
			name:      "int8 vector",
			fieldData: &Int8VectorFieldData{Dim: 2, Nullable: true},
			rows:      []int8{1, 2, 3, 4, 5, 6},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fieldData.AppendRows(tt.rows, validData)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "compact")
			assert.Equal(t, 0, tt.fieldData.RowNum())
			assert.Empty(t, nullableVectorValidDataForTest(tt.fieldData))
		})
	}
}

func nullableVectorValidDataForTest(fieldData FieldData) []bool {
	switch data := fieldData.(type) {
	case *BinaryVectorFieldData:
		return data.ValidData
	case *FloatVectorFieldData:
		return data.ValidData
	case *Float16VectorFieldData:
		return data.ValidData
	case *BFloat16VectorFieldData:
		return data.ValidData
	case *SparseFloatVectorFieldData:
		return data.ValidData
	case *Int8VectorFieldData:
		return data.ValidData
	default:
		return nil
	}
}

func TestVectorArrayFieldData_NullableAppendAndGetRow(t *testing.T) {
	fd := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data:        make([]*schemapb.VectorField, 0),
		ValidData:   make([]bool, 0),
		Nullable:    true,
	}

	vec0 := makeFloatVec(4, 1, 2, 3, 4)
	vec2 := makeFloatVec(4, 5, 6, 7, 8)

	require.NoError(t, fd.AppendRow(vec0))
	require.NoError(t, fd.AppendRow(nil))
	require.NoError(t, fd.AppendRow(vec2))

	assert.Equal(t, 3, fd.RowNum(), "RowNum should count all rows including null placeholders")
	assert.Equal(t, 3, len(fd.Data), "Plan B: Data is dense; null rows hold empty placeholders")
	assert.Equal(t, []bool{true, false, true}, fd.GetValidData())
	assert.True(t, fd.GetNullable())

	assert.Equal(t, vec0, fd.GetRow(0))
	assert.Nil(t, fd.GetRow(1), "null row should return nil despite placeholder in Data")
	assert.Equal(t, vec2, fd.GetRow(2))
}

func TestVectorArrayFieldData_NonNullable(t *testing.T) {
	fd := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data:        make([]*schemapb.VectorField, 0),
		Nullable:    false,
	}

	vec := makeFloatVec(4, 1, 2, 3, 4)
	require.NoError(t, fd.AppendRow(vec))

	assert.Equal(t, 1, fd.RowNum())
	assert.False(t, fd.GetNullable())
	assert.Nil(t, fd.GetValidData())
	assert.Equal(t, vec, fd.GetRow(0))
}

func TestVectorArrayFieldData_AppendValidDataRows(t *testing.T) {
	fd := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data: []*schemapb.VectorField{
			makeFloatVec(4, 1, 2, 3, 4),
			makeFloatVec(4, 5, 6, 7, 8),
		},
		Nullable: true,
	}

	err := fd.AppendValidDataRows([]bool{true, false, true})
	require.NoError(t, err)

	assert.Equal(t, []bool{true, false, true}, fd.GetValidData())

	assert.NoError(t, fd.AppendValidDataRows(nil))
	assert.Error(t, fd.AppendValidDataRows("bad"))
}

func TestVectorArrayFieldData_GetMemorySize(t *testing.T) {
	fd := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data:        []*schemapb.VectorField{makeFloatVec(4, 1, 2, 3, 4)},
		ValidData:   []bool{true, false},
		Nullable:    true,
	}

	assert.Greater(t, fd.GetMemorySize(), 0)
}

func TestVectorArrayFieldData_AllNull(t *testing.T) {
	fd := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data:        make([]*schemapb.VectorField, 0),
		ValidData:   make([]bool, 0),
		Nullable:    true,
	}

	for i := 0; i < 3; i++ {
		require.NoError(t, fd.AppendRow(nil))
	}

	assert.Equal(t, 3, fd.RowNum())
	assert.Equal(t, 3, len(fd.Data), "Plan B: Data is dense, null rows hold placeholders")
	for i := 0; i < 3; i++ {
		assert.Nil(t, fd.GetRow(i))
	}
}

func TestNewFieldData_NullableArrayOfVector(t *testing.T) {
	schema := &schemapb.FieldSchema{
		FieldID:     100,
		Name:        "vec_arr",
		DataType:    schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector,
		Nullable:    true,
		TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
	}
	fd, err := NewFieldData(schemapb.DataType_ArrayOfVector, schema, 10)
	require.NoError(t, err)

	vafd, ok := fd.(*VectorArrayFieldData)
	require.True(t, ok)
	assert.True(t, vafd.Nullable)
	assert.NotNil(t, vafd.ValidData)
	assert.Equal(t, int64(4), vafd.Dim)
}

func TestNewFieldData_ElementNullableArrayPayloads(t *testing.T) {
	arraySchema := &schemapb.FieldSchema{
		FieldID:         100,
		Name:            "arr",
		DataType:        schemapb.DataType_Array,
		ElementType:     schemapb.DataType_Int64,
		ElementNullable: true,
	}
	fd, err := NewFieldData(schemapb.DataType_Array, arraySchema, 10)
	require.NoError(t, err)

	arrayData, ok := fd.(*ArrayFieldData)
	require.True(t, ok)
	assert.True(t, arrayData.ElementNullable)
	assert.NotNil(t, arrayData.Data)

	vectorArraySchema := &schemapb.FieldSchema{
		FieldID:         101,
		Name:            "vec_arr",
		DataType:        schemapb.DataType_ArrayOfVector,
		ElementType:     schemapb.DataType_FloatVector,
		ElementNullable: true,
		TypeParams:      []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
	}
	fd, err = NewFieldData(schemapb.DataType_ArrayOfVector, vectorArraySchema, 10)
	require.NoError(t, err)

	vectorArrayData, ok := fd.(*VectorArrayFieldData)
	require.True(t, ok)
	assert.True(t, vectorArrayData.ElementNullable)
	assert.NotNil(t, vectorArrayData.Data)
}

func TestNewFieldData_NullableVectorCap(t *testing.T) {
	cases := []struct {
		dataType schemapb.DataType
		scaled   int
		check    func(t *testing.T, fd FieldData, scaled int)
	}{
		{schemapb.DataType_FloatVector, 1000 * 128, func(t *testing.T, fd FieldData, scaled int) {
			assert.Equal(t, scaled, cap(fd.(*FloatVectorFieldData).Data))
		}},
		{schemapb.DataType_Float16Vector, 1000 * 128 * 2, func(t *testing.T, fd FieldData, scaled int) {
			assert.Equal(t, scaled, cap(fd.(*Float16VectorFieldData).Data))
		}},
		{schemapb.DataType_BFloat16Vector, 1000 * 128 * 2, func(t *testing.T, fd FieldData, scaled int) {
			assert.Equal(t, scaled, cap(fd.(*BFloat16VectorFieldData).Data))
		}},
		{schemapb.DataType_BinaryVector, 1000 * 128 / 8, func(t *testing.T, fd FieldData, scaled int) {
			assert.Equal(t, scaled, cap(fd.(*BinaryVectorFieldData).Data))
		}},
		{schemapb.DataType_Int8Vector, 1000 * 128, func(t *testing.T, fd FieldData, scaled int) {
			assert.Equal(t, scaled, cap(fd.(*Int8VectorFieldData).Data))
		}},
	}
	for _, c := range cases {
		newSchema := func(nullable bool) *schemapb.FieldSchema {
			return &schemapb.FieldSchema{
				FieldID:    100,
				Name:       "vec",
				DataType:   c.dataType,
				Nullable:   nullable,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}},
			}
		}
		fd, err := NewFieldData(c.dataType, newSchema(false), 1000)
		require.NoError(t, err)
		c.check(t, fd, c.scaled)

		fd, err = NewFieldData(c.dataType, newSchema(true), 1000)
		require.NoError(t, err)
		c.check(t, fd, 1000)
	}
}

// These tests characterize the existing logical-size accounting, not Go heap
// usage. Keep expectations independent of GetRowSize so a future incremental
// implementation can be checked against both this baseline and GetMemorySize.
func TestFieldMemorySizeNullableAppend(t *testing.T) {
	tests := []struct {
		name  string
		field FieldData
		row   any
		// Empty, one null, null + value, null + value + null.
		want [4]int
	}{
		{"bool", &BoolFieldData{Nullable: true}, true, [4]int{1, 3, 5, 7}},
		{"int8", &Int8FieldData{Nullable: true}, int8(1), [4]int{1, 3, 5, 7}},
		{"int16", &Int16FieldData{Nullable: true}, int16(1), [4]int{1, 4, 7, 10}},
		{"int32", &Int32FieldData{Nullable: true}, int32(1), [4]int{1, 6, 11, 16}},
		{"int64", &Int64FieldData{Nullable: true}, int64(1), [4]int{1, 10, 19, 28}},
		{"float", &FloatFieldData{Nullable: true}, float32(1), [4]int{1, 6, 11, 16}},
		{"double", &DoubleFieldData{Nullable: true}, float64(1), [4]int{1, 10, 19, 28}},
		{"timestamptz", &TimestamptzFieldData{Nullable: true}, int64(1), [4]int{1, 10, 19, 28}},
		{"string", &StringFieldData{Nullable: true}, "中", [4]int{1, 18, 38, 55}},
		{"json", &JSONFieldData{Nullable: true}, []byte("{}"), [4]int{1, 18, 37, 54}},
		{"geometry", &GeometryFieldData{Nullable: true}, []byte{1, 2, 3}, [4]int{1, 18, 38, 55}},
		// Compact nullable vectors store no payload for null rows and charge
		// 16 bytes per non-null L2P entry, in addition to the validity byte.
		{"binary", &BinaryVectorFieldData{Nullable: true, Dim: 8}, []byte{1}, [4]int{13, 14, 32, 33}},
		{"float_vector", &FloatVectorFieldData{Nullable: true, Dim: 2}, []float32{1, 2}, [4]int{13, 14, 39, 40}},
		{"float16", &Float16VectorFieldData{Nullable: true, Dim: 2}, []byte{0, 0, 0, 0}, [4]int{13, 14, 35, 36}},
		{"bfloat16", &BFloat16VectorFieldData{Nullable: true, Dim: 2}, []byte{0, 0, 0, 0}, [4]int{13, 14, 35, 36}},
		{"int8_vector", &Int8VectorFieldData{Nullable: true, Dim: 2}, []int8{1, 2}, [4]int{13, 14, 33, 34}},
		{"sparse", &SparseFloatVectorFieldData{Nullable: true}, typeutil.CreateSparseFloatRow([]uint32{0}, []float32{1}), [4]int{9, 10, 39, 40}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want[0], tt.field.GetMemorySize())
			for i, row := range []any{nil, tt.row, nil} {
				require.NoError(t, tt.field.AppendRow(row))
				require.Equal(t, tt.want[i+1], tt.field.GetMemorySize(), "after append %d", i)
			}
		})
	}
}

func TestFieldMemorySizeSparseEncodingBoundaries(t *testing.T) {
	field := &SparseFloatVectorFieldData{}
	require.Equal(t, 9, field.GetMemorySize())
	// Each one-entry row is 8 bytes plus a one-byte tag and length.
	// Dim=127 encodes in one byte; Dim=128 encodes in two bytes.
	for i, step := range []struct {
		index uint32
		want  int
	}{
		{126, 21}, // 9 fixed + 10 contents + 2 Dim
		{127, 32}, // 9 fixed + 20 contents + 3 Dim
		{0, 42},   // smaller row dimension does not shrink the batch Dim
	} {
		require.NoError(t, field.AppendRow(typeutil.CreateSparseFloatRow([]uint32{step.index}, []float32{1})))
		require.Equal(t, step.want, field.GetMemorySize(), "step %d", i)
	}
	indices := make([]uint32, 16)
	values := make([]float32, 16)
	for i := range indices {
		indices[i], values[i] = uint32(i), 1
	}
	// 128-byte contents needs a two-byte length prefix: 1 + 2 + 128.
	require.NoError(t, field.AppendRow(typeutil.CreateSparseFloatRow(indices, values)))
	require.Equal(t, 173, field.GetMemorySize())
	// An empty, non-null sparse row still has a protobuf tag and length.
	require.NoError(t, field.AppendRow([]byte{}))
	require.Equal(t, 175, field.GetMemorySize())
}

func TestFieldMemorySizeArrayValidity(t *testing.T) {
	row := &schemapb.ScalarField{
		Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 0}}},
		ValidData: []bool{true, false},
	}
	field := &ArrayFieldData{ElementType: schemapb.DataType_Int64, Nullable: true, ElementNullable: true}
	require.Equal(t, 2, field.GetMemorySize())
	require.NoError(t, field.AppendRow(nil))
	require.Equal(t, 3, field.GetMemorySize())
	require.NoError(t, field.AppendRow(row))
	// Two flags + two row-validity bytes + 16 payload + two element-validity bytes.
	require.Equal(t, 22, field.GetMemorySize())

	// Preserve the current string-array quirk: each non-nil string leaf
	// contributes an extra byte through StringFieldData.GetMemorySize.
	strings := &ArrayFieldData{ElementType: schemapb.DataType_VarChar}
	require.NoError(t, strings.AppendRow(&schemapb.ScalarField{
		Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", ""}}},
	}))
	require.Equal(t, 36, strings.GetMemorySize()) // 2 + (17 + 16 + 1)
}

func TestFieldMemorySizeVectorArrayValidity(t *testing.T) {
	field := &VectorArrayFieldData{Dim: 2, ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true}
	require.Equal(t, 2, field.GetMemorySize())
	require.NoError(t, field.AppendRow(nil))
	// AppendRow(nil) creates a non-nil empty vector: its Dim costs 4 bytes.
	require.Equal(t, 7, field.GetMemorySize())
	require.NoError(t, field.AppendRow(&schemapb.VectorField{
		Dim:       2,
		Data:      &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}},
		ValidData: []bool{true, false},
	}))
	require.Equal(t, 22, field.GetMemorySize()) // 7 + 8 payload + 4 Dim + 2 element-validity + 1 row-validity
}

func TestFieldMemorySizeSplitAppendAndFailure(t *testing.T) {
	field := &StringFieldData{Nullable: true}
	require.NoError(t, field.AppendDataRows([]string{"a", ""}))
	require.Equal(t, 34, field.GetMemorySize())
	require.NoError(t, field.AppendValidDataRows([]bool{true, false}))
	require.Equal(t, 36, field.GetMemorySize())
	require.NoError(t, field.AppendRows([]string{"中"}, []bool{true}))
	require.Equal(t, 56, field.GetMemorySize())
	// AppendRows mutates data before checking the validity argument's type.
	require.Error(t, field.AppendRows([]string{"b"}, "invalid validity"))
	require.Equal(t, 73, field.GetMemorySize())
}

// Expected sizes are explicit compatibility baselines, independent of the
// production sizing functions. shared is charged once after the first row.
type fieldMemoryCase struct {
	dataType                       schemapb.DataType
	row, batch                     any
	fixed, perRow, shared, mapping int
}

func fieldMemoryCases() []fieldMemoryCase {
	sparse := typeutil.CreateSparseFloatRow([]uint32{0}, []float32{1})
	array := &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}
	vector := &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 8)}}}
	return []fieldMemoryCase{
		{schemapb.DataType_Bool, true, []bool{true}, 1, 1, 0, 0},
		{schemapb.DataType_Int8, int8(1), []int8{1}, 1, 1, 0, 0},
		{schemapb.DataType_Int16, int16(1), []int16{1}, 1, 2, 0, 0},
		{schemapb.DataType_Int32, int32(1), []int32{1}, 1, 4, 0, 0},
		{schemapb.DataType_Int64, int64(1), []int64{1}, 1, 8, 0, 0},
		{schemapb.DataType_Timestamptz, int64(1), []int64{1}, 1, 8, 0, 0},
		{schemapb.DataType_Float, float32(1), []float32{1}, 1, 4, 0, 0},
		{schemapb.DataType_Double, float64(1), []float64{1}, 1, 8, 0, 0},
		{schemapb.DataType_VarChar, "中", []string{"中"}, 1, 19, 0, 0},
		{schemapb.DataType_String, "中", []string{"中"}, 1, 19, 0, 0},
		{schemapb.DataType_JSON, []byte("{}"), [][]byte{[]byte("{}")}, 1, 18, 0, 0},
		{schemapb.DataType_Geometry, []byte{1, 2, 3}, [][]byte{{1, 2, 3}}, 1, 19, 0, 0},
		{schemapb.DataType_BinaryVector, []byte{1}, []byte{1}, 13, 1, 0, 16},
		{schemapb.DataType_FloatVector, make([]float32, 8), make([]float32, 8), 13, 32, 0, 16},
		{schemapb.DataType_Float16Vector, make([]byte, 16), make([]byte, 16), 13, 16, 0, 16},
		{schemapb.DataType_BFloat16Vector, make([]byte, 16), make([]byte, 16), 13, 16, 0, 16},
		{schemapb.DataType_Int8Vector, make([]int8, 8), make([]int8, 8), 13, 8, 0, 16},
		{schemapb.DataType_SparseFloatVector, sparse, &SparseFloatVectorFieldData{SparseFloatArray: schemapb.SparseFloatArray{Contents: [][]byte{sparse}, Dim: 1}}, 9, 10, 2, 16},
		{schemapb.DataType_Array, array, []*schemapb.ScalarField{array}, 2, 16, 0, 0},
		{schemapb.DataType_ArrayOfVector, vector, []*schemapb.VectorField{vector}, 2, 36, 0, 0},
	}
}

func (c fieldMemoryCase) newField(t *testing.T, nullable bool, capacity int) FieldData {
	t.Helper()
	element := schemapb.DataType_Int64
	if c.dataType == schemapb.DataType_ArrayOfVector {
		element = schemapb.DataType_FloatVector
	}
	field, err := NewFieldData(c.dataType, &schemapb.FieldSchema{
		DataType: c.dataType, Nullable: nullable, ElementType: element,
		TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}},
	}, capacity)
	require.NoError(t, err)
	return field
}

func TestFieldMemorySizeAppendPaths(t *testing.T) {
	for _, c := range fieldMemoryCases() {
		for _, nullable := range []bool{false, true} {
			for _, capacity := range []int{0, 64} {
				for _, path := range []string{"row", "rows", "split"} {
					t.Run(fmt.Sprintf("%s/nullable=%t/cap=%d/%s", c.dataType, nullable, capacity, path), func(t *testing.T) {
						field := c.newField(t, nullable, capacity)
						require.Equal(t, c.fixed, field.GetMemorySize())
						validitySize := 0
						var validity any
						if nullable {
							validity, validitySize = []bool{true}, 1+c.mapping
						}
						for n := 1; n <= 2; n++ {
							want := c.fixed + c.shared + n*(c.perRow+validitySize)
							switch path {
							case "row":
								require.NoError(t, field.AppendRow(c.row))
							case "rows":
								require.NoError(t, field.AppendRows(c.batch, validity))
							case "split":
								require.NoError(t, field.AppendDataRows(c.batch))
								require.Equal(t, want-validitySize, field.GetMemorySize(), "data only")
								require.NoError(t, field.AppendValidDataRows(validity))
							}
							require.Equal(t, want, field.GetMemorySize(), "rows=%d", n)
							require.Equal(t, n, field.RowNum())
						}
					})
				}
			}
		}
	}
}

func TestFieldMemorySizeArrayElementTypes(t *testing.T) {
	tests := []struct {
		element schemapb.DataType
		row     *schemapb.ScalarField
		payload int
	}{
		{schemapb.DataType_Bool, &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false}}}}, 2},
		{schemapb.DataType_Int8, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}}, 2},
		{schemapb.DataType_Int16, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}}, 4},
		{schemapb.DataType_Int32, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}}, 8},
		{schemapb.DataType_Int64, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}, 16},
		{schemapb.DataType_Timestamptz, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}, 16},
		{schemapb.DataType_Float, &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1, 2}}}}, 8},
		{schemapb.DataType_Double, &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1, 2}}}}, 16},
		{schemapb.DataType_String, &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"中", ""}}}}, 36},
		{schemapb.DataType_VarChar, &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"中", ""}}}}, 36},
	}
	for _, tt := range tests {
		t.Run(tt.element.String(), func(t *testing.T) {
			field := &ArrayFieldData{ElementType: tt.element, Nullable: true, ElementNullable: true}
			require.Equal(t, 2, field.GetMemorySize())
			tt.row.ValidData = []bool{true, false}
			require.NoError(t, field.AppendRows([]*schemapb.ScalarField{nil, tt.row}, []bool{false, true}))
			require.Equal(t, 2+2+2+tt.payload, field.GetMemorySize())
		})
	}
}

func TestFieldMemorySizeVectorArrayElementTypes(t *testing.T) {
	tests := []struct {
		element schemapb.DataType
		row     *schemapb.VectorField
		payload int
	}{
		{schemapb.DataType_BinaryVector, &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2}}}, 2},
		{schemapb.DataType_FloatVector, &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 16)}}}, 64},
		{schemapb.DataType_Float16Vector, &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_Float16Vector{Float16Vector: make([]byte, 32)}}, 32},
		{schemapb.DataType_BFloat16Vector, &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: make([]byte, 32)}}, 32},
		{schemapb.DataType_Int8Vector, &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_Int8Vector{Int8Vector: make([]byte, 16)}}, 16},
	}
	for _, tt := range tests {
		t.Run(tt.element.String(), func(t *testing.T) {
			field := &VectorArrayFieldData{Dim: 8, ElementType: tt.element, Nullable: true, ElementNullable: true}
			require.Equal(t, 2, field.GetMemorySize())
			tt.row.ValidData = []bool{true, false}
			require.NoError(t, field.AppendRows([]*schemapb.VectorField{nil, tt.row}, []bool{false, true}))
			// nil contributes no dimension; the non-nil row contributes four bytes.
			require.Equal(t, 2+2+2+4+tt.payload, field.GetMemorySize())
		})
	}
}

func TestStringFieldDataTrackedAppend(t *testing.T) {
	for _, dataType := range []schemapb.DataType{schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text} {
		for _, nullable := range []bool{false, true} {
			for _, capacity := range []int{0, 64} {
				t.Run(fmt.Sprintf("%s/nullable=%t/cap=%d", dataType, nullable, capacity), func(t *testing.T) {
					field := NewStringFieldData(dataType, nullable, capacity)
					legacy := &StringFieldData{Data: make([]string, 0, capacity), DataType: dataType, Nullable: nullable}
					if nullable {
						legacy.ValidData = make([]bool, 0, capacity)
					}
					require.Equal(t, 1, field.GetTrackedMemorySize())
					require.Equal(t, capacity, cap(field.Data))
					require.Equal(t, dataType, field.GetDataType())
					steps := []struct {
						name         string
						tracked, old func() error
					}{
						{"row", func() error { return field.TrackedAppendRow("中") }, func() error { return legacy.AppendRow("中") }},
						{"empty row", func() error { return field.TrackedAppendRow("") }, func() error { return legacy.AppendRow("") }},
						{"null row", func() error { return field.TrackedAppendRow(nil) }, func() error { return legacy.AppendRow(nil) }},
						{"invalid row", func() error { return field.TrackedAppendRow(42) }, func() error { return legacy.AppendRow(42) }},
						{"batch", func() error { return field.TrackedAppendRows([]string{"a", "", "你好"}, []bool{true, false, true}) }, func() error { return legacy.AppendRows([]string{"a", "", "你好"}, []bool{true, false, true}) }},
						{"data only", func() error { return field.TrackedAppendDataRows([]string{"split", ""}) }, func() error { return legacy.AppendDataRows([]string{"split", ""}) }},
						{"validity only", func() error { return field.TrackedAppendValidDataRows([]bool{true, false}) }, func() error { return legacy.AppendValidDataRows([]bool{true, false}) }},
						{"nil validity", func() error { return field.TrackedAppendValidDataRows(nil) }, func() error { return legacy.AppendValidDataRows(nil) }},
						{"empty batch", func() error { return field.TrackedAppendRows([]string(nil), []bool(nil)) }, func() error { return legacy.AppendRows([]string(nil), []bool(nil)) }},
						{"invalid data", func() error { return field.TrackedAppendRows(42, []bool{true}) }, func() error { return legacy.AppendRows(42, []bool{true}) }},
						{"partial success", func() error { return field.TrackedAppendRows([]string{"retained"}, "bad validity") }, func() error { return legacy.AppendRows([]string{"retained"}, "bad validity") }},
						{"after error", func() error { return field.TrackedAppendRow("last") }, func() error { return legacy.AppendRow("last") }},
					}
					for _, step := range steps {
						t.Run(step.name, func(t *testing.T) {
							got, want := step.tracked(), step.old()
							if want != nil {
								require.EqualError(t, got, want.Error())
							} else {
								require.NoError(t, got)
							}
							require.Equal(t, legacy.Data, field.Data)
							require.Equal(t, legacy.ValidData, field.ValidData)
							require.Equal(t, legacy.GetMemorySize(), field.GetTrackedMemorySize())
							require.Equal(t, field.GetMemorySize(), field.GetTrackedMemorySize())
						})
					}
				})
			}
		}
	}
}

func TestStringFieldDataTrackedSizeBaseline(t *testing.T) {
	field := NewStringFieldData(schemapb.DataType_VarChar, true, 64)
	require.Equal(t, 1, field.GetTrackedMemorySize())
	require.NoError(t, field.TrackedAppendRow(nil))
	require.Equal(t, 18, field.GetTrackedMemorySize())
	require.NoError(t, field.TrackedAppendRow("中"))
	require.Equal(t, 38, field.GetTrackedMemorySize())
	require.NoError(t, field.TrackedAppendDataRows([]string{"a", ""}))
	require.Equal(t, 71, field.GetTrackedMemorySize())
	require.NoError(t, field.TrackedAppendValidDataRows([]bool{true, false}))
	require.Equal(t, 73, field.GetTrackedMemorySize())
	require.Error(t, field.TrackedAppendRows([]string{"b"}, "invalid"))
	require.Equal(t, 90, field.GetTrackedMemorySize())
	require.Equal(t, field.GetMemorySize(), field.GetTrackedMemorySize())
}

func TestScalarFieldDataTrackedAppend(t *testing.T) {
	cases := []struct {
		name               string
		newField           func(bool, int) FieldData
		row, batch         any
		rowSize, batchSize int
	}{
		{"Bool", func(n bool, c int) FieldData { return NewBoolFieldData(n, c) }, true, []bool{true, false}, 1, 2},
		{"Int8", func(n bool, c int) FieldData { return NewInt8FieldData(n, c) }, int8(1), []int8{1, 0}, 1, 2},
		{"Int16", func(n bool, c int) FieldData { return NewInt16FieldData(n, c) }, int16(1), []int16{1, 0}, 2, 4},
		{"Int32", func(n bool, c int) FieldData { return NewInt32FieldData(n, c) }, int32(1), []int32{1, 0}, 4, 8},
		{"Int64", func(n bool, c int) FieldData { return NewInt64FieldData(n, c) }, int64(1), []int64{1, 0}, 8, 16},
		{"Float", func(n bool, c int) FieldData { return NewFloatFieldData(n, c) }, float32(1), []float32{1, 0}, 4, 8},
		{"Double", func(n bool, c int) FieldData { return NewDoubleFieldData(n, c) }, float64(1), []float64{1, 0}, 8, 16},
		{"Timestamptz", func(n bool, c int) FieldData { return NewTimestamptzFieldData(n, c) }, int64(1), []int64{1, 0}, 8, 16},
		{"JSON", func(n bool, c int) FieldData { return NewJSONFieldData(n, c) }, []byte("{}"), [][]byte{[]byte("{}"), nil}, 18, 34},
		{"Geometry", func(n bool, c int) FieldData { return NewGeometryFieldData(n, c) }, []byte{1, 2, 3}, [][]byte{{1, 2, 3}, nil}, 19, 35},
		{"GeometryString", func(n bool, c int) FieldData { return NewGeometryFieldData(n, c) }, "中", []string{"中", ""}, 19, 35},
	}
	for _, tc := range cases {
		for _, nullable := range []bool{false, true} {
			for _, capacity := range []int{0, 64} {
				t.Run(fmt.Sprintf("%s/nullable=%t/cap=%d", tc.name, nullable, capacity), func(t *testing.T) {
					field, legacy := tc.newField(nullable, capacity), tc.newField(nullable, capacity)
					want := 1
					check := func() {
						t.Helper()
						require.Equal(t, want, field.GetTrackedMemorySize())
						require.Equal(t, legacy.GetMemorySize(), field.GetTrackedMemorySize())
						require.Equal(t, field.GetMemorySize(), field.GetTrackedMemorySize())
						require.Equal(t, legacy.GetDataRows(), field.GetDataRows())
						require.Equal(t, legacy.GetValidData(), field.GetValidData())
					}
					check()
					// Multiple batches exercise growth beyond the initial capacity.
					for i := 0; i < 40; i++ {
						require.NoError(t, field.TrackedAppendRow(tc.row))
						require.NoError(t, legacy.AppendRow(tc.row))
						want += tc.rowSize
						if nullable {
							want++
						}
						check()
						require.NoError(t, field.TrackedAppendRows(tc.batch, []bool{true, false}))
						require.NoError(t, legacy.AppendRows(tc.batch, []bool{true, false}))
						want += tc.batchSize + 2
						check()
					}
					require.NoError(t, field.TrackedAppendDataRows(tc.batch))
					require.NoError(t, legacy.AppendDataRows(tc.batch))
					want += tc.batchSize
					check()
					require.NoError(t, field.TrackedAppendValidDataRows([]bool{true, false}))
					require.NoError(t, legacy.AppendValidDataRows([]bool{true, false}))
					want += 2
					check()
					require.NoError(t, field.TrackedAppendValidDataRows(nil))
					require.NoError(t, legacy.AppendValidDataRows(nil))
					check()
					got, old := field.TrackedAppendRow(nil), legacy.AppendRow(nil)
					if nullable {
						require.NoError(t, got)
						require.NoError(t, old)
						if tc.name == "JSON" || tc.name == "Geometry" || tc.name == "GeometryString" {
							want += 17
						} else {
							want += tc.rowSize + 1
						}
					} else {
						require.Error(t, old)
						require.EqualError(t, got, old.Error())
					}
					check()
					got, old = field.TrackedAppendRow(struct{}{}), legacy.AppendRow(struct{}{})
					require.Error(t, old)
					require.EqualError(t, got, old.Error())
					check()
					got, old = field.TrackedAppendRows(struct{}{}, []bool{true}), legacy.AppendRows(struct{}{}, []bool{true})
					require.Error(t, old)
					require.EqualError(t, got, old.Error())
					check()
					got, old = field.TrackedAppendRows(tc.batch, "invalid validity"), legacy.AppendRows(tc.batch, "invalid validity")
					require.Error(t, old)
					require.EqualError(t, got, old.Error())
					want += tc.batchSize
					check()
					require.NoError(t, field.TrackedAppendRow(tc.row))
					require.NoError(t, legacy.AppendRow(tc.row))
					want += tc.rowSize
					if nullable {
						want++
					}
					check()
				})
			}
		}
	}
}

func TestArrayFieldDataTrackedAppend(t *testing.T) {
	tests := []struct {
		element schemapb.DataType
		row     *schemapb.ScalarField
		payload int
	}{
		{schemapb.DataType_Bool, &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false}}}}, 2},
		{schemapb.DataType_Int8, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}}, 2},
		{schemapb.DataType_Int16, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}}, 4},
		{schemapb.DataType_Int32, &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}}, 8},
		{schemapb.DataType_Int64, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}, 16},
		{schemapb.DataType_Timestamptz, &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}, 16},
		{schemapb.DataType_Float, &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1, 2}}}}, 8},
		{schemapb.DataType_Double, &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1, 2}}}}, 16},
		{schemapb.DataType_String, &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"中", ""}}}}, 36},
		{schemapb.DataType_VarChar, &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"中", ""}}}}, 36},
	}

	tests = append(tests, struct {
		element schemapb.DataType
		row     *schemapb.ScalarField
		payload int
	}{
		schemapb.DataType_Array,
		nestedArrayData(schemapb.DataType_Array, nestedArrayData(schemapb.DataType_Int16, nestedArrayIntData(1, 2), nestedArrayIntData(3))), 6,
	})

	for _, tt := range tests {
		for _, elementNullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/elementNullable=%t", tt.element, elementNullable), func(t *testing.T) {
				tt.row.ValidData = nil
				elementSize := 0
				if elementNullable {
					tt.row.ValidData = []bool{true, false}
					elementSize = 2
				}

				runTrackedArrayAppend(t, func(nullable bool, capacity int) FieldData {
					field := NewArrayFieldData(tt.element, nullable, elementNullable, capacity)
					require.Equal(t, capacity, cap(field.Data))
					require.Equal(t, tt.element, field.GetElementType())
					require.Equal(t, elementNullable, field.GetElementNullable())
					return field
				}, tt.row, []*schemapb.ScalarField{tt.row, nil}, []*schemapb.ScalarField(nil), (*schemapb.ScalarField)(nil), tt.payload+elementSize, 0)
			})
		}
	}
}

func TestVectorArrayFieldDataTrackedAppend(t *testing.T) {
	tests := []struct {
		element schemapb.DataType
		row     *schemapb.VectorField
		payload int
	}{
		{schemapb.DataType_BinaryVector, &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2}}}, 2},
		{schemapb.DataType_FloatVector, &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 16)}}}, 64},
		{schemapb.DataType_Float16Vector, &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_Float16Vector{Float16Vector: make([]byte, 32)}}, 32},
		{schemapb.DataType_BFloat16Vector, &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: make([]byte, 32)}}, 32},
		{schemapb.DataType_Int8Vector, &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_Int8Vector{Int8Vector: make([]byte, 16)}}, 16},
	}

	for _, tt := range tests {
		for _, elementNullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/elementNullable=%t", tt.element, elementNullable), func(t *testing.T) {
				tt.row.ValidData = nil
				elementSize := 0
				if elementNullable {
					tt.row.ValidData = []bool{true, false}
					elementSize = 2
				}

				runTrackedArrayAppend(t, func(nullable bool, capacity int) FieldData {
					field := NewVectorArrayFieldData(tt.element, 8, nullable, elementNullable, capacity)
					require.Equal(t, capacity, cap(field.Data))
					require.Equal(t, tt.element, field.ElementType)
					require.EqualValues(t, 8, field.Dim)
					require.Equal(t, elementNullable, field.GetElementNullable())
					return field
				}, tt.row, []*schemapb.VectorField{tt.row, nil}, []*schemapb.VectorField(nil), (*schemapb.VectorField)(nil), tt.payload+4+elementSize, 4)
			})
		}
	}
}

// batch contains one non-nil row and one nil pointer. A nil interface passed
// to AppendRow can have a different cost: VectorArray creates a placeholder.
func runTrackedArrayAppend(t *testing.T, newField func(bool, int) FieldData, row, batch, emptyBatch, nilRow any, rowSize, nullSize int) {
	t.Helper()
	for _, nullable := range []bool{false, true} {
		for _, capacity := range []int{0, 8} {
			t.Run(fmt.Sprintf("nullable=%t/cap=%d", nullable, capacity), func(t *testing.T) {
				field, legacy := newField(nullable, capacity), newField(nullable, capacity)
				want := 2
				check := func() {
					t.Helper()
					require.Equal(t, want, field.GetTrackedMemorySize())
					require.Equal(t, legacy.GetMemorySize(), field.GetTrackedMemorySize())
					require.Equal(t, field.GetMemorySize(), field.GetTrackedMemorySize())
					require.Equal(t, legacy.GetDataRows(), field.GetDataRows())
					require.Equal(t, legacy.GetValidData(), field.GetValidData())
				}
				check()
				for i := 0; i < 8; i++ {
					require.NoError(t, field.TrackedAppendRow(row))
					require.NoError(t, legacy.AppendRow(row))
					want += rowSize
					if nullable {
						want++
					}
					check()
					require.NoError(t, field.TrackedAppendRows(batch, []bool{true, false}))
					require.NoError(t, legacy.AppendRows(batch, []bool{true, false}))
					want += rowSize + 2
					check()
				}
				require.NoError(t, field.TrackedAppendDataRows(batch))
				require.NoError(t, legacy.AppendDataRows(batch))
				want += rowSize
				check()
				require.NoError(t, field.TrackedAppendValidDataRows([]bool{true, false}))
				require.NoError(t, legacy.AppendValidDataRows([]bool{true, false}))
				want += 2
				check()
				require.NoError(t, field.TrackedAppendRows(emptyBatch, nil))
				require.NoError(t, legacy.AppendRows(emptyBatch, nil))
				require.NoError(t, field.TrackedAppendValidDataRows(nil))
				require.NoError(t, legacy.AppendValidDataRows(nil))
				check()
				// Typed nil pointers are accepted as data, even for non-nullable fields.
				require.NoError(t, field.TrackedAppendRow(nilRow))
				require.NoError(t, legacy.AppendRow(nilRow))
				if nullable {
					want++
				}
				check()
				got, old := field.TrackedAppendRow(nil), legacy.AppendRow(nil)
				if nullable {
					require.NoError(t, got)
					require.NoError(t, old)
					want += nullSize + 1
				} else {
					require.Error(t, old)
					require.EqualError(t, got, old.Error())
				}
				check()
				got, old = field.TrackedAppendRow(42), legacy.AppendRow(42)
				require.Error(t, old)
				require.EqualError(t, got, old.Error())
				check()
				got, old = field.TrackedAppendRows(42, []bool{true}), legacy.AppendRows(42, []bool{true})
				require.Error(t, old)
				require.EqualError(t, got, old.Error())
				check()
				got, old = field.TrackedAppendRows(batch, "invalid validity"), legacy.AppendRows(batch, "invalid validity")
				require.Error(t, old)
				require.EqualError(t, got, old.Error())
				want += rowSize
				check()
				require.NoError(t, field.TrackedAppendRow(row))
				require.NoError(t, legacy.AppendRow(row))
				want += rowSize
				if nullable {
					want++
				}
				check()
			})
		}
	}
}

func TestVectorFieldDataTrackedAppend(t *testing.T) {
	constructors := map[schemapb.DataType]func(bool, int) FieldData{
		schemapb.DataType_BinaryVector:      func(n bool, c int) FieldData { return NewBinaryVectorFieldData(8, n, c) },
		schemapb.DataType_FloatVector:       func(n bool, c int) FieldData { return NewFloatVectorFieldData(8, n, c) },
		schemapb.DataType_Float16Vector:     func(n bool, c int) FieldData { return NewFloat16VectorFieldData(8, n, c) },
		schemapb.DataType_BFloat16Vector:    func(n bool, c int) FieldData { return NewBFloat16VectorFieldData(8, n, c) },
		schemapb.DataType_Int8Vector:        func(n bool, c int) FieldData { return NewInt8VectorFieldData(8, n, c) },
		schemapb.DataType_SparseFloatVector: func(n bool, c int) FieldData { return NewSparseFloatVectorFieldData(n, c) },
	}
	for _, tc := range fieldMemoryCases() {
		newField, ok := constructors[tc.dataType]
		if !ok {
			continue
		}
		for _, nullable := range []bool{false, true} {
			for _, capacity := range []int{0, 8} {
				t.Run(fmt.Sprintf("%s/nullable=%t/cap=%d", tc.dataType, nullable, capacity), func(t *testing.T) {
					field, legacy := newField(nullable, capacity), newField(nullable, capacity)
					want := tc.fixed
					check := func() {
						t.Helper()
						require.Equal(t, want, field.GetTrackedMemorySize())
						require.Equal(t, legacy.GetMemorySize(), field.GetTrackedMemorySize())
						require.Equal(t, field.GetMemorySize(), field.GetTrackedMemorySize())
						require.Equal(t, legacy.GetDataRows(), field.GetDataRows())
						require.Equal(t, legacy.GetValidData(), field.GetValidData())
					}
					apply := func(name string, tracked, old func() error, delta int, fail bool) {
						t.Helper()
						t.Run(name, func(t *testing.T) {
							got, expected := tracked(), old()
							if fail {
								require.Error(t, expected)
								require.EqualError(t, got, expected.Error())
							} else {
								require.NoError(t, expected)
								require.NoError(t, got)
							}
							want += delta
							check()
						})
					}
					check()
					single := tc.perRow
					if nullable {
						single += 17
					}
					for i := 0; i < 12; i++ {
						delta := single
						if i == 0 {
							delta += tc.shared
						}
						apply(fmt.Sprintf("row-%d", i), func() error { return field.TrackedAppendRow(tc.row) }, func() error { return legacy.AppendRow(tc.row) }, delta, false)
					}
					nullDelta := 0
					if nullable {
						nullDelta = 1
					}
					apply("null", func() error { return field.TrackedAppendRow(nil) }, func() error { return legacy.AppendRow(nil) }, nullDelta, !nullable)
					// Explicit validity is stored and mapped even for a non-nullable field.
					apply("compact batch", func() error { return field.TrackedAppendRows(tc.batch, []bool{true, false}) }, func() error { return legacy.AppendRows(tc.batch, []bool{true, false}) }, tc.perRow+18, false)
					apply("data only", func() error { return field.TrackedAppendDataRows(tc.batch) }, func() error { return legacy.AppendDataRows(tc.batch) }, tc.perRow, false)
					apply("nil validity during split", func() error { return field.TrackedAppendValidDataRows(nil) }, func() error { return legacy.AppendValidDataRows(nil) }, 0, false)
					apply("typed nil validity during split", func() error { return field.TrackedAppendValidDataRows([]bool(nil)) }, func() error { return legacy.AppendValidDataRows([]bool(nil)) }, 0, nullable)
					if nullable {
						apply("invalid validity count", func() error { return field.TrackedAppendValidDataRows([]bool{false}) }, func() error { return legacy.AppendValidDataRows([]bool{false}) }, 0, true)
					}
					apply("validity only", func() error { return field.TrackedAppendValidDataRows([]bool{true, false}) }, func() error { return legacy.AppendValidDataRows([]bool{true, false}) }, 18, false)
					apply("nil validity", func() error { return field.TrackedAppendValidDataRows(nil) }, func() error { return legacy.AppendValidDataRows(nil) }, 0, false)
					apply("invalid row type", func() error { return field.TrackedAppendRow(42) }, func() error { return legacy.AppendRow(42) }, 0, true)
					apply("invalid data type", func() error { return field.TrackedAppendDataRows(42) }, func() error { return legacy.AppendDataRows(42) }, 0, true)
					apply("invalid batch type", func() error { return field.TrackedAppendRows(42, []bool{true}) }, func() error { return legacy.AppendRows(42, []bool{true}) }, 0, true)
					delta := tc.perRow
					if nullable {
						delta = 0
					}
					apply("invalid validity type", func() error { return field.TrackedAppendRows(tc.batch, "invalid") }, func() error { return legacy.AppendRows(tc.batch, "invalid") }, delta, true)
					apply("after error", func() error { return field.TrackedAppendRow(tc.row) }, func() error { return legacy.AppendRow(tc.row) }, single, false)
				})
			}
		}
	}
}

func TestSparseFloatVectorFieldDataTrackedEncoding(t *testing.T) {
	field := NewSparseFloatVectorFieldData(false, 64)
	for _, step := range []struct {
		index uint32
		want  int
	}{{126, 21}, {127, 32}, {0, 42}, {16383, 53}} {
		require.NoError(t, field.TrackedAppendRow(typeutil.CreateSparseFloatRow([]uint32{step.index}, []float32{1})))
		require.Equal(t, step.want, field.GetTrackedMemorySize())
		require.Equal(t, field.GetMemorySize(), field.GetTrackedMemorySize())
	}
	indices, values := make([]uint32, 16), make([]float32, 16)
	for i := range indices {
		indices[i], values[i] = uint32(i), 1
	}
	require.NoError(t, field.TrackedAppendRow(typeutil.CreateSparseFloatRow(indices, values)))
	require.Equal(t, 184, field.GetTrackedMemorySize()) // 128-byte row + tag + two-byte length.
	require.NoError(t, field.TrackedAppendRow([]byte{}))
	require.Equal(t, 186, field.GetTrackedMemorySize())
	require.Equal(t, field.GetMemorySize(), field.GetTrackedMemorySize())
	require.Error(t, field.TrackedAppendRow([]byte{1}))
	require.Equal(t, 186, field.GetTrackedMemorySize())
	// An empty source can still increase the shared Dim encoding size.
	source := &SparseFloatVectorFieldData{SparseFloatArray: schemapb.SparseFloatArray{Dim: 1 << 21}}
	require.NoError(t, field.TrackedAppendDataRows(source))
	require.Equal(t, 187, field.GetTrackedMemorySize())
	require.Equal(t, field.GetMemorySize(), field.GetTrackedMemorySize())
}

func TestSparseFloatVectorFieldDataTrackedAppendAllRows(t *testing.T) {
	field := NewSparseFloatVectorFieldData(false, 8)
	source := NewSparseFloatVectorFieldData(true, 8)
	require.NoError(t, source.TrackedAppendRow(nil))
	require.NoError(t, source.TrackedAppendRow(typeutil.CreateSparseFloatRow([]uint32{127}, []float32{1})))
	for i := 1; i <= 2; i++ {
		field.TrackedAppendAllRows(source)
		require.True(t, field.Nullable)
		require.Equal(t, 9+3+i*(10+2+16), field.GetTrackedMemorySize())
		require.Equal(t, field.GetMemorySize(), field.GetTrackedMemorySize())
		require.Nil(t, field.GetRow((i-1)*2))
		require.Equal(t, source.GetRow(1), field.GetRow((i-1)*2+1))
	}
}

func TestTrackedAppendValidDataRowsInvalidType(t *testing.T) {
	fields := []FieldData{
		NewBoolFieldData(true, 0),
		NewInt8FieldData(true, 0),
		NewInt16FieldData(true, 0),
		NewInt32FieldData(true, 0),
		NewInt64FieldData(true, 0),
		NewFloatFieldData(true, 0),
		NewDoubleFieldData(true, 0),
		NewTimestamptzFieldData(true, 0),
		NewJSONFieldData(true, 0),
		NewGeometryFieldData(true, 0),

		NewStringFieldData(schemapb.DataType_VarChar, true, 0),
		NewArrayFieldData(schemapb.DataType_Int64, true, true, 0),
		NewVectorArrayFieldData(schemapb.DataType_FloatVector, 8, true, true, 0),
		NewBinaryVectorFieldData(8, true, 0),
		NewFloatVectorFieldData(8, true, 0),
		NewFloat16VectorFieldData(8, true, 0),
		NewBFloat16VectorFieldData(8, true, 0),
		NewInt8VectorFieldData(8, true, 0),
		NewSparseFloatVectorFieldData(true, 0),
	}
	for _, field := range fields {
		t.Run(field.GetDataType().String(), func(t *testing.T) {
			before := field.GetTrackedMemorySize()
			got := field.TrackedAppendValidDataRows("invalid")
			want := field.AppendValidDataRows("invalid")
			require.Error(t, want)
			require.EqualError(t, got, want.Error())
			require.Empty(t, field.GetValidData())
			require.Equal(t, before, field.GetTrackedMemorySize())
			require.Equal(t, field.GetMemorySize(), field.GetTrackedMemorySize())
		})
	}
}

func TestNewFieldDataTrackedSize(t *testing.T) {
	cases := fieldMemoryCases()
	cases = append(cases, fieldMemoryCase{schemapb.DataType_Text, "中", []string{"中"}, 1, 19, 0, 0})
	for _, tc := range cases {
		for _, nullable := range []bool{false, true} {
			for _, capacity := range []int{0, 64} {
				t.Run(fmt.Sprintf("%s/nullable=%t/cap=%d", tc.dataType, nullable, capacity), func(t *testing.T) {
					field := tc.newField(t, nullable, capacity)
					tracked := field
					require.Equal(t, tc.fixed, tracked.GetTrackedMemorySize())
					require.Zero(t, field.RowNum())
					require.Equal(t, tc.dataType, field.GetDataType())
					require.Equal(t, nullable, field.GetNullable())
					wantCapacity := capacity
					if tc.dataType == schemapb.DataType_SparseFloatVector {
						wantCapacity = 0 // Preserve the original sparse factory's reservation policy.
					} else if !nullable {
						switch tc.dataType {
						case schemapb.DataType_FloatVector, schemapb.DataType_Int8Vector:
							wantCapacity *= 8
						case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
							wantCapacity *= 16
						}
					}
					require.Equal(t, wantCapacity, reflect.ValueOf(field.GetDataRows()).Cap())
					if nullable {
						require.Equal(t, capacity, cap(field.GetValidData()))
					} else {
						require.Nil(t, field.GetValidData())
					}
					for n := 1; n <= 2; n++ {
						require.NoError(t, tracked.TrackedAppendRow(tc.row))
						want := tc.fixed + tc.shared + n*tc.perRow
						if nullable {
							want += n * (1 + tc.mapping)
						}
						require.Equal(t, want, tracked.GetTrackedMemorySize())
						require.Equal(t, field.GetMemorySize(), tracked.GetTrackedMemorySize())
					}
				})
			}
		}
	}
}

func TestNewFieldDataMissingDimension(t *testing.T) {
	for _, dataType := range []schemapb.DataType{
		schemapb.DataType_BinaryVector, schemapb.DataType_FloatVector,
		schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector, schemapb.DataType_ArrayOfVector,
	} {
		t.Run(dataType.String(), func(t *testing.T) {
			field, err := NewFieldData(dataType, &schemapb.FieldSchema{DataType: dataType}, 0)
			require.Error(t, err)
			require.Nil(t, field)
		})
	}
}

// BenchmarkFieldDataMemoryAccounting compares whole batches with and without
// incremental accounting. Input decoding is excluded; each batch starts with
// the same 1024-row reservation as the binlog reader. An operation is one batch.
func BenchmarkFieldDataMemoryAccounting(b *testing.B) {
	indices, values := make([]uint32, 64), make([]float32, 64)
	for i := range indices {
		indices[i], values[i] = uint32(i*3), float32(i+1)
	}
	sparse := typeutil.CreateSparseFloatRow(indices, values)
	text := strings.Repeat("x", 1024)
	cases := []struct {
		name     string
		newField func(bool, int) FieldData
		row      any
	}{
		{"Int64", func(nullable bool, capacity int) FieldData { return NewInt64FieldData(nullable, capacity) }, int64(42)},
		{"String1024", func(nullable bool, capacity int) FieldData {
			return NewStringFieldData(schemapb.DataType_VarChar, nullable, capacity)
		}, text},
		{"JSON1024", func(nullable bool, capacity int) FieldData { return NewJSONFieldData(nullable, capacity) }, []byte(`{"value":"` + text + `"}`)},
		{"Sparse64", func(nullable bool, capacity int) FieldData { return NewSparseFloatVectorFieldData(nullable, capacity) }, sparse},
	}
	for _, tc := range cases {
		rowCounts := []int{1000, 10000, 100000}
		if tc.name == "Int64" {
			rowCounts = append(rowCounts, 1000000)
		}
		for _, nullable := range []bool{false, true} {
			for _, rowCount := range rowCounts {
				b.Run(fmt.Sprintf("%s/nullable=%t/rows=%d", tc.name, nullable, rowCount), func(b *testing.B) {
					// Reuse immutable payloads: this measures append/accounting, not
					// decoding or allocating the incoming row payload. Nullable inputs
					// contain 10% nulls, including both validity and mapping updates.
					rows := make([]any, rowCount)
					for i := range rows {
						if !nullable || i%10 != 0 {
							rows[i] = tc.row
						}
					}
					// Verify identical sizes at each checkpoint outside timed work.
					legacy, tracked := tc.newField(nullable, 1024), tc.newField(nullable, 1024)
					for i, row := range rows {
						require.NoError(b, legacy.AppendRow(row))
						require.NoError(b, tracked.TrackedAppendRow(row))
						if (i+1)%100 == 0 {
							require.Equal(b, legacy.GetMemorySize(), tracked.GetTrackedMemorySize())
						}
					}
					require.Equal(b, legacy.RowNum(), tracked.RowNum())
					for _, checkEvery := range []int{0, 100} {
						for _, useTracked := range []bool{false, true} {
							method := "Scan"
							if useTracked {
								method = "Tracked"
							}
							b.Run(fmt.Sprintf("check=%d/%s", checkEvery, method), func(b *testing.B) {
								var field FieldData
								var size int
								b.ReportAllocs()
								b.ResetTimer()
								for n := 0; n < b.N; n++ {
									field = tc.newField(nullable, 1024)
									appendRow, getSize := field.AppendRow, field.GetMemorySize
									if useTracked {
										appendRow, getSize = field.TrackedAppendRow, field.GetTrackedMemorySize
									}
									for i, row := range rows {
										if err := appendRow(row); err != nil {
											b.Fatal(err)
										}
										if checkEvery != 0 && (i+1)%checkEvery == 0 {
											size = getSize()
										}
									}
								}
								b.StopTimer()
								require.Equal(b, rowCount, field.RowNum())
								if useTracked {
									require.Equal(b, field.GetMemorySize(), field.GetTrackedMemorySize())
								}
								if checkEvery != 0 {
									require.Equal(b, field.GetMemorySize(), size)
								}
								b.ReportMetric(float64(rowCount)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
							})
						}
					}
				})
			}
		}
	}
}
