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
