package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestInsertDataSuite(t *testing.T) {
	suite.Run(t, new(InsertDataSuite))
}

func TestArrayFieldDataSuite(t *testing.T) {
	suite.Run(t, new(ArrayFieldDataSuite))
}

type InsertDataSuite struct {
	suite.Suite

	schema *schemapb.CollectionSchema

	iDataOneRow  *InsertData
	iDataTwoRows *InsertData
	iDataEmpty   *InsertData
}

func (s *InsertDataSuite) SetupSuite() {
	s.schema = genTestCollectionMeta().Schema
}

func (s *InsertDataSuite) TestInsertData() {
	s.Run("nil schema", func() {
		idata, err := NewInsertData(nil)
		s.Error(err)
		s.Nil(idata)
	})

	s.Run("nullable field schema", func() {
		tests := []struct {
			description string
			dataType    schemapb.DataType
			typeParams  []*commonpb.KeyValuePair
			nullable    bool
		}{
			{"nullable bool field", schemapb.DataType_Bool, nil, true},
			{"nullable int8 field", schemapb.DataType_Int8, nil, true},
			{"nullable int16 field", schemapb.DataType_Int16, nil, true},
			{"nullable int32 field", schemapb.DataType_Int32, nil, true},
			{"nullable int64 field", schemapb.DataType_Int64, nil, true},
			{"nullable float field", schemapb.DataType_Float, nil, true},
			{"nullable double field", schemapb.DataType_Double, nil, true},
			{"nullable json field", schemapb.DataType_JSON, nil, true},
			{"nullable array field", schemapb.DataType_Array, nil, true},
			{"nullable mol field", schemapb.DataType_Mol, nil, true},
			{"nullable string/varchar field", schemapb.DataType_String, nil, true},
			{"nullable binary vector field", schemapb.DataType_BinaryVector, []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}, true},
			{"nullable float vector field", schemapb.DataType_FloatVector, []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}, true},
			{"nullable float16 vector field", schemapb.DataType_Float16Vector, []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}, true},
			{"nullable bfloat16 vector field", schemapb.DataType_BFloat16Vector, []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}, true},
			{"nullable sparse float vector field", schemapb.DataType_SparseFloatVector, nil, true},
			{"nullable int8 vector field", schemapb.DataType_Int8Vector, []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}, true},
			{"non-nullable binary vector field", schemapb.DataType_BinaryVector, []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}, false},
			{"non-nullable float vector field", schemapb.DataType_FloatVector, []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}, false},
			{"non-nullable float16 vector field", schemapb.DataType_Float16Vector, []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}, false},
			{"non-nullable bfloat16 vector field", schemapb.DataType_BFloat16Vector, []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}, false},
			{"non-nullable sparse float vector field", schemapb.DataType_SparseFloatVector, nil, false},
			{"non-nullable int8 vector field", schemapb.DataType_Int8Vector, []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}, false},
		}

		for _, test := range tests {
			s.Run(test.description, func() {
				schema := &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{
							DataType:   test.dataType,
							Nullable:   test.nullable,
							TypeParams: test.typeParams,
						},
					},
				}
				_, err := NewInsertData(schema)
				s.Nil(err)
			})
		}
	})

	s.Run("invalid schema", func() {
		tests := []struct {
			description string
			invalidType schemapb.DataType
		}{
			{"binary vector without dim", schemapb.DataType_BinaryVector},
			{"float vector without dim", schemapb.DataType_FloatVector},
			{"float16 vector without dim", schemapb.DataType_Float16Vector},
			{"bfloat16 vector without dim", schemapb.DataType_BFloat16Vector},
			{"int8 vector without dim", schemapb.DataType_Int8Vector},
		}

		for _, test := range tests {
			s.Run(test.description, func() {
				schema := &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{
							DataType: test.invalidType,
						},
					},
				}
				idata, err := NewInsertData(schema)
				s.Error(err)
				s.Nil(idata)
			})
		}
	})

	s.Run("empty iData", func() {
		idata := &InsertData{}
		s.True(idata.IsEmpty())
		s.Equal(0, idata.GetRowNum())
		s.Equal(0, idata.GetMemorySize())

		err := idata.Append(map[FieldID]interface{}{1: struct{}{}})
		s.Error(err)
	})

	s.Run("init by New", func() {
		s.True(s.iDataEmpty.IsEmpty())
		s.Equal(0, s.iDataEmpty.GetRowNum())
		s.Equal(161, s.iDataEmpty.GetMemorySize())

		s.False(s.iDataOneRow.IsEmpty())
		s.Equal(1, s.iDataOneRow.GetRowNum())
		s.Equal(535, s.iDataOneRow.GetMemorySize())

		s.False(s.iDataTwoRows.IsEmpty())
		s.Equal(2, s.iDataTwoRows.GetRowNum())
		s.Equal(734, s.iDataTwoRows.GetMemorySize())

		for _, field := range s.iDataTwoRows.Data {
			s.Equal(2, field.RowNum())

			err := field.AppendRow(struct{}{})
			log.Warn("error", zap.Error(err))
			s.ErrorIs(err, merr.ErrParameterInvalid)
		}
	})
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
	s.Equal(s.iDataEmpty.Data[ArrayField].GetMemorySize(), 1)
	// +9 bytes: Nullable(1) + L2PMapping.GetMemorySize()(8)
	s.Equal(s.iDataEmpty.Data[BinaryVectorField].GetMemorySize(), 4+9)
	s.Equal(s.iDataEmpty.Data[FloatVectorField].GetMemorySize(), 4+9)
	s.Equal(s.iDataEmpty.Data[Float16VectorField].GetMemorySize(), 4+9)
	s.Equal(s.iDataEmpty.Data[BFloat16VectorField].GetMemorySize(), 4+9)
	s.Equal(s.iDataEmpty.Data[SparseFloatVectorField].GetMemorySize(), 0+9)
	s.Equal(s.iDataEmpty.Data[Int8VectorField].GetMemorySize(), 4+9)
	s.Equal(s.iDataEmpty.Data[StructSubInt32Field].GetMemorySize(), 1)
	s.Equal(s.iDataEmpty.Data[StructSubFloatVectorField].GetMemorySize(), 0)

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
	s.Equal(s.iDataOneRow.Data[ArrayField].GetMemorySize(), 3*4+1)
	// +9 bytes: Nullable(1) + L2PMapping.GetMemorySize()(8)
	s.Equal(s.iDataOneRow.Data[BinaryVectorField].GetMemorySize(), 5+9)
	s.Equal(s.iDataOneRow.Data[FloatVectorField].GetMemorySize(), 20+9)
	s.Equal(s.iDataOneRow.Data[Float16VectorField].GetMemorySize(), 12+9)
	s.Equal(s.iDataOneRow.Data[BFloat16VectorField].GetMemorySize(), 12+9)
	s.Equal(s.iDataOneRow.Data[SparseFloatVectorField].GetMemorySize(), 28+9)
	s.Equal(s.iDataOneRow.Data[Int8VectorField].GetMemorySize(), 8+9)
	s.Equal(s.iDataOneRow.Data[StructSubInt32Field].GetMemorySize(), 3*4+1)
	s.Equal(s.iDataOneRow.Data[StructSubFloatVectorField].GetMemorySize(), 3*4*2+4)

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
	s.Equal(s.iDataTwoRows.Data[ArrayField].GetMemorySize(), 25)
	// +9 bytes: Nullable(1) + L2PMapping.GetMemorySize()(8)
	s.Equal(s.iDataTwoRows.Data[BinaryVectorField].GetMemorySize(), 6+9)
	s.Equal(s.iDataTwoRows.Data[FloatVectorField].GetMemorySize(), 36+9)
	s.Equal(s.iDataTwoRows.Data[Float16VectorField].GetMemorySize(), 20+9)
	s.Equal(s.iDataTwoRows.Data[BFloat16VectorField].GetMemorySize(), 20+9)
	s.Equal(s.iDataTwoRows.Data[SparseFloatVectorField].GetMemorySize(), 54+9)
	s.Equal(s.iDataTwoRows.Data[Int8VectorField].GetMemorySize(), 12+9)
	s.Equal(s.iDataTwoRows.Data[StructSubInt32Field].GetMemorySize(), 3*4+2*4+1)
	s.Equal(s.iDataTwoRows.Data[StructSubFloatVectorField].GetMemorySize(), 3*4*2+4+2*4*2+4)
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

func (s *InsertDataSuite) SetupTest() {
	var err error
	s.iDataEmpty, err = NewInsertData(s.schema)
	s.Require().NoError(err)
	s.True(s.iDataEmpty.IsEmpty())
	s.Equal(0, s.iDataEmpty.GetRowNum())
	s.Equal(161, s.iDataEmpty.GetMemorySize())

	row1 := map[FieldID]interface{}{
		RowIDField:                     int64(3),
		TimestampField:                 int64(3),
		BoolField:                      true,
		Int8Field:                      int8(3),
		Int16Field:                     int16(3),
		Int32Field:                     int32(3),
		Int64Field:                     int64(3),
		FloatField:                     float32(3),
		DoubleField:                    float64(3),
		StringField:                    "str",
		BinaryVectorField:              []byte{0},
		FloatVectorField:               []float32{4, 5, 6, 7},
		Float16VectorField:             []byte{0, 0, 0, 0, 255, 255, 255, 255},
		BFloat16VectorField:            []byte{0, 0, 0, 0, 255, 255, 255, 255},
		SparseFloatVectorField:         typeutil.CreateSparseFloatRow([]uint32{0, 1, 2}, []float32{4, 5, 6}),
		Int8VectorField:                []int8{-4, -5, 6, 7},
		NullableFloatVectorField:       []float32{1.0, 2.0, 3.0, 4.0},
		NullableBinaryVectorField:      []byte{1},
		NullableFloat16VectorField:     []byte{1, 2, 3, 4, 5, 6, 7, 8},
		NullableBFloat16VectorField:    []byte{1, 2, 3, 4, 5, 6, 7, 8},
		NullableInt8VectorField:        []int8{1, 2, 3, 4},
		NullableSparseFloatVectorField: typeutil.CreateSparseFloatRow([]uint32{0, 1, 2}, []float32{4, 5, 6}),
		ArrayField: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
			},
		},
		JSONField: []byte(`{"batch":3}`),
		StructSubInt32Field: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
			},
		},
		StructSubFloatVectorField: &schemapb.VectorField{
			Dim: 2,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}},
			},
		},
	}

	s.iDataOneRow, err = NewInsertData(s.schema)
	s.Require().NoError(err)
	err = s.iDataOneRow.Append(row1)
	s.Require().NoError(err)

	for fID, field := range s.iDataOneRow.Data {
		s.Equal(row1[fID], field.GetRow(0))
	}

	row2 := map[FieldID]interface{}{
		RowIDField:                     int64(1),
		TimestampField:                 int64(1),
		BoolField:                      false,
		Int8Field:                      int8(1),
		Int16Field:                     int16(1),
		Int32Field:                     int32(1),
		Int64Field:                     int64(1),
		FloatField:                     float32(1),
		DoubleField:                    float64(1),
		StringField:                    string("str"),
		BinaryVectorField:              []byte{0},
		FloatVectorField:               []float32{4, 5, 6, 7},
		Float16VectorField:             []byte{1, 2, 3, 4, 5, 6, 7, 8},
		BFloat16VectorField:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
		SparseFloatVectorField:         typeutil.CreateSparseFloatRow([]uint32{2, 3, 4}, []float32{4, 5, 6}),
		Int8VectorField:                []int8{-128, -5, 6, 127},
		NullableFloatVectorField:       nil,
		NullableBinaryVectorField:      nil,
		NullableFloat16VectorField:     nil,
		NullableBFloat16VectorField:    nil,
		NullableInt8VectorField:        nil,
		NullableSparseFloatVectorField: nil,
		ArrayField: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
			},
		},
		JSONField: []byte(`{"batch":1}`),
		StructSubInt32Field: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{1, 2}},
			},
		},
		StructSubFloatVectorField: &schemapb.VectorField{
			Dim: 2,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}},
			},
		},
	}

	s.iDataTwoRows, err = NewInsertData(s.schema)
	s.Require().NoError(err)
	err = s.iDataTwoRows.Append(row1)
	s.Require().NoError(err)
	err = s.iDataTwoRows.Append(row2)
	s.Require().NoError(err)
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
	s.Equal(11, insertData.GetMemorySize())
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
	s.Equal(126, insertData.GetMemorySize())
	s.False(insertData.IsEmpty())
	s.Equal(115, insertData.GetRowSize(0))
}

func TestMolFieldData(t *testing.T) {
	t.Run("Test MolFieldData - Basic Operations", func(t *testing.T) {
		// Test non-nullable MolFieldData
		molData := &MolFieldData{
			Data:      make([][]byte, 0),
			Nullable:  false,
			ValidData: nil,
		}

		// Test RowNum
		assert.Equal(t, 0, molData.RowNum())

		// Test AppendRow with []byte
		testData1 := []byte("CCO") // Ethanol SMILES
		err := molData.AppendRow(testData1)
		assert.NoError(t, err)
		assert.Equal(t, 1, molData.RowNum())
		assert.Equal(t, testData1, molData.Data[0])

		// Test AppendRow with string
		testData2 := "CC" // Ethane SMILES
		err = molData.AppendRow(testData2)
		assert.NoError(t, err)
		assert.Equal(t, 2, molData.RowNum())
		assert.Equal(t, []byte(testData2), molData.Data[1])

		// Test GetRow
		row0 := molData.GetRow(0)
		assert.Equal(t, testData1, row0)
		row1 := molData.GetRow(1)
		assert.Equal(t, []byte(testData2), row1)

		// Test GetDataRows
		dataRows := molData.GetDataRows()
		assert.Equal(t, molData.Data, dataRows)

		// Test GetDataType
		assert.Equal(t, schemapb.DataType_Mol, molData.GetDataType())

		// Test GetNullable
		assert.Equal(t, false, molData.GetNullable())

		// Test GetMemorySize
		memorySize := molData.GetMemorySize()
		assert.Greater(t, memorySize, 0)

		// Test GetRowSize
		rowSize0 := molData.GetRowSize(0)
		assert.Equal(t, len(testData1)+16, rowSize0)
		rowSize1 := molData.GetRowSize(1)
		assert.Equal(t, len(testData2)+16, rowSize1)
	})

	t.Run("Test MolFieldData - Nullable", func(t *testing.T) {
		molData := &MolFieldData{
			Data:      make([][]byte, 0),
			Nullable:  true,
			ValidData: make([]bool, 0),
		}

		// Test AppendRow with nil (nullable)
		err := molData.AppendRow(nil)
		assert.NoError(t, err)
		assert.Equal(t, 1, molData.RowNum())
		assert.Equal(t, 1, len(molData.ValidData))
		assert.False(t, molData.ValidData[0])

		// Test GetRow with null value
		row := molData.GetRow(0)
		assert.Nil(t, row)

		// Test AppendRow with valid data
		testData := []byte("CCO")
		err = molData.AppendRow(testData)
		assert.NoError(t, err)
		assert.Equal(t, 2, molData.RowNum())
		assert.True(t, molData.ValidData[1])
		assert.Equal(t, testData, molData.Data[1])

		// Test GetValidData
		validData := molData.GetValidData()
		assert.Equal(t, []bool{false, true}, validData)
	})

	t.Run("Test MolFieldData - AppendRows", func(t *testing.T) {
		molData := &MolFieldData{
			Data:      make([][]byte, 0),
			Nullable:  false,
			ValidData: nil,
		}

		// Test AppendDataRows with [][]byte
		testData := [][]byte{
			[]byte("CCO"),
			[]byte("CC"),
			[]byte("C"),
		}
		err := molData.AppendDataRows(testData)
		assert.NoError(t, err)
		assert.Equal(t, 3, molData.RowNum())
		assert.Equal(t, testData, molData.Data)

		// Test AppendDataRows with []string
		molData2 := &MolFieldData{
			Data:      make([][]byte, 0),
			Nullable:  false,
			ValidData: nil,
		}
		testData2 := []string{"CCO", "CC", "C"}
		err = molData2.AppendDataRows(testData2)
		assert.NoError(t, err)
		assert.Equal(t, 3, molData2.RowNum())
		for i, s := range testData2 {
			assert.Equal(t, []byte(s), molData2.Data[i])
		}

		// Test AppendValidDataRows
		molData3 := &MolFieldData{
			Data:      make([][]byte, 0),
			Nullable:  true,
			ValidData: make([]bool, 0),
		}
		err = molData3.AppendDataRows(testData)
		assert.NoError(t, err)
		validData := []bool{true, false, true}
		err = molData3.AppendValidDataRows(validData)
		assert.NoError(t, err)
		assert.Equal(t, validData, molData3.ValidData)
	})

	t.Run("Test MolFieldData - Error Cases", func(t *testing.T) {
		molData := &MolFieldData{
			Data:      make([][]byte, 0),
			Nullable:  false,
			ValidData: nil,
		}

		// Test AppendRow with invalid type
		err := molData.AppendRow(123)
		assert.Error(t, err)

		// Test AppendDataRows with invalid type
		err = molData.AppendDataRows([]int{1, 2, 3})
		assert.Error(t, err)

		// Test AppendValidDataRows with invalid type
		molData2 := &MolFieldData{
			Data:      make([][]byte, 0),
			Nullable:  true,
			ValidData: make([]bool, 0),
		}
		err = molData2.AppendValidDataRows([]int{1, 2})
		assert.Error(t, err)
	})

	t.Run("Test MolFieldData - NewFieldData", func(t *testing.T) {
		// Test non-nullable
		fieldSchema := &schemapb.FieldSchema{
			FieldID:    100,
			Name:       "mol_field",
			DataType:   schemapb.DataType_Mol,
			Nullable:   false,
			TypeParams: nil,
		}
		fieldData, err := NewFieldData(schemapb.DataType_Mol, fieldSchema, 10)
		assert.NoError(t, err)
		assert.NotNil(t, fieldData)
		molData, ok := fieldData.(*MolFieldData)
		assert.True(t, ok)
		assert.False(t, molData.Nullable)
		assert.Nil(t, molData.ValidData)

		// Test nullable
		fieldSchema2 := &schemapb.FieldSchema{
			FieldID:    101,
			Name:       "mol_field_nullable",
			DataType:   schemapb.DataType_Mol,
			Nullable:   true,
			TypeParams: nil,
		}
		fieldData2, err := NewFieldData(schemapb.DataType_Mol, fieldSchema2, 10)
		assert.NoError(t, err)
		assert.NotNil(t, fieldData2)
		molData2, ok := fieldData2.(*MolFieldData)
		assert.True(t, ok)
		assert.True(t, molData2.Nullable)
		assert.NotNil(t, molData2.ValidData)
	})

	t.Run("Test MolFieldData - AppendRows with ValidData", func(t *testing.T) {
		molData := &MolFieldData{
			Data:      make([][]byte, 0),
			Nullable:  true,
			ValidData: make([]bool, 0),
		}

		testData := [][]byte{
			[]byte("CCO"),
			[]byte("CC"),
			[]byte("C"),
		}
		validData := []bool{true, false, true}

		err := molData.AppendRows(testData, validData)
		assert.NoError(t, err)
		assert.Equal(t, 3, molData.RowNum())
		assert.Equal(t, testData, molData.Data)
		assert.Equal(t, validData, molData.ValidData)
	})

	t.Run("Test MolFieldData - AppendRows nil validData", func(t *testing.T) {
		molData := &MolFieldData{
			Data:      make([][]byte, 0),
			Nullable:  false,
			ValidData: nil,
		}

		testData := [][]byte{[]byte("CCO"), []byte("CC")}
		err := molData.AppendRows(testData, nil)
		assert.NoError(t, err)
		assert.Equal(t, 2, molData.RowNum())
	})

	t.Run("Test MolFieldData - AppendValidDataRows wrong type", func(t *testing.T) {
		molData := &MolFieldData{
			Data:      make([][]byte, 0),
			Nullable:  true,
			ValidData: make([]bool, 0),
		}

		err := molData.AppendValidDataRows("invalid")
		assert.Error(t, err)
	})

	t.Run("Test MolFieldData - GetMemorySize non-empty", func(t *testing.T) {
		molData := &MolFieldData{
			Data:      [][]byte{[]byte("CCO"), []byte("CC(=O)O"), []byte("c1ccccc1")},
			Nullable:  false,
			ValidData: nil,
		}
		size := molData.GetMemorySize()
		assert.Greater(t, size, 0)
		expectedMin := (3 + 16) + (7 + 16) + (8 + 16)
		assert.GreaterOrEqual(t, size, expectedMin)
	})

	t.Run("Test MolFieldData - GetRow nullable with mixed data", func(t *testing.T) {
		molData := &MolFieldData{
			Data:      [][]byte{[]byte("CCO"), nil, []byte("C")},
			Nullable:  true,
			ValidData: []bool{true, false, true},
		}

		assert.Equal(t, []byte("CCO"), molData.GetRow(0))
		assert.Nil(t, molData.GetRow(1))
		assert.Equal(t, []byte("C"), molData.GetRow(2))
	})
}
