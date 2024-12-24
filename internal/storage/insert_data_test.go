package storage

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
		}{
			{"nullable bool field", schemapb.DataType_Bool},
			{"nullable int8 field", schemapb.DataType_Int8},
			{"nullable int16 field", schemapb.DataType_Int16},
			{"nullable int32 field", schemapb.DataType_Int32},
			{"nullable int64 field", schemapb.DataType_Int64},
			{"nullable float field", schemapb.DataType_Float},
			{"nullable double field", schemapb.DataType_Double},
			{"nullable json field", schemapb.DataType_JSON},
			{"nullable array field", schemapb.DataType_Array},
			{"nullable string/varchar field", schemapb.DataType_String},
		}

		for _, test := range tests {
			s.Run(test.description, func() {
				schema := &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{
							DataType: test.dataType,
							Nullable: true,
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
		s.Equal(32, s.iDataEmpty.GetMemorySize())

		s.False(s.iDataOneRow.IsEmpty())
		s.Equal(1, s.iDataOneRow.GetRowNum())
		s.Equal(199, s.iDataOneRow.GetMemorySize())

		s.False(s.iDataTwoRows.IsEmpty())
		s.Equal(2, s.iDataTwoRows.GetRowNum())
		s.Equal(364, s.iDataTwoRows.GetMemorySize())

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
	s.Equal(s.iDataEmpty.Data[BinaryVectorField].GetMemorySize(), 4)
	s.Equal(s.iDataEmpty.Data[FloatVectorField].GetMemorySize(), 4)
	s.Equal(s.iDataEmpty.Data[Float16VectorField].GetMemorySize(), 4)
	s.Equal(s.iDataEmpty.Data[BFloat16VectorField].GetMemorySize(), 4)
	s.Equal(s.iDataEmpty.Data[SparseFloatVectorField].GetMemorySize(), 0)
	s.Equal(s.iDataEmpty.Data[Int8VectorField].GetMemorySize(), 4)

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
	s.Equal(s.iDataOneRow.Data[BinaryVectorField].GetMemorySize(), 5)
	s.Equal(s.iDataOneRow.Data[FloatVectorField].GetMemorySize(), 20)
	s.Equal(s.iDataOneRow.Data[Float16VectorField].GetMemorySize(), 12)
	s.Equal(s.iDataOneRow.Data[BFloat16VectorField].GetMemorySize(), 12)
	s.Equal(s.iDataOneRow.Data[SparseFloatVectorField].GetMemorySize(), 28)
	s.Equal(s.iDataOneRow.Data[Int8VectorField].GetMemorySize(), 8)

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
	s.Equal(s.iDataTwoRows.Data[BinaryVectorField].GetMemorySize(), 6)
	s.Equal(s.iDataTwoRows.Data[FloatVectorField].GetMemorySize(), 36)
	s.Equal(s.iDataTwoRows.Data[Float16VectorField].GetMemorySize(), 20)
	s.Equal(s.iDataTwoRows.Data[BFloat16VectorField].GetMemorySize(), 20)
	s.Equal(s.iDataTwoRows.Data[SparseFloatVectorField].GetMemorySize(), 54)
	s.Equal(s.iDataTwoRows.Data[Int8VectorField].GetMemorySize(), 12)
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
}

func (s *InsertDataSuite) TestGetDataType() {
	for _, field := range s.schema.GetFields() {
		fieldData, ok := s.iDataOneRow.Data[field.GetFieldID()]
		s.True(ok)
		s.Equal(field.GetDataType(), fieldData.GetDataType())
	}
}

func (s *InsertDataSuite) TestGetNullable() {
	for _, field := range s.schema.GetFields() {
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
	s.Equal(32, s.iDataEmpty.GetMemorySize())

	row1 := map[FieldID]interface{}{
		RowIDField:             int64(3),
		TimestampField:         int64(3),
		BoolField:              true,
		Int8Field:              int8(3),
		Int16Field:             int16(3),
		Int32Field:             int32(3),
		Int64Field:             int64(3),
		FloatField:             float32(3),
		DoubleField:            float64(3),
		StringField:            "str",
		BinaryVectorField:      []byte{0},
		FloatVectorField:       []float32{4, 5, 6, 7},
		Float16VectorField:     []byte{0, 0, 0, 0, 255, 255, 255, 255},
		BFloat16VectorField:    []byte{0, 0, 0, 0, 255, 255, 255, 255},
		SparseFloatVectorField: typeutil.CreateSparseFloatRow([]uint32{0, 1, 2}, []float32{4, 5, 6}),
		Int8VectorField:        []int8{-4, -5, 6, 7},
		ArrayField: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
			},
		},
		JSONField: []byte(`{"batch":3}`),
	}

	s.iDataOneRow, err = NewInsertData(s.schema)
	s.Require().NoError(err)
	err = s.iDataOneRow.Append(row1)
	s.Require().NoError(err)

	for fID, field := range s.iDataOneRow.Data {
		s.Equal(row1[fID], field.GetRow(0))
	}

	row2 := map[FieldID]interface{}{
		RowIDField:             int64(1),
		TimestampField:         int64(1),
		BoolField:              false,
		Int8Field:              int8(1),
		Int16Field:             int16(1),
		Int32Field:             int32(1),
		Int64Field:             int64(1),
		FloatField:             float32(1),
		DoubleField:            float64(1),
		StringField:            string("str"),
		BinaryVectorField:      []byte{0},
		FloatVectorField:       []float32{4, 5, 6, 7},
		Float16VectorField:     []byte{1, 2, 3, 4, 5, 6, 7, 8},
		BFloat16VectorField:    []byte{1, 2, 3, 4, 5, 6, 7, 8},
		SparseFloatVectorField: typeutil.CreateSparseFloatRow([]uint32{2, 3, 4}, []float32{4, 5, 6}),
		Int8VectorField:        []int8{-128, -5, 6, 127},
		ArrayField: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
			},
		},
		JSONField: []byte(`{"batch":1}`),
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
