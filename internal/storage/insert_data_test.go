package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestInsertDataSuite(t *testing.T) {
	suite.Run(t, new(InsertDataSuite))
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
		s.Equal(165, s.iDataEmpty.GetMemorySize())

		s.False(s.iDataOneRow.IsEmpty())
		s.Equal(1, s.iDataOneRow.GetRowNum())
		s.Equal(539, s.iDataOneRow.GetMemorySize())

		s.False(s.iDataTwoRows.IsEmpty())
		s.Equal(2, s.iDataTwoRows.GetRowNum())
		s.Equal(738, s.iDataTwoRows.GetMemorySize())

		for _, field := range s.iDataTwoRows.Data {
			s.Equal(2, field.RowNum())

			err := field.AppendRow(struct{}{})
			mlog.Warn(context.TODO(), "error", mlog.Err(err))
			s.ErrorIs(err, merr.ErrParameterInvalid)
		}
	})
}

func (s *InsertDataSuite) SetupTest() {
	var err error
	s.iDataEmpty, err = NewInsertData(s.schema)
	s.Require().NoError(err)
	s.True(s.iDataEmpty.IsEmpty())
	s.Equal(0, s.iDataEmpty.GetRowNum())
	s.Equal(165, s.iDataEmpty.GetMemorySize())

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

func TestInsertMemorySizeCapacityAndFieldChanges(t *testing.T) {
	field := &StringFieldData{Data: make([]string, 0, 1024), ValidData: make([]bool, 0, 1024)}
	require.Equal(t, 1, field.GetMemorySize(), "reserved capacity is not charged")
	data := &InsertData{}
	require.Zero(t, data.GetMemorySize())
	data.Data = map[FieldID]FieldData{100: field}
	data.Infos = []BlobInfo{{Length: 1000}}
	require.Equal(t, 1, data.GetMemorySize(), "Infos is not charged")
	require.NoError(t, field.AppendRow("a"))
	require.Equal(t, 18, data.GetMemorySize())
	data.Data[101] = &TimestamptzFieldData{Data: []int64{1, 2}}
	require.Equal(t, 35, data.GetMemorySize())
	data.Data[100] = &JSONFieldData{Data: [][]byte{nil, []byte("{}")}}
	require.Equal(t, 52, data.GetMemorySize()) // JSON 35 + timestamps 17
	delete(data.Data, 101)
	require.Equal(t, 35, data.GetMemorySize())
}

func TestInsertMemorySizeAllFields(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		data := &InsertData{Data: make(map[FieldID]FieldData)}
		row := make(map[FieldID]interface{})
		cases := fieldMemoryCases()
		fixed := 0
		for i, c := range cases {
			id := FieldID(100 + i)
			data.Data[id] = c.newField(t, nullable, 64)
			row[id] = c.row
			fixed += c.fixed
		}
		require.Equal(t, fixed, data.GetMemorySize())
		for n := 1; n <= 2; n++ {
			require.NoError(t, data.Append(row))
			total := 0
			for i, c := range cases {
				want := c.fixed + c.shared + n*c.perRow
				if nullable {
					want += n * (1 + c.mapping)
				}
				require.Equal(t, want, data.Data[FieldID(100+i)].GetMemorySize(), "%s", c.dataType)
				total += want
			}
			require.Equal(t, total, data.GetMemorySize())
		}
	}
}
