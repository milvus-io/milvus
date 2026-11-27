package storage

import (
	"context"
	"fmt"
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
	// +2 bytes: Nullable and ElementNullable flags; VectorArrayFieldData has no L2PMapping.
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

func TestNewInsertDataTrackingWithFunctionOutputs(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "input", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "output", DataType: schemapb.DataType_VarChar, IsFunctionOutput: true},
	}}
	untracked, err := NewInsertData(schema)
	require.NoError(t, err)
	require.NotContains(t, untracked.Data, FieldID(101))

	tracked, err := NewInsertDataBuilder(schema).WithFunctionOutputFields().WithMemoryTracking().Build()
	require.NoError(t, err)
	require.Contains(t, tracked.Data, FieldID(101))
	require.NoError(t, tracked.Append(map[FieldID]interface{}{100: "input", 101: "output"}))
	require.Equal(t, "output", tracked.Data[101].GetRow(0))
	require.Equal(t, 45, checkedMemorySize(t, tracked))
}

func TestInsertDataTrackedFieldSizes(t *testing.T) {
	cases := fieldMemoryCases()
	cases = append(cases, fieldMemoryCase{schemapb.DataType_Text, "中", []string{"中"}, 1, 19, 0, 0})
	for _, nullable := range []bool{false, true} {
		for _, capacity := range []int{0, 64} {
			t.Run(fmt.Sprintf("nullable=%t/cap=%d", nullable, capacity), func(t *testing.T) {
				schema := &schemapb.CollectionSchema{}
				expected := make(map[FieldID]int)
				for i, tc := range cases {
					element := schemapb.DataType_Int64
					if tc.dataType == schemapb.DataType_ArrayOfVector {
						element = schemapb.DataType_FloatVector
					}
					id := FieldID(100 + i)
					schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
						FieldID: id, Name: fmt.Sprintf("field_%d", id), DataType: tc.dataType,
						Nullable: nullable, ElementType: element,
						TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}},
					})
					expected[id] = tc.fixed
				}
				data, err := NewInsertDataBuilder(schema).WithCapacity(capacity).WithFunctionOutputFields().WithMemoryTracking().Build()
				require.NoError(t, err)
				require.Len(t, data.Data, len(cases))
				check := func() {
					t.Helper()
					total, trackedTotal := 0, 0
					for id, field := range data.Data {
						require.Equal(t, expected[id], checkedMemorySize(t, field), "field %d", id)
						require.Equal(t, field.GetMemorySize(), checkedMemorySize(t, field), "field %d", id)
						total += expected[id]
						trackedTotal += checkedMemorySize(t, field)
					}
					require.Equal(t, total, trackedTotal)
					require.Equal(t, data.GetMemorySize(), trackedTotal)
				}
				check()
				for i, tc := range cases {
					id := FieldID(100 + i)
					field := data.Data[id] // Exercise the public FieldData interface.
					require.NoError(t, field.AppendRow(tc.row))
					expected[id] += tc.perRow + tc.shared
					if nullable {
						expected[id] += 1 + tc.mapping
					}
					check()
					var validity interface{}
					validitySize := 0
					if nullable {
						validity, validitySize = []bool{true}, 1+tc.mapping
					}
					require.NoError(t, field.AppendRows(tc.batch, validity))
					expected[id] += tc.perRow + validitySize
					check()
					require.NoError(t, field.AppendDataRows(tc.batch))
					expected[id] += tc.perRow
					check()
					require.NoError(t, field.AppendValidDataRows(validity))
					expected[id] += validitySize
					check()
				}
			})
		}
	}
}

func TestInsertDataTrackedSizeAndFieldChanges(t *testing.T) {
	data := &InsertData{Data: map[FieldID]FieldData{100: newFieldDataForTest[*StringFieldData](&schemapb.FieldSchema{
		DataType: schemapb.DataType_VarChar,
		Nullable: true,
	}, 0, true)}}
	require.NoError(t, data.Data[100].AppendRow("中"))
	require.Equal(t, 21, checkedMemorySize(t, data))
	require.Equal(t, data.GetMemorySize(), checkedMemorySize(t, data))
	require.Equal(t, 21, checkedMemorySize(t, data))
	require.NoError(t, data.Data[100].AppendRow(nil))
	require.Equal(t, 38, checkedMemorySize(t, data))
	require.ErrorIs(t, data.Data[100].AppendRow(42), merr.ErrParameterInvalid)
	require.Equal(t, 38, checkedMemorySize(t, data))
	data.Data[100] = newFieldDataForTest[*JSONFieldData](&schemapb.FieldSchema{
		DataType: schemapb.DataType_JSON,
		Nullable: false,
	}, 0, true)
	require.NoError(t, data.Data[100].AppendRow([]byte("{}")))
	require.Equal(t, 19, checkedMemorySize(t, data))
	data.Data[101] = newFixedFieldDataForTest(schemapb.DataType_Int64, false, 0, 0)
	require.NoError(t, data.Data[101].AppendRow(int64(1)))
	require.Equal(t, 28, checkedMemorySize(t, data))
	require.Equal(t, data.GetMemorySize(), checkedMemorySize(t, data))
	delete(data.Data, 100)
	require.Equal(t, 9, checkedMemorySize(t, data))
	require.Zero(t, checkedMemorySize(t, &InsertData{}))
}

func TestInsertDataMemoryTrackingMixedFields(t *testing.T) {
	tracked := newFieldDataForTest[*StringFieldData](&schemapb.FieldSchema{
		DataType: schemapb.DataType_VarChar,
		Nullable: false,
	}, 0, true)
	require.NoError(t, tracked.AppendRow("tracked"))
	untracked := newFieldDataForTest[*JSONFieldData](&schemapb.FieldSchema{
		DataType: schemapb.DataType_JSON,
		Nullable: false,
	}, 0, false)
	require.NoError(t, untracked.AppendRow([]byte("{}")))
	data := &InsertData{Data: map[FieldID]FieldData{100: tracked, 101: untracked, 102: newFixedFieldDataForTest(schemapb.DataType_Int64, false, 0, 0)}}
	require.Equal(t, scannedMemorySize(data), data.GetMemorySize())
	// Changes to an untracked field are picked up by the next aggregate scan.
	require.NoError(t, untracked.AppendRow([]byte("{\"value\":1}")))
	require.Equal(t, scannedMemorySize(data), data.GetMemorySize())
	delete(data.Data, 101)
	require.Equal(t, scannedMemorySize(data), data.GetMemorySize())
	data.Data[100] = newFieldDataForTest[*JSONFieldData](&schemapb.FieldSchema{
		DataType: schemapb.DataType_JSON,
		Nullable: false,
	}, 0, false)
	require.Equal(t, scannedMemorySize(data), data.GetMemorySize())
}

// Tracking and function outputs are independent construction options.
func TestInsertDataBuilderOptions(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "input", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "output", DataType: schemapb.DataType_VarChar, IsFunctionOutput: true},
	}}
	for _, tracked := range []bool{false, true} {
		for _, outputs := range []bool{false, true} {
			t.Run(fmt.Sprintf("tracked=%t/outputs=%t", tracked, outputs), func(t *testing.T) {
				builder := NewInsertDataBuilder(schema).WithCapacity(16)
				if tracked {
					builder.WithMemoryTracking()
				}
				if outputs {
					builder.WithFunctionOutputFields()
				}
				data, err := builder.Build()
				require.NoError(t, err)
				_, hasOutput := data.Data[101]
				require.Equal(t, outputs, hasOutput)
				for _, field := range data.Data {
					strings := field.(*StringFieldData)
					require.Equal(t, 16, cap(strings.Data))
					require.Equal(t, tracked, strings.trackMemory)
					require.NoError(t, field.AppendRow("hello"))
					require.Equal(t, scannedMemorySize(field), field.GetMemorySize())
				}
			})
		}
	}
}

func TestInsertDataBuilderReuse(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_VarChar},
		{FieldID: 101, DataType: schemapb.DataType_VarChar, IsFunctionOutput: true},
	}}
	builder := NewInsertDataBuilder(schema).WithCapacity(8).WithMemoryTracking()
	first, err := builder.Build()
	require.NoError(t, err)
	second, err := builder.Build()
	require.NoError(t, err)
	require.NoError(t, first.Append(map[FieldID]interface{}{100: "first"}))
	require.Zero(t, second.GetRowNum())
	require.NotSame(t, first.Data[100], second.Data[100])

	third, err := builder.WithCapacity(16).WithFunctionOutputFields().Build()
	require.NoError(t, err)
	require.Contains(t, third.Data, FieldID(101))
	require.NotContains(t, first.Data, FieldID(101))
	require.NotContains(t, second.Data, FieldID(101))
	require.Equal(t, 8, cap(second.Data[100].(*StringFieldData).Data))
	require.Equal(t, 16, cap(third.Data[100].(*StringFieldData).Data))
}

func TestInsertDataCopyFrom(t *testing.T) {
	src := &InsertData{Data: map[FieldID]FieldData{
		100: &Int64FieldData{Data: []int64{1, 2, 3}},
		101: &StringFieldData{Data: []string{"a", "b", "c"}, Nullable: true, ValidData: []bool{true, false, true}},
	}}
	t.Run("copies_selected_rows", func(t *testing.T) {
		dst := &InsertData{Data: map[FieldID]FieldData{
			100: &Int64FieldData{},
			101: &StringFieldData{Nullable: true},
		}}
		require.NoError(t, dst.CopyFrom(src, []int{2, 0}))
		require.Equal(t, 2, dst.Data[100].RowNum())
		require.Equal(t, []any{"c", "a"}, []any{dst.Data[101].GetRow(0), dst.Data[101].GetRow(1)})
		require.Equal(t, []bool{true, true}, dst.Data[101].GetValidData())
	})
	t.Run("missing_field", func(t *testing.T) {
		dst := &InsertData{Data: map[FieldID]FieldData{100: &Int64FieldData{}}}
		require.ErrorIs(t, dst.CopyFrom(src, []int{0}), merr.ErrServiceInternal)
	})
	t.Run("incompatible_column_falls_back_to_append_row", func(t *testing.T) {
		dst := &InsertData{Data: map[FieldID]FieldData{
			100: &Int64FieldData{},
			101: &StringFieldData{Nullable: true},
		}}
		mismatched := &InsertData{Data: map[FieldID]FieldData{
			100: &Int64FieldData{Data: []int64{1}},
			101: &Int64FieldData{Data: []int64{7}},
		}}
		require.ErrorIs(t, dst.CopyFrom(mismatched, []int{0}), merr.ErrParameterInvalid)
	})
}
