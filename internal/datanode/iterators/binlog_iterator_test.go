package iterator

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

func TestInsertBinlogIteratorSuite(t *testing.T) {
	suite.Run(t, new(InsertBinlogIteratorSuite))
}

const (
	CollectionID        = 10000
	PartitionID         = 10001
	SegmentID           = 10002
	RowIDField          = 0
	TimestampField      = 1
	BoolField           = 100
	Int8Field           = 101
	Int16Field          = 102
	Int32Field          = 103
	Int64Field          = 104
	FloatField          = 105
	DoubleField         = 106
	StringField         = 107
	BinaryVectorField   = 108
	FloatVectorField    = 109
	ArrayField          = 110
	JSONField           = 111
	Float16VectorField  = 112
	BFloat16VectorField = 113
)

type InsertBinlogIteratorSuite struct {
	suite.Suite

	i *BinlogIterator
}

func (s *InsertBinlogIteratorSuite) TestBinlogIterator() {
	insertData, meta := genTestInsertData()
	writer := storage.NewInsertCodecWithSchema(meta)
	blobs, err := writer.Serialize(PartitionID, SegmentID, insertData)
	s.Require().NoError(err)

	values := [][]byte{}
	for _, b := range blobs {
		values = append(values, b.Value)
	}
	s.Run("invalid blobs", func() {
		iter, err := NewInsertBinlogIterator([][]byte{}, Int64Field, schemapb.DataType_Int64, nil)
		s.Error(err)
		s.Nil(iter)
	})

	s.Run("invalid pk type", func() {
		iter, err := NewInsertBinlogIterator(values, Int64Field, schemapb.DataType_Float, &Label{segmentID: 19530})
		s.NoError(err)

		_, err = iter.Next()
		s.Error(err)
	})

	s.Run("normal", func() {
		iter, err := NewInsertBinlogIterator(values, Int64Field, schemapb.DataType_Int64, &Label{segmentID: 19530})
		s.NoError(err)

		rows := []interface{}{}
		var idx int = 0 // row number

		for iter.HasNext() {
			labeled, err := iter.Next()
			s.NoError(err)
			s.Equal(int64(19530), labeled.GetSegmentID())

			rows = append(rows, labeled.data)

			label := labeled.GetLabel()
			s.NotNil(label)
			s.EqualValues(19530, label.segmentID)
			s.EqualValues(19530, labeled.GetSegmentID())

			insertRow, ok := labeled.data.(*InsertRow)
			s.True(ok)

			s.EqualValues(insertData.Data[TimestampField].GetRow(idx).(int64), labeled.GetTimestamp())
			s.Equal(insertData.Data[Int64Field].GetRow(idx).(int64), labeled.GetPk().GetValue().(int64))
			s.Equal(insertData.Data[RowIDField].GetRow(idx).(int64), insertRow.ID)
			s.Equal(insertData.Data[BoolField].GetRow(idx).(bool), insertRow.Value[BoolField].(bool))
			s.Equal(insertData.Data[Int8Field].GetRow(idx).(int8), insertRow.Value[Int8Field].(int8))
			s.Equal(insertData.Data[Int16Field].GetRow(idx).(int16), insertRow.Value[Int16Field].(int16))
			s.Equal(insertData.Data[Int32Field].GetRow(idx).(int32), insertRow.Value[Int32Field].(int32))
			s.Equal(insertData.Data[Int64Field].GetRow(idx).(int64), insertRow.Value[Int64Field].(int64))
			s.Equal(insertData.Data[Int64Field].GetRow(idx).(int64), insertRow.Value[Int64Field].(int64))
			s.Equal(insertData.Data[FloatField].GetRow(idx).(float32), insertRow.Value[FloatField].(float32))
			s.Equal(insertData.Data[DoubleField].GetRow(idx).(float64), insertRow.Value[DoubleField].(float64))
			s.Equal(insertData.Data[StringField].GetRow(idx).(string), insertRow.Value[StringField].(string))
			s.Equal(insertData.Data[ArrayField].GetRow(idx).(*schemapb.ScalarField).GetIntData().Data, insertRow.Value[ArrayField].(*schemapb.ScalarField).GetIntData().Data)
			s.Equal(insertData.Data[JSONField].GetRow(idx).([]byte), insertRow.Value[JSONField].([]byte))
			s.Equal(insertData.Data[BinaryVectorField].GetRow(idx).([]byte), insertRow.Value[BinaryVectorField].([]byte))
			s.Equal(insertData.Data[FloatVectorField].GetRow(idx).([]float32), insertRow.Value[FloatVectorField].([]float32))
			s.Equal(insertData.Data[Float16VectorField].GetRow(idx).([]byte), insertRow.Value[Float16VectorField].([]byte))
			s.Equal(insertData.Data[BFloat16VectorField].GetRow(idx).([]byte), insertRow.Value[BFloat16VectorField].([]byte))

			idx++
		}

		s.Equal(2, len(rows))

		_, err = iter.Next()
		s.ErrorIs(err, ErrNoMoreRecord)

		iter.Dispose()
		iter.WaitForDisposed()

		_, err = iter.Next()
		s.ErrorIs(err, ErrDisposed)
	})
}

func genTestInsertData() (*storage.InsertData, *etcdpb.CollectionMeta) {
	meta := &etcdpb.CollectionMeta{
		ID:            CollectionID,
		CreateTime:    1,
		SegmentIDs:    []int64{SegmentID},
		PartitionTags: []string{"partition_0", "partition_1"},
		Schema: &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			AutoID:      true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      RowIDField,
					Name:         "row_id",
					IsPrimaryKey: false,
					Description:  "row_id",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      TimestampField,
					Name:         "Timestamp",
					IsPrimaryKey: false,
					Description:  "Timestamp",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      BoolField,
					Name:         "field_bool",
					IsPrimaryKey: false,
					Description:  "bool",
					DataType:     schemapb.DataType_Bool,
				},
				{
					FieldID:      Int8Field,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "int8",
					DataType:     schemapb.DataType_Int8,
				},
				{
					FieldID:      Int16Field,
					Name:         "field_int16",
					IsPrimaryKey: false,
					Description:  "int16",
					DataType:     schemapb.DataType_Int16,
				},
				{
					FieldID:      Int32Field,
					Name:         "field_int32",
					IsPrimaryKey: false,
					Description:  "int32",
					DataType:     schemapb.DataType_Int32,
				},
				{
					FieldID:      Int64Field,
					Name:         "field_int64",
					IsPrimaryKey: true,
					Description:  "int64",
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      FloatField,
					Name:         "field_float",
					IsPrimaryKey: false,
					Description:  "float",
					DataType:     schemapb.DataType_Float,
				},
				{
					FieldID:      DoubleField,
					Name:         "field_double",
					IsPrimaryKey: false,
					Description:  "double",
					DataType:     schemapb.DataType_Double,
				},
				{
					FieldID:      StringField,
					Name:         "field_string",
					IsPrimaryKey: false,
					Description:  "string",
					DataType:     schemapb.DataType_String,
				},
				{
					FieldID:     ArrayField,
					Name:        "field_int32_array",
					Description: "int32 array",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
				},
				{
					FieldID:     JSONField,
					Name:        "field_json",
					Description: "json",
					DataType:    schemapb.DataType_JSON,
				},
				{
					FieldID:      BinaryVectorField,
					Name:         "field_binary_vector",
					IsPrimaryKey: false,
					Description:  "binary_vector",
					DataType:     schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "8"},
					},
				},
				{
					FieldID:      FloatVectorField,
					Name:         "field_float_vector",
					IsPrimaryKey: false,
					Description:  "float_vector",
					DataType:     schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "4"},
					},
				},
				{
					FieldID:      Float16VectorField,
					Name:         "field_float16_vector",
					IsPrimaryKey: false,
					Description:  "float16_vector",
					DataType:     schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "4"},
					},
				},
				{
					FieldID:      BFloat16VectorField,
					Name:         "field_bfloat16_vector",
					IsPrimaryKey: false,
					Description:  "bfloat16_vector",
					DataType:     schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "4"},
					},
				},
			},
		},
	}
	insertData := storage.InsertData{
		Data: map[int64]storage.FieldData{
			RowIDField: &storage.Int64FieldData{
				Data: []int64{3, 4},
			},
			TimestampField: &storage.Int64FieldData{
				Data: []int64{3, 4},
			},
			BoolField: &storage.BoolFieldData{
				Data: []bool{true, false},
			},
			Int8Field: &storage.Int8FieldData{
				Data: []int8{3, 4},
			},
			Int16Field: &storage.Int16FieldData{
				Data: []int16{3, 4},
			},
			Int32Field: &storage.Int32FieldData{
				Data: []int32{3, 4},
			},
			Int64Field: &storage.Int64FieldData{
				Data: []int64{3, 4},
			},
			FloatField: &storage.FloatFieldData{
				Data: []float32{3, 4},
			},
			DoubleField: &storage.DoubleFieldData{
				Data: []float64{3, 4},
			},
			StringField: &storage.StringFieldData{
				Data: []string{"3", "4"},
			},
			BinaryVectorField: &storage.BinaryVectorFieldData{
				Data: []byte{0, 255},
				Dim:  8,
			},
			FloatVectorField: &storage.FloatVectorFieldData{
				Data: []float32{4, 5, 6, 7, 4, 5, 6, 7},
				Dim:  4,
			},
			ArrayField: &storage.ArrayFieldData{
				ElementType: schemapb.DataType_Int32,
				Data: []*schemapb.ScalarField{
					{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{3, 2, 1}},
						},
					},
					{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{6, 5, 4}},
						},
					},
				},
			},
			JSONField: &storage.JSONFieldData{
				Data: [][]byte{
					[]byte(`{"batch":2}`),
					[]byte(`{"key":"world"}`),
				},
			},
			Float16VectorField: &storage.Float16VectorFieldData{
				Data: []byte{0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255},
				Dim:  4,
			},
			BFloat16VectorField: &storage.BFloat16VectorFieldData{
				Data: []byte{0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255},
				Dim:  4,
			},
		},
	}

	return &insertData, meta
}
