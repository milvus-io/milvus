package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func TestInsertCodec(t *testing.T) {
	Schema := &etcdpb.CollectionMeta{
		ID:            1,
		CreateTime:    1,
		SegmentIDs:    []int64{0, 1},
		PartitionTags: []string{"partition_0", "partition_1"},
		Schema: &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			AutoID:      true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      0,
					Name:         "row_id",
					IsPrimaryKey: false,
					Description:  "row_id",
					DataType:     schemapb.DataType_INT64,
				},
				{
					FieldID:      1,
					Name:         "Ts",
					IsPrimaryKey: false,
					Description:  "Ts",
					DataType:     schemapb.DataType_INT64,
				},
				{
					FieldID:      100,
					Name:         "field_bool",
					IsPrimaryKey: false,
					Description:  "description_2",
					DataType:     schemapb.DataType_BOOL,
				},
				{
					FieldID:      101,
					Name:         "field_int8",
					IsPrimaryKey: false,
					Description:  "description_3",
					DataType:     schemapb.DataType_INT8,
				},
				{
					FieldID:      102,
					Name:         "field_int16",
					IsPrimaryKey: false,
					Description:  "description_4",
					DataType:     schemapb.DataType_INT16,
				},
				{
					FieldID:      103,
					Name:         "field_int32",
					IsPrimaryKey: false,
					Description:  "description_5",
					DataType:     schemapb.DataType_INT32,
				},
				{
					FieldID:      104,
					Name:         "field_int64",
					IsPrimaryKey: false,
					Description:  "description_6",
					DataType:     schemapb.DataType_INT64,
				},
				{
					FieldID:      105,
					Name:         "field_float",
					IsPrimaryKey: false,
					Description:  "description_7",
					DataType:     schemapb.DataType_FLOAT,
				},
				{
					FieldID:      106,
					Name:         "field_double",
					IsPrimaryKey: false,
					Description:  "description_8",
					DataType:     schemapb.DataType_DOUBLE,
				},
				{
					FieldID:      107,
					Name:         "field_string",
					IsPrimaryKey: false,
					Description:  "description_9",
					DataType:     schemapb.DataType_STRING,
				},
				{
					FieldID:      108,
					Name:         "field_binary_vector",
					IsPrimaryKey: false,
					Description:  "description_10",
					DataType:     schemapb.DataType_VECTOR_BINARY,
				},
				{
					FieldID:      109,
					Name:         "field_float_vector",
					IsPrimaryKey: false,
					Description:  "description_11",
					DataType:     schemapb.DataType_VECTOR_FLOAT,
				},
			},
		},
	}
	insertCodec := NewInsertCodec(Schema)
	insertDataFirst := &InsertData{
		Data: map[int64]FieldData{
			0: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{3, 4},
			},
			1: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{3, 4},
			},
			100: &BoolFieldData{
				NumRows: 2,
				Data:    []bool{true, false},
			},
			101: &Int8FieldData{
				NumRows: 2,
				Data:    []int8{3, 4},
			},
			102: &Int16FieldData{
				NumRows: 2,
				Data:    []int16{3, 4},
			},
			103: &Int32FieldData{
				NumRows: 2,
				Data:    []int32{3, 4},
			},
			104: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{3, 4},
			},
			105: &FloatFieldData{
				NumRows: 2,
				Data:    []float32{3, 4},
			},
			106: &DoubleFieldData{
				NumRows: 2,
				Data:    []float64{3, 4},
			},
			107: &StringFieldData{
				NumRows: 2,
				Data:    []string{"3", "4"},
			},
			108: &BinaryVectorFieldData{
				NumRows: 2,
				Data:    []byte{0, 255},
				Dim:     8,
			},
			109: &FloatVectorFieldData{
				NumRows: 2,
				Data:    []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7},
				Dim:     8,
			},
		},
	}

	insertDataSecond := &InsertData{
		Data: map[int64]FieldData{
			0: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{1, 2},
			},
			1: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{1, 2},
			},
			100: &BoolFieldData{
				NumRows: 2,
				Data:    []bool{true, false},
			},
			101: &Int8FieldData{
				NumRows: 2,
				Data:    []int8{1, 2},
			},
			102: &Int16FieldData{
				NumRows: 2,
				Data:    []int16{1, 2},
			},
			103: &Int32FieldData{
				NumRows: 2,
				Data:    []int32{1, 2},
			},
			104: &Int64FieldData{
				NumRows: 2,
				Data:    []int64{1, 2},
			},
			105: &FloatFieldData{
				NumRows: 2,
				Data:    []float32{1, 2},
			},
			106: &DoubleFieldData{
				NumRows: 2,
				Data:    []float64{1, 2},
			},
			107: &StringFieldData{
				NumRows: 2,
				Data:    []string{"1", "2"},
			},
			108: &BinaryVectorFieldData{
				NumRows: 2,
				Data:    []byte{0, 255},
				Dim:     8,
			},
			109: &FloatVectorFieldData{
				NumRows: 2,
				Data:    []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7},
				Dim:     8,
			},
		},
	}
	firstBlobs, err := insertCodec.Serialize(1, 1, insertDataFirst)
	assert.Nil(t, err)
	for _, blob := range firstBlobs {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 100)
	}
	secondBlobs, err := insertCodec.Serialize(1, 1, insertDataSecond)
	assert.Nil(t, err)
	for _, blob := range secondBlobs {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 99)
	}
	resultBlobs := append(firstBlobs, secondBlobs...)
	partitionID, segmentID, resultData, err := insertCodec.Deserialize(resultBlobs)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), partitionID)
	assert.Equal(t, int64(1), segmentID)
	assert.Equal(t, 4, resultData.Data[0].(*Int64FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[1].(*Int64FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[100].(*BoolFieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[101].(*Int8FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[102].(*Int16FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[103].(*Int32FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[104].(*Int64FieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[105].(*FloatFieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[106].(*DoubleFieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[107].(*StringFieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[108].(*BinaryVectorFieldData).NumRows)
	assert.Equal(t, 4, resultData.Data[109].(*FloatVectorFieldData).NumRows)
	assert.Equal(t, []int64{1, 2, 3, 4}, resultData.Data[0].(*Int64FieldData).Data)
	assert.Equal(t, []int64{1, 2, 3, 4}, resultData.Data[1].(*Int64FieldData).Data)
	assert.Equal(t, []bool{true, false, true, false}, resultData.Data[100].(*BoolFieldData).Data)
	assert.Equal(t, []int8{1, 2, 3, 4}, resultData.Data[101].(*Int8FieldData).Data)
	assert.Equal(t, []int16{1, 2, 3, 4}, resultData.Data[102].(*Int16FieldData).Data)
	assert.Equal(t, []int32{1, 2, 3, 4}, resultData.Data[103].(*Int32FieldData).Data)
	assert.Equal(t, []int64{1, 2, 3, 4}, resultData.Data[104].(*Int64FieldData).Data)
	assert.Equal(t, []float32{1, 2, 3, 4}, resultData.Data[105].(*FloatFieldData).Data)
	assert.Equal(t, []float64{1, 2, 3, 4}, resultData.Data[106].(*DoubleFieldData).Data)
	assert.Equal(t, []string{"1", "2", "3", "4"}, resultData.Data[107].(*StringFieldData).Data)
	assert.Equal(t, []byte{0, 255, 0, 255}, resultData.Data[108].(*BinaryVectorFieldData).Data)
	assert.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7},
		resultData.Data[109].(*FloatVectorFieldData).Data)
	assert.Nil(t, insertCodec.Close())
}
func TestDDCodec(t *testing.T) {
	dataDefinitionCodec := NewDataDefinitionCodec(int64(1))
	ts := []Timestamp{
		1,
		2,
		3,
		4,
	}
	ddRequests := []string{
		"CreateCollection",
		"DropCollection",
		"CreatePartition",
		"DropPartition",
	}
	eventTypeCodes := []EventTypeCode{
		CreateCollectionEventType,
		DropCollectionEventType,
		CreatePartitionEventType,
		DropPartitionEventType,
	}
	blobs, err := dataDefinitionCodec.Serialize(ts, ddRequests, eventTypeCodes)
	assert.Nil(t, err)
	for _, blob := range blobs {
		blob.Key = fmt.Sprintf("1/data_definition/3/4/5/%d", 99)
	}
	resultTs, resultRequests, err := dataDefinitionCodec.Deserialize(blobs)
	assert.Nil(t, err)
	assert.Equal(t, resultTs, ts)
	assert.Equal(t, resultRequests, ddRequests)
	assert.Nil(t, dataDefinitionCodec.Close())
}

func TestIndexCodec(t *testing.T) {
	indexCodec := NewIndexCodec()
	blobs := []*Blob{
		{
			"12345",
			[]byte{1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7},
		},
		{
			"6666",
			[]byte{6, 6, 6, 6, 6, 1, 2, 3, 4, 5, 6, 7},
		},
		{
			"8885",
			[]byte{8, 8, 8, 8, 8, 8, 8, 8, 2, 3, 4, 5, 6, 7},
		},
	}
	blobsInput, err := indexCodec.Serialize(blobs)
	assert.Nil(t, err)
	assert.Equal(t, blobs, blobsInput)
	blobsOutput, err := indexCodec.Deserialize(blobs)
	assert.Nil(t, err)
	assert.Equal(t, blobsOutput, blobsInput)
}
