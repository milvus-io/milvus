package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func TestInsertCodec(t *testing.T) {
	base := Base{
		Version:  1,
		CommitID: 1,
		TenantID: 1,
		Schema: &etcdpb.CollectionMeta{
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
		},
	}
	insertCodec := &InsertCodec{
		base,
		make([]func() error, 0),
	}
	insertData := &InsertData{
		Data: map[int64]FieldData{
			1: Int64FieldData{
				NumRows: 2,
				data:    []int64{1, 2},
			},
			100: BoolFieldData{
				NumRows: 2,
				data:    []bool{true, false},
			},
			101: Int8FieldData{
				NumRows: 2,
				data:    []int8{1, 2},
			},
			102: Int16FieldData{
				NumRows: 2,
				data:    []int16{1, 2},
			},
			103: Int32FieldData{
				NumRows: 2,
				data:    []int32{1, 2},
			},
			104: Int64FieldData{
				NumRows: 2,
				data:    []int64{1, 2},
			},
			105: FloatFieldData{
				NumRows: 2,
				data:    []float32{1, 2},
			},
			106: DoubleFieldData{
				NumRows: 2,
				data:    []float64{1, 2},
			},
			107: StringFieldData{
				NumRows: 2,
				data:    []string{"1", "2"},
			},
			108: BinaryVectorFieldData{
				NumRows: 8,
				data:    []byte{0, 255, 0, 1, 0, 1, 0, 1},
				dim:     8,
			},
			109: FloatVectorFieldData{
				NumRows: 1,
				data:    []float32{0, 1, 2, 3, 4, 5, 6, 7},
				dim:     8,
			},
		},
	}
	blobs, err := insertCodec.Serialize(1, 1, insertData)
	assert.Nil(t, err)
	partitionID, segmentID, resultData, err := insertCodec.Deserialize(blobs)
	assert.Nil(t, err)
	assert.Equal(t, partitionID, int64(1))
	assert.Equal(t, segmentID, int64(1))
	assert.Equal(t, resultData, insertData)
	assert.Nil(t, insertCodec.Close())
}
func TestDDCodec(t *testing.T) {
	base := Base{
		Version:  1,
		CommitID: 1,
		TenantID: 1,
		Schema: &etcdpb.CollectionMeta{
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
						Name:         "field_1",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_INT32,
					},
					{
						Name:         "field_2",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_INT64,
					},
					{
						Name:         "field_3",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_STRING,
					},
					{
						Name:         "field_3",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_STRING,
					},
					{
						Name:         "field_3",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_STRING,
					},
				},
			},
		},
	}
	dataDefinitionCodec := &DataDefinitionCodec{
		base,
		make([]func() error, 0),
	}
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
	resultTs, resultRequests, err := dataDefinitionCodec.Deserialize(blobs)
	assert.Nil(t, err)
	assert.Equal(t, resultTs, ts)
	assert.Equal(t, resultRequests, ddRequests)
	assert.Nil(t, dataDefinitionCodec.Close())
}

func TestIndexCodec(t *testing.T) {
	indexCodec := &IndexCodec{
		Base{},
	}
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
