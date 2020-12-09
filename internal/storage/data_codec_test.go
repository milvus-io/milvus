package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func TestInsertCodecWriter(t *testing.T) {
	base := Base{
		Version:  1,
		CommitID: 1,
		TanentID: 1,
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
						Name:         "field_bool",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_BOOL,
					},
					{
						Name:         "field_int8",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_INT8,
					},
					{
						Name:         "field_int16",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_INT16,
					},
					{
						Name:         "field_int32",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_INT32,
					},
					{
						Name:         "field_int64",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_INT64,
					},
					{
						Name:         "field_float",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_FLOAT,
					},
					{
						Name:         "field_double",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_DOUBLE,
					},
					{
						Name:         "field_string",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_STRING,
					},
					{
						Name:         "field_binary_vector",
						IsPrimaryKey: false,
						Description:  "description_1",
						DataType:     schemapb.DataType_VECTOR_BINARY,
					},
					{
						Name:         "field_float_vector",
						IsPrimaryKey: false,
						Description:  "description_1",
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
		Data: map[int]FieldData{
			0: BoolFieldData{
				NumRows: 2,
				data:    []bool{true, false},
			},
			1: Int8FieldData{
				NumRows: 2,
				data:    []int8{1, 2},
			},
			2: Int16FieldData{
				NumRows: 2,
				data:    []int16{1, 2},
			},
			3: Int32FieldData{
				NumRows: 2,
				data:    []int32{1, 2},
			},
			4: Int64FieldData{
				NumRows: 2,
				data:    []int64{1, 2},
			},
			5: FloatFieldData{
				NumRows: 2,
				data:    []float32{1, 2},
			},
			6: DoubleFieldData{
				NumRows: 2,
				data:    []float64{1, 2},
			},
			7: StringFieldData{
				NumRows: 2,
				data:    []string{"1", "2"},
			},
			8: BinaryVectorFieldData{
				NumRows: 8,
				data:    []byte{0, 255, 0, 1, 0, 1, 0, 1},
				dim:     8,
			},
			9: FloatVectorFieldData{
				NumRows: 1,
				data:    []float32{0, 1, 2, 3, 4, 5, 6, 7},
				dim:     8,
			},
		},
	}
	blobs, err := insertCodec.Serialize(1, 1, 1, insertData, []Timestamp{0, 1})
	assert.Nil(t, err)
	partitionID, segmentID, resultData, err := insertCodec.Deserialize(blobs)
	assert.Nil(t, err)
	assert.Equal(t, partitionID, int64(1))
	assert.Equal(t, segmentID, int64(1))
	assert.Equal(t, insertData, resultData)
	assert.Nil(t, insertCodec.Close())
}
func TestDDCodecWriter(t *testing.T) {
	base := Base{
		Version:  1,
		CommitID: 1,
		TanentID: 1,
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
	blobs, err := dataDefinitionCodec.Serialize(1, ts, ddRequests, eventTypeCodes)
	assert.Nil(t, err)
	resultTs, resultRequests, err := dataDefinitionCodec.Deserialize(blobs)
	assert.Nil(t, err)
	assert.Equal(t, resultTs, ts)
	assert.Equal(t, resultRequests, ddRequests)
	assert.Nil(t, dataDefinitionCodec.Close())
}
