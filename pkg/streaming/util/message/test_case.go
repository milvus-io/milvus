//go:build test
// +build test

package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func CreateTestInsertMessage(t *testing.T, segmentID int64, totalRows int, timetick uint64, messageID MessageID) MutableMessage {
	timestamps := make([]uint64, 0, totalRows)
	for i := 0; i < totalRows; i++ {
		timestamps = append(timestamps, uint64(0))
	}
	rowIDs := make([]int64, 0, totalRows)
	for i := 0; i < totalRows; i++ {
		rowIDs = append(rowIDs, int64(i))
	}
	intFieldArray := make([]int32, 0, totalRows)
	for i := 0; i < totalRows; i++ {
		intFieldArray = append(intFieldArray, int32(i))
	}
	boolFieldArray := make([]bool, 0, totalRows)
	for i := 0; i < totalRows; i++ {
		boolFieldArray = append(boolFieldArray, i%2 == 0)
	}
	fieldsData := []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "f1",
			FieldId:   1,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: intFieldArray,
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Bool,
			FieldName: "f2",
			FieldId:   2,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: boolFieldArray,
						},
					},
				},
			},
		},
	}
	msg, err := NewInsertMessageBuilderV1().
		WithHeader(&InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*PartitionSegmentAssignment{
				{
					PartitionId:       2,
					Rows:              uint64(totalRows),
					BinarySize:        10000,
					SegmentAssignment: &SegmentAssignment{SegmentId: segmentID},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				Timestamp: 100,
				SourceID:  1,
			},
			ShardName:      "v1",
			DbName:         "test_name",
			CollectionName: "test_name",
			PartitionName:  "test_name",
			DbID:           1,
			CollectionID:   1,
			PartitionID:    2,
			SegmentID:      0,
			Version:        msgpb.InsertDataVersion_ColumnBased,
			FieldsData:     fieldsData,
			RowIDs:         rowIDs,
			Timestamps:     timestamps,
			NumRows:        uint64(totalRows),
		}).
		WithVChannel("v1").
		BuildMutable()
	if err != nil {
		panic(err)
	}
	msg.WithTimeTick(timetick)
	msg.WithLastConfirmed(messageID)
	return msg
}

func CreateTestDropCollectionMessage(t *testing.T, collectionID int64, timetick uint64, messageID MessageID) MutableMessage {
	header := &DropCollectionMessageHeader{
		CollectionId: collectionID,
	}
	payload := &msgpb.DropCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     collectionID,
			Timestamp: timetick,
		},
		DbName:         "db",
		CollectionName: "collection",
		DbID:           1,
		CollectionID:   collectionID,
	}
	msg, err := NewDropCollectionMessageBuilderV1().
		WithHeader(header).
		WithBody(payload).
		WithVChannel("v1").
		BuildMutable()
	assert.NoError(t, err)
	msg.WithTimeTick(timetick)
	msg.WithLastConfirmed(messageID)
	return msg
}

func CreateTestCreateCollectionMessage(t *testing.T, collectionID int64, timetick uint64, messageID MessageID) MutableMessage {
	header := &CreateCollectionMessageHeader{
		CollectionId: collectionID,
		PartitionIds: []int64{2},
	}
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "ID", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
		},
	}
	schemaBytes, err := proto.Marshal(schema)
	if err != nil {
		panic(err)
	}
	payload := &msgpb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_CreateCollection,
			MsgID:     collectionID,
			Timestamp: 100,
		},
		DbName:         "db",
		CollectionName: "collection",
		PartitionName:  "partition",
		DbID:           1,
		CollectionID:   collectionID,
		Schema:         schemaBytes,
	}

	msg, err := NewCreateCollectionMessageBuilderV1().
		WithHeader(header).
		WithBody(payload).
		WithVChannel("v1").
		BuildMutable()
	assert.NoError(t, err)
	msg.WithTimeTick(timetick)
	msg.WithLastConfirmed(messageID)
	return msg
}

func CreateTestCreateSegmentMessage(t *testing.T, collectionID int64, timetick uint64, messageID MessageID) MutableMessage {
	payload := &CreateSegmentMessageBody{}
	msg, err := NewCreateSegmentMessageBuilderV2().
		WithHeader(&CreateSegmentMessageHeader{
			CollectionId:   collectionID,
			PartitionId:    1,
			SegmentId:      1,
			StorageVersion: 1,
			MaxSegmentSize: 1024,
		}).
		WithBody(payload).
		WithVChannel("v1").
		BuildMutable()
	assert.NoError(t, err)
	msg.WithTimeTick(timetick)
	msg.WithLastConfirmed(messageID)
	return msg
}

func CreateTestTimeTickSyncMessage(t *testing.T, collectionID int64, timetick uint64, messageID MessageID) MutableMessage {
	msg, err := NewTimeTickMessageBuilderV1().
		WithHeader(&TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				Timestamp: timetick,
			},
		}).
		WithAllVChannel().
		BuildMutable()
	assert.NoError(t, err)
	msg.WithTimeTick(timetick)
	msg.WithLastConfirmed(messageID)
	return msg
}

// CreateTestEmptyInsertMesage creates an empty insert message for testing
func CreateTestEmptyInsertMesage(msgID int64, extraProperties map[string]string) MutableMessage {
	msg, err := NewInsertMessageBuilderV1().
		WithHeader(&InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*PartitionSegmentAssignment{
				{
					PartitionId: 2,
					Rows:        1000,
					BinarySize:  1024 * 1024,
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_Insert,
				MsgID:   msgID,
			},
		}).
		WithVChannel("v1").
		WithProperties(extraProperties).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return msg
}
