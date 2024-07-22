//go:build test
// +build test

package message

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
		WithMessageHeader(&InsertMessageHeader{
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
		WithPayload(&msgpb.InsertRequest{
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
		}).BuildMutable()
	if err != nil {
		panic(err)
	}
	msg.WithVChannel("v1")
	msg.WithTimeTick(timetick)
	msg.WithLastConfirmed(messageID)
	return msg
}

func CreateTestCreateCollectionMessage(t *testing.T, collectionID int64, timetick uint64, messageID MessageID) MutableMessage {
	header := &CreateCollectionMessageHeader{
		CollectionId: collectionID,
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
	}

	msg, err := NewCreateCollectionMessageBuilderV1().
		WithMessageHeader(header).
		WithPayload(payload).
		BuildMutable()
	assert.NoError(t, err)
	msg.WithVChannel("v1")
	msg.WithTimeTick(timetick)
	msg.WithLastConfirmed(messageID)
	return msg
}

// CreateTestEmptyInsertMesage creates an empty insert message for testing
func CreateTestEmptyInsertMesage(msgID int64, extraProperties map[string]string) MutableMessage {
	msg, err := NewInsertMessageBuilderV1().
		WithMessageHeader(&InsertMessageHeader{}).
		WithPayload(&msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_Insert,
				MsgID:   msgID,
			},
		}).
		WithProperties(extraProperties).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return msg
}
