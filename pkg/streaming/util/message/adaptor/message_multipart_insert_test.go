//go:build test
// +build test

package adaptor

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
)

// createTestMultiPartitionInsertMessage creates a test insert message with multiple partitions
func createTestMultiPartitionInsertMessage(t *testing.T, collectionID int64, partitionRowCounts map[int64]int, tt uint64, id message.MessageID) message.ImmutableMessage {
	// Sort partition IDs for deterministic behavior
	sortedPartitionIDs := make([]int64, 0, len(partitionRowCounts))
	for partitionID := range partitionRowCounts {
		sortedPartitionIDs = append(sortedPartitionIDs, partitionID)
	}
	sort.Slice(sortedPartitionIDs, func(i, j int) bool {
		return sortedPartitionIDs[i] < sortedPartitionIDs[j]
	})

	// Calculate total rows and build fields data
	totalRows := 0
	for _, count := range partitionRowCounts {
		totalRows += count
	}

	// Create field data for all rows (in partition order)
	timestamps := make([]uint64, totalRows)
	rowIDs := make([]int64, totalRows)
	intFieldData := make([]int32, totalRows)

	rowIdx := 0
	for _, partitionID := range sortedPartitionIDs {
		count := partitionRowCounts[partitionID]
		for i := 0; i < count; i++ {
			timestamps[rowIdx] = tt
			rowIDs[rowIdx] = int64(rowIdx)
			intFieldData[rowIdx] = int32(rowIdx * 10)
			rowIdx++
		}
	}

	fieldsData := []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "id",
			FieldId:   1,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: rowIDs,
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Int32,
			FieldName: "value",
			FieldId:   2,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: intFieldData,
						},
					},
				},
			},
		},
	}

	// Build partition assignments (sorted by partition ID)
	partitionAssignments := make([]*messagespb.PartitionSegmentAssignment, 0, len(partitionRowCounts))
	for _, partitionID := range sortedPartitionIDs {
		count := partitionRowCounts[partitionID]
		partitionAssignments = append(partitionAssignments, &messagespb.PartitionSegmentAssignment{
			PartitionId: partitionID,
			Rows:        uint64(count),
			BinarySize:  10000,
			SegmentAssignment: &messagespb.SegmentAssignment{
				SegmentId: 100 + partitionID, // Test segment ID = 100 + partitionID
			},
		})
	}

	mutableMsg, err := message.NewInsertMessageBuilderV1().
		WithVChannel("test-vchannel").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: collectionID,
			Partitions:   partitionAssignments,
		}).
		WithBody(&msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				Timestamp: tt,
			},
			CollectionName: "test_collection",
			CollectionID:   collectionID,
			NumRows:        uint64(totalRows),
			Timestamps:     timestamps,
			RowIDs:         rowIDs,
			FieldsData:     fieldsData,
			Version:        msgpb.InsertDataVersion_ColumnBased,
		}).
		BuildMutable()
	require.NoError(t, err)

	immutableMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)

	return immutableMsg
}

// Test_parseMultiPartitionInsertMsg_TwoPartitions tests splitting a message with 2 partitions
func Test_parseMultiPartitionInsertMsg_TwoPartitions(t *testing.T) {
	collectionID := int64(1)
	tt := uint64(100)
	id := rmq.NewRmqID(1)

	// Partition 100: 10 rows, Partition 200: 5 rows
	partitionRowCounts := map[int64]int{
		100: 10,
		200: 5,
	}

	msg := createTestMultiPartitionInsertMessage(t, collectionID, partitionRowCounts, tt, id)

	// Parse the message
	tsMsgs, err := parseMultiPartitionInsertMsg(msg)
	require.NoError(t, err)

	// Should split into 2 InsertMsg
	assert.Equal(t, 2, len(tsMsgs))

	// Verify first partition (Partition 100, rows 0-9)
	insertMsg1, ok := tsMsgs[0].(*msgstream.InsertMsg)
	require.True(t, ok)
	assert.Equal(t, int64(100), insertMsg1.PartitionID)
	assert.Equal(t, int64(100+100), insertMsg1.SegmentID) // SegmentID = 100 + partitionID
	assert.Equal(t, uint64(10), insertMsg1.NumRows)
	assert.Equal(t, 10, len(insertMsg1.Timestamps))

	// Verify second partition (Partition 200, rows 10-14)
	insertMsg2, ok := tsMsgs[1].(*msgstream.InsertMsg)
	require.True(t, ok)
	assert.Equal(t, int64(200), insertMsg2.PartitionID)
	assert.Equal(t, int64(100+200), insertMsg2.SegmentID)
	assert.Equal(t, uint64(5), insertMsg2.NumRows)
	assert.Equal(t, 5, len(insertMsg2.Timestamps))

	// Verify field data is correctly split
	// Partition 1: rows [0, 10)
	assert.Equal(t, 2, len(insertMsg1.FieldsData))
	assert.Equal(t, 10, len(insertMsg1.FieldsData[0].GetScalars().GetLongData().GetData()))
	assert.Equal(t, int64(0), insertMsg1.FieldsData[0].GetScalars().GetLongData().GetData()[0])
	assert.Equal(t, int64(9), insertMsg1.FieldsData[0].GetScalars().GetLongData().GetData()[9])

	// Partition 2: rows [10, 15)
	assert.Equal(t, 2, len(insertMsg2.FieldsData))
	assert.Equal(t, 5, len(insertMsg2.FieldsData[0].GetScalars().GetLongData().GetData()))
	assert.Equal(t, int64(10), insertMsg2.FieldsData[0].GetScalars().GetLongData().GetData()[0])
	assert.Equal(t, int64(14), insertMsg2.FieldsData[0].GetScalars().GetLongData().GetData()[4])
}

// Test_parseMultiPartitionInsertMsg_ThreePartitions tests splitting a message with 3 partitions
func Test_parseMultiPartitionInsertMsg_ThreePartitions(t *testing.T) {
	collectionID := int64(2)
	tt := uint64(200)
	id := rmq.NewRmqID(2)

	// 3 partitions with different row counts
	partitionRowCounts := map[int64]int{
		100: 3,
		200: 7,
		300: 5,
	}

	msg := createTestMultiPartitionInsertMessage(t, collectionID, partitionRowCounts, tt, id)

	// Parse the message
	tsMsgs, err := parseMultiPartitionInsertMsg(msg)
	require.NoError(t, err)

	// Should split into 3 InsertMsg
	assert.Equal(t, 3, len(tsMsgs))

	// Verify total rows
	totalRows := 0
	for _, tsMsg := range tsMsgs {
		insertMsg := tsMsg.(*msgstream.InsertMsg)
		totalRows += int(insertMsg.NumRows)
	}
	assert.Equal(t, 15, totalRows)

	// Verify partition IDs are correct
	expectedPartitionIDs := []int64{100, 200, 300}
	for i, tsMsg := range tsMsgs {
		insertMsg := tsMsg.(*msgstream.InsertMsg)
		assert.Equal(t, expectedPartitionIDs[i], insertMsg.PartitionID)
	}
}

// Test_parseMultiPartitionInsertMsg_EmptyPartition tests handling of partition with 0 rows
func Test_parseMultiPartitionInsertMsg_EmptyPartition(t *testing.T) {
	collectionID := int64(3)
	tt := uint64(300)
	id := rmq.NewRmqID(3)

	// Partition with 0 rows (edge case)
	partitionRowCounts := map[int64]int{
		100: 10,
		200: 0, // Empty partition
		300: 5,
	}

	msg := createTestMultiPartitionInsertMessage(t, collectionID, partitionRowCounts, tt, id)

	// Parse the message
	tsMsgs, err := parseMultiPartitionInsertMsg(msg)
	require.NoError(t, err)

	// Should have 3 messages (empty partition is also included with 0 rows)
	assert.Equal(t, 3, len(tsMsgs))

	// Verify all partitions are present
	partitionIDs := make([]int64, len(tsMsgs))
	rowCounts := make(map[int64]uint64)
	for i, tsMsg := range tsMsgs {
		insertMsg := tsMsg.(*msgstream.InsertMsg)
		partitionIDs[i] = insertMsg.PartitionID
		rowCounts[insertMsg.PartitionID] = insertMsg.NumRows
	}
	assert.Contains(t, partitionIDs, int64(100))
	assert.Contains(t, partitionIDs, int64(200))
	assert.Contains(t, partitionIDs, int64(300))

	// Verify row counts are correct
	assert.Equal(t, uint64(10), rowCounts[100])
	assert.Equal(t, uint64(0), rowCounts[200]) // Empty partition has 0 rows
	assert.Equal(t, uint64(5), rowCounts[300])
}

// Test_NewMsgPackFromMessage_MultiPartitionInsert tests the full flow through NewMsgPackFromMessage
func Test_NewMsgPackFromMessage_MultiPartitionInsert(t *testing.T) {
	collectionID := int64(4)
	tt := uint64(400)
	id := rmq.NewRmqID(4)

	partitionRowCounts := map[int64]int{
		100: 5,
		200: 3,
	}

	msg := createTestMultiPartitionInsertMessage(t, collectionID, partitionRowCounts, tt, id)

	// Convert through NewMsgPackFromMessage
	msgPack, err := NewMsgPackFromMessage(msg)
	require.NoError(t, err)
	require.NotNil(t, msgPack)

	// Should have 2 messages (one per partition)
	assert.Equal(t, 2, len(msgPack.Msgs))

	// Verify messages are InsertMsg
	for _, tsMsg := range msgPack.Msgs {
		_, ok := tsMsg.(*msgstream.InsertMsg)
		assert.True(t, ok)
	}
}

// Test_calculateRowRanges tests the helper function for row range calculation
func Test_calculateRowRanges(t *testing.T) {
	partitions := []*messagespb.PartitionSegmentAssignment{
		{PartitionId: 100, Rows: 10},
		{PartitionId: 200, Rows: 5},
		{PartitionId: 300, Rows: 15},
	}

	ranges := calculateRowRanges(partitions)

	assert.Equal(t, 3, len(ranges))

	// Partition 0: [0, 10)
	assert.Equal(t, 0, ranges[0].start)
	assert.Equal(t, 10, ranges[0].end)

	// Partition 1: [10, 15)
	assert.Equal(t, 10, ranges[1].start)
	assert.Equal(t, 15, ranges[1].end)

	// Partition 2: [15, 30)
	assert.Equal(t, 15, ranges[2].start)
	assert.Equal(t, 30, ranges[2].end)
}
