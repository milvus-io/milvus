//go:build test
// +build test

package adaptor

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
)

// Test_parseMultiPartitionUpsertMsg_TwoPartitions tests parsing a multi-partition upsert message with 2 partitions
func Test_parseMultiPartitionUpsertMsg_TwoPartitions(t *testing.T) {
	collectionID := int64(1)
	partitionRowCounts := map[int64]int{
		100: 3, // partition 100 has 3 rows
		200: 2, // partition 200 has 2 rows
	}
	tt := uint64(1000)
	id := rmq.NewRmqID(1)

	// Create multi-partition upsert message
	upsertMsg := createTestMultiPartitionUpsertMessage(t, collectionID, partitionRowCounts, tt, id)

	// Parse the upsert message
	tsMsgs, err := parseMultiPartitionUpsertMsg(upsertMsg)
	require.NoError(t, err)

	// Should produce 4 messages: 2 partitions * 2 messages per partition (delete + insert)
	require.Equal(t, 4, len(tsMsgs))

	// Check delete and insert messages for partition 100 (should be first due to sorting)
	deleteMsg1, ok := tsMsgs[0].(*msgstream.DeleteMsg)
	require.True(t, ok)
	require.Equal(t, tt, deleteMsg1.BeginTs())

	insertMsg1, ok := tsMsgs[1].(*msgstream.InsertMsg)
	require.True(t, ok)
	require.Equal(t, int64(100), insertMsg1.GetPartitionID())
	require.Equal(t, uint64(3), insertMsg1.GetNumRows())
	require.Equal(t, tt, insertMsg1.BeginTs())
	require.NotZero(t, insertMsg1.GetSegmentID()) // Segment ID should be set

	// Check delete and insert messages for partition 200
	deleteMsg2, ok := tsMsgs[2].(*msgstream.DeleteMsg)
	require.True(t, ok)
	require.Equal(t, tt, deleteMsg2.BeginTs())

	insertMsg2, ok := tsMsgs[3].(*msgstream.InsertMsg)
	require.True(t, ok)
	require.Equal(t, int64(200), insertMsg2.GetPartitionID())
	require.Equal(t, uint64(2), insertMsg2.GetNumRows())
	require.Equal(t, tt, insertMsg2.BeginTs())
	require.NotZero(t, insertMsg2.GetSegmentID())
}

// Test_parseMultiPartitionUpsertMsg_ThreePartitions tests with 3 partitions
func Test_parseMultiPartitionUpsertMsg_ThreePartitions(t *testing.T) {
	collectionID := int64(2)
	partitionRowCounts := map[int64]int{
		100: 5,
		200: 3,
		300: 7,
	}
	tt := uint64(2000)
	id := rmq.NewRmqID(2)

	upsertMsg := createTestMultiPartitionUpsertMessage(t, collectionID, partitionRowCounts, tt, id)

	tsMsgs, err := parseMultiPartitionUpsertMsg(upsertMsg)
	require.NoError(t, err)

	// Should produce 6 messages: 3 partitions * 2 messages per partition
	require.Equal(t, 6, len(tsMsgs))

	// Verify total insert rows (every other message is an insert)
	totalInsertRows := 0
	insertCount := 0
	for i, tsMsg := range tsMsgs {
		if i%2 == 1 { // Odd indices are insert messages
			insertMsg := tsMsg.(*msgstream.InsertMsg)
			totalInsertRows += int(insertMsg.NumRows)
			insertCount++
		}
	}
	assert.Equal(t, 15, totalInsertRows) // 5+3+7
	assert.Equal(t, 3, insertCount)
}

// Test_parseMultiPartitionUpsertMsg_EmptyPartition tests handling of partition with 0 rows
func Test_parseMultiPartitionUpsertMsg_EmptyPartition(t *testing.T) {
	collectionID := int64(3)
	partitionRowCounts := map[int64]int{
		100: 10,
		200: 0, // Empty partition
		300: 5,
	}
	tt := uint64(3000)
	id := rmq.NewRmqID(3)

	upsertMsg := createTestMultiPartitionUpsertMessage(t, collectionID, partitionRowCounts, tt, id)

	tsMsgs, err := parseMultiPartitionUpsertMsg(upsertMsg)
	require.NoError(t, err)

	// Should only produce 4 messages: 2 non-empty partitions * 2 messages per partition
	// Empty partition is skipped
	require.Equal(t, 4, len(tsMsgs))

	// Verify partition IDs and row counts
	partitionRowCounts2 := make(map[int64]uint64)
	for i, tsMsg := range tsMsgs {
		if i%2 == 1 { // Insert messages
			insertMsg := tsMsg.(*msgstream.InsertMsg)
			partitionRowCounts2[insertMsg.GetPartitionID()] = insertMsg.GetNumRows()
		}
	}
	assert.Equal(t, uint64(10), partitionRowCounts2[100])
	assert.Equal(t, uint64(5), partitionRowCounts2[300])
	_, hasEmpty := partitionRowCounts2[200]
	assert.False(t, hasEmpty) // Empty partition should be skipped
}

// Test_parseUpsertMsg_MultiPartitionDetection tests that parseUpsertMsg correctly detects multi-partition messages
func Test_parseUpsertMsg_MultiPartitionDetection(t *testing.T) {
	collectionID := int64(4)
	partitionRowCounts := map[int64]int{
		100: 4,
		200: 6,
	}
	tt := uint64(4000)
	id := rmq.NewRmqID(4)

	upsertMsg := createTestMultiPartitionUpsertMessage(t, collectionID, partitionRowCounts, tt, id)

	// Call parseUpsertMsg which should detect multi-partition and route to parseMultiPartitionUpsertMsg
	tsMsgs, err := parseUpsertMsg(upsertMsg)
	require.NoError(t, err)

	// Should produce 4 messages: 2 partitions * 2 messages per partition
	require.Equal(t, 4, len(tsMsgs))
}

// Helper function to create a test multi-partition upsert message
func createTestMultiPartitionUpsertMessage(t *testing.T, collectionID int64, partitionRowCounts map[int64]int, tt uint64, id message.MessageID) message.ImmutableMessage {
	// Sort partition IDs for deterministic behavior
	sortedPartitionIDs := make([]int64, 0, len(partitionRowCounts))
	for partitionID := range partitionRowCounts {
		sortedPartitionIDs = append(sortedPartitionIDs, partitionID)
	}
	sort.Slice(sortedPartitionIDs, func(i, j int) bool {
		return sortedPartitionIDs[i] < sortedPartitionIDs[j]
	})

	// Calculate total rows and build contiguous field data
	totalRows := 0
	for _, count := range partitionRowCounts {
		totalRows += count
	}

	// Create field data for all rows
	timestamps := make([]uint64, totalRows)
	rowIDs := make([]int64, totalRows)
	intData := make([]int64, totalRows)
	for i := 0; i < totalRows; i++ {
		timestamps[i] = tt
		rowIDs[i] = int64(i)
		intData[i] = int64(i * 10)
	}

	// Create InsertRequest
	insertReq := &msgpb.InsertRequest{
		CollectionID: collectionID,
		NumRows:      uint64(totalRows),
		Timestamps:   timestamps,
		RowIDs:       rowIDs,
		FieldsData: []*schemapb.FieldData{
			{
				FieldId:   101,
				FieldName: "id",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: intData},
						},
					},
				},
			},
		},
	}

	// Create DeleteRequest with some test primary keys
	deletePKs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
		},
	}
	deleteReq := &msgpb.DeleteRequest{
		CollectionID: collectionID,
		PrimaryKeys:  deletePKs,
		NumRows:      3,
	}

	// Build partition assignments
	partitionAssignments := make([]*message.PartitionSegmentAssignment, 0, len(sortedPartitionIDs))
	for _, partitionID := range sortedPartitionIDs {
		rowCount := partitionRowCounts[partitionID]
		partitionAssignments = append(partitionAssignments, &message.PartitionSegmentAssignment{
			PartitionId: partitionID,
			Rows:        uint64(rowCount),
			SegmentAssignment: &message.SegmentAssignment{
				SegmentId: partitionID + 1000, // Use partition ID + 1000 as segment ID for testing
			},
		})
	}

	// Create upsert message body
	upsertBody := &message.UpsertMessageBody{
		InsertRequest: insertReq,
		DeleteRequest: deleteReq,
	}

	// Build upsert message
	mutableMsg, err := message.NewUpsertMessageBuilderV2().
		WithVChannel("test-channel").
		WithHeader(&message.UpsertMessageHeader{
			CollectionId: collectionID,
			Partitions:   partitionAssignments,
			DeleteRows:   3,
		}).
		WithBody(upsertBody).
		BuildMutable()
	require.NoError(t, err)

	immutableMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)
	return immutableMsg
}
