//go:build test
// +build test

package adaptor

import (
	"fmt"
	"testing"
	"time"

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

// createTestUpsertMessage creates a test upsert message with both insert and delete parts
func createTestUpsertMessage(t *testing.T, collectionID, partitionID int64, numRows int, tt uint64, id message.MessageID) message.ImmutableMessage {
	// Create delete part (primary keys to delete)
	deleteIDs := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		deleteIDs[i] = int64(i)
	}

	// Create insert part (new data)
	timestamps := make([]uint64, numRows)
	rowIDs := make([]int64, numRows)
	intFieldData := make([]int32, numRows)
	for i := 0; i < numRows; i++ {
		timestamps[i] = tt
		rowIDs[i] = int64(i)
		intFieldData[i] = int32(i * 10)
	}

	fieldsData := []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "id",
			FieldId:   1,
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

	mutableMsg, err := message.NewUpsertMessageBuilderV2().
		WithHeader(&messagespb.UpsertMessageHeader{
			CollectionId: collectionID,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: partitionID,
					Rows:        uint64(numRows),
					BinarySize:  10000,
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: 100, // Test segment ID
					},
				},
			},
		}).
		WithBody(&messagespb.UpsertMessageBody{
			DeleteRequest: &msgpb.DeleteRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Delete,
					Timestamp: tt,
				},
				CollectionName: "test_collection",
				CollectionID:   collectionID,
				PartitionName:  "test_partition",
				PartitionID:    partitionID,
				PrimaryKeys: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: deleteIDs,
						},
					},
				},
				NumRows: int64(numRows),
			},
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_Insert,
					Timestamp: tt,
				},
				CollectionName: "test_collection",
				CollectionID:   collectionID,
				PartitionName:  "test_partition",
				PartitionID:    partitionID,
				NumRows:        uint64(numRows),
				FieldsData:     fieldsData,
				Timestamps:     timestamps,
				RowIDs:         rowIDs,
			},
		}).
		WithVChannel("test-vchannel").
		BuildMutable()

	require.NoError(t, err)
	return mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)
}

// TestParseUpsertMsg tests the parseUpsertMsg function
func TestParseUpsertMsg(t *testing.T) {
	t.Run("successful_parse_with_both_delete_and_insert", func(t *testing.T) {
		id := rmq.NewRmqID(1)
		tt := uint64(time.Now().UnixNano())
		numRows := 100

		upsertMsg := createTestUpsertMessage(t, 1, 2, numRows, tt, id)

		tsMsgs, err := parseUpsertMsg(upsertMsg)
		assert.NoError(t, err)
		assert.Len(t, tsMsgs, 2, "should have delete and insert messages")

		// Verify delete message
		deleteMsg, ok := tsMsgs[0].(*msgstream.DeleteMsg)
		assert.True(t, ok, "first message should be delete")
		assert.Equal(t, int64(numRows), deleteMsg.NumRows)
		assert.Len(t, deleteMsg.Timestamps, numRows)
		assert.Equal(t, "test-vchannel", deleteMsg.ShardName)
		for _, ts := range deleteMsg.Timestamps {
			assert.Equal(t, tt, ts)
		}

		// Verify insert message
		insertMsg, ok := tsMsgs[1].(*msgstream.InsertMsg)
		assert.True(t, ok, "second message should be insert")
		assert.Equal(t, int64(100), insertMsg.SegmentID, "should have segment ID from header")
		assert.Equal(t, "test-vchannel", insertMsg.ShardName)
		assert.Len(t, insertMsg.Timestamps, numRows)
	})

	t.Run("delete_with_no_numrows_fallback", func(t *testing.T) {
		// Test the fallback logic when NumRows is not set in DeleteRequest
		id := rmq.NewRmqID(1)
		tt := uint64(time.Now().UnixNano())
		numRows := 50

		deleteIDs := make([]int64, numRows)
		for i := 0; i < numRows; i++ {
			deleteIDs[i] = int64(i)
		}

		mutableMsg, err := message.NewUpsertMessageBuilderV2().
			WithHeader(&messagespb.UpsertMessageHeader{
				CollectionId: 1,
				Partitions: []*messagespb.PartitionSegmentAssignment{
					{
						PartitionId: 2,
						Rows:        uint64(numRows),
						SegmentAssignment: &messagespb.SegmentAssignment{
							SegmentId: 100,
						},
					},
				},
			}).
			WithBody(&messagespb.UpsertMessageBody{
				DeleteRequest: &msgpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						Timestamp: tt,
					},
					CollectionID: 1,
					PartitionID:  2,
					PrimaryKeys: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: deleteIDs,
							},
						},
					},
					// NumRows intentionally not set to test fallback
				},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Insert,
						Timestamp: tt,
					},
					CollectionID: 1,
					PartitionID:  2,
					NumRows:      uint64(numRows),
					FieldsData:   []*schemapb.FieldData{},
				},
			}).
			WithVChannel("test-vchannel").
			BuildMutable()

		require.NoError(t, err)
		immutableMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)

		tsMsgs, err := parseUpsertMsg(immutableMsg)
		assert.NoError(t, err)
		assert.Len(t, tsMsgs, 2)

		deleteMsg := tsMsgs[0].(*msgstream.DeleteMsg)
		assert.Equal(t, int64(numRows), deleteMsg.NumRows, "should calculate from primary keys")
		assert.Len(t, deleteMsg.Timestamps, numRows)
	})

	t.Run("missing_segment_assignment_error", func(t *testing.T) {
		id := rmq.NewRmqID(1)
		tt := uint64(time.Now().UnixNano())

		// Create upsert without segment assignment
		mutableMsg, err := message.NewUpsertMessageBuilderV2().
			WithHeader(&messagespb.UpsertMessageHeader{
				CollectionId: 1,
				Partitions:   []*messagespb.PartitionSegmentAssignment{
					// Empty - no assignment
				},
			}).
			WithBody(&messagespb.UpsertMessageBody{
				DeleteRequest: &msgpb.DeleteRequest{
					Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
					CollectionID: 1,
					PartitionID:  2,
					NumRows:      10,
				},
				InsertRequest: &msgpb.InsertRequest{
					Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
					CollectionID: 1,
					PartitionID:  2,
					NumRows:      10,
				},
			}).
			WithVChannel("test-vchannel").
			BuildMutable()

		require.NoError(t, err)
		immutableMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)

		_, err = parseUpsertMsg(immutableMsg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "partition assignment not found")
	})

	t.Run("zero_segment_id_error", func(t *testing.T) {
		id := rmq.NewRmqID(1)
		tt := uint64(time.Now().UnixNano())

		// Create upsert with segment ID = 0
		mutableMsg, err := message.NewUpsertMessageBuilderV2().
			WithHeader(&messagespb.UpsertMessageHeader{
				CollectionId: 1,
				Partitions: []*messagespb.PartitionSegmentAssignment{
					{
						PartitionId: 2,
						SegmentAssignment: &messagespb.SegmentAssignment{
							SegmentId: 0, // Invalid
						},
					},
				},
			}).
			WithBody(&messagespb.UpsertMessageBody{
				DeleteRequest: &msgpb.DeleteRequest{
					Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
					CollectionID: 1,
					PartitionID:  2,
					NumRows:      10,
				},
				InsertRequest: &msgpb.InsertRequest{
					Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
					CollectionID: 1,
					PartitionID:  2,
					NumRows:      10,
				},
			}).
			WithVChannel("test-vchannel").
			BuildMutable()

		require.NoError(t, err)
		immutableMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)

		_, err = parseUpsertMsg(immutableMsg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "segment id is 0")
	})

	t.Run("nil_primary_keys_safety", func(t *testing.T) {
		// Test the nil safety when PrimaryKeys is nil
		id := rmq.NewRmqID(1)
		tt := uint64(time.Now().UnixNano())

		mutableMsg, err := message.NewUpsertMessageBuilderV2().
			WithHeader(&messagespb.UpsertMessageHeader{
				CollectionId: 1,
				Partitions: []*messagespb.PartitionSegmentAssignment{
					{
						PartitionId: 2,
						SegmentAssignment: &messagespb.SegmentAssignment{
							SegmentId: 100,
						},
					},
				},
			}).
			WithBody(&messagespb.UpsertMessageBody{
				DeleteRequest: &msgpb.DeleteRequest{
					Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
					CollectionID: 1,
					PartitionID:  2,
					PrimaryKeys:  nil, // Explicitly nil to test safety
					NumRows:      0,   // Also 0 to trigger fallback
				},
				InsertRequest: &msgpb.InsertRequest{
					Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
					CollectionID: 1,
					PartitionID:  2,
					NumRows:      10,
				},
			}).
			WithVChannel("test-vchannel").
			BuildMutable()

		require.NoError(t, err)
		immutableMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)

		// Should not panic
		tsMsgs, err := parseUpsertMsg(immutableMsg)
		assert.NoError(t, err)
		assert.Len(t, tsMsgs, 2)

		deleteMsg := tsMsgs[0].(*msgstream.DeleteMsg)
		assert.Equal(t, int64(0), deleteMsg.NumRows)
	})

	t.Run("string_primary_keys_fallback", func(t *testing.T) {
		// Test the string ID primary keys fallback logic
		id := rmq.NewRmqID(1)
		tt := uint64(time.Now().UnixNano())
		numRows := 30

		deleteStrIDs := make([]string, numRows)
		for i := 0; i < numRows; i++ {
			deleteStrIDs[i] = fmt.Sprintf("id_%d", i)
		}

		mutableMsg, err := message.NewUpsertMessageBuilderV2().
			WithHeader(&messagespb.UpsertMessageHeader{
				CollectionId: 1,
				Partitions: []*messagespb.PartitionSegmentAssignment{
					{
						PartitionId: 2,
						Rows:        uint64(numRows),
						SegmentAssignment: &messagespb.SegmentAssignment{
							SegmentId: 100,
						},
					},
				},
			}).
			WithBody(&messagespb.UpsertMessageBody{
				DeleteRequest: &msgpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Delete,
						Timestamp: tt,
					},
					CollectionID: 1,
					PartitionID:  2,
					PrimaryKeys: &schemapb.IDs{
						IdField: &schemapb.IDs_StrId{
							StrId: &schemapb.StringArray{
								Data: deleteStrIDs,
							},
						},
					},
					// NumRows intentionally not set to test fallback
				},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Insert,
						Timestamp: tt,
					},
					CollectionID: 1,
					PartitionID:  2,
					NumRows:      uint64(numRows),
					FieldsData:   []*schemapb.FieldData{},
				},
			}).
			WithVChannel("test-vchannel").
			BuildMutable()

		require.NoError(t, err)
		immutableMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)

		tsMsgs, err := parseUpsertMsg(immutableMsg)
		assert.NoError(t, err)
		assert.Len(t, tsMsgs, 2)

		deleteMsg := tsMsgs[0].(*msgstream.DeleteMsg)
		assert.Equal(t, int64(numRows), deleteMsg.NumRows, "should calculate from string primary keys")
		assert.Len(t, deleteMsg.Timestamps, numRows)
	})
}

// TestNewMsgPackFromUpsertMessage tests converting upsert message to msgpack
func TestNewMsgPackFromUpsertMessage(t *testing.T) {
	t.Run("single_upsert_message", func(t *testing.T) {
		id := rmq.NewRmqID(1)
		tt := uint64(time.Now().UnixNano())

		upsertMsg := createTestUpsertMessage(t, 1, 2, 100, tt, id)

		pack, err := NewMsgPackFromMessage(upsertMsg)
		assert.NoError(t, err)
		assert.NotNil(t, pack)
		assert.Equal(t, tt, pack.BeginTs)
		assert.Equal(t, tt, pack.EndTs)
		// Upsert splits into delete + insert
		assert.Len(t, pack.Msgs, 2)

		// Verify message order
		deleteMsg, ok := pack.Msgs[0].(*msgstream.DeleteMsg)
		assert.True(t, ok)
		assert.Equal(t, int64(100), deleteMsg.NumRows)

		insertMsg, ok := pack.Msgs[1].(*msgstream.InsertMsg)
		assert.True(t, ok)
		assert.Equal(t, int64(100), insertMsg.SegmentID)
	})
}

// Note: parseTxnMsg with Upsert messages inside is tested in integration tests
// (test_upsert.py Test 2: large upsert that gets split into Txn by proxy)
// Unit testing Txn messages requires complex BeginTxn/CommitTxn message setup
// that is better covered at integration level.
