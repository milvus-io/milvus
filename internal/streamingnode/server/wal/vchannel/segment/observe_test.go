package segment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestObserveInsertMode(t *testing.T) {
	raw := newObserveTestInsert(t, 10, []*messagespb.PartitionSegmentAssignment{
		newObserveTestAssignment(1, 3, 4),
	})
	batches, err := BuildInsertBatches(raw)
	require.NoError(t, err)
	batch := batches[1]

	tests := []struct {
		name             string
		mode             moduleapi.ObserveMode
		metaCheckpoint   uint64
		expectedRows     uint64
		expectedBuffered int
	}{
		{name: "meta only", mode: moduleapi.ObserveModeMetaOnly, expectedRows: 3},
		{name: "data only", mode: moduleapi.ObserveModeDataOnly, metaCheckpoint: 10, expectedBuffered: 1},
		{name: "meta and data", mode: moduleapi.ObserveModeMetaAndData, expectedRows: 3, expectedBuffered: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			view := newObserveTestSegment(test.metaCheckpoint, 0)
			owner := message.NewOwnedImmutableMessage(raw, nil)
			dispatch := owner.Clone()
			require.True(t, view.ObserveInsert(context.Background(), dispatch, batch, test.mode))
			dispatch.Release()
			owner.Release()

			meta := view.AssignmentMeta()
			assert.Equal(t, uint64(10), meta.GetCheckpointTimeTick())
			assert.Equal(t, test.expectedRows, meta.GetStat().GetModifiedRows())
			view.mu.Lock()
			assert.Len(t, view.pending.entries, test.expectedBuffered)
			pending := view.pending.takeAll()
			view.mu.Unlock()
			releaseMessages(pending.retainedHandles())
		})
	}
}

func TestObserveInsertDataOnlyUsesLocalCheckpoint(t *testing.T) {
	raw := newObserveTestInsert(t, 10, []*messagespb.PartitionSegmentAssignment{
		newObserveTestAssignment(1, 3, 4),
	})
	batches, err := BuildInsertBatches(raw)
	require.NoError(t, err)
	view := newObserveTestSegment(10, 10)
	owner := message.NewOwnedImmutableMessage(raw, nil)
	dispatch := owner.Clone()

	assert.False(t, view.ObserveInsert(
		context.Background(),
		dispatch,
		batches[1],
		moduleapi.ObserveModeDataOnly,
	))
	dispatch.Release()
	owner.Release()
	view.mu.Lock()
	defer view.mu.Unlock()
	assert.Empty(t, view.pending.entries)
}

func TestBuildInsertBatchesAggregatesTxnBySegment(t *testing.T) {
	txnContext := message.TxnContext{TxnID: 1}
	messageID := walimplstest.NewTestMessageID(1)
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnContext).
		WithTimeTick(1).
		WithLastConfirmed(messageID).
		IntoImmutableMessage(messageID)
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	builder.Add(newObserveTestInsert(t, 2, []*messagespb.PartitionSegmentAssignment{
		newObserveTestAssignment(1, 2, 3),
	}))
	builder.Add(newObserveTestInsert(t, 3, []*messagespb.PartitionSegmentAssignment{
		newObserveTestAssignment(1, 5, 7),
		newObserveTestAssignment(2, 11, 13),
	}))
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnContext).
		WithTimeTick(10).
		WithLastConfirmed(messageID).
		IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)

	batches, err := BuildInsertBatches(txn)
	require.NoError(t, err)
	require.Len(t, batches, 2)
	assert.Len(t, batches[1].assignments, 2)
	assert.Equal(t, uint64(7), batches[1].rows)
	assert.Equal(t, uint64(10), batches[1].binarySize)
	assert.Equal(t, uint64(10), batches[1].timeTick)
	assert.Len(t, batches[2].assignments, 1)
	assert.Equal(t, uint64(11), batches[2].rows)
	assert.Equal(t, uint64(13), batches[2].binarySize)
}

func newObserveTestSegment(metaCheckpoint, dataCheckpoint uint64) *SegmentView {
	return NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:              1,
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     metaCheckpoint,
			DataCheckpointTimeTick: dataCheckpoint,
			Stat:                   &streamingpb.SegmentAssignmentStat{},
		},
		metaCheckpoint,
		dataCheckpoint,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{owner: testSegmentOwner{}},
	)
}

func newObserveTestAssignment(segmentID int64, rows, binarySize uint64) *messagespb.PartitionSegmentAssignment {
	return &messagespb.PartitionSegmentAssignment{
		Rows:       rows,
		BinarySize: binarySize,
		SegmentAssignment: &messagespb.SegmentAssignment{
			SegmentId: segmentID,
		},
	}
}

func newObserveTestInsert(
	t *testing.T,
	timetick uint64,
	assignments []*messagespb.PartitionSegmentAssignment,
) message.ImmutableMessage {
	t.Helper()
	mutable := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{CollectionId: 1, Partitions: assignments}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}
