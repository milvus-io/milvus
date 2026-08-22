package segment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestObserveInsertUsesSingleCheckpoint(t *testing.T) {
	raw := newObserveTestInsert(t, 10, []*messagespb.PartitionSegmentAssignment{
		newObserveTestAssignment(1, 3, 4),
	})
	batches, err := BuildInsertBatches(raw)
	require.NoError(t, err)
	batch := batches[1]

	view := newObserveTestSegment(0)
	owner := message.NewOwnedImmutableMessage(raw, nil)
	dispatch := owner.Clone()
	require.True(t, view.ObserveInsert(context.Background(), dispatch, batch))
	dispatch.Release()
	owner.Release()

	meta := view.AssignmentMeta()
	assert.Equal(t, uint64(0), meta.GetCheckpointTimeTick())
	assert.Equal(t, uint64(3), meta.GetStat().GetModifiedRows())
	assert.Nil(t, view.ConsumeDirtyAndGetSnapshot())
	view.mu.Lock()
	assert.Len(t, view.pending.entries, 1)
	pending := view.pending.takeAll()
	view.mu.Unlock()
	releaseMessages(pending.retainedHandles())
}

func TestObserveInsertSkipsPersistedCheckpoint(t *testing.T) {
	raw := newObserveTestInsert(t, 10, []*messagespb.PartitionSegmentAssignment{
		newObserveTestAssignment(1, 3, 4),
	})
	batches, err := BuildInsertBatches(raw)
	require.NoError(t, err)
	view := newObserveTestSegment(10)
	owner := message.NewOwnedImmutableMessage(raw, nil)
	dispatch := owner.Clone()

	assert.False(t, view.ObserveInsert(context.Background(), dispatch, batches[1]))
	dispatch.Release()
	owner.Release()
	view.mu.Lock()
	defer view.mu.Unlock()
	assert.Empty(t, view.pending.entries)
}

type durableSnapshotTestPackWriter struct {
	pack *flushPack
}

func (w *durableSnapshotTestPackWriter) FlushInsertBuffer(_ context.Context, pack *flushPack) (*flushResult, error) {
	w.pack = pack
	return &flushResult{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
		Binlogs:     []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 10, ToTimeTick: 10}},
		Statistics:  &datapb.Statistics{InsertBinlogSize: 123},
		DeltaBinlog: []*datapb.FieldBinlog{{FieldID: 100}},
	}}, nil
}

func TestSegmentSnapshotContainsOnlyDurableInsertEffects(t *testing.T) {
	writer := &durableSnapshotTestPackWriter{}
	view := NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: 5,
			PersistedStorage:   &streamingpb.L1SegmentPersistedStorage{},
			Stat:               &streamingpb.SegmentAssignmentStat{ModifiedRows: 1, ModifiedBinarySize: 2},
		},
		5,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{packWriter: writer, owner: testSegmentOwner{}},
	)

	observe := func(timetick, rows, bytes uint64) {
		raw := newObserveTestInsert(t, timetick, []*messagespb.PartitionSegmentAssignment{
			newObserveTestAssignment(1, rows, bytes),
		})
		batches, err := BuildInsertBatches(raw)
		require.NoError(t, err)
		owner := message.NewOwnedImmutableMessage(raw, nil)
		dispatch := owner.Clone()
		require.True(t, view.ObserveInsert(context.Background(), dispatch, batches[1]))
		dispatch.Release()
		owner.Release()
	}

	observe(10, 3, 4)
	view.mu.Lock()
	firstFlush := view.newFlushL1BufferTaskLocked()
	view.mu.Unlock()
	observe(20, 5, 6)

	require.NoError(t, firstFlush.Execute(context.Background()))
	require.NotNil(t, writer.pack)
	assert.Equal(t, uint64(10), writer.pack.Meta.GetCheckpointTimeTick())
	assert.Equal(t, uint64(4), writer.pack.Meta.GetStat().GetModifiedRows())
	assert.Equal(t, uint64(6), writer.pack.Meta.GetStat().GetModifiedBinarySize())
	snapshot := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(10), snapshot.GetCheckpointTimeTick())
	assert.Equal(t, uint64(4), snapshot.GetStat().GetModifiedRows())
	assert.Equal(t, uint64(6), snapshot.GetStat().GetModifiedBinarySize())
	require.Len(t, snapshot.GetPersistedStorage().GetBinlogs(), 1)
	assert.Equal(t, int64(123), snapshot.GetPersistedStorage().GetStatistics().GetInsertBinlogSize())
	require.Len(t, snapshot.GetPersistedStorage().GetDeltaBinlog(), 1)
	assert.Equal(t, int64(100), snapshot.GetPersistedStorage().GetDeltaBinlog()[0].GetFieldID())

	// The live view already includes the later pending insert, but that effect
	// must not leak into the catalog snapshot before its object write completes.
	live := view.AssignmentMeta()
	assert.Equal(t, uint64(9), live.GetStat().GetModifiedRows())
	assert.Equal(t, uint64(12), live.GetStat().GetModifiedBinarySize())
	view.MarkSnapshotPersisted(snapshot)
	assert.False(t, view.HasDirty())

	view.mu.Lock()
	pending := view.pending.takeAll()
	view.mu.Unlock()
	releaseMessages(pending.retainedHandles())
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

func newObserveTestSegment(checkpoint uint64) *SegmentView {
	return NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:          1,
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: checkpoint,
			Stat:               &streamingpb.SegmentAssignmentStat{},
		},
		checkpoint,
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
