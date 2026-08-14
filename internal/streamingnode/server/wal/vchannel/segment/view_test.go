package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestConsumeDirtySnapshotKeepsStableInFlightView(t *testing.T) {
	view := NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:              1,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
		},
		10,
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{},
	)

	view.mu.Lock()
	view.meta.CheckpointTimeTick = 20
	view.meta.DataCheckpointTimeTick = 20
	view.dirty = true
	view.mu.Unlock()

	first := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, first)
	assert.Equal(t, uint64(20), first.GetCheckpointTimeTick())
	assert.Equal(t, uint64(20), first.GetDataCheckpointTimeTick())

	view.mu.Lock()
	view.meta.CheckpointTimeTick = 30
	view.meta.DataCheckpointTimeTick = 30
	view.dirty = true
	view.mu.Unlock()

	inFlight := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, inFlight)
	assert.Equal(t, uint64(20), inFlight.GetCheckpointTimeTick())
	assert.Equal(t, uint64(20), inFlight.GetDataCheckpointTimeTick())

	view.MarkSnapshotPersisted(first)

	next := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, next)
	assert.Equal(t, uint64(30), next.GetCheckpointTimeTick())
	assert.Equal(t, uint64(30), next.GetDataCheckpointTimeTick())

	view.MarkSnapshotPersisted(next)
	assert.Nil(t, view.ConsumeDirtyAndGetSnapshot())
}

func TestSegmentViewsShareDefaultFlushPolicy(t *testing.T) {
	first := NewSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{}, nil)
	second := NewSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{}, nil)

	assert.Same(t, first.flushPolicy, second.flushPolicy)
}

func TestSharedDefaultFlushPolicyObservesDynamicConfig(t *testing.T) {
	policy := newDefaultWriteOnlyFlushPolicy()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.FlushInsertBufferSize.Key, "1"))
	defer params.Reset(params.DataNodeCfg.FlushInsertBufferSize.Key)

	assert.True(t, policy.ShouldFlush(writeOnlyInsertBuffer{
		entries:    make([]message.RetainedImmutableMessage, 1),
		binarySize: 2,
	}, 0))
}

func TestSegmentViewNotifiesSharedOwnerWithoutPerViewCallbacks(t *testing.T) {
	owner := &recordingSegmentViewOwner{}
	view := NewSegmentViewFromMetaWithConfig(
		&streamingpb.SegmentAssignmentMeta{SegmentId: 10},
		nil,
		ViewConfig{Owner: owner},
	)

	view.NotifyDataUpdated()
	assert.Equal(t, int64(10), owner.segmentID)
	assert.Same(t, view, owner.view)

	event := walview.SegmentSealedEvent{SegmentID: 10}
	view.NotifySegmentSealed(event)
	assert.Equal(t, event, owner.sealed)
}

func TestSegmentTombstoneWaitsForOldestQueryViewDataVersion(t *testing.T) {
	view := NewSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		SegmentId:              10,
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		CheckpointTimeTick:     20,
		DataCheckpointTimeTick: 20,
		SealedAtDataVersion:    &viewpb.DataVersion{StreamingVersion: 2},
	}, nil)

	assert.False(t, view.TryFinalizeTombstoneAt(qviews.DataVersion{StreamingVersion: 1}, true))
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, view.AssignmentMeta().GetState())
	assert.True(t, view.TryFinalizeTombstoneAt(qviews.DataVersion{StreamingVersion: 2}, true))
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, view.AssignmentMeta().GetState())
}

func TestSegmentTombstoneWaitsForFinalCommitDataVersion(t *testing.T) {
	view := NewSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		SegmentId:              10,
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		CheckpointTimeTick:     20,
		DataCheckpointTimeTick: 20,
	}, nil)

	assert.False(t, view.TryFinalizeTombstoneAt(qviews.DataVersion{}, false))
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, view.AssignmentMeta().GetState())
}

type recordingSegmentViewOwner struct {
	segmentID int64
	view      *SegmentView
	sealed    walview.SegmentSealedEvent
}

func (o *recordingSegmentViewOwner) SegmentDataUpdated(segmentID int64, view *SegmentView) {
	o.segmentID = segmentID
	o.view = view
}

func (o *recordingSegmentViewOwner) SegmentSealed(event walview.SegmentSealedEvent) {
	o.sealed = event
}

func TestFlushedSegmentSnapshotReturnsFilteredSealedSegment(t *testing.T) {
	view := NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:       1,
			PartitionId:        10,
			SegmentId:          100,
			Vchannel:           "ch",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 50,
			SealedAtDataVersion: &viewpb.DataVersion{
				StreamingVersion: 10,
				CompactVersion:   1,
			},
		},
		50,
		50,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{},
	)

	_, visible := view.VisibleSnapshot("ch", qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1})
	flushed, flushedSegment := view.FlushedSegmentSnapshot("ch", qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1})

	require.False(t, visible)
	require.True(t, flushedSegment)
	assert.Equal(t, int64(100), flushed.SegmentID)
	assert.Equal(t, int64(10), flushed.PartitionID)
	assert.Equal(t, uint64(50), flushed.FlushTimeTick)
	assert.Equal(t, qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}, flushed.SealedAtDataVersion)
}

func TestPrunePendingFlushChunksClearsDiscardedBackingSlots(t *testing.T) {
	view := &SegmentView{
		meta: &streamingpb.SegmentAssignmentMeta{DataCheckpointTimeTick: 20},
		pendingFlushChunks: []writeOnlyInsertBuffer{
			{entries: make([]message.RetainedImmutableMessage, 1), toTimeTick: 10},
			{entries: make([]message.RetainedImmutableMessage, 1), toTimeTick: 15},
			{entries: make([]message.RetainedImmutableMessage, 1), toTimeTick: 30},
		},
	}

	view.prunePendingFlushChunksLocked()

	require.Len(t, view.pendingFlushChunks, 1)
	assert.Equal(t, uint64(30), view.pendingFlushChunks[0].toTimeTick)
	backing := view.pendingFlushChunks[:cap(view.pendingFlushChunks)]
	for _, chunk := range backing[len(view.pendingFlushChunks):] {
		assert.Nil(t, chunk.entries)
	}
}

func TestPendingFlushChunkSearchBoundaries(t *testing.T) {
	chunks := []writeOnlyInsertBuffer{
		{toTimeTick: 10},
		{toTimeTick: 15},
		{toTimeTick: 15},
		{toTimeTick: 30},
	}

	assert.Equal(t, 0, firstPendingFlushChunkAtOrAfter(chunks, 10))
	assert.Equal(t, 1, firstPendingFlushChunkAtOrAfter(chunks, 11))
	assert.Equal(t, 1, firstPendingFlushChunkAtOrAfter(chunks, 15))
	assert.Equal(t, 3, firstPendingFlushChunkAtOrAfter(chunks, 16))
	assert.Equal(t, len(chunks), firstPendingFlushChunkAtOrAfter(chunks, 31))

	assert.Equal(t, 0, firstPendingFlushChunkAfter(chunks, 9))
	assert.Equal(t, 1, firstPendingFlushChunkAfter(chunks, 10))
	assert.Equal(t, 3, firstPendingFlushChunkAfter(chunks, 15))
	assert.Equal(t, len(chunks), firstPendingFlushChunkAfter(chunks, 30))
}
