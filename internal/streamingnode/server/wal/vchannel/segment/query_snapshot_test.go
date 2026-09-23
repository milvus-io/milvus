package segment

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQuerySnapshotUsesDurableBaseAndAllUnpublishedChunks(t *testing.T) {
	view := newSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1", State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, CheckpointTimeTick: 10, Stat: &streamingpb.SegmentAssignmentStat{ModifiedRows: 5}}, nil)
	var released atomic.Int32
	first, firstOwner := newTrackedRetained(t, 20, &released)
	second, secondOwner := newTrackedRetained(t, 30, &released)
	defer firstOwner.Release()
	defer secondOwner.Release()
	chunk := writeOnlyInsertBuffer{}
	chunk.appendMessage(first, 1, 1)
	view.pendingFlushChunks = append(view.pendingFlushChunks, chunk)
	view.pending.appendMessage(second, 1, 1)
	view.meta.CheckpointTimeTick = 30
	view.meta.Stat.ModifiedRows = 7
	snapshot, ok := view.VisibleSnapshot("v1", qviews.DataVersion{})
	require.True(t, ok)
	require.Equal(t, uint64(10), snapshot.Assignment.GetCheckpointTimeTick())
	require.Equal(t, uint64(5), snapshot.Assignment.GetStat().GetModifiedRows())
	require.Len(t, snapshot.Data.InsertMessages, 2)
	first.Release()
	second.Release()
	require.Equal(t, int32(2), released.Load())
	require.Equal(t, uint64(20), snapshot.Data.InsertMessages[0].TimeTick())
	require.Equal(t, uint64(30), snapshot.Data.InsertMessages[1].TimeTick())
	snapshot.Assignment.Stat.ModifiedRows = 100
	require.Equal(t, uint64(5), view.durableMeta.GetStat().GetModifiedRows())
}

func TestQuerySnapshotUsesCommittedSealedVersion(t *testing.T) {
	view := newSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1", State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, CheckpointTimeTick: 10, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 2}}, nil)
	_, ok := view.VisibleSnapshot("v1", qviews.DataVersion{StreamingVersion: 1})
	require.True(t, ok, "old view retains the committed segment as growing")
	_, ok = view.VisibleSnapshot("v1", qviews.DataVersion{StreamingVersion: 2})
	require.False(t, ok)
	flushed, ok := view.FlushedSegmentSnapshot("v1", qviews.DataVersion{StreamingVersion: 2})
	require.True(t, ok)
	require.Equal(t, uint64(10), flushed.FlushTimeTick)
	view.meta.SealedAtDataVersion = nil
	_, ok = view.VisibleSnapshot("v1", qviews.DataVersion{})
	require.False(t, ok, "discarded terminal segments have no query data")
}
