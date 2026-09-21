package segment

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestClosingSegmentReplaysFlushAfterIntermediatePackSnapshot(t *testing.T) {
	ctx := context.Background()
	publish := mockey.Mock((*segmentLifecycleWriter).PersistGrowingSegment).Return(nil).Build()
	defer publish.UnPatch()
	commit := mockey.Mock((*segmentLifecycleWriter).CommitL1Segment).Return(nil, context.DeadlineExceeded).Build()
	defer commit.UnPatch()
	scheduler := &recordingSegmentScheduler{}
	config := runtimeConfig{packWriter: &durableSnapshotTestPackWriter{}, lifecycle: &segmentLifecycleWriter{}, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: scheduler}}
	view := newSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, CheckpointTimeTick: 5,
		Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5}, PersistedStorage: &streamingpb.L1SegmentPersistedStorage{},
	}, nil, config)
	insert := newObserveTestInsert(t, 10, []*messagespb.PartitionSegmentAssignment{newObserveTestAssignment(1, 3, 4)})
	batches, err := BuildInsertBatches(insert)
	require.NoError(t, err)
	owner := message.NewOwnedImmutableMessage(insert, nil)
	dispatch := owner.Clone()
	require.True(t, view.ObserveInsert(ctx, dispatch, batches[1]))
	dispatch.Release()
	owner.Release()
	flush := message.NewFlushMessageBuilderV2().WithVChannel("v1").WithHeader(&message.FlushMessageHeader{}).
		WithBody(&message.FlushMessageBody{}).MustBuildMutable().WithTimeTick(20).IntoImmutableMessage(walimplstest.NewTestMessageID(20))
	observeFlush := func(view *SegmentView) {
		owner := message.NewOwnedImmutableMessage(flush, nil)
		dispatch := owner.Clone()
		require.True(t, view.Flush(ctx, dispatch))
		dispatch.Release()
		owner.Release()
	}
	observeFlush(view)
	require.False(t, view.IsGrowing())
	require.Error(t, scheduler.tasks[0].Execute(ctx))
	snapshot := view.ConsumeDirtyAndGetSnapshot()
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, snapshot.State)
	require.Equal(t, uint64(10), snapshot.CheckpointTimeTick)
	require.Nil(t, snapshot.SealedAtDataVersion)
	require.Equal(t, uint64(3), snapshot.Stat.ModifiedRows)
	// Crash before final publication: the runtime close must be reconstructed by
	// WAL replay, while the already published pack must not be written twice.
	recovered := newSegmentViewFromMeta(snapshot, nil, config)
	require.True(t, recovered.IsGrowing())
	observeFlush(recovered)
	commit.UnPatch()
	success := mockey.Mock((*segmentLifecycleWriter).CommitL1Segment).Return(&viewpb.DataVersion{StreamingVersion: 7}, nil).Build()
	defer success.UnPatch()
	require.NoError(t, scheduler.tasks[1].Execute(ctx))
	final := recovered.ConsumeDirtyAndGetSnapshot()
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, final.State)
	require.Equal(t, uint64(20), final.CheckpointTimeTick)
	require.EqualValues(t, 7, final.SealedAtDataVersion.StreamingVersion)
	require.Equal(t, 1, publish.Times())
}
