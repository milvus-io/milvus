package segment

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestSealedRecoveryReplaysTailBeforeFinalCommit(t *testing.T) {
	ctx := context.Background()
	publish := mockey.Mock((*segmentLifecycleWriter).PersistGrowingSegment).Return(nil).Build()
	defer publish.UnPatch()
	commit := mockey.Mock((*segmentLifecycleWriter).CommitL1Segment).Return(nil, context.DeadlineExceeded).Build()
	defer commit.UnPatch()
	writer := &durableSnapshotTestPackWriter{}
	config := runtimeConfig{packWriter: writer, lifecycle: &segmentLifecycleWriter{}, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}}
	recovered := &streamingpb.SegmentAssignmentMeta{SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED, CheckpointTimeTick: 5, PersistedStorage: &streamingpb.L1SegmentPersistedStorage{}, Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 2, ModifiedRows: 1}}
	view := newSegmentViewFromMeta(recovered, nil, config)
	_, writable := view.WritePathRecoveryState()
	require.False(t, writable)
	raw := newObserveTestInsert(t, 10, []*messagespb.PartitionSegmentAssignment{newObserveTestAssignment(1, 3, 4)})
	batches, err := BuildInsertBatches(raw)
	require.NoError(t, err)
	owner := message.NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	require.True(t, view.ObserveInsert(ctx, retained, batches[1]))
	retained.Release()
	owner.Release()
	require.Nil(t, view.ConsumeDirtyAndGetSnapshot(), "observed tail is not durable yet")
	view.ResumePendingRecovery()
	require.NotNil(t, view.pendingFinalCommit)
	require.Error(t, view.pendingFinalCommit.Execute(ctx))
	// Data publication succeeded, final publication did not. Restart must still
	// finish the seal, without replaying/recounting the now-durable insert pack.
	snapshot := view.ConsumeDirtyAndGetSnapshot()
	require.EqualValues(t, 4, snapshot.GetStat().GetModifiedRows())
	require.EqualValues(t, 10, snapshot.GetCheckpointTimeTick())
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED, snapshot.GetState())
	require.Len(t, snapshot.GetPersistedStorage().GetBinlogs(), 1)
	encoded, err := proto.Marshal(snapshot)
	require.NoError(t, err)
	restored := &streamingpb.SegmentAssignmentMeta{}
	require.NoError(t, proto.Unmarshal(encoded, restored))
	second := newSegmentViewFromMeta(restored, nil, config)
	commit.UnPatch()
	success := mockey.Mock((*segmentLifecycleWriter).CommitL1Segment).Return(&viewpb.DataVersion{StreamingVersion: 7}, nil).Build()
	defer success.UnPatch()
	second.ResumePendingRecovery()
	require.NoError(t, second.pendingFinalCommit.Execute(ctx))
	final := second.ConsumeDirtyAndGetSnapshot()
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, final.GetState())
	require.EqualValues(t, 4, final.GetStat().GetModifiedRows())
	require.NotNil(t, final.GetSealedAtDataVersion())
	require.Equal(t, 1, publish.Times(), "restart must not publish the insert prefix twice")
}
