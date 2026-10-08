package segment

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestEmptyFinalCommitWaitsForRetirementAndRecovers(t *testing.T) {
	ctx := context.Background()
	calls := 0
	save := mockey.Mock((*coordStub).SaveBinlogPaths).To(func(_ *coordStub, _ context.Context, req *datapb.SaveBinlogPathsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
		require.True(t, req.GetFlushed())
		require.True(t, req.GetDropped())
		require.Empty(t, req.GetManifestPath())
		require.Empty(t, req.GetField2BinlogPaths())
		calls++
		if calls == 1 {
			// DataCoord may have persisted Dropped before the response was lost.
			return nil, context.DeadlineExceeded
		}
		return dataview.FlushResultStatus(nil), nil
	}).Build()
	defer save.UnPatch()
	write := mockey.Mock((*growingBulkPackWriter).FlushInsertBuffer).Return(nil, nil).Build()
	defer write.UnPatch()
	scheduler := &recordingSegmentScheduler{}
	config := runtimeConfig{
		lifecycle:  &segmentLifecycleWriter{coord: &coordStub{}},
		packWriter: &growingBulkPackWriter{}, owner: testSegmentOwner{},
		runtime: moduleapi.Runtime{Scheduler: scheduler},
	}
	meta := &streamingpb.SegmentAssignmentMeta{
		SegmentId: 1, Vchannel: "v1", CheckpointTimeTick: 5,
		State:          streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		StorageVersion: storage.StorageV3,
		Stat:           &streamingpb.SegmentAssignmentStat{Level: datapb.SegmentLevel_L1},
	}
	view := newSegmentViewFromMeta(meta, nil, config)
	flush := message.NewFlushMessageBuilderV2().WithVChannel("v1").WithHeader(&message.FlushMessageHeader{}).
		WithBody(&message.FlushMessageBody{}).MustBuildMutable().WithTimeTick(20).IntoImmutableMessage(walimplstest.NewTestMessageID(20))
	done := false
	owned := message.NewOwnedImmutableMessage(flush, func() { done = true })
	dispatch := owned.Clone()
	require.True(t, view.Flush(ctx, dispatch))
	dispatch.Release()
	owned.Release()
	require.True(t, errors.Is(scheduler.tasks[0].Execute(ctx), nodescheduler.ErrDelay))
	require.False(t, done)
	require.False(t, view.finalCommitDone.Load())
	require.Equal(t, meta.State, view.AssignmentMeta().State)

	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	require.True(t, done)
	snapshot := view.ConsumeDirtyAndGetSnapshot()
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, snapshot.State)
	require.Equal(t, uint64(20), snapshot.CheckpointTimeTick)
	require.Nil(t, snapshot.SealedAtDataVersion)
	require.Empty(t, snapshot.GetPersistedStorage().GetManifestPath())
	require.Equal(t, 0, write.Times(), "empty retirement must not write data or a manifest")
	require.False(t, view.TombstonedCleanupReady(21), "GC must wait for the tombstone snapshot")
	view.MarkSnapshotPersisted(snapshot)
	require.False(t, view.TombstonedCleanupReady(20), "GC must wait for the global checkpoint")
	require.True(t, view.TombstonedCleanupReady(21))

	// A crash before saving the tombstone replays the old growing snapshot.
	replayed := newSegmentViewFromMeta(meta, nil, config)
	owner := message.NewOwnedImmutableMessage(flush, nil)
	dispatch = owner.Clone()
	require.True(t, replayed.Flush(ctx, dispatch))
	dispatch.Release()
	owner.Release()
	require.NoError(t, scheduler.tasks[1].Execute(ctx))
	require.True(t, replayed.finalCommitDone.Load())
	require.Equal(t, 3, calls)

	// Once the tombstone is durable, no version or further commit is needed.
	recovered := newSegmentViewFromMeta(snapshot, nil, config)
	recovered.ResumePendingRecovery()
	require.True(t, recovered.EnsureFinalCommit())
	require.Empty(t, recovered.pendingTasks)
	require.Equal(t, 3, calls)
}

func TestFinalCommitUsesCumulativeRows(t *testing.T) {
	save := mockey.Mock((*coordStub).SaveBinlogPaths).To(func(_ *coordStub, _ context.Context, req *datapb.SaveBinlogPathsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
		require.False(t, req.GetDropped(), "previously persisted rows must not be retired as an empty segment")
		require.True(t, req.GetFlushed())
		return dataview.FlushResultStatus(nil), nil
	}).Build()
	defer save.UnPatch()
	meta := newCommitL1SegmentTestMeta()
	meta.Stat.ModifiedRows = 10
	writer := &segmentLifecycleWriter{coord: &coordStub{}}
	_, err := writer.CommitL1Segment(context.Background(), meta)
	require.NoError(t, err)
}
