package growingruntime

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPreparePartitionsReplaysSnapshotAndLiveTail(t *testing.T) {
	mockey.PatchConvey("new partitions catch up before publication", t, func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		r := newRuntime()
		defer r.Close()
		captured := make(chan struct{})
		snapshot := walview.VChannelWALView{BaseGrowingTimeTick: 10, SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{{SegmentID: 2, PartitionID: 20}}}}
		view := walview.VChannelWALView{PartitionIDs: []int64{10}, SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{{SegmentID: 1, PartitionID: 10}}}, CaptureQueryView: func(_ *viewpb.QueryViewMeta, install func(walview.VChannelWALView)) bool {
			install(snapshot)
			close(captured)
			return true
		}}
		require.NoError(t, r.Prepare(ctx, view))
		original := r.segments[1]
		loading := make(chan struct{})
		continueLoad := make(chan struct{})
		var prepare func(*Runtime, context.Context, walview.VChannelWALView) error
		mockey.Mock((*Runtime).prepareSegments).Origin(&prepare).To(func(next *Runtime, ctx context.Context, view walview.VChannelWALView) error {
			close(loading)
			<-continueLoad
			return prepare(next, ctx, view)
		}).Build()
		done := make(chan error, 1)
		go func() { done <- r.PreparePartitions(ctx, &viewpb.QueryViewMeta{LoadInfoVersion: 2}, []int64{20}) }()
		<-captured
		<-loading
		// A queued prefix event must be consumed by the serving dispatcher before
		// the already materialized partition snapshot can be merged.
		create := func(id int64, tick uint64) walview.VChannelResourceEvent {
			msg := message.NewCreateSegmentMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: 20, SegmentId: id}).WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(int64(tick)))
			return walview.VChannelResourceEvent{Message: msg}
		}
		r.ApplyLiveEvent(ctx, create(2, 10))
		r.ApplyLiveEvent(ctx, create(3, 11))
		close(continueLoad)
		require.NoError(t, <-done)
		require.ElementsMatch(t, []int64{1, 2, 3}, r.SegmentIDs())
		require.Same(t, original, r.segments[1], "retained views keep their existing resources")
		require.True(t, r.partitionLoaded(20))
		require.True(t, r.partitionLoaded(10))
		require.NoError(t, r.PreparePartitions(ctx, &viewpb.QueryViewMeta{}, []int64{20}), "already covered scope does not recapture")
	})
}

func TestPreparePartitionsFailureDoesNotPublish(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		r := newRuntime()
		ctx, cancel := context.WithCancel(t.Context())
		view := walview.VChannelWALView{PartitionIDs: []int64{10}, CaptureQueryView: func(_ *viewpb.QueryViewMeta, install func(walview.VChannelWALView)) bool {
			if canceled {
				install(walview.VChannelWALView{BaseGrowingTimeTick: 10})
				cancel()
				return true
			}
			return false
		}}
		require.NoError(t, r.Prepare(ctx, view))
		require.Error(t, r.PreparePartitions(ctx, &viewpb.QueryViewMeta{}, []int64{20}))
		require.False(t, r.partitionLoaded(20))
		require.Nil(t, r.refresh)
		require.Zero(t, r.queryRefs)
		cancel()
		r.Close()
	}
}

func TestPreparePartitionsDoesNotWaitForInsertTransform(t *testing.T) {
	mockey.PatchConvey("an insert-only prefix does not need a future transform", t, func() {
		mockey.Mock(drainDeleteReplay).Return(nil, nil).Build()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		r := newRuntime()
		defer r.Close()
		view := walview.VChannelWALView{PartitionIDs: []int64{10}, CaptureQueryView: func(_ *viewpb.QueryViewMeta, install func(walview.VChannelWALView)) bool {
			install(walview.VChannelWALView{BaseGrowingTimeTick: 10, BaseTransformTimeTick: 10})
			return true
		}}
		require.NoError(t, r.Prepare(ctx, view))
		r.markGrowingTimeTick(10)
		// No transform exists at 10: waiting for its frontier would stall until a
		// future barrier, which can itself be blocked behind the full stage queue.
		require.Zero(t, r.AppliedTransformTimeTick())
		require.NoError(t, r.PreparePartitions(ctx, &viewpb.QueryViewMeta{}, []int64{20}))
		require.True(t, r.partitionLoaded(20))
	})
}

func TestPreparePartitionsPreconditionsAndRetainedFloor(t *testing.T) {
	r := newRuntime()
	require.NoError(t, r.Prepare(t.Context(), walview.VChannelWALView{PartitionIDs: []int64{10}}))
	require.ErrorIs(t, r.PreparePartitions(t.Context(), &viewpb.QueryViewMeta{}, []int64{20}), merr.ErrServiceInternal)
	require.Zero(t, r.queryRefs)
	r.prepareMu.Lock()
	err := r.PreparePartitions(t.Context(), &viewpb.QueryViewMeta{}, []int64{20})
	r.prepareMu.Unlock()
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	r.Truncate(qviews.DataVersion{StreamingVersion: 2})
	r.view.CaptureQueryView = func(meta *viewpb.QueryViewMeta, install func(walview.VChannelWALView)) bool {
		require.Equal(t, int64(2), meta.GetVersion().GetDataVersion().GetStreamingVersion())
		install(walview.VChannelWALView{})
		return true
	}
	require.NoError(t, r.PreparePartitions(t.Context(), &viewpb.QueryViewMeta{Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}}}, []int64{20}))
	r.Close()
	require.ErrorIs(t, r.PreparePartitions(t.Context(), &viewpb.QueryViewMeta{}, []int64{30}), context.Canceled)
	stage := &partitionRefresh{version: qviews.DataVersion{StreamingVersion: 2}}
	require.False(t, stage.includes(walview.VChannelResourceEvent{}))
	require.False(t, stage.includes(walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SealedAtDataVersion: qviews.DataVersion{StreamingVersion: 2}}}))
	require.True(t, stage.includes(walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SealedAtDataVersion: qviews.DataVersion{StreamingVersion: 3}}}))
}

func TestPreparePartitionsCancellationDuringSnapshotLoad(t *testing.T) {
	mockey.PatchConvey("runtime cancellation aborts I/O without publishing scope", t, func() {
		runtimeCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		r := newRuntime()
		defer r.Close()
		view := walview.VChannelWALView{
			PartitionIDs: []int64{10}, RuntimeContext: runtimeCtx,
			CaptureQueryView: func(_ *viewpb.QueryViewMeta, install func(walview.VChannelWALView)) bool {
				install(walview.VChannelWALView{})
				return true
			},
		}
		require.NoError(t, r.Prepare(t.Context(), view))
		mockey.Mock((*Runtime).prepareSegments).To(func(_ *Runtime, ctx context.Context, _ walview.VChannelWALView) error {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		}).Build()
		require.ErrorIs(t, r.PreparePartitions(t.Context(), &viewpb.QueryViewMeta{}, []int64{20}), context.Canceled)
		require.False(t, r.partitionLoaded(20))
		require.Zero(t, r.queryRefs)
	})
}
