package growingruntime

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
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
		done := make(chan error, 1)
		go func() { done <- r.PreparePartitions(ctx, &viewpb.QueryViewMeta{LoadInfoVersion: 2}, []int64{20}) }()
		<-captured
		// A queued prefix event must be consumed by the serving dispatcher before
		// the already materialized partition snapshot can be merged.
		create := func(id int64, tick uint64) walview.VChannelResourceEvent {
			msg := message.NewCreateSegmentMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: 20, SegmentId: id}).WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(int64(tick)))
			return walview.VChannelResourceEvent{Message: msg}
		}
		r.ApplyLiveEvent(ctx, create(2, 10))
		r.ApplyLiveEvent(ctx, create(3, 11))
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
