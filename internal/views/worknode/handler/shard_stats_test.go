package handler

import (
	"context"
	"io"
	"slices"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestShardStatsChangedBatchesAndReconnect(t *testing.T) {
	mockey.PatchConvey("snapshot, changes and reconnect use one sender", t, func() {
		samples := make([]qviews.ShardStats, 600)
		for i := range samples {
			samples[i] = qviews.ShardStats{ShardID: qviews.ShardID{ReplicaID: int64(i + 1), VChannel: "p_1v0"}, RequestCount: 1, LastAccessUnixNanos: 10}
		}
		mockey.Mock((*mockHandler).CollectShardStats).To(func(*mockHandler) []qviews.ShardStats { return slices.Clone(samples) }).Build()
		server := NewViewSyncServer(&mockHandler{})
		stream := newMockServerStream(t.Context())
		pending := newPendingReports()
		defer pending.Close()
		cache := make(map[qviews.ShardID]qviews.ShardStats)
		require.False(t, server.sendShardStats(stream, pending, cache))
		for _, n := range []int{256, 256, 88} {
			require.Len(t, (<-stream.sendCh).GetStats().GetShards(), n)
		}
		require.False(t, server.sendShardStats(stream, pending, cache))
		require.Empty(t, stream.sendCh)
		samples[0].RequestCount = 2
		require.False(t, server.sendShardStats(stream, pending, cache))
		require.Len(t, (<-stream.sendCh).GetStats().GetShards(), 1)
		require.False(t, server.sendShardStats(stream, pending, make(map[qviews.ShardID]qviews.ShardStats)))
		require.Len(t, stream.sendCh, 3)
		for len(stream.sendCh) > 0 {
			<-stream.sendCh
		}
		pending.Update(newTestSNView(1, viewpb.QueryViewState_QueryViewStateUp))
		samples[0].RequestCount = 3
		require.False(t, server.sendShardStats(stream, pending, cache))
		require.NotNil(t, (<-stream.sendCh).GetViews())
		require.NotNil(t, (<-stream.sendCh).GetStats())
		samples[0].RequestCount = 4
		pending.SetCloseResponse()
		require.True(t, server.sendShardStats(stream, pending, cache))
		require.NotNil(t, (<-stream.sendCh).GetClose())
		require.Empty(t, stream.sendCh)
	})
}

func TestShardStatsSendFailureAndCancellation(t *testing.T) {
	mockey.PatchConvey("failure never commits the sent cache", t, func() {
		mockey.Mock((*mockHandler).CollectShardStats).Return([]qviews.ShardStats{{ShardID: qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"}, RequestCount: 1}}).Build()
		server := NewViewSyncServer(&mockHandler{})
		pending := newPendingReports()
		defer pending.Close()
		stream := newMockServerStream(t.Context())
		cache := make(map[qviews.ShardID]qviews.ShardStats)
		patch := mockey.Mock((*mockServerStream).Send).Return(io.ErrClosedPipe).Build()
		require.True(t, server.sendShardStats(stream, pending, cache))
		require.Empty(t, cache)
		patch.UnPatch()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.True(t, server.sendShardStats(newMockServerStream(ctx), pending, cache))
		pending.Close()
		require.True(t, server.sendShardStats(stream, pending, cache))
	})
}
