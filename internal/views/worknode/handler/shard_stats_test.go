package handler

import (
	"context"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func statsTestWindow(n int) qviews.ShardStatsWindow {
	w := qviews.ShardStatsWindow{StartUnixNanos: 10, EndUnixNanos: 20, Shards: make(map[qviews.ShardID]qviews.ShardStats)}
	for i := range n {
		id := qviews.ShardID{ReplicaID: int64(i + 1), VChannel: "p_1v0"}
		w.Shards[id] = qviews.ShardStats{ShardID: id, RequestCount: 1, LastAccessUnixNanos: 15}
	}
	return w
}

func TestShardStatsWindowBatchesAndStatePriority(t *testing.T) {
	mockey.PatchConvey("take once, send bounded windows, never replay", t, func() {
		window := statsTestWindow(600)
		mockey.Mock((*mockHandler).TakeShardStats).To(func(*mockHandler) qviews.ShardStatsWindow {
			taken := window
			window = qviews.ShardStatsWindow{}
			return taken
		}).Build()
		server := NewViewSyncServer(&mockHandler{})
		stream := newMockServerStream(t.Context())
		pending := newPendingReports()
		defer pending.Close()
		pending.Update(newTestSNView(1, viewpb.QueryViewState_QueryViewStateUp))
		require.False(t, server.sendShardStats(stream, pending))
		require.NotNil(t, (<-stream.sendCh).GetViews())
		for _, n := range []int{256, 256, 88} {
			report := (<-stream.sendCh).GetStats()
			require.Len(t, report.Shards, n)
			require.EqualValues(t, 10, report.WindowStartUnixNanos)
			require.EqualValues(t, 20, report.WindowEndUnixNanos)
		}
		require.False(t, server.sendShardStats(stream, pending))
		require.Empty(t, stream.sendCh)
		window = statsTestWindow(1)
		pending.SetCloseResponse()
		require.True(t, server.sendShardStats(stream, pending))
		require.NotNil(t, (<-stream.sendCh).GetClose())
		require.Empty(t, stream.sendCh)
		require.Empty(t, window.Shards, "closing discards the detached window")
	})
}

func TestShardStatsWindowFailureAndCancellation(t *testing.T) {
	mockey.PatchConvey("failed windows are dropped", t, func() {
		window := statsTestWindow(600)
		mockey.Mock((*mockHandler).TakeShardStats).To(func(*mockHandler) qviews.ShardStatsWindow {
			taken := window
			window = qviews.ShardStatsWindow{}
			return taken
		}).Build()
		server := NewViewSyncServer(&mockHandler{})
		pending := newPendingReports()
		defer pending.Close()
		stream := newMockServerStream(t.Context())
		patch := mockey.Mock((*mockServerStream).Send).Return(io.ErrClosedPipe).Build()
		require.True(t, server.sendShardStats(stream, pending))
		patch.UnPatch()
		require.False(t, server.sendShardStats(stream, pending))
		require.Empty(t, stream.sendCh, "reconnection cannot replay a lost window")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		window = statsTestWindow(1)
		require.True(t, server.sendShardStats(newMockServerStream(ctx), pending))
		window = statsTestWindow(1)
		pending.Close()
		require.True(t, server.sendShardStats(stream, pending))
	})
}
