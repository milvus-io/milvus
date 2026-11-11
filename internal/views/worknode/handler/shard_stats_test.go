package handler

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"

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

func TestShardStatsSizeTriggerRateLimit(t *testing.T) {
	mockey.PatchConvey("coalesce size notifications without delaying state or timer reports", t, func() {
		synctest.Test(t, func(t *testing.T) {
			ready := make(chan struct{}, 1)
			takes := make(chan time.Time, 16)
			mockey.Mock((*mockHandler).ShardStatsReady).Return((<-chan struct{})(ready)).Build()
			mockey.Mock((*mockHandler).TakeShardStats).To(func(*mockHandler) qviews.ShardStatsWindow {
				takes <- time.Now()
				select {
				case <-ready:
				default:
				}
				return statsTestWindow(1)
			}).Build()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			pending := newPendingReports()
			defer pending.Close()
			stream := newMockServerStream(ctx)
			go NewViewSyncServer(&mockHandler{}).sendLoop(ctx, stream, pending)
			synctest.Wait()
			require.Len(t, takes, 1)
			previous := <-takes
			require.NotNil(t, (<-stream.sendCh).GetStats())
			// Each of three consecutive windows reaches the size threshold
			// immediately. Repeated signals must not postpone the deadline.
			for window := 1; window <= 3; window++ {
				ready <- struct{}{}
				synctest.Wait()
				ready <- struct{}{}
				pending.Update(newTestSNView(int64(window), viewpb.QueryViewState_QueryViewStateUp))
				synctest.Wait()
				require.NotNil(t, (<-stream.sendCh).GetViews())
				time.Sleep(qviews.ShardStatsMinInterval - time.Nanosecond)
				synctest.Wait()
				require.Empty(t, stream.sendCh)
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				require.Len(t, takes, 1)
				taken := <-takes
				require.Equal(t, qviews.ShardStatsMinInterval, taken.Sub(previous))
				previous = taken
				require.NotNil(t, (<-stream.sendCh).GetStats())
			}
			// A size notification after the minimum gap sends immediately.
			time.Sleep(2 * time.Second)
			ready <- struct{}{}
			synctest.Wait()
			require.Len(t, takes, 1)
			previous = <-takes
			require.NotNil(t, (<-stream.sendCh).GetStats())
			// With no size notification, the normal five-second timer remains.
			time.Sleep(qviews.ShardStatsInterval - time.Nanosecond)
			synctest.Wait()
			require.Empty(t, stream.sendCh)
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			require.Len(t, takes, 1)
			require.Equal(t, qviews.ShardStatsInterval, (<-takes).Sub(previous))
			require.NotNil(t, (<-stream.sendCh).GetStats())
			// Closing during the cooldown must not wait for stats to become due.
			ready <- struct{}{}
			synctest.Wait()
			pending.SetCloseResponse()
			synctest.Wait()
			require.NotNil(t, (<-stream.sendCh).GetClose())
			time.Sleep(qviews.ShardStatsInterval)
			synctest.Wait()
			require.Empty(t, takes)
			require.Empty(t, stream.sendCh)
		})
	})
}
