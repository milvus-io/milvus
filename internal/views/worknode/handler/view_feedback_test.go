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

func feedbackTestWindow(n int) qviews.ViewFeedbackWindow {
	w := qviews.ViewFeedbackWindow{StartUnixNanos: 10, EndUnixNanos: 20, Shards: make(map[qviews.ShardID]qviews.ViewFeedback)}
	for i := range n {
		id := qviews.ShardID{ReplicaID: int64(i + 1), VChannel: "p_1v0"}
		w.Shards[id] = qviews.ViewFeedback{ShardID: id, RequestCount: 1, LastAccessUnixNanos: 15}
	}
	return w
}

func TestViewFeedbackWindowBatchesAndStatePriority(t *testing.T) {
	mockey.PatchConvey("take once, send bounded windows, never replay", t, func() {
		window := feedbackTestWindow(600)
		mockey.Mock((*mockHandler).TakeViewFeedback).To(func(*mockHandler) qviews.ViewFeedbackWindow {
			taken := window
			window = qviews.ViewFeedbackWindow{}
			return taken
		}).Build()
		server := NewViewSyncServer(&mockHandler{})
		stream := newMockServerStream(t.Context())
		pending := newPendingReports()
		defer pending.Close()
		pending.Update(newTestSNView(1, viewpb.QueryViewState_QueryViewStateUp))
		require.False(t, server.sendViewFeedback(stream, pending))
		require.NotNil(t, (<-stream.sendCh).GetViews())
		for _, n := range []int{256, 256, 88} {
			report := (<-stream.sendCh).GetViewFeedback()
			require.Len(t, report.Shards, n)
			require.EqualValues(t, 10, report.WindowStartUnixNanos)
			require.EqualValues(t, 20, report.WindowEndUnixNanos)
		}
		require.False(t, server.sendViewFeedback(stream, pending))
		require.Empty(t, stream.sendCh)
		window = feedbackTestWindow(1)
		pending.SetCloseResponse()
		require.True(t, server.sendViewFeedback(stream, pending))
		require.NotNil(t, (<-stream.sendCh).GetClose())
		require.Empty(t, stream.sendCh)
		require.Empty(t, window.Shards, "closing discards the detached window")
	})
}

func TestViewFeedbackWindowFailureAndCancellation(t *testing.T) {
	mockey.PatchConvey("failed windows are dropped", t, func() {
		window := feedbackTestWindow(600)
		mockey.Mock((*mockHandler).TakeViewFeedback).To(func(*mockHandler) qviews.ViewFeedbackWindow {
			taken := window
			window = qviews.ViewFeedbackWindow{}
			return taken
		}).Build()
		server := NewViewSyncServer(&mockHandler{})
		pending := newPendingReports()
		defer pending.Close()
		stream := newMockServerStream(t.Context())
		patch := mockey.Mock((*mockServerStream).Send).Return(io.ErrClosedPipe).Build()
		require.True(t, server.sendViewFeedback(stream, pending))
		patch.UnPatch()
		require.False(t, server.sendViewFeedback(stream, pending))
		require.Empty(t, stream.sendCh, "reconnection cannot replay a lost window")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		window = feedbackTestWindow(1)
		require.True(t, server.sendViewFeedback(newMockServerStream(ctx), pending))
		window = feedbackTestWindow(1)
		pending.Close()
		require.True(t, server.sendViewFeedback(stream, pending))
	})
}

func TestViewFeedbackSizeTriggerRateLimit(t *testing.T) {
	mockey.PatchConvey("coalesce size notifications without delaying state or timer reports", t, func() {
		synctest.Test(t, func(t *testing.T) {
			ready := make(chan struct{}, 1)
			takes := make(chan time.Time, 16)
			mockey.Mock((*mockHandler).ViewFeedbackReady).Return((<-chan struct{})(ready)).Build()
			mockey.Mock((*mockHandler).TakeViewFeedback).To(func(*mockHandler) qviews.ViewFeedbackWindow {
				takes <- time.Now()
				select {
				case <-ready:
				default:
				}
				return feedbackTestWindow(1)
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
			require.NotNil(t, (<-stream.sendCh).GetViewFeedback())
			// Each of three consecutive windows reaches the size threshold
			// immediately. Repeated signals must not postpone the deadline.
			for window := 1; window <= 3; window++ {
				ready <- struct{}{}
				synctest.Wait()
				ready <- struct{}{}
				pending.Update(newTestSNView(int64(window), viewpb.QueryViewState_QueryViewStateUp))
				synctest.Wait()
				require.NotNil(t, (<-stream.sendCh).GetViews())
				time.Sleep(qviews.ViewFeedbackMinInterval - time.Nanosecond)
				synctest.Wait()
				require.Empty(t, stream.sendCh)
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				require.Len(t, takes, 1)
				taken := <-takes
				require.Equal(t, qviews.ViewFeedbackMinInterval, taken.Sub(previous))
				previous = taken
				require.NotNil(t, (<-stream.sendCh).GetViewFeedback())
			}
			// A size notification after the minimum gap sends immediately.
			time.Sleep(2 * time.Second)
			ready <- struct{}{}
			synctest.Wait()
			require.Len(t, takes, 1)
			previous = <-takes
			require.NotNil(t, (<-stream.sendCh).GetViewFeedback())
			// With no size notification, the normal five-second timer remains.
			time.Sleep(qviews.ViewFeedbackInterval - time.Nanosecond)
			synctest.Wait()
			require.Empty(t, stream.sendCh)
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			require.Len(t, takes, 1)
			require.Equal(t, qviews.ViewFeedbackInterval, (<-takes).Sub(previous))
			require.NotNil(t, (<-stream.sendCh).GetViewFeedback())
			// Closing during the cooldown must not wait for feedback to become due.
			ready <- struct{}{}
			synctest.Wait()
			pending.SetCloseResponse()
			synctest.Wait()
			require.NotNil(t, (<-stream.sendCh).GetClose())
			time.Sleep(qviews.ViewFeedbackInterval)
			synctest.Wait()
			require.Empty(t, takes)
			require.Empty(t, stream.sendCh)
		})
	})
}
