package handler

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestResourceWatermarksPeriodicWithoutViews(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		pending := newPendingReports()
		defer pending.Close()
		stream := newMockServerStream(ctx)
		var measurement atomic.Pointer[viewpb.NodeResourceWatermarks]
		server := NewViewSyncServer(&mockHandler{}, WithResourceWatermarks(measurement.Load))
		go server.sendLoop(ctx, stream, pending)
		synctest.Wait()
		require.Empty(t, stream.sendCh, "uninitialized loader is unknown")
		measurement.Store(&viewpb.NodeResourceWatermarks{Memory: 90, Disk: 10})
		time.Sleep(qviews.ViewFeedbackInterval)
		synctest.Wait()
		require.Equal(t, 90.0, (<-stream.sendCh).GetResourceWatermarks().GetMemory())
		measurement.Store(&viewpb.NodeResourceWatermarks{Memory: 20, Disk: 80})
		time.Sleep(qviews.ViewFeedbackInterval)
		synctest.Wait()
		require.Equal(t, 80.0, (<-stream.sendCh).GetResourceWatermarks().GetDisk())
		pending.SetCloseResponse()
		synctest.Wait()
		require.NotNil(t, (<-stream.sendCh).GetClose())
		time.Sleep(qviews.ViewFeedbackInterval)
		synctest.Wait()
		require.Empty(t, stream.sendCh)
	})
}

func TestResourceWatermarksSendFailure(t *testing.T) {
	mockey.PatchConvey("one writer stops on failed resource report", t, func() {
		server := NewViewSyncServer(&mockHandler{}, WithResourceWatermarks(func() *viewpb.NodeResourceWatermarks { return &viewpb.NodeResourceWatermarks{} }))
		mockey.Mock((*mockServerStream).Send).Return(io.ErrClosedPipe).Build()
		require.True(t, server.sendResourceWatermarks(newMockServerStream(t.Context())))
		server.sendLoop(t.Context(), newMockServerStream(t.Context()), newPendingReports())
	})
}
