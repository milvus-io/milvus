package transformlog_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	"github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryNodeBufferSurvivesPhysicalStreamReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	firstSummary := walsummary.NewManager(walsummary.ManagerConfig{})
	secondSummary := walsummary.NewManager(walsummary.ManagerConfig{})
	firstSummary.ObserveQueryBarrier("p_1v0", 10)
	secondSummary.ObserveQueryBarrier("p_1v0", 20)
	first := walsummary.NewStream(firstSummary)
	second := walsummary.NewStream(secondSummary)
	defer first.Close()
	defer second.Close()
	var acquisitions atomic.Int32
	stream := transformlog.NewResumableStream(ctx, "p", func(context.Context, string) (wal.TransformLogStream, error) {
		if acquisitions.Add(1) == 1 {
			return first, nil
		}
		return second, nil
	})
	defer stream.Close()
	manager := wal.NewTransformLogErrorAccesser(context.Canceled)
	patch := mockey.Mock(mockey.GetMethod(manager, "AcquireStream")).Return(stream, nil).Build()
	defer patch.UnPatch()
	buffer := transformlogbuffer.New(manager, 1)
	view := qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{Vchannel: "p_1v0"}, &viewpb.QueryViewOfQueryNode{NodeId: 1}).(*qviews.QueryViewAtQueryNode)
	guard, err := buffer.Acquire(ctx, view)
	require.NoError(t, err)
	defer guard.Release()
	require.NoError(t, guard.WaitTransformVisible(ctx, 10))
	// Ending the physical attempt must not terminate the logical Buffer stream.
	require.NoError(t, first.Close())
	require.NoError(t, guard.WaitTransformVisible(ctx, 20))
	require.EqualValues(t, 2, acquisitions.Load())
	select {
	case <-stream.Done():
		t.Fatal("physical reconnect terminated the logical stream")
	default:
	}
}
