package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestHandlerCloseUnblocksPendingDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := make(chan struct{}, 1)
	handler := newServerEventHandler(ctx, 1, "v", func(wal.TransformLogStreamEvent) error { called <- struct{}{}; return nil })
	result := make(chan error, 1)
	go func() { result <- handler.Handle(wal.TransformLogStreamEvent{}) }()
	handler.Close()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("unready handler blocked subscription close")
	}
	handler.markReady()
	require.NoError(t, handler.Handle(wal.TransformLogStreamEvent{Err: context.Canceled}))
	select {
	case <-called:
		t.Fatal("closed handler forwarded cancellation as a terminal subscription error")
	default:
	}
	other := newServerEventHandler(ctx, 2, "v", func(wal.TransformLogStreamEvent) error { return nil })
	cancel()
	require.ErrorIs(t, other.Handle(wal.TransformLogStreamEvent{}), context.Canceled)
}

func TestStreamCancellationReleasesBackpressure(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(map[bool]string{false: "queue_full", true: "send_pending"}[queued], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := &SubscribeServer{ctx: ctx, outgoing: make(chan response)}
			result := make(chan error, 1)
			go func() { result <- server.send(&streamingpb.TransformResponse{}) }()
			if queued {
				select {
				case <-server.outgoing:
				case <-time.After(time.Second):
					t.Fatal("response not queued")
				}
			}
			cancel()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("transport shutdown blocked on a slow peer")
			}
		})
	}
}
