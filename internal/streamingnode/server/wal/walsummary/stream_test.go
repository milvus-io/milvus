package walsummary

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type recordingTransformHandler struct {
	events chan wal.TransformLogStreamEvent
	done   chan struct{}
}

func (h *recordingTransformHandler) Handle(event wal.TransformLogStreamEvent) error {
	h.events <- event
	return nil
}
func (h *recordingTransformHandler) Close() { close(h.done) }
func newRecordingTransformHandler() *recordingTransformHandler {
	return &recordingTransformHandler{events: make(chan wal.TransformLogStreamEvent, 16), done: make(chan struct{})}
}

func TestQueryStreamWaitsForBoundedCoverage(t *testing.T) {
	manager := NewManager(ManagerConfig{})
	manager.ObserveMessage(context.Background(), newTestDeleteMessage(t, "v1", 10, 1, 10))
	stream := NewStream(manager)
	defer stream.Close()
	handler := newRecordingTransformHandler()
	sub, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", EndTimeTick: 20, Handler: handler})
	require.NoError(t, err)
	defer sub.Close()
	select {
	case event := <-handler.events:
		require.Equal(t, uint64(10), event.Entry.GetTimeTick())
	case <-time.After(time.Second):
		t.Fatal("missing first delete")
	}
	select {
	case event := <-handler.events:
		t.Fatalf("premature bounded completion: %v", event)
	case <-time.After(20 * time.Millisecond):
	}
	manager.ObserveMessage(context.Background(), newTestDeleteMessage(t, "v1", 20, 1, 20))
	select {
	case <-handler.done:
	case <-time.After(time.Second):
		t.Fatal("bounded subscription did not complete")
	}
	require.Equal(t, uint64(20), (<-handler.events).Entry.GetTimeTick())
	require.Equal(t, uint64(20), (<-handler.events).SyncUp.TimeTick)
	select {
	case <-stream.Done():
		t.Fatal("subscription closed shared stream")
	default:
	}
}

func TestQueryStreamStartsAtRetainedBoundary(t *testing.T) {
	for _, end := range []uint64{5, 10, 20} {
		t.Run(fmt.Sprint(end), func(t *testing.T) {
			manager := NewManager(ManagerConfig{})
			manager.InitLastAcked(10)
			stream := NewStream(manager)
			defer stream.Close()
			handler := newRecordingTransformHandler()
			sub, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", EndTimeTick: end, Handler: handler})
			require.NoError(t, err)
			defer sub.Close()
			if end > 10 {
				// Clamping the start must not manufacture coverage through a future end.
				select {
				case event := <-handler.events:
					t.Fatalf("premature completion: %v", event)
				case <-time.After(20 * time.Millisecond):
				}
				manager.ObserveMessage(context.Background(), newTestDeleteMessage(t, "v1", 20, 1, 20))
				require.Equal(t, uint64(20), nextTransformEvent(t, handler).Entry.GetTimeTick())
			}
			event := nextTransformEvent(t, handler)
			require.NoError(t, event.Err)
			require.NotNil(t, event.SyncUp)
			require.Equal(t, end, event.SyncUp.TimeTick)
			select {
			case <-handler.done:
			case <-time.After(time.Second):
				t.Fatal("bounded replay did not finish")
			}
		})
	}
}

func TestQueryStreamResumesAfterGC(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10, 1, 10))
	require.NoError(t, persistSummary(ctx, manager))
	manager.AdvanceGCTimeTick("v1", 10)
	manager.cfg.RetentionMaxBytes = 1
	require.NoError(t, gcSummary(ctx, manager))
	// Exercise the durable GC boundary, not only an in-memory notification.
	restored := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, restored.Restore(ctx))
	restored.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20, 1, 20))
	require.NoError(t, persistSummary(ctx, restored))
	stream := NewStream(restored)
	defer stream.Close()
	handler := newRecordingTransformHandler()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: handler})
	require.NoError(t, err)
	defer sub.Close()
	require.Equal(t, uint64(20), nextTransformEvent(t, handler).Entry.GetTimeTick())
	require.Equal(t, uint64(20), nextTransformEvent(t, handler).SyncUp.TimeTick)
	restored.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 30, 1, 30))
	require.Equal(t, uint64(30), nextTransformEvent(t, handler).Entry.GetTimeTick())
	require.Equal(t, uint64(30), nextTransformEvent(t, handler).SyncUp.TimeTick)
	require.NoError(t, stream.Close())
	require.ErrorIs(t, nextTransformEvent(t, handler).Err, context.Canceled)
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: handler})
	require.ErrorIs(t, err, context.Canceled)
}

func TestQueryRetentionPinsMaterializedDeletes(t *testing.T) {
	manager := NewManager(ManagerConfig{RetentionMaxBytes: 1})
	manager.manifest.Chunks = []*streamingpb.PChannelSummaryChunkIndexEntry{{Generation: 1, ObjectSize: 10, Vchannels: []*streamingpb.VChannelSummaryChunkIndex{{Vchannel: "v1", Transform: &streamingpb.VChannelSummaryTransformIndex{EndTimeTick: 20}}}}}
	manager.gcFrontiers["v1"] = 100
	manager.SetQueryRetention("v1", 9)
	require.Empty(t, manager.computeRetention(), "materialized deletes still belong to the old growing view")
	manager.SetQueryRetention("v1", math.MaxUint64)
	require.Len(t, manager.computeRetention(), 1)
}
