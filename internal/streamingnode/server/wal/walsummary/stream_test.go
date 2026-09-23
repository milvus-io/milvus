package walsummary

import (
	"context"
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

func TestQueryStreamRejectsTruncatedHistoryAndCancelsTail(t *testing.T) {
	manager := NewManager(ManagerConfig{})
	manager.manifest.TransformFastForwardTimeTick = map[string]uint64{"v1": 10}
	stream := NewStream(manager)
	handler := newRecordingTransformHandler()
	sub, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", EndTimeTick: 20, Handler: handler})
	require.NoError(t, err)
	select {
	case <-handler.done:
	case <-time.After(time.Second):
		t.Fatal("truncated read did not stop")
	}
	require.ErrorIs(t, (<-handler.events).Err, wal.ErrTransformLogStartPointTruncated)
	require.NoError(t, sub.Close())
	tail := newRecordingTransformHandler()
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v2", EndTimeTick: 20, Handler: tail})
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.ErrorIs(t, (<-tail.events).Err, context.Canceled)
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: tail})
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
