package transformlog

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

func TestTransformLogStreamManagerCatchupThenDispatch(t *testing.T) {
	ctx := context.Background()
	transformLog := New(Config{VChannel: "v1"})
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 10), appendOption{}).Appended)
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 20), appendOption{}).Appended)
	manager := NewStreamManager("pchannel")
	manager.Register("v1", transformLog)

	stream, err := manager.AcquireStream(ctx, "pchannel")
	require.NoError(t, err)
	defer stream.Close()

	handler1 := newRecordingStreamHandler()
	sub1, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            handler1,
	})
	require.NoError(t, err)
	defer sub1.Close()

	handler2 := newRecordingStreamHandler()
	sub2, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 10,
		Handler:            handler2,
	})
	require.NoError(t, err)
	defer sub2.Close()

	assert.Equal(t, uint64(10), recvStreamEvent(t, handler1.events).Entry.GetTimeTick())
	assert.Equal(t, uint64(20), recvStreamEvent(t, handler1.events).Entry.GetTimeTick())
	require.NotNil(t, recvStreamEvent(t, handler1.events).CaughtUp)

	assert.Equal(t, uint64(20), recvStreamEvent(t, handler2.events).Entry.GetTimeTick())
	require.NotNil(t, recvStreamEvent(t, handler2.events).CaughtUp)

	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 30), appendOption{}).Appended)
	assert.Equal(t, uint64(30), recvStreamEvent(t, handler1.events).Entry.GetTimeTick())
	assert.Equal(t, uint64(30), recvStreamEvent(t, handler2.events).Entry.GetTimeTick())
}

func TestTransformLogStreamManagerBoundedReplayEmitsCaughtUpAndCloses(t *testing.T) {
	ctx := context.Background()
	transformLog := New(Config{VChannel: "v1"})
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 10), appendOption{}).Appended)
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 20), appendOption{}).Appended)
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 30), appendOption{}).Appended)
	manager := NewStreamManager("pchannel")
	manager.Register("v1", transformLog)

	stream, err := manager.AcquireStream(ctx, "pchannel")
	require.NoError(t, err)
	defer stream.Close()

	handler := newRecordingStreamHandler()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		EndTimeTick:        20,
		Handler:            handler,
	})
	require.NoError(t, err)

	assert.Equal(t, uint64(10), recvStreamEvent(t, handler.events).Entry.GetTimeTick())
	assert.Equal(t, uint64(20), recvStreamEvent(t, handler.events).Entry.GetTimeTick())
	require.NotNil(t, recvStreamEvent(t, handler.events).CaughtUp)
	require.Eventually(t, func() bool {
		select {
		case <-handler.closed:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, sub.Close())
	requireNoStreamEvent(t, handler.events)
}

func TestTransformLogStreamManagerRemovesRegisteredLog(t *testing.T) {
	ctx := context.Background()
	transformLog := New(Config{VChannel: "v1"})
	manager := NewStreamManager("pchannel")
	manager.Register("v1", transformLog)
	manager.Remove("v1")

	stream, err := manager.AcquireStream(ctx, "pchannel")
	require.NoError(t, err)
	defer stream.Close()

	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            newRecordingStreamHandler(),
	})
	require.Error(t, err)
}

type recordingStreamHandler struct {
	events chan wal.TransformLogStreamEvent
	closed chan struct{}
	once   sync.Once
}

func newRecordingStreamHandler() *recordingStreamHandler {
	return &recordingStreamHandler{
		events: make(chan wal.TransformLogStreamEvent, 16),
		closed: make(chan struct{}),
	}
}

func (h *recordingStreamHandler) Handle(event wal.TransformLogStreamEvent) error {
	h.events <- event
	return nil
}

func (h *recordingStreamHandler) Close() {
	h.once.Do(func() {
		close(h.closed)
	})
}

func requireNoStreamEvent(t *testing.T, ch <-chan wal.TransformLogStreamEvent) {
	t.Helper()
	select {
	case event := <-ch:
		t.Fatalf("unexpected stream event: %+v", event)
	case <-time.After(20 * time.Millisecond):
	}
}

func recvStreamEvent(t *testing.T, ch <-chan wal.TransformLogStreamEvent) wal.TransformLogStreamEvent {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(time.Second):
		t.Fatal("timeout waiting stream event")
		return wal.TransformLogStreamEvent{}
	}
}
