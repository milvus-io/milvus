package transformlog

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
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
	syncUp1 := recvStreamEvent(t, handler1.events)
	require.NotNil(t, syncUp1.SyncUp)
	assert.Equal(t, uint64(20), syncUp1.SyncUp.TimeTick)

	assert.Equal(t, uint64(20), recvStreamEvent(t, handler2.events).Entry.GetTimeTick())
	syncUp2 := recvStreamEvent(t, handler2.events)
	require.NotNil(t, syncUp2.SyncUp)
	assert.Equal(t, uint64(20), syncUp2.SyncUp.TimeTick)

	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 30), appendOption{}).Appended)
	assert.Equal(t, uint64(30), recvStreamEvent(t, handler1.events).Entry.GetTimeTick())
	assert.Equal(t, uint64(30), recvStreamEvent(t, handler2.events).Entry.GetTimeTick())
}

func TestTransformLogStreamManagerBoundedReplayEmitsSyncUpAndCloses(t *testing.T) {
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
	syncUp := recvStreamEvent(t, handler.events)
	require.NotNil(t, syncUp.SyncUp)
	assert.Equal(t, uint64(20), syncUp.SyncUp.TimeTick)
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

func TestTransformLogStreamManagerCatchupDrainsDeletesAppendedAfterSubscribe(t *testing.T) {
	ctx := context.Background()
	store := newBlockingReadStore()
	require.NoError(t, store.WriteTransformLogChunk(ctx, "v1", &streamingpb.TransformLogChunk{
		ChunkId: 0,
		Entries: []*streamingpb.TransformLogEntry{
			testTransformLogDeleteEntry(10, 1),
		},
	}))
	transformLog := New(Config{
		VChannel: "v1",
		Store:    store,
		Meta: &streamingpb.VChannelTransformLogMeta{
			CheckpointTimeTick: 10,
			NextChunkId:        1,
		},
	})
	manager := NewStreamManager("pchannel")
	manager.Register("v1", transformLog)

	stream, err := manager.AcquireStream(ctx, "pchannel")
	require.NoError(t, err)
	defer stream.Close()

	handler := newRecordingStreamHandler()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            handler,
	})
	require.NoError(t, err)
	defer sub.Close()

	store.waitReadStarted(t)
	require.True(t, transformLog.append(newTransformLogTestDeleteMessage(t, 20), appendOption{}).Appended)
	store.release()

	first := recvStreamEvent(t, handler.events)
	require.NotNil(t, first.Entry)
	assert.Equal(t, uint64(10), first.Entry.GetTimeTick())
	second := recvStreamEvent(t, handler.events)
	require.NotNil(t, second.Entry)
	assert.Equal(t, uint64(20), second.Entry.GetTimeTick())
	syncUp := recvStreamEvent(t, handler.events)
	require.NotNil(t, syncUp.SyncUp)
	assert.Equal(t, uint64(20), syncUp.SyncUp.TimeTick)
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

type blockingReadStore struct {
	*memoryStore
	readStarted chan struct{}
	releaseRead chan struct{}
	once        sync.Once
}

func newBlockingReadStore() *blockingReadStore {
	return &blockingReadStore{
		memoryStore: newMemoryStore(),
		readStarted: make(chan struct{}),
		releaseRead: make(chan struct{}),
	}
}

func (s *blockingReadStore) ReadTransformLogChunk(ctx context.Context, vchannel string, chunkID uint64) (*streamingpb.TransformLogChunk, error) {
	s.once.Do(func() {
		close(s.readStarted)
	})
	select {
	case <-s.releaseRead:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.memoryStore.ReadTransformLogChunk(ctx, vchannel, chunkID)
}

func (s *blockingReadStore) waitReadStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.readStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting transform log chunk read")
	}
}

func (s *blockingReadStore) release() {
	close(s.releaseRead)
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
