package transformlog

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestResumableStreamResubscribesFromLastDeliveredTimeTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	streams := []*fakeTransformLogStream{newFakeTransformLogStream(), newFakeTransformLogStream()}
	var acquired []string
	stream := NewResumableStream(ctx, "pchannel", func(ctx context.Context, pchannel string) (wal.TransformLogStream, error) {
		mu.Lock()
		defer mu.Unlock()
		acquired = append(acquired, pchannel)
		return streams[len(acquired)-1], nil
	})
	defer stream.Close()

	handler1 := newRecordingHandler()
	sub1 := subscribeStreamForTest(t, ctx, stream, wal.TransformLogSubscriptionOption{
		VChannel:           "pchannel_100v0",
		StartAfterTimeTick: 10,
		Handler:            handler1,
	})
	handler2 := newRecordingHandler()
	sub2 := subscribeStreamForTest(t, ctx, stream, wal.TransformLogSubscriptionOption{
		VChannel:           "pchannel_200v0",
		StartAfterTimeTick: 20,
		Handler:            handler2,
	})

	first := streams[0]
	first.emit(wal.TransformLogStreamEvent{
		SubscriptionID: first.subscriptionID("pchannel_100v0"),
		VChannel:       "pchannel_100v0",
		Entry:          &streamingpb.TransformLogEntry{TimeTick: 11},
	})
	event := recvStreamEvent(t, handler1.events)
	require.Equal(t, sub1.ID(), event.SubscriptionID)
	require.Equal(t, uint64(11), event.Entry.GetTimeTick())

	first.emit(wal.TransformLogStreamEvent{
		SubscriptionID: first.subscriptionID("pchannel_200v0"),
		VChannel:       "pchannel_200v0",
		Entry:          &streamingpb.TransformLogEntry{TimeTick: 21},
	})
	event = recvStreamEvent(t, handler2.events)
	require.Equal(t, sub2.ID(), event.SubscriptionID)
	require.Equal(t, uint64(21), event.Entry.GetTimeTick())

	first.finish(grpcstatus.Error(codes.Unavailable, "stream broken"))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(acquired) == 2
	}, time.Second, 10*time.Millisecond)
	second := streams[1]
	require.Eventually(t, func() bool {
		return second.startAfter("pchannel_100v0") == 11 &&
			second.startAfter("pchannel_200v0") == 21
	}, time.Second, 10*time.Millisecond)

	second.emit(wal.TransformLogStreamEvent{
		SubscriptionID: second.subscriptionID("pchannel_100v0"),
		VChannel:       "pchannel_100v0",
		Entry:          &streamingpb.TransformLogEntry{TimeTick: 12},
	})
	event = recvStreamEvent(t, handler1.events)
	require.Equal(t, sub1.ID(), event.SubscriptionID)
	require.Equal(t, uint64(12), event.Entry.GetTimeTick())
}

func TestResumableStreamKeepsSubscriptionOnSubscribeError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	underlying := newFakeTransformLogStream()
	underlying.subscribeErrByVChan["pchannel_100v0"] = wal.ErrTransformLogStartPointTruncated
	stream := NewResumableStream(ctx, "pchannel", func(ctx context.Context, pchannel string) (wal.TransformLogStream, error) {
		return underlying, nil
	})
	defer stream.Close()

	handler := newRecordingHandler()
	sub := subscribeStreamForTest(t, ctx, stream, wal.TransformLogSubscriptionOption{
		VChannel:           "pchannel_100v0",
		StartAfterTimeTick: 10,
		Handler:            handler,
	})
	event := recvStreamEvent(t, handler.events)
	require.Equal(t, sub.ID(), event.SubscriptionID)
	require.Equal(t, "pchannel_100v0", event.VChannel)
	require.ErrorIs(t, event.Err, wal.ErrTransformLogStartPointTruncated)
}

func subscribeStreamForTest(t *testing.T, ctx context.Context, stream wal.TransformLogStream, opt wal.TransformLogSubscriptionOption) wal.TransformLogSubscription {
	t.Helper()
	sub, err := stream.Subscribe(ctx, opt)
	require.NoError(t, err)
	return sub
}

func recvStreamEvent(t *testing.T, ch <-chan wal.TransformLogStreamEvent) wal.TransformLogStreamEvent {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(time.Second):
		t.Fatal("timeout waiting transform log stream event")
		return wal.TransformLogStreamEvent{}
	}
}

type fakeTransformLogStream struct {
	mu                  sync.Mutex
	nextID              int64
	optsByID            map[int64]wal.TransformLogSubscriptionOption
	idsByVChan          map[string]int64
	subscribeErrByVChan map[string]error
	done                chan struct{}
	err                 error
	finishOnce          sync.Once
}

func newFakeTransformLogStream() *fakeTransformLogStream {
	return &fakeTransformLogStream{
		optsByID:            make(map[int64]wal.TransformLogSubscriptionOption),
		idsByVChan:          make(map[string]int64),
		subscribeErrByVChan: make(map[string]error),
		done:                make(chan struct{}),
	}
}

func (s *fakeTransformLogStream) Subscribe(_ context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.subscribeErrByVChan[opt.VChannel]; err != nil {
		return nil, err
	}
	s.nextID++
	id := s.nextID
	s.optsByID[id] = opt
	s.idsByVChan[opt.VChannel] = id
	return fakeTransformLogSubscription{id: id, vchannel: opt.VChannel}, nil
}

func (s *fakeTransformLogStream) Done() <-chan struct{} {
	return s.done
}

func (s *fakeTransformLogStream) Error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *fakeTransformLogStream) Close() error {
	s.finish(nil)
	return nil
}

func (s *fakeTransformLogStream) emit(event wal.TransformLogStreamEvent) {
	s.mu.Lock()
	opt := s.optsByID[event.SubscriptionID]
	s.mu.Unlock()
	_ = opt.Handler.Handle(event)
}

func (s *fakeTransformLogStream) finish(err error) {
	s.finishOnce.Do(func() {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		close(s.done)
	})
}

func (s *fakeTransformLogStream) subscriptionID(vchannel string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.idsByVChan[vchannel]
}

func (s *fakeTransformLogStream) startAfter(vchannel string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.idsByVChan[vchannel]
	return s.optsByID[id].StartAfterTimeTick
}

type fakeTransformLogSubscription struct {
	id       int64
	vchannel string
}

func (s fakeTransformLogSubscription) ID() int64 {
	return s.id
}

func (s fakeTransformLogSubscription) VChannel() string {
	return s.vchannel
}

func (s fakeTransformLogSubscription) Close() error {
	return nil
}

type recordingHandler struct {
	events chan wal.TransformLogStreamEvent
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{events: make(chan wal.TransformLogStreamEvent, 16)}
}

func (h *recordingHandler) Handle(event wal.TransformLogStreamEvent) error {
	h.events <- event
	return nil
}

func (h *recordingHandler) Close() {}
