package transformlog

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestEventStreamPublishesSubscriptionEventsOnSharedOutput(t *testing.T) {
	ctx := context.Background()
	fakeStream := newFakeSubscribeTransformClient(ctx)
	handlerClient := streamingpb.NewStreamingNodeHandlerServiceClient(nil)
	patch := mockey.Mock(mockey.GetMethod(handlerClient, "SubscribeTransform")).Return(fakeStream, nil).Build()
	defer patch.UnPatch()

	stream, err := CreateEventStream(ctx, &EventStreamOptions{Assignment: testAssignment()}, handlerClient)
	require.NoError(t, err)
	defer stream.Close()

	handler1 := newRecordingHandler()
	sub1 := subscribeEventForTest(t, ctx, stream, "v1", 10, handler1, fakeStream)
	handler2 := newRecordingHandler()
	sub2 := subscribeEventForTest(t, ctx, stream, "v2", 20, handler2, fakeStream)

	fakeStream.recv(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_MessageBatch{
			MessageBatch: &streamingpb.TransformMessageBatch{
				SubscriptionId: sub2.ID(),
				Vchannel:       "v2",
				Entries:        []*streamingpb.TransformLogEntry{{TimeTick: 21}},
			},
		},
	})
	event := recvEvent(t, handler2.events)
	require.Equal(t, sub2.ID(), event.SubscriptionID)
	require.Equal(t, "v2", event.VChannel)
	require.Equal(t, uint64(21), event.Entry.GetTimeTick())

	fakeStream.recv(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_SyncUp{
			SyncUp: &streamingpb.TransformSubscriptionSyncUp{
				SubscriptionId: sub1.ID(),
				Vchannel:       "v1",
				TimeTick:       30,
			},
		},
	})
	event = recvEvent(t, handler1.events)
	require.Equal(t, sub1.ID(), event.SubscriptionID)
	require.Equal(t, "v1", event.VChannel)
	require.Equal(t, uint64(30), event.SyncUp.TimeTick)
}

func TestEventStreamReportsStreamErrorOnStreamOnly(t *testing.T) {
	ctx := context.Background()
	fakeStream := newFakeSubscribeTransformClient(ctx)
	handlerClient := streamingpb.NewStreamingNodeHandlerServiceClient(nil)
	patch := mockey.Mock(mockey.GetMethod(handlerClient, "SubscribeTransform")).Return(fakeStream, nil).Build()
	defer patch.UnPatch()

	stream, err := CreateEventStream(ctx, &EventStreamOptions{Assignment: testAssignment()}, handlerClient)
	require.NoError(t, err)
	defer stream.Close()

	handler := newRecordingHandler()
	sub := subscribeEventForTest(t, ctx, stream, "v1", 10, handler, fakeStream)
	streamErr := errors.New("stream broken")
	fakeStream.fail(streamErr)

	requireNoEvent(t, handler.events)
	require.Eventually(t, func() bool {
		select {
		case <-stream.Done():
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	require.ErrorIs(t, stream.Error(), streamErr)
	require.ErrorIs(t, sub.Close(), streamErr)
}

func TestEventStreamSubscriptionErrorEndsSharedStream(t *testing.T) {
	ctx := context.Background()
	fakeStream := newFakeSubscribeTransformClient(ctx)
	handlerClient := streamingpb.NewStreamingNodeHandlerServiceClient(nil)
	patch := mockey.Mock(mockey.GetMethod(handlerClient, "SubscribeTransform")).Return(fakeStream, nil).Build()
	defer patch.UnPatch()

	stream, err := CreateEventStream(ctx, &EventStreamOptions{Assignment: testAssignment()}, handlerClient)
	require.NoError(t, err)
	defer stream.Close()

	handler := newRecordingHandler()
	sub := subscribeEventForTest(t, ctx, stream, "v1", 10, handler, fakeStream)
	siblingHandler := newRecordingHandler()
	sibling := subscribeEventForTest(t, ctx, stream, "v2", 20, siblingHandler, fakeStream)
	subErr := streamingstatus.NewIgnoreOperation("subscription failed")
	fakeStream.recv(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_SubscriptionError{
			SubscriptionError: &streamingpb.TransformSubscriptionError{
				SubscriptionId: sub.ID(),
				Vchannel:       "v1",
				Error:          subErr.AsPBError(),
			},
		},
	})

	recvEvent(t, stream.Done())
	require.True(t, streamingstatus.AsStreamingError(stream.Error()).IsIgnoredOperation())
	require.Error(t, sub.Close())
	require.Error(t, sibling.Close())
	require.Empty(t, handler.events)
	require.Empty(t, siblingHandler.events)
	_ = fakeStream.CloseSend()
}

func subscribeEventForTest(
	t *testing.T,
	ctx context.Context,
	stream *EventStream,
	vchannel string,
	startAfter uint64,
	handler wal.TransformLogEventHandler,
	fakeStream *fakeSubscribeTransformClient,
) wal.TransformLogSubscription {
	t.Helper()
	resultCh := make(chan struct {
		sub wal.TransformLogSubscription
		err error
	}, 1)
	go func() {
		sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
			VChannel:           vchannel,
			StartAfterTimeTick: startAfter,
			Handler:            handler,
		})
		resultCh <- struct {
			sub wal.TransformLogSubscription
			err error
		}{sub: sub, err: err}
	}()
	req := fakeStream.sent(t)
	create := req.GetCreate()
	require.NotNil(t, create)
	require.Equal(t, vchannel, create.GetVchannel())
	require.Equal(t, startAfter, create.GetStartAfterTimeTick())
	fakeStream.recv(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_Create{
			Create: &streamingpb.CreateTransformSubscriptionResponse{
				SubscriptionId:     create.GetSubscriptionId(),
				Vchannel:           vchannel,
				StartAfterTimeTick: startAfter,
			},
		},
	})
	result := <-resultCh
	require.NoError(t, result.err)
	return result.sub
}

type recordingHandler struct {
	events chan wal.TransformLogStreamEvent
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{events: make(chan wal.TransformLogStreamEvent, 16)}
}

func (h *recordingHandler) Handle(event wal.TransformLogStreamEvent) {
	h.events <- event
}

func (h *recordingHandler) Close() {}

func recvEvent[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(time.Second):
		t.Fatal("timeout waiting event")
		var zero T
		return zero
	}
}

func requireNoEvent[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case event := <-ch:
		t.Fatalf("unexpected event: %+v", event)
	case <-time.After(20 * time.Millisecond):
	}
}

func testAssignment() *types.PChannelInfoAssigned {
	return &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}
}

type fakeSubscribeTransformClient struct {
	ctx     context.Context
	sendCh  chan *streamingpb.TransformRequest
	recvCh  chan *streamingpb.TransformResponse
	errCh   chan error
	closeCh chan struct{}
	once    sync.Once
}

func newFakeSubscribeTransformClient(ctx context.Context) *fakeSubscribeTransformClient {
	f := &fakeSubscribeTransformClient{
		ctx:     ctx,
		sendCh:  make(chan *streamingpb.TransformRequest, 16),
		recvCh:  make(chan *streamingpb.TransformResponse, 16),
		errCh:   make(chan error, 1),
		closeCh: make(chan struct{}),
	}
	f.recv(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CreateStream{CreateStream: &streamingpb.CreateTransformStreamResponse{}}})
	return f
}

func (f *fakeSubscribeTransformClient) sent(t *testing.T) *streamingpb.TransformRequest {
	t.Helper()
	select {
	case req := <-f.sendCh:
		return req
	case <-time.After(time.Second):
		t.Fatal("timeout waiting sent request")
		return nil
	}
}

func (f *fakeSubscribeTransformClient) recv(resp *streamingpb.TransformResponse) {
	f.recvCh <- resp
}

func (f *fakeSubscribeTransformClient) fail(err error) {
	f.errCh <- err
}

func (f *fakeSubscribeTransformClient) Send(req *streamingpb.TransformRequest) error {
	select {
	case f.sendCh <- req:
		return nil
	case <-f.closeCh:
		return io.EOF
	}
}

func (f *fakeSubscribeTransformClient) Recv() (*streamingpb.TransformResponse, error) {
	select {
	case resp := <-f.recvCh:
		return resp, nil
	case err := <-f.errCh:
		return nil, err
	case <-f.closeCh:
		return nil, io.EOF
	}
}

func (f *fakeSubscribeTransformClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (f *fakeSubscribeTransformClient) Trailer() metadata.MD {
	return nil
}

func (f *fakeSubscribeTransformClient) CloseSend() error {
	f.once.Do(func() {
		close(f.closeCh)
	})
	return nil
}

func (f *fakeSubscribeTransformClient) Context() context.Context {
	return f.ctx
}

func (f *fakeSubscribeTransformClient) SendMsg(m interface{}) error {
	return nil
}

func (f *fakeSubscribeTransformClient) RecvMsg(m interface{}) error {
	return nil
}

var (
	_ streamingpb.StreamingNodeHandlerService_SubscribeTransformClient = (*fakeSubscribeTransformClient)(nil)
	_ grpc.ClientStream                                                = (*fakeSubscribeTransformClient)(nil)
)

func TestEventStreamCancelPendingSubscription(t *testing.T) {
	transport := newFakeSubscribeTransformClient(t.Context())
	client := streamingpb.NewStreamingNodeHandlerServiceClient(nil)
	patch := mockey.Mock(mockey.GetMethod(client, "SubscribeTransform")).Return(transport, nil).Build()
	defer patch.UnPatch()
	stream, err := CreateEventStream(t.Context(), &EventStreamOptions{Assignment: testAssignment()}, client)
	require.NoError(t, err)
	defer stream.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: newRecordingHandler()})
		done <- err
	}()
	create := transport.sent(t).GetCreate()
	require.NotNil(t, create)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, create.GetSubscriptionId(), transport.sent(t).GetCloseSubscription().GetSubscriptionId())
}

func TestEventStreamCreationAndSendFailures(t *testing.T) {
	client := streamingpb.NewStreamingNodeHandlerServiceClient(nil)
	failure := errors.New("transport unavailable")
	patch := mockey.Mock(mockey.GetMethod(client, "SubscribeTransform")).Return(nil, failure).Build()
	_, err := CreateEventStream(t.Context(), &EventStreamOptions{Assignment: testAssignment()}, client)
	require.ErrorIs(t, err, failure)
	patch.UnPatch()
	transport := newFakeSubscribeTransformClient(t.Context())
	patch = mockey.Mock(mockey.GetMethod(client, "SubscribeTransform")).Return(transport, nil).Build()
	defer patch.UnPatch()
	stream, err := CreateEventStream(t.Context(), &EventStreamOptions{Assignment: testAssignment()}, client)
	require.NoError(t, err)
	send := mockey.Mock((*fakeSubscribeTransformClient).Send).Return(failure).Build()
	defer send.UnPatch()
	defer transport.CloseSend()
	_, err = stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: newRecordingHandler()})
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, stream.Close(), failure)
}

func TestEventStreamClassifiesClosing(t *testing.T) {
	ctx := t.Context()
	fake := newFakeSubscribeTransformClient(ctx)
	client := streamingpb.NewStreamingNodeHandlerServiceClient(nil)
	patch := mockey.Mock(mockey.GetMethod(client, "SubscribeTransform")).Return(fake, nil).Build()
	defer patch.UnPatch()
	stream, err := CreateEventStream(ctx, &EventStreamOptions{Assignment: testAssignment()}, client)
	require.NoError(t, err)
	defer stream.Close()
	stream.markClosing()
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v3", Handler: newRecordingHandler()})
	require.True(t, streamingstatus.AsStreamingError(err).IsOnShutdown())
}

func TestEventSubscriptionAbortBeforeClose(t *testing.T) {
	ctx := t.Context()
	fake := newFakeSubscribeTransformClient(ctx)
	client := streamingpb.NewStreamingNodeHandlerServiceClient(nil)
	patch := mockey.Mock(mockey.GetMethod(client, "SubscribeTransform")).Return(fake, nil).Build()
	defer patch.UnPatch()
	stream, err := CreateEventStream(ctx, &EventStreamOptions{Assignment: testAssignment()}, client)
	require.NoError(t, err)
	defer stream.Close()
	handler := newRecordingHandler()
	entered, abort := make(chan struct{}), make(chan struct{})
	var abortOnce sync.Once
	unblock := func() { abortOnce.Do(func() { close(abort) }) }
	defer unblock()
	handlePatch := mockey.Mock((*recordingHandler).Handle).To(func(_ *recordingHandler, _ wal.TransformLogStreamEvent) {
		close(entered)
		<-abort
	}).Build()
	defer handlePatch.UnPatch()
	sub := subscribeEventForTest(t, ctx, stream, "v1", 0, handler, fake)
	fake.recv(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_SyncUp{SyncUp: &streamingpb.TransformSubscriptionSyncUp{SubscriptionId: sub.ID(), TimeTick: 10}}})
	recvEvent(t, entered)
	closed := make(chan error, 1)
	go func() { closed <- sub.Close() }()
	require.Equal(t, sub.ID(), fake.sent(t).GetCloseSubscription().GetSubscriptionId())
	fake.recv(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CloseSubscription{CloseSubscription: &streamingpb.CloseTransformSubscriptionResponse{SubscriptionId: sub.ID()}}})
	// The ack is queued behind Handle. An independent abort releases delivery.
	unblock()
	require.NoError(t, recvEvent(t, closed))
}
