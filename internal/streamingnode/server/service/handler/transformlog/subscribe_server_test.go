package transformlog

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestSubscribeServerCloseSubscriptionAck(t *testing.T) {
	stream := newFakeSubscribeTransformServer(context.Background())
	logStream := newFakeTransformLogStream()
	server := &SubscribeServer{
		logStream: logStream,
		stream:    stream,
		subs:      make(map[int64]wal.TransformLogSubscription),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Execute()
	}()

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_Create{
			Create: &streamingpb.CreateTransformSubscriptionRequest{
				SubscriptionId:     10,
				Vchannel:           "v1",
				StartAfterTimeTick: 100,
			},
		},
	})
	createResp := stream.sent(t)
	require.Equal(t, int64(10), createResp.GetCreate().GetSubscriptionId())

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_CloseSubscription{
			CloseSubscription: &streamingpb.CloseTransformSubscriptionRequest{SubscriptionId: 10},
		},
	})
	closeResp := stream.sent(t)
	require.Equal(t, int64(10), closeResp.GetCloseSubscription().GetSubscriptionId())
	require.Equal(t, "v1", closeResp.GetCloseSubscription().GetVchannel())
	require.True(t, logStream.subscription(10).closed)

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_CloseStream{
			CloseStream: &streamingpb.CloseTransformStreamRequest{},
		},
	})
	closeStreamResp := stream.sent(t)
	require.NotNil(t, closeStreamResp.GetCloseStream())
	require.NoError(t, <-errCh)
}

func TestSubscribeServerClosesLogStreamOnCreateSendError(t *testing.T) {
	stream := newFakeSubscribeTransformServer(context.Background())
	stream.sendErr = io.ErrClosedPipe
	logStream := newFakeTransformLogStream()
	server := &SubscribeServer{
		logStream: logStream,
		stream:    stream,
		subs:      make(map[int64]wal.TransformLogSubscription),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Execute()
	}()

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_Create{
			Create: &streamingpb.CreateTransformSubscriptionRequest{
				SubscriptionId:     10,
				Vchannel:           "v1",
				StartAfterTimeTick: 100,
			},
		},
	})

	require.ErrorIs(t, <-errCh, io.ErrClosedPipe)
	require.True(t, logStream.closed)
	require.True(t, logStream.subscription(10).closed)
}

type fakeTransformLogStream struct {
	mu     sync.Mutex
	subs   map[int64]*fakeTransformLogSubscription
	done   chan struct{}
	closed bool
}

func newFakeTransformLogStream() *fakeTransformLogStream {
	return &fakeTransformLogStream{
		subs: make(map[int64]*fakeTransformLogSubscription),
		done: make(chan struct{}),
	}
}

func (s *fakeTransformLogStream) Subscribe(_ context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	sub := &fakeTransformLogSubscription{
		id:       opt.SubscriptionID,
		vchannel: opt.VChannel,
	}
	s.mu.Lock()
	s.subs[sub.id] = sub
	s.mu.Unlock()
	return sub, nil
}

func (s *fakeTransformLogStream) Done() <-chan struct{} {
	return s.done
}

func (s *fakeTransformLogStream) Error() error {
	return nil
}

func (s *fakeTransformLogStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeTransformLogStream) subscription(id int64) *fakeTransformLogSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subs[id]
}

type fakeTransformLogSubscription struct {
	id       int64
	vchannel string
	closed   bool
}

func (s *fakeTransformLogSubscription) ID() int64 {
	return s.id
}

func (s *fakeTransformLogSubscription) VChannel() string {
	return s.vchannel
}

func (s *fakeTransformLogSubscription) Close() error {
	s.closed = true
	return nil
}

type fakeSubscribeTransformServer struct {
	ctx     context.Context
	recvCh  chan *streamingpb.TransformRequest
	sendCh  chan *streamingpb.TransformResponse
	sendErr error
}

func newFakeSubscribeTransformServer(ctx context.Context) *fakeSubscribeTransformServer {
	return &fakeSubscribeTransformServer{
		ctx:    ctx,
		recvCh: make(chan *streamingpb.TransformRequest, 16),
		sendCh: make(chan *streamingpb.TransformResponse, 16),
	}
}

func (s *fakeSubscribeTransformServer) recv(req *streamingpb.TransformRequest) {
	s.recvCh <- req
}

func (s *fakeSubscribeTransformServer) sent(t *testing.T) *streamingpb.TransformResponse {
	t.Helper()
	select {
	case resp := <-s.sendCh:
		return resp
	case <-time.After(time.Second):
		t.Fatal("timeout waiting response")
		return nil
	}
}

func (s *fakeSubscribeTransformServer) Send(resp *streamingpb.TransformResponse) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sendCh <- resp
	return nil
}

func (s *fakeSubscribeTransformServer) Recv() (*streamingpb.TransformRequest, error) {
	req, ok := <-s.recvCh
	if !ok {
		return nil, io.EOF
	}
	return req, nil
}

func (s *fakeSubscribeTransformServer) SetHeader(metadata.MD) error {
	return nil
}

func (s *fakeSubscribeTransformServer) SendHeader(metadata.MD) error {
	return nil
}

func (s *fakeSubscribeTransformServer) SetTrailer(metadata.MD) {
}

func (s *fakeSubscribeTransformServer) Context() context.Context {
	return s.ctx
}

func (s *fakeSubscribeTransformServer) SendMsg(m interface{}) error {
	return nil
}

func (s *fakeSubscribeTransformServer) RecvMsg(m interface{}) error {
	return nil
}

var _ streamingpb.StreamingNodeHandlerService_SubscribeTransformServer = (*fakeSubscribeTransformServer)(nil)
