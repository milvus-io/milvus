package transformlog

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestSubscribeServerCloseSubscriptionAck(t *testing.T) {
	stream := newFakeSubscribeTransformServer(context.Background())
	scanner := newFakeTransformLogScanner()
	server := &SubscribeServer{
		accesser: fakeTransformLogAccesser{scanner: scanner},
		stream:   stream,
		scanners: make(map[int64]*subscription),
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
	require.True(t, scanner.closed)

	stream.recv(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_CloseStream{
			CloseStream: &streamingpb.CloseTransformStreamRequest{},
		},
	})
	closeStreamResp := stream.sent(t)
	require.NotNil(t, closeStreamResp.GetCloseStream())
	require.NoError(t, <-errCh)
}

type fakeTransformLogAccesser struct {
	scanner transformlogapi.Scanner
}

func (a fakeTransformLogAccesser) Read(context.Context, transformlogapi.ReadOption) transformlogapi.Scanner {
	return a.scanner
}

type fakeTransformLogScanner struct {
	ch     chan transformlogapi.Event
	done   chan struct{}
	closed bool
	once   sync.Once
}

func newFakeTransformLogScanner() *fakeTransformLogScanner {
	return &fakeTransformLogScanner{
		ch:   make(chan transformlogapi.Event),
		done: make(chan struct{}),
	}
}

func (s *fakeTransformLogScanner) Name() string {
	return "fake"
}

func (s *fakeTransformLogScanner) Chan() <-chan transformlogapi.Event {
	return s.ch
}

func (s *fakeTransformLogScanner) Error() error {
	return nil
}

func (s *fakeTransformLogScanner) Done() <-chan struct{} {
	return s.done
}

func (s *fakeTransformLogScanner) Close() error {
	s.once.Do(func() {
		s.closed = true
		close(s.done)
	})
	return nil
}

type fakeSubscribeTransformServer struct {
	ctx    context.Context
	recvCh chan *streamingpb.TransformRequest
	sendCh chan *streamingpb.TransformResponse
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
