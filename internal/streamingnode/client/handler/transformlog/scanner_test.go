package transformlog

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestStreamMultiplexesSubscriptionsAndCloseAck(t *testing.T) {
	ctx := context.Background()
	fakeStream := newFakeSubscribeTransformClient(ctx)
	handlerClient := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	handlerClient.EXPECT().SubscribeTransform(mock.Anything, mock.Anything).Return(fakeStream, nil).Once()

	stream, err := CreateStream(ctx, &StreamOptions{Assignment: testAssignment()}, handlerClient)
	require.NoError(t, err)
	defer stream.Close()

	sub1 := subscribeForTest(t, ctx, stream, "v1", 10, fakeStream)
	sub2 := subscribeForTest(t, ctx, stream, "v2", 20, fakeStream)

	fakeStream.recv(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_MessageBatch{
			MessageBatch: &streamingpb.TransformMessageBatch{
				SubscriptionId: 2,
				Vchannel:       "v2",
				Entries:        []*streamingpb.TransformLogEntry{{TimeTick: 21}},
			},
		},
	})
	require.Equal(t, uint64(21), recvEvent(t, sub2.Chan()).Entry.GetTimeTick())

	fakeStream.recv(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_MessageBatch{
			MessageBatch: &streamingpb.TransformMessageBatch{
				SubscriptionId: 1,
				Vchannel:       "v1",
				Entries:        []*streamingpb.TransformLogEntry{{TimeTick: 11}},
			},
		},
	})
	require.Equal(t, uint64(11), recvEvent(t, sub1.Chan()).Entry.GetTimeTick())

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- sub1.Close()
	}()
	closeReq := fakeStream.sent(t)
	require.Equal(t, int64(1), closeReq.GetCloseSubscription().GetSubscriptionId())
	fakeStream.recv(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_CloseSubscription{
			CloseSubscription: &streamingpb.CloseTransformSubscriptionResponse{
				SubscriptionId: 1,
				Vchannel:       "v1",
			},
		},
	})
	require.NoError(t, <-closeDone)

	fakeStream.recv(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_CaughtUp{
			CaughtUp: &streamingpb.TransformSubscriptionCaughtUp{
				SubscriptionId:     2,
				Vchannel:           "v2",
				StartAfterTimeTick: 20,
			},
		},
	})
	require.Equal(t, uint64(20), recvEvent(t, sub2.Chan()).CaughtUp.StartAfterTimeTick)

	closeDone = make(chan error, 1)
	go func() {
		closeDone <- sub2.Close()
	}()
	closeReq = fakeStream.sent(t)
	require.Equal(t, int64(2), closeReq.GetCloseSubscription().GetSubscriptionId())
	fakeStream.recv(&streamingpb.TransformResponse{
		Response: &streamingpb.TransformResponse_CloseSubscription{
			CloseSubscription: &streamingpb.CloseTransformSubscriptionResponse{
				SubscriptionId: 2,
				Vchannel:       "v2",
			},
		},
	})
	require.NoError(t, <-closeDone)
	closeStreamReq := fakeStream.sent(t)
	require.NotNil(t, closeStreamReq.GetCloseStream())
	require.Eventually(t, func() bool {
		select {
		case <-stream.Done():
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestStreamRejectsSubscribeAfterClosing(t *testing.T) {
	ctx := context.Background()
	fakeStream := newFakeSubscribeTransformClient(ctx)
	handlerClient := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	handlerClient.EXPECT().SubscribeTransform(mock.Anything, mock.Anything).Return(fakeStream, nil).Once()

	stream, err := CreateStream(ctx, &StreamOptions{Assignment: testAssignment()}, handlerClient)
	require.NoError(t, err)
	defer stream.Close()

	stream.markClosing()
	scanner, err := stream.Subscribe(ctx, transformReadOption("v1", 10))
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, scanner)
	requireNoSentRequest(t, fakeStream)
}

func subscribeForTest(
	t *testing.T,
	ctx context.Context,
	stream *Stream,
	vchannel string,
	startAfter uint64,
	fakeStream *fakeSubscribeTransformClient,
) *remoteSubscription {
	t.Helper()
	resultCh := make(chan struct {
		scanner *remoteSubscription
		err     error
	}, 1)
	go func() {
		scanner, err := stream.Subscribe(ctx, transformReadOption(vchannel, startAfter))
		result := struct {
			scanner *remoteSubscription
			err     error
		}{err: err}
		if scanner != nil {
			result.scanner = scanner.(*remoteSubscription)
		}
		resultCh <- result
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
	return result.scanner
}

func transformReadOption(vchannel string, startAfter uint64) transformlogapi.ReadOption {
	return transformlogapi.ReadOption{
		Name:               vchannel,
		VChannel:           vchannel,
		StartAfterTimeTick: startAfter,
	}
}

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

func requireNoSentRequest(t *testing.T, f *fakeSubscribeTransformClient) {
	t.Helper()
	select {
	case req := <-f.sendCh:
		t.Fatalf("unexpected sent request: %v", req)
	case <-time.After(50 * time.Millisecond):
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
	closeCh chan struct{}
	once    sync.Once
}

func newFakeSubscribeTransformClient(ctx context.Context) *fakeSubscribeTransformClient {
	return &fakeSubscribeTransformClient{
		ctx:     ctx,
		sendCh:  make(chan *streamingpb.TransformRequest, 16),
		recvCh:  make(chan *streamingpb.TransformResponse, 16),
		closeCh: make(chan struct{}),
	}
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
