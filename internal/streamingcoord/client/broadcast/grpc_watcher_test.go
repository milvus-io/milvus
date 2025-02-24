package broadcast

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestWatcher(t *testing.T) {
	s := newMockServer(t, 0)

	b := grpcWatcherBuilder{broadcastService: s}
	w, err := b.Build(context.Background())
	assert.NoError(t, err)

	done := make(chan struct{})
	cnt := 0
	go func() {
		defer close(done)
		for range w.EventChan() {
			cnt++
		}
	}()
	for i := 0; i < 10; i++ {
		err = w.ObserveResourceKeyEvent(context.Background(), message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c1")))
		assert.NoError(t, err)
	}
	time.Sleep(10 * time.Millisecond)
	w.Close()
	<-done
	assert.Equal(t, 10, cnt)
	err = w.ObserveResourceKeyEvent(context.Background(), message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c1")))
	assert.Error(t, err)

	// Test ungraceful close
	s = newMockServer(t, 10*time.Second)
	b2 := grpcWatcherBuilder{broadcastService: s}
	w2, err := b2.Build(context.Background())
	assert.NoError(t, err)
	w2.Close()
}

func newMockServer(t *testing.T, sendDelay time.Duration) lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient] {
	s := mock_lazygrpc.NewMockService[streamingpb.StreamingCoordBroadcastServiceClient](t)
	c := mock_streamingpb.NewMockStreamingCoordBroadcastServiceClient(t)
	s.EXPECT().GetService(mock.Anything).Return(c, nil)
	var ctx context.Context
	cc := mock_streamingpb.NewMockStreamingCoordBroadcastService_WatchClient(t)
	c.EXPECT().Watch(mock.Anything).RunAndReturn(func(ctx2 context.Context, co ...grpc.CallOption) (streamingpb.StreamingCoordBroadcastService_WatchClient, error) {
		ctx = ctx2
		return cc, nil
	})
	c.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(&streamingpb.BroadcastResponse{
		Results: map[string]*streamingpb.ProduceMessageResponseResult{
			"v1": {
				Id: &messagespb.MessageID{
					Id: walimplstest.NewTestMessageID(1).Marshal(),
				},
			},
		},
		BroadcastId: 1,
	}, nil).Maybe()
	c.EXPECT().Ack(mock.Anything, mock.Anything).Return(&streamingpb.BroadcastAckResponse{}, nil).Maybe()

	output := make(chan *streamingpb.BroadcastWatchRequest, 10)
	cc.EXPECT().Recv().RunAndReturn(func() (*streamingpb.BroadcastWatchResponse, error) {
		var result *streamingpb.BroadcastWatchRequest
		var ok bool
		select {
		case result, ok = <-output:
			if !ok {
				return nil, io.EOF
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		switch cmd := result.Command.(type) {
		case *streamingpb.BroadcastWatchRequest_Close:
			return &streamingpb.BroadcastWatchResponse{
				Response: &streamingpb.BroadcastWatchResponse_Close{Close: &streamingpb.CloseBroadcastWatchResponse{}},
			}, nil
		case *streamingpb.BroadcastWatchRequest_CreateEventWatch:
			return &streamingpb.BroadcastWatchResponse{
				Response: &streamingpb.BroadcastWatchResponse_EventDone{
					EventDone: &streamingpb.BroadcastEventWatchResponse{
						Event: cmd.CreateEventWatch.Event,
					},
				},
			}, nil
		default:
			panic("unknown command")
		}
	})
	cc.EXPECT().Send(mock.Anything).RunAndReturn(func(bwr *streamingpb.BroadcastWatchRequest) error {
		select {
		case <-time.After(sendDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case output <- bwr:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	cc.EXPECT().CloseSend().RunAndReturn(func() error {
		close(output)
		return nil
	})
	return s
}
