package broadcast

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestBroadcast(t *testing.T) {
	s := newMockServer(t, 0)
	bs := NewGRPCBroadcastService(s)
	msg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	_, err := bs.Broadcast(context.Background(), msg)
	assert.NoError(t, err)
	msg1 := msg.WithBroadcastID(1).SplitIntoMutableMessage()
	immutableMsg1 := msg1[0].IntoImmutableMessage(rmq.NewRmqID(1))
	err = bs.Ack(context.Background(), immutableMsg1)
	assert.NoError(t, err)

	// The append gate's wait travels as its own RPC: the broadcast id and the
	// vchannels go out on the request, and the answer is the absence of an error.
	assert.NoError(t, bs.WaitVChannelsAcked(context.Background(), 7, []string{"v1", "v2"}))
}

// TestWaitVChannelsAckedRoundTrip asserts the client puts the caller's arguments
// on the wire unchanged, and hands back the transport's error as it stands --
// the append gate treats every failure here as transient and retries, so a
// rewritten error would be a rewritten retry decision.
func TestWaitVChannelsAckedRoundTrip(t *testing.T) {
	s := mock_lazygrpc.NewMockService[streamingpb.StreamingCoordBroadcastServiceClient](t)
	c := mock_streamingpb.NewMockStreamingCoordBroadcastServiceClient(t)
	s.EXPECT().GetService(mock.Anything).Return(c, nil)

	var got *streamingpb.WaitVChannelsAckedRequest
	c.EXPECT().WaitVChannelsAcked(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *streamingpb.WaitVChannelsAckedRequest, opts ...grpc.CallOption) (*streamingpb.WaitVChannelsAckedResponse, error) {
			got = req
			if len(req.GetVchannels()) == 0 {
				return nil, context.Canceled
			}
			return &streamingpb.WaitVChannelsAckedResponse{}, nil
		})

	bs := NewGRPCBroadcastService(s)
	assert.NoError(t, bs.WaitVChannelsAcked(context.Background(), 11, []string{"v1"}))
	assert.EqualValues(t, 11, got.GetBroadcastId())
	assert.Equal(t, []string{"v1"}, got.GetVchannels())

	assert.ErrorIs(t, bs.WaitVChannelsAcked(context.Background(), 12, nil), context.Canceled)

	// A coord that cannot be reached at all fails the wait the same way, so the
	// gate retries instead of appending out of order.
	unreachable := mock_lazygrpc.NewMockService[streamingpb.StreamingCoordBroadcastServiceClient](t)
	unreachable.EXPECT().GetService(mock.Anything).Return(nil, context.DeadlineExceeded)
	assert.ErrorIs(t,
		NewGRPCBroadcastService(unreachable).WaitVChannelsAcked(context.Background(), 13, []string{"v1"}),
		context.DeadlineExceeded)
}

func newMockServer(t *testing.T, sendDelay time.Duration) lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient] {
	s := mock_lazygrpc.NewMockService[streamingpb.StreamingCoordBroadcastServiceClient](t)
	c := mock_streamingpb.NewMockStreamingCoordBroadcastServiceClient(t)
	s.EXPECT().GetService(mock.Anything).Return(c, nil)
	c.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(&streamingpb.BroadcastResponse{
		Results: map[string]*streamingpb.ProduceMessageResponseResult{
			"v1": {
				Id: walimplstest.NewTestMessageID(1).IntoProto(),
			},
		},
		BroadcastId: 1,
	}, nil).Maybe()
	c.EXPECT().Ack(mock.Anything, mock.Anything).Return(&streamingpb.BroadcastAckResponse{}, nil).Maybe()
	c.EXPECT().WaitVChannelsAcked(mock.Anything, mock.Anything).Return(&streamingpb.WaitVChannelsAckedResponse{}, nil).Maybe()
	return s
}
