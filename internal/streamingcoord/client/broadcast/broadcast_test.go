package broadcast

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

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
	err = bs.Ack(context.Background(), immutableMsg1, nil)
	assert.NoError(t, err)
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
	return s
}

func TestBroadcastAckSendsExtra(t *testing.T) {
	s := mock_lazygrpc.NewMockService[streamingpb.StreamingCoordBroadcastServiceClient](t)
	c := mock_streamingpb.NewMockStreamingCoordBroadcastServiceClient(t)
	s.EXPECT().GetService(mock.Anything).Return(c, nil)
	extra, err := anypb.New(&streamingpb.BroadcastAckResponse{})
	assert.NoError(t, err)
	c.EXPECT().Ack(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *streamingpb.BroadcastAckRequest, _ ...grpc.CallOption) (*streamingpb.BroadcastAckResponse, error) {
			assert.True(t, proto.Equal(extra, req.GetExtra()))
			return &streamingpb.BroadcastAckResponse{}, nil
		})
	bs := NewGRPCBroadcastService(s)
	msg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast().
		WithBroadcastID(1).
		SplitIntoMutableMessage()[0].
		IntoImmutableMessage(rmq.NewRmqID(1))
	assert.NoError(t, bs.Ack(context.Background(), msg, extra))
}
