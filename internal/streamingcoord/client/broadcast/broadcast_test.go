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
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
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

func TestWaitVChannelsAcked(t *testing.T) {
	s := mock_lazygrpc.NewMockService[streamingpb.StreamingCoordBroadcastServiceClient](t)
	c := mock_streamingpb.NewMockStreamingCoordBroadcastServiceClient(t)
	s.EXPECT().GetService(mock.Anything).Return(c, nil).Twice()
	bs := NewGRPCBroadcastService(s)

	c.EXPECT().WaitVChannelsAcked(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *streamingpb.WaitVChannelsAckedRequest, _ ...grpc.CallOption) (*streamingpb.WaitVChannelsAckedResponse, error) {
			assert.Equal(t, uint64(7), req.GetBroadcastId())
			assert.Equal(t, []string{"v1"}, req.GetVchannels())
			assert.Equal(t, messagespb.MessageType_SplitShard, req.GetMessageType())
			return &streamingpb.WaitVChannelsAckedResponse{TimeTick: 99}, nil
		}).Once()
	tick, err := bs.WaitVChannelsAcked(context.Background(), message.MessageTypeSplitShard, 7, []string{"v1"})
	assert.NoError(t, err)
	assert.Equal(t, uint64(99), tick)

	// A failure of the call is returned as it is.
	c.EXPECT().WaitVChannelsAcked(mock.Anything, mock.Anything).Return(nil, context.Canceled).Once()
	_, err = bs.WaitVChannelsAcked(context.Background(), message.MessageTypeSplitShard, 7, []string{"v1"})
	assert.ErrorIs(t, err, context.Canceled)

	// So is a failure to reach the coordinator.
	s.EXPECT().GetService(mock.Anything).Unset()
	s.EXPECT().GetService(mock.Anything).Return(nil, context.DeadlineExceeded).Once()
	_, err = bs.WaitVChannelsAcked(context.Background(), message.MessageTypeSplitShard, 7, []string{"v1"})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestReportVChannelAppended(t *testing.T) {
	s := mock_lazygrpc.NewMockService[streamingpb.StreamingCoordBroadcastServiceClient](t)
	c := mock_streamingpb.NewMockStreamingCoordBroadcastServiceClient(t)
	s.EXPECT().GetService(mock.Anything).Return(c, nil).Twice()
	bs := NewGRPCBroadcastService(s)

	c.EXPECT().ReportVChannelAppended(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *streamingpb.ReportVChannelAppendedRequest, _ ...grpc.CallOption) (*streamingpb.ReportVChannelAppendedResponse, error) {
			assert.Equal(t, uint64(7), req.GetBroadcastId())
			assert.Equal(t, "v1", req.GetVchannel())
			assert.Equal(t, uint64(99), req.GetTimeTick())
			return &streamingpb.ReportVChannelAppendedResponse{}, nil
		}).Once()
	assert.NoError(t, bs.ReportVChannelAppended(context.Background(), 7, "v1", 99))

	c.EXPECT().ReportVChannelAppended(mock.Anything, mock.Anything).Return(nil, context.Canceled).Once()
	assert.ErrorIs(t, bs.ReportVChannelAppended(context.Background(), 7, "v1", 99), context.Canceled)

	s.EXPECT().GetService(mock.Anything).Unset()
	s.EXPECT().GetService(mock.Anything).Return(nil, context.DeadlineExceeded).Once()
	assert.ErrorIs(t, bs.ReportVChannelAppended(context.Background(), 7, "v1", 99), context.DeadlineExceeded)
}
