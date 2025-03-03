package broadcast

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestBroadcast(t *testing.T) {
	s := newMockServer(t, 0)
	bs := NewGRPCBroadcastService(walimplstest.WALName, s)
	msg, _ := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"v1"}, message.NewCollectionNameResourceKey("r1")).
		BuildBroadcast()
	_, err := bs.Broadcast(context.Background(), msg)
	assert.NoError(t, err)
	err = bs.Ack(context.Background(), types.BroadcastAckRequest{
		VChannel:    "v1",
		BroadcastID: 1,
	})
	assert.NoError(t, err)
	bs.Close()
}

func newMockServer(t *testing.T, sendDelay time.Duration) lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient] {
	s := mock_lazygrpc.NewMockService[streamingpb.StreamingCoordBroadcastServiceClient](t)
	c := mock_streamingpb.NewMockStreamingCoordBroadcastServiceClient(t)
	s.EXPECT().GetService(mock.Anything).Return(c, nil)
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
	return s
}
