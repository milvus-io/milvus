package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// BroadcastService is the interface of the broadcast service.
type BroadcastService interface {
	streamingpb.StreamingCoordBroadcastServiceServer
}

// NewBroadcastService creates a new broadcast service.
func NewBroadcastService(bc *syncutil.Future[broadcaster.Broadcaster]) BroadcastService {
	return &broadcastServceImpl{
		broadcaster: bc,
	}
}

// broadcastServiceeeeImpl is the implementation of the broadcast service.
type broadcastServceImpl struct {
	broadcaster *syncutil.Future[broadcaster.Broadcaster]
}

// Broadcast broadcasts the message to all channels.
func (s *broadcastServceImpl) Broadcast(ctx context.Context, req *streamingpb.BroadcastRequest) (*streamingpb.BroadcastResponse, error) {
	broadcaster, err := s.broadcaster.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	results, err := broadcaster.Broadcast(ctx, message.NewBroadcastMutableMessage(req.Message.Payload, req.Message.Properties))
	if err != nil {
		return nil, err
	}
	protoResult := make(map[string]*streamingpb.ProduceMessageResponseResult, len(results.AppendResults))
	for vchannel, result := range results.AppendResults {
		protoResult[vchannel] = result.IntoProto()
	}
	return &streamingpb.BroadcastResponse{Results: protoResult}, nil
}
