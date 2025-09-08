package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// BroadcastService is the interface of the broadcast service.
type BroadcastService interface {
	streamingpb.StreamingCoordBroadcastServiceServer
}

// NewBroadcastService creates a new broadcast service.
func NewBroadcastService(bc *syncutil.Future[broadcaster.Broadcaster]) BroadcastService {
	return &broadcastServceImpl{
		broadcaster: bc,
		walName:     util.MustSelectWALName(),
	}
}

// broadcastServiceeeeImpl is the implementation of the broadcast service.
type broadcastServceImpl struct {
	broadcaster *syncutil.Future[broadcaster.Broadcaster]
	walName     string
}

// Broadcast broadcasts the message to all channels.
func (s *broadcastServceImpl) Broadcast(ctx context.Context, req *streamingpb.BroadcastRequest) (*streamingpb.BroadcastResponse, error) {
	broadcaster, err := s.broadcaster.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	results, err := broadcaster.Broadcast(ctx, message.NewBroadcastMutableMessageBeforeAppend(req.Message.Payload, req.Message.Properties))
	if err != nil {
		return nil, err
	}
	protoResult := make(map[string]*streamingpb.ProduceMessageResponseResult, len(results.AppendResults))
	for vchannel, result := range results.AppendResults {
		protoResult[vchannel] = result.IntoProto()
	}
	return &streamingpb.BroadcastResponse{
		BroadcastId: results.BroadcastID,
		Results:     protoResult,
	}, nil
}

// Ack acknowledges the message at the specified vchannel.
func (s *broadcastServceImpl) Ack(ctx context.Context, req *streamingpb.BroadcastAckRequest) (*streamingpb.BroadcastAckResponse, error) {
	broadcaster, err := s.broadcaster.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.Message == nil {
		// before 2.6.1, the request don't have the message field, only have the broadcast id and vchannel.
		// so we need to use the legacy ack interface.
		if err := broadcaster.LegacyAck(ctx, req.BroadcastId, req.Vchannel); err != nil {
			return nil, err
		}
		return &streamingpb.BroadcastAckResponse{}, nil
	}
	if err := broadcaster.Ack(ctx, message.NewImmutableMessageFromProto(s.walName, req.Message)); err != nil {
		return nil, err
	}
	return &streamingpb.BroadcastAckResponse{}, nil
}
