package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// BroadcastService is the interface of the broadcast service.
type BroadcastService interface {
	streamingpb.StreamingCoordBroadcastServiceServer
}

// NewBroadcastService creates a new broadcast service.
func NewBroadcastService() BroadcastService {
	return &broadcastServceImpl{}
}

// broadcastServiceeeeImpl is the implementation of the broadcast service.
type broadcastServceImpl struct{}

// Broadcast broadcasts the message to all channels.
func (s *broadcastServceImpl) Broadcast(ctx context.Context, req *streamingpb.BroadcastRequest) (*streamingpb.BroadcastResponse, error) {
	msg := message.NewBroadcastMutableMessageBeforeAppend(req.Message.Payload, req.Message.Properties)
	api, err := broadcast.StartBroadcastWithResourceKeys(ctx, msg.BroadcastHeader().ResourceKeys.Collect()...)
	if err != nil {
		return nil, err
	}
	results, err := api.Broadcast(ctx, msg)
	if err != nil {
		return nil, err
	}
	protoResult := make(map[string]*streamingpb.ProduceMessageResponseResult, len(results.AppendResults))
	for vchannel, result := range results.AppendResults {
		protoResult[vchannel] = &streamingpb.ProduceMessageResponseResult{
			Id:              result.MessageID.IntoProto(),
			Timetick:        result.TimeTick,
			LastConfirmedId: result.LastConfirmedMessageID.IntoProto(),
		}
	}
	return &streamingpb.BroadcastResponse{
		BroadcastId: results.BroadcastID,
		Results:     protoResult,
	}, nil
}

// Ack acknowledges the message at the specified vchannel.
func (s *broadcastServceImpl) Ack(ctx context.Context, req *streamingpb.BroadcastAckRequest) (*streamingpb.BroadcastAckResponse, error) {
	broadcaster, err := broadcast.GetWithContext(ctx)
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
	if err := broadcaster.Ack(ctx, message.NewImmutableMesasge(
		message.MustUnmarshalMessageID(req.Message.Id),
		req.Message.Payload,
		req.Message.Properties,
	)); err != nil {
		return nil, err
	}
	return &streamingpb.BroadcastAckResponse{}, nil
}
