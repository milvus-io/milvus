package broadcast

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// NewGRPCBroadcastService creates a new broadcast service with grpc.
func NewGRPCBroadcastService(service lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient]) *GRPCBroadcastServiceImpl {
	return &GRPCBroadcastServiceImpl{
		service: service,
	}
}

// GRPCBroadcastServiceImpl is the implementation of BroadcastService based on grpc service.
// If the streaming coord is not deployed at current node, these implementation will be used.
type GRPCBroadcastServiceImpl struct {
	service lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient]
}

func (c *GRPCBroadcastServiceImpl) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	client, err := c.service.GetService(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.Broadcast(ctx, &streamingpb.BroadcastRequest{
		Message: msg.IntoMessageProto(),
	})
	if err != nil {
		return nil, err
	}
	results := make(map[string]*types.AppendResult, len(resp.Results))
	for channel, result := range resp.Results {
		msgID, err := message.UnmarshalMessageID(result.Id)
		if err != nil {
			return nil, err
		}
		results[channel] = &types.AppendResult{
			MessageID: msgID,
			TimeTick:  result.GetTimetick(),
			TxnCtx:    message.NewTxnContextFromProto(result.GetTxnContext()),
			Extra:     result.GetExtra(),
		}
	}
	return &types.BroadcastAppendResult{
		BroadcastID:   resp.BroadcastId,
		AppendResults: results,
	}, nil
}

func (c *GRPCBroadcastServiceImpl) Ack(ctx context.Context, msg message.ImmutableMessage) error {
	client, err := c.service.GetService(ctx)
	if err != nil {
		return err
	}
	_, err = client.Ack(ctx, &streamingpb.BroadcastAckRequest{
		BroadcastId: msg.BroadcastHeader().BroadcastID,
		Vchannel:    msg.VChannel(),
		Message:     msg.IntoImmutableMessageProto(),
	})
	return err
}

// WaitVChannelsAcked blocks until every given vchannel of a broadcast is
// acknowledged at the streaming coord, and returns the greatest time tick they
// were acknowledged at. An error is returned as it is: the caller calls again.
func (c *GRPCBroadcastServiceImpl) WaitVChannelsAcked(ctx context.Context, typ message.MessageType, broadcastID uint64, vchannels []string) (uint64, error) {
	client, err := c.service.GetService(ctx)
	if err != nil {
		return 0, err
	}
	resp, err := client.WaitVChannelsAcked(ctx, &streamingpb.WaitVChannelsAckedRequest{
		BroadcastId: broadcastID,
		Vchannels:   vchannels,
		MessageType: messagespb.MessageType(typ),
	})
	if err != nil {
		return 0, err
	}
	return resp.GetTimeTick(), nil
}

// ReportVChannelAppended tells the streaming coord that the copy of a
// broadcast on vchannel was appended to the WAL of this cluster at timeTick.
func (c *GRPCBroadcastServiceImpl) ReportVChannelAppended(ctx context.Context, broadcastID uint64, vchannel string, timeTick uint64) error {
	client, err := c.service.GetService(ctx)
	if err != nil {
		return err
	}
	_, err = client.ReportVChannelAppended(ctx, &streamingpb.ReportVChannelAppendedRequest{
		BroadcastId: broadcastID,
		Vchannel:    vchannel,
		TimeTick:    timeTick,
	})
	return err
}
