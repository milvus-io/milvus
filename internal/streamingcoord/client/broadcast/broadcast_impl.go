package broadcast

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
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

// WaitVChannelsAcked blocks until every named vchannel of the broadcast has been
// acked at the streaming coord, or ctx ends.
//
// The RPC error is returned as it stands, never reclassified: everything that
// can fail here -- the context ending, the coord being unreachable or shutting
// down -- is transient, and the replicate stream that calls this retries.
func (c *GRPCBroadcastServiceImpl) WaitVChannelsAcked(ctx context.Context, broadcastID uint64, vchannels []string) error {
	client, err := c.service.GetService(ctx)
	if err != nil {
		return err
	}
	_, err = client.WaitVChannelsAcked(ctx, &streamingpb.WaitVChannelsAckedRequest{
		BroadcastId: broadcastID,
		Vchannels:   vchannels,
	})
	return err
}
