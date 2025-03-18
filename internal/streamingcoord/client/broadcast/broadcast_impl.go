package broadcast

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// NewGRPCBroadcastService creates a new broadcast service with grpc.
func NewGRPCBroadcastService(walName string, service lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient]) *GRPCBroadcastServiceImpl {
	return &GRPCBroadcastServiceImpl{
		walName: walName,
		service: service,
	}
}

// GRPCBroadcastServiceImpl is the implementation of BroadcastService based on grpc service.
// If the streaming coord is not deployed at current node, these implementation will be used.
type GRPCBroadcastServiceImpl struct {
	walName string
	service lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient]
}

func (c *GRPCBroadcastServiceImpl) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	client, err := c.service.GetService(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.Broadcast(ctx, &streamingpb.BroadcastRequest{
		Message: &messagespb.Message{
			Payload:    msg.Payload(),
			Properties: msg.Properties().ToRawMap(),
		},
	})
	if err != nil {
		return nil, err
	}
	results := make(map[string]*types.AppendResult, len(resp.Results))
	for channel, result := range resp.Results {
		msgID, err := message.UnmarshalMessageID(c.walName, result.Id.Id)
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

func (c *GRPCBroadcastServiceImpl) Ack(ctx context.Context, req types.BroadcastAckRequest) error {
	client, err := c.service.GetService(ctx)
	if err != nil {
		return err
	}
	_, err = client.Ack(ctx, &streamingpb.BroadcastAckRequest{
		BroadcastId: req.BroadcastID,
		Vchannel:    req.VChannel,
	})
	return err
}

func (c *GRPCBroadcastServiceImpl) Close() {
}
