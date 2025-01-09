package broadcast

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

// NewBroadcastService creates a new broadcast service.
func NewBroadcastService(walName string, service lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient]) *BroadcastServiceImpl {
	return &BroadcastServiceImpl{
		walName: walName,
		service: service,
	}
}

// BroadcastServiceImpl is the implementation of BroadcastService.
type BroadcastServiceImpl struct {
	walName string
	service lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient]
}

// Broadcast sends a broadcast message to the streaming coord to perform a broadcast.
func (c *BroadcastServiceImpl) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
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
	return &types.BroadcastAppendResult{AppendResults: results}, nil
}
