package broadcast

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var logger = log.With(log.FieldComponent("broadcast-client"))

// NewGRPCBroadcastService creates a new broadcast service with grpc.
func NewGRPCBroadcastService(walName string, service lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient]) *GRPCBroadcastServiceImpl {
	rw := newResumingWatcher(&grpcWatcherBuilder{
		broadcastService: service,
	}, &typeutil.BackoffTimerConfig{
		Default: 5 * time.Second,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: 50 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     5 * time.Second,
		},
	})
	return &GRPCBroadcastServiceImpl{
		walName: walName,
		service: service,
		w:       rw,
	}
}

// GRPCBroadcastServiceImpl is the implementation of BroadcastService based on grpc service.
// If the streaming coord is not deployed at current node, these implementation will be used.
type GRPCBroadcastServiceImpl struct {
	walName string
	service lazygrpc.Service[streamingpb.StreamingCoordBroadcastServiceClient]
	w       *resumingWatcher
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

func (c *GRPCBroadcastServiceImpl) BlockUntilEvent(ctx context.Context, ev *message.BroadcastEvent) error {
	return c.w.ObserveResourceKeyEvent(ctx, ev)
}

func (c *GRPCBroadcastServiceImpl) Close() {
	c.w.Close()
}
