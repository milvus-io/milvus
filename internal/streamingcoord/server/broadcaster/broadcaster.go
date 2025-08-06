package broadcaster

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type Broadcaster interface {
	// Broadcast broadcasts the message to all channels.
	Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error)

	// Ack acknowledges the message at the specified vchannel.
	Ack(ctx context.Context, req types.BroadcastAckRequest) error

	// Close closes the broadcaster.
	Close()
}

// AppendOperator is used to append messages, there's only two implement of this interface:
// 1. streaming.WAL()
// 2. old msgstream interface [deprecated]
type AppendOperator interface {
	AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses
}
