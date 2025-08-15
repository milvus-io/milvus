package broadcaster

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type Broadcaster interface {
	// Broadcast broadcasts the message to all channels.
	Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error)

	// LegacyAck is the legacy ack interface for the 2.6.0 import message.
	LegacyAck(ctx context.Context, broadcastID uint64, vchannel string) error

	// Ack acknowledges the message at the specified vchannel.
	Ack(ctx context.Context, msg message.ImmutableMessage) error

	// Close closes the broadcaster.
	Close()
}

// AppendOperator is used to append messages, there's only two implement of this interface:
// 1. streaming.WAL()
// 2. old msgstream interface [deprecated]
type AppendOperator interface {
	AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses
}
