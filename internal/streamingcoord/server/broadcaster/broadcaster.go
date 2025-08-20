package broadcaster

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type Broadcaster interface {
	// WithResourceKeys sets the resource keys of the broadcast operation.
	// It will acquire locks of the resource keys and return the broadcast api.
	// Once the broadcast api is returned, the Close() method of the broadcast api should be called to release the resource safely.
	WithResourceKeys(ctx context.Context, resourceKeys ...message.ResourceKey) (BroadcastAPI, error)

	// LegacyAck is the legacy ack interface for the 2.6.0 import message.
	LegacyAck(ctx context.Context, broadcastID uint64, vchannel string) error

	// Ack acknowledges the message at the specified vchannel.
	Ack(ctx context.Context, msg message.ImmutableMessage) error

	// Close closes the broadcaster.
	Close()
}

type BroadcastAPI interface {
	// Broadcast broadcasts the message to all channels.
	Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error)

	// Close releases the resource keys that broadcast api holds.
	Close()
}

// AppendOperator is used to append messages, there's only two implement of this interface:
// 1. streaming.WAL()
// 2. old msgstream interface [deprecated]
type AppendOperator interface {
	AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses
}
