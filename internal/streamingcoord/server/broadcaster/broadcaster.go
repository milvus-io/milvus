package broadcaster

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
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

type AppendOperator = registry.AppendOperator
