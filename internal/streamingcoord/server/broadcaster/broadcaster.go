package broadcaster

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

type Broadcaster interface {
	// Broadcast broadcasts the message to all channels.
	Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error)

	// Close closes the broadcaster.
	Close()
}

// AppendOperator is used to append messages, there's only two implement of this interface:
// 1. streaming.WAL()
// 2. old msgstream interface
type AppendOperator interface {
	AppendMessages(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses
}
