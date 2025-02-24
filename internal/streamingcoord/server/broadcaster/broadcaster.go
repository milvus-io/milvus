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

	// Watch watches the broadcast event.
	NewWatcher() (Watcher, error)

	// Close closes the broadcaster.
	Close()
}

// Watcher is the interface for watching the broadcast event.
type Watcher interface {
	// ObserveResourceKeyEvent observes the resource key event.
	ObserveResourceKeyEvent(ctx context.Context, ev *message.BroadcastEvent) error

	// EventChan returns the event channel.
	EventChan() <-chan *message.BroadcastEvent

	// Close closes the watcher.
	Close()
}

type AppendOperator = registry.AppendOperator
