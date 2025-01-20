package broadcast

import (
	"context"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

type WatcherBuilder interface {
	Build(ctx context.Context) (Watcher, error)
}

type Watcher interface {
	// ObserveResourceKeyEvent observes the resource key event.
	ObserveResourceKeyEvent(ctx context.Context, ev *message.BroadcastEvent) error

	// EventChan returns the event channel.
	EventChan() <-chan *message.BroadcastEvent

	// Close closes the watcher.
	Close()
}
