package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// ReplicaWatchHandler consumes an owned full snapshot. Physical streams invoke it from
// their delivery loop: it must not block or synchronously close the stream or
// subscription. The resuming client isolates application callbacks separately.
type ReplicaWatchHandler func(qviews.ReplicaSnapshot)

// ReplicaWatchStream is one owner-bound attempt. It never reconnects itself.
// Handlers must not block or synchronously close this stream/subscription.
// Local WAL and RPC adapters implement this same domain interface.
type ReplicaWatchStream interface {
	Subscribe(context.Context, string, ReplicaWatchHandler) (ReplicaWatchSubscription, error)
	Done() <-chan struct{}
	Error() error
	Close() error
}

type ReplicaWatchSubscription interface {
	Snapshot() qviews.ReplicaSnapshot
	Close() error
}

// ReplicaWatchProvider exposes direct local subscription, without wire-protocol emulation.
type ReplicaWatchProvider interface {
	AcquireReplicaWatchStream(context.Context) (ReplicaWatchStream, error)
}
