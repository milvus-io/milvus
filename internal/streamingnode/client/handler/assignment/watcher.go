package assignment

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

var _ Watcher = (*watcherImpl)(nil)

// Watcher is the interface for the channel assignment.
type Watcher interface {
	// Get gets the channel assignment.
	Get(ctx context.Context, channel string) *types.PChannelInfoAssigned

	// Watch watches the channel assignment.
	// Block until new term is coming.
	Watch(ctx context.Context, channel string, previous *types.PChannelInfoAssigned) error

	// Close stop the watcher.
	Close()
}
