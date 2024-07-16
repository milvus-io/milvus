package manager

import (
	"context"

	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

type ManagerClient interface {
	// WatchNodeChanged returns a channel that receive a node change.
	WatchNodeChanged(ctx context.Context) (<-chan struct{}, error)

	// CollectStatus collects status of all wal instances in all streamingnode.
	CollectAllStatus(ctx context.Context) (map[int64]*types.StreamingNodeStatus, error)

	// Assign a wal instance for the channel on log node of given server id.
	Assign(ctx context.Context, pchannel types.PChannelInfoAssigned) error

	// Remove the wal instance for the channel on log node of given server id.
	Remove(ctx context.Context, pchannel types.PChannelInfoAssigned) error

	// Close closes the manager client.
	Close()
}
