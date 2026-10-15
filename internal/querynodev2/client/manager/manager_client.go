package manager

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ManagerClient provides service discovery and gRPC connections to QueryNodes.
// It wraps etcd session service discovery, following the same pattern as
// StreamingNode's ManagerClient.
//
// Note: this branch only carries the interface contract. The implementation
// (etcd session discovery + lazy gRPC connection) lives on the qv feature
// branch and is out of scope for the coordinator-side balancer split.
type ManagerClient interface {
	// RegisterNodeChangedNotifier registers a callback for QueryNode membership changes.
	// The notifier must be non-blocking.
	RegisterNodeChangedNotifier(notifier func())

	// GetAllQueryNodes fetches all discovered QueryNode info.
	// The result is fetched from service discovery, so there's no RPC call.
	GetAllQueryNodes(ctx context.Context) (map[int64]*NodeInfo, error)

	// CreateViewSyncClient returns a ViewSyncServiceClient routed to the given QueryNode.
	CreateViewSyncClient(ctx context.Context, queryNodeID int64) (viewpb.ViewSyncServiceClient, error)

	// Close closes the manager client and releases resources.
	Close()
}

// NodeInfo is the basic QueryNode identity discovered from session service discovery.
type NodeInfo struct {
	ServerID     int64
	Address      string
	Stopping     bool
	ServerLabels map[string]string
}
