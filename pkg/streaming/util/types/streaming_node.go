package types

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	ErrStopping = errors.New("streaming node is stopping")
	ErrNotAlive = errors.New("streaming node is not alive")
)

// AssignmentDiscoverWatcher is the interface for watching the assignment discovery.
type AssignmentDiscoverWatcher interface {
	// AssignmentDiscover watches the assignment discovery.
	// The callback will be called when the discovery is changed.
	// The final error will be returned when the watcher is closed or broken.
	AssignmentDiscover(ctx context.Context, cb func(*VersionedStreamingNodeAssignments) error) error
}

// VersionedStreamingNodeAssignments is the relation between server and channels with version.
type VersionedStreamingNodeAssignments struct {
	Version     typeutil.VersionInt64Pair
	Assignments map[int64]StreamingNodeAssignment
}

// StreamingNodeAssignment is the relation between server and channels.
type StreamingNodeAssignment struct {
	NodeInfo StreamingNodeInfo
	Channels map[string]PChannelInfo
}

// StreamingNodeInfo is the relation between server and channels.
type StreamingNodeInfo struct {
	ServerID int64
	Address  string
}

// StreamingNodeStatus is the information of a streaming node.
type StreamingNodeStatus struct {
	StreamingNodeInfo
	// TODO: balance attributes should added here in future.
	Err error
}

// IsHealthy returns whether the streaming node is healthy.
func (n *StreamingNodeStatus) IsHealthy() bool {
	return n.Err == nil
}

// ErrorOfNode returns the error of the streaming node.
func (n *StreamingNodeStatus) ErrorOfNode() error {
	if n == nil {
		return ErrNotAlive
	}
	return n.Err
}
