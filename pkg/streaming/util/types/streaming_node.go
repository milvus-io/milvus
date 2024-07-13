package types

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	ErrStopping = errors.New("streaming node is stopping")
	ErrNotAlive = errors.New("streaming node is not alive")
)

// VersionedStreamingNodeAssignments is the relation between server and channels with version.
type VersionedStreamingNodeAssignments struct {
	Version     typeutil.VersionInt64Pair
	Assignments map[int64]StreamingNodeAssignment
}

// StreamingNodeAssignment is the relation between server and channels.
type StreamingNodeAssignment struct {
	NodeInfo StreamingNodeInfo
	Channels []PChannelInfo
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
