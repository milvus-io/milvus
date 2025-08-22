package types

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	ErrStopping = errors.New("streaming node is stopping")
	ErrNotAlive = errors.New("streaming node is not alive")
	ErrFrozen   = errors.New("streaming node is frozen")
)

// AssignmentDiscoverWatcher is the interface for watching the assignment discovery.
type AssignmentDiscoverWatcher interface {
	// AssignmentDiscover watches the assignment discovery.
	// The callback will be called when the discovery is changed.
	// The final error will be returned when the watcher is closed or broken.
	AssignmentDiscover(ctx context.Context, cb func(*VersionedStreamingNodeAssignments) error) error

	AssignmentRebalanceTrigger
}

// AssignmentRebalanceTrigger is the interface for triggering the re-balance of the pchannel.
type AssignmentRebalanceTrigger interface {
	// ReportStreamingError is used to report the streaming error.
	// Trigger a re-balance of the pchannel.
	ReportAssignmentError(ctx context.Context, pchannel PChannelInfo, err error) error
}

// VersionedStreamingNodeAssignments is the relation between server and channels with version.
type VersionedStreamingNodeAssignments struct {
	Version                typeutil.VersionInt64Pair
	Assignments            map[int64]StreamingNodeAssignment
	CChannel               *streamingpb.CChannelAssignment
	ReplicateConfiguration *milvuspb.ReplicateConfiguration
}

// PChannelOfCChannel returns the pchannel of the cchannel.
func (v *VersionedStreamingNodeAssignments) PChannelOfCChannel() string {
	return v.CChannel.Meta.Pchannel
}

// RoutePChannelByReplicate returns the pchannel of the cchannel.
func (v *VersionedStreamingNodeAssignments) RoutePChannelByReplicate(sourcePChannel string, currentClusterID string) string {
	var (
		sourceCluster  *milvuspb.MilvusCluster
		currentCluster *milvuspb.MilvusCluster
		channelIndex   = -1
	)
	for _, cluster := range v.ReplicateConfiguration.GetClusters() {
		if cluster.GetClusterId() == currentClusterID {
			currentCluster = cluster
		}
		for i, pchannel := range cluster.GetPchannels() {
			if pchannel == sourcePChannel {
				sourceCluster = cluster
				channelIndex = i
				break
			}
		}
	}
	if currentCluster == nil {
		panic(fmt.Sprintf("current cluster %s not found in replicate configuration", currentClusterID))
	}
	if channelIndex == -1 {
		panic(fmt.Sprintf("source pchannel %s not found in replicate configuration", sourcePChannel))
	}
	if len(currentCluster.GetPchannels()) != len(sourceCluster.GetPchannels()) {
		panic(fmt.Sprintf("pchannels count not match, current: %v, source: %v", currentCluster.GetPchannels(), sourceCluster.GetPchannels()))
	}
	return currentCluster.GetPchannels()[channelIndex]
}

// StreamingNodeAssignment is the relation between server and channels.
type StreamingNodeAssignment struct {
	NodeInfo StreamingNodeInfo
	Channels map[string]PChannelInfo
}

// NewStreamingNodeInfoFromProto creates a StreamingNodeInfo from proto.
func NewStreamingNodeInfoFromProto(proto *streamingpb.StreamingNodeInfo) StreamingNodeInfo {
	return StreamingNodeInfo{
		ServerID: proto.ServerId,
		Address:  proto.Address,
	}
}

// NewProtoFromStreamingNodeInfo creates a proto from StreamingNodeInfo.
func NewProtoFromStreamingNodeInfo(info StreamingNodeInfo) *streamingpb.StreamingNodeInfo {
	return &streamingpb.StreamingNodeInfo{
		ServerId: info.ServerID,
		Address:  info.Address,
	}
}

// StreamingNodeInfo is the relation between server and channels.
type StreamingNodeInfo struct {
	ServerID int64
	Address  string
}

// String returns the string representation of the streaming node info.
func (n StreamingNodeInfo) String() string {
	return fmt.Sprintf("%d@%s", n.ServerID, n.Address)
}

// StreamingNodeStatus is the information of a streaming node.
type StreamingNodeStatus struct {
	StreamingNodeInfo
	Metrics StreamingNodeMetrics
	Err     error
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
