package types

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	ErrStopping     = errors.New("streaming node is stopping")
	ErrNotAlive     = errors.New("streaming node is not alive")
	ErrFrozen       = errors.New("streaming node is frozen")
	ErrFileResource = errors.New("streaming node is not sync the file resource successfully")
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
	StreamingVersion      *streamingpb.StreamingVersion
	Version               typeutil.VersionInt64Pair
	Assignments           map[int64]StreamingNodeAssignment
	CChannel              *streamingpb.CChannelAssignment
	ReplicateConfigHelper *replicateutil.ConfigHelper
}

// PChannelOfCChannel returns the pchannel of the cchannel.
func (v *VersionedStreamingNodeAssignments) PChannelOfCChannel() string {
	return v.CChannel.Meta.Pchannel
}

// StreamingNodeAssignment is the relation between server and channels.
type StreamingNodeAssignment struct {
	NodeInfo          StreamingNodeInfo
	Channels          map[string]PChannelInfo
	SecondaryChannels map[string]PChannelInfo
	ShardAssignment   ShardAssignmentInfo
}

type ShardAssignmentInfo struct {
	PChannelAssignments []PChannelShardAssignment
}

type PChannelShardAssignment struct {
	PChannel string
	Entries  []ShardAssignmentEntry
}

type ShardAssignmentEntry struct {
	CollectionID int64
	ShardIndex   int32
	ReplicaID    int64
}

// NewShardAssignmentInfoFromProto creates a ShardAssignmentInfo from proto.
func NewShardAssignmentInfoFromProto(proto *streamingpb.ShardAssignmentInfo) ShardAssignmentInfo {
	if proto == nil {
		return ShardAssignmentInfo{}
	}
	pchannelAssignments := make([]PChannelShardAssignment, 0, len(proto.PchannelAssignments))
	for _, pchannelAssignment := range proto.PchannelAssignments {
		entries := make([]ShardAssignmentEntry, 0, len(pchannelAssignment.Entries))
		for _, entry := range pchannelAssignment.Entries {
			entries = append(entries, ShardAssignmentEntry{
				CollectionID: entry.GetCollectionId(),
				ShardIndex:   entry.GetShardIndex(),
				ReplicaID:    entry.GetReplicaId(),
			})
		}
		pchannelAssignments = append(pchannelAssignments, PChannelShardAssignment{
			PChannel: pchannelAssignment.GetPchannel(),
			Entries:  entries,
		})
	}
	return ShardAssignmentInfo{PChannelAssignments: pchannelAssignments}
}

// NewProtoFromShardAssignmentInfo creates a proto from ShardAssignmentInfo.
func NewProtoFromShardAssignmentInfo(info ShardAssignmentInfo) *streamingpb.ShardAssignmentInfo {
	pchannelAssignments := make([]*streamingpb.PChannelShardAssignment, 0, len(info.PChannelAssignments))
	for _, pchannelAssignment := range info.PChannelAssignments {
		entries := make([]*streamingpb.ShardAssignmentEntry, 0, len(pchannelAssignment.Entries))
		for _, entry := range pchannelAssignment.Entries {
			entries = append(entries, &streamingpb.ShardAssignmentEntry{
				CollectionId: entry.CollectionID,
				ShardIndex:   entry.ShardIndex,
				ReplicaId:    entry.ReplicaID,
			})
		}
		pchannelAssignments = append(pchannelAssignments, &streamingpb.PChannelShardAssignment{
			Pchannel: pchannelAssignment.PChannel,
			Entries:  entries,
		})
	}
	return &streamingpb.ShardAssignmentInfo{PchannelAssignments: pchannelAssignments}
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

// StreamingNodeInfoWithResourceGroup extends StreamingNodeInfo with resource group information.
type StreamingNodeInfoWithResourceGroup struct {
	StreamingNodeInfo
	ResourceGroup string // Resource group label from session's ServerLabels, if empty, it means the streaming node doesn't have a resource group.
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
