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
	NodeInfo    StreamingNodeInfo
	Channels    map[string]PChannelInfo
	WALReplicas map[ChannelID]WALReplicaInfo
}

// WALReplicaInfo is the service-discovery projection of one WAL replica.
type WALReplicaInfo struct {
	ChannelID         ChannelID
	AccessMode        AccessMode
	ResourceGroup     string
	PChannelWriteTerm int64
	AssignmentEpoch   int64
	State             streamingpb.PChannelMetaState
}

// WALReplicaInfoAssigned binds a WAL replica to its serviceable StreamingNode.
type WALReplicaInfoAssigned struct {
	Replica WALReplicaInfo
	Node    StreamingNodeInfo
}

// NewWALReplicaInfoFromProto creates a WALReplicaInfo from proto.
func NewWALReplicaInfoFromProto(replica *streamingpb.WALReplicaInfo) WALReplicaInfo {
	if replica == nil {
		return WALReplicaInfo{}
	}
	return WALReplicaInfo{
		ChannelID: ChannelID{
			Name:         replica.GetPchannel(),
			WALReplicaID: replica.GetWalReplicaId(),
		},
		AccessMode:        AccessMode(replica.GetAccessMode()),
		ResourceGroup:     replica.GetResourceGroup(),
		PChannelWriteTerm: replica.GetPchannelWriteTerm(),
		AssignmentEpoch:   replica.GetAssignmentEpoch(),
		State:             replica.GetState(),
	}
}

// NewProtoFromWALReplicaInfo creates a proto from WALReplicaInfo.
func NewProtoFromWALReplicaInfo(info WALReplicaInfo) *streamingpb.WALReplicaInfo {
	return &streamingpb.WALReplicaInfo{
		Pchannel:          info.ChannelID.Name,
		WalReplicaId:      info.ChannelID.WALReplicaID,
		AccessMode:        streamingpb.PChannelAccessMode(info.AccessMode),
		ResourceGroup:     info.ResourceGroup,
		PchannelWriteTerm: info.PChannelWriteTerm,
		AssignmentEpoch:   info.AssignmentEpoch,
		State:             info.State,
	}
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
