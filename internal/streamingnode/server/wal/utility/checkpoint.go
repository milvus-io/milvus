package utility

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const (
	RecoveryMagicStreamingInitialized int64 = 1 // the vchannel info is set into the catalog.
	RecoveryMagicRecoveryStorageV2    int64 = 2 // recovery metadata uses one published global checkpoint.
)

// NewWALCheckpointFromProto creates a new WALCheckpoint from a protobuf message.
func NewWALCheckpointFromProto(cp *streamingpb.WALCheckpoint) *WALCheckpoint {
	if cp == nil {
		return nil
	}
	return &WALCheckpoint{
		MessageID: message.MustUnmarshalMessageID(cp.MessageId),
		TimeTick:  cp.TimeTick,
		Magic:     cp.RecoveryMagic,
	}
}

// WALCheckpoint represents a consume checkpoint in the Write-Ahead Log (WAL).
type WALCheckpoint struct {
	MessageID message.MessageID // should always be not nil.
	TimeTick  uint64
	Magic     int64
}

// IntoProto converts the WALCheckpoint to a protobuf message.
func (c *WALCheckpoint) IntoProto() *streamingpb.WALCheckpoint {
	if c == nil {
		return nil
	}
	return &streamingpb.WALCheckpoint{
		MessageId:     message.MustMarshalMessageID(c.MessageID),
		TimeTick:      c.TimeTick,
		RecoveryMagic: c.Magic,
	}
}

// Clone creates a new WALCheckpoint with the same values as the original.
func (c *WALCheckpoint) Clone() *WALCheckpoint {
	return &WALCheckpoint{
		MessageID: c.MessageID,
		TimeTick:  c.TimeTick,
		Magic:     c.Magic,
	}
}

// PChannelRecoveryControlMetaFromLegacyCheckpoint converts the control fields
// embedded in the legacy WAL checkpoint into the standalone recovery-control
// component used by RecoveryStorage V2.
func PChannelRecoveryControlMetaFromLegacyCheckpoint(cp *streamingpb.WALCheckpoint) *streamingpb.PChannelRecoveryControlMeta {
	if cp == nil {
		return nil
	}
	control := &streamingpb.PChannelRecoveryControlMeta{
		CheckpointTimeTick: cp.GetTimeTick(),
	}
	if cp.GetReplicateConfig() != nil {
		control.ReplicateConfig = proto.Clone(cp.GetReplicateConfig()).(*commonpb.ReplicateConfiguration)
	}
	if cp.GetReplicateCheckpoint() != nil {
		control.ReplicateCheckpoint = proto.Clone(cp.GetReplicateCheckpoint()).(*commonpb.ReplicateCheckpoint)
	}
	if cp.GetAlterWalState() != nil {
		control.AlterWalState = proto.Clone(cp.GetAlterWalState()).(*streamingpb.AlterWALState)
	}
	return control
}

// NewReplicateCheckpointFromProto creates a new ReplicateCheckpoint from a protobuf message.
func NewReplicateCheckpointFromProto(cp *commonpb.ReplicateCheckpoint) *ReplicateCheckpoint {
	if cp == nil {
		return nil
	}
	return &ReplicateCheckpoint{
		MessageID: message.MustUnmarshalMessageID(cp.MessageId),
		ClusterID: cp.ClusterId,
		PChannel:  cp.Pchannel,
		TimeTick:  cp.TimeTick,
	}
}

// ReplicateCheckpoint represents a source milvus cluster checkpoint.
// It's used to recover the replication state for remote source cluster.
type ReplicateCheckpoint struct {
	ClusterID string            // the cluster id of the source cluster.
	PChannel  string            // the pchannel of the source cluster.
	MessageID message.MessageID // the last confirmed message id of the last replicated message, may be nil when initializing.
	TimeTick  uint64            // the time tick of the last replicated message.
}

// IntoProto converts the ReplicateCheckpoint to a protobuf message.
func (c *ReplicateCheckpoint) IntoProto() *commonpb.ReplicateCheckpoint {
	if c == nil {
		return nil
	}
	return &commonpb.ReplicateCheckpoint{
		ClusterId: c.ClusterID,
		Pchannel:  c.PChannel,
		MessageId: message.MustMarshalMessageID(c.MessageID),
		TimeTick:  c.TimeTick,
	}
}

// Clone creates a new ReplicateCheckpoint with the same values as the original.
func (c *ReplicateCheckpoint) Clone() *ReplicateCheckpoint {
	if c == nil {
		return nil
	}
	return &ReplicateCheckpoint{
		ClusterID: c.ClusterID,
		PChannel:  c.PChannel,
		MessageID: c.MessageID,
		TimeTick:  c.TimeTick,
	}
}
