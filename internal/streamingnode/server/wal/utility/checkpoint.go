package utility

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

const (
	RecoveryMagicStreamingInitialized int64 = 1 // the vchannel info is set into the catalog.
	// the checkpoint is set into the catalog.
)

// NewWALCheckpointFromProto creates a new WALCheckpoint from a protobuf message.
func NewWALCheckpointFromProto(cp *streamingpb.WALCheckpoint) *WALCheckpoint {
	if cp == nil {
		return nil
	}
	return &WALCheckpoint{
		MessageID:           message.MustUnmarshalMessageID(cp.MessageId),
		TimeTick:            cp.TimeTick,
		Magic:               cp.RecoveryMagic,
		ReplicateConfig:     cp.ReplicateConfig,
		ReplicateCheckpoint: NewReplicateCheckpointFromProto(cp.ReplicateCheckpoint),
	}
}

// WALCheckpoint represents a consume checkpoint in the Write-Ahead Log (WAL).
type WALCheckpoint struct {
	MessageID           message.MessageID // should always be not nil.
	TimeTick            uint64
	Magic               int64
	ReplicateCheckpoint *ReplicateCheckpoint
	ReplicateConfig     *commonpb.ReplicateConfiguration
}

// IntoProto converts the WALCheckpoint to a protobuf message.
func (c *WALCheckpoint) IntoProto() *streamingpb.WALCheckpoint {
	if c == nil {
		return nil
	}
	return &streamingpb.WALCheckpoint{
		MessageId:           message.MustMarshalMessageID(c.MessageID),
		TimeTick:            c.TimeTick,
		RecoveryMagic:       c.Magic,
		ReplicateConfig:     c.ReplicateConfig,
		ReplicateCheckpoint: c.ReplicateCheckpoint.IntoProto(),
	}
}

// Clone creates a new WALCheckpoint with the same values as the original.
func (c *WALCheckpoint) Clone() *WALCheckpoint {
	return &WALCheckpoint{
		MessageID:           c.MessageID,
		TimeTick:            c.TimeTick,
		Magic:               c.Magic,
		ReplicateConfig:     c.ReplicateConfig,
		ReplicateCheckpoint: c.ReplicateCheckpoint.Clone(),
	}
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
