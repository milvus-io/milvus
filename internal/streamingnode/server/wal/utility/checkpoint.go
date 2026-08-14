package utility

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const (
	RecoveryMagicStreamingInitialized int64 = 1 // the vchannel info is set into the catalog.
	RecoveryMagicRecoveryStorageV2    int64 = 2 // legacy recovery metadata has a safe data checkpoint.
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
		AlterWalState:       cp.AlterWalState,
		DataCheckpoint:      NewWALConsumeCheckpointFromProto(cp.DataCheckpoint),
	}
}

// WALCheckpoint represents a consume checkpoint in the Write-Ahead Log (WAL).
type WALCheckpoint struct {
	MessageID           message.MessageID // should always be not nil.
	TimeTick            uint64
	Magic               int64
	ReplicateCheckpoint *ReplicateCheckpoint
	ReplicateConfig     *commonpb.ReplicateConfiguration
	AlterWalState       *streamingpb.AlterWALState
	DataCheckpoint      *WALConsumeCheckpoint
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
		AlterWalState:       c.AlterWalState,
		DataCheckpoint:      c.DataCheckpoint.IntoProto(),
	}
}

// Clone creates a new WALCheckpoint with the same values as the original.
func (c *WALCheckpoint) Clone() *WALCheckpoint {
	var replicateConfig *commonpb.ReplicateConfiguration
	if c.ReplicateConfig != nil {
		replicateConfig = proto.Clone(c.ReplicateConfig).(*commonpb.ReplicateConfiguration)
	}
	var alterWalState *streamingpb.AlterWALState
	if c.AlterWalState != nil {
		alterWalState = proto.Clone(c.AlterWalState).(*streamingpb.AlterWALState)
	}
	return &WALCheckpoint{
		MessageID:           c.MessageID,
		TimeTick:            c.TimeTick,
		Magic:               c.Magic,
		ReplicateConfig:     replicateConfig,
		ReplicateCheckpoint: c.ReplicateCheckpoint.Clone(),
		AlterWalState:       alterWalState,
		DataCheckpoint:      c.DataCheckpoint.Clone(),
	}
}

// NewWALConsumeCheckpointFromProto creates a new WALConsumeCheckpoint from a protobuf message.
func NewWALConsumeCheckpointFromProto(cp *streamingpb.WALConsumeCheckpoint) *WALConsumeCheckpoint {
	if cp == nil {
		return nil
	}
	return &WALConsumeCheckpoint{
		MessageID: message.MustUnmarshalMessageID(cp.MessageId),
		TimeTick:  cp.TimeTick,
	}
}

// WALConsumeCheckpoint represents a physical consume point in the WAL.
type WALConsumeCheckpoint struct {
	MessageID message.MessageID
	TimeTick  uint64
}

// IntoProto converts the WALConsumeCheckpoint to a protobuf message.
func (c *WALConsumeCheckpoint) IntoProto() *streamingpb.WALConsumeCheckpoint {
	if c == nil {
		return nil
	}
	return &streamingpb.WALConsumeCheckpoint{
		MessageId: message.MustMarshalMessageID(c.MessageID),
		TimeTick:  c.TimeTick,
	}
}

// Clone creates a new WALConsumeCheckpoint with the same values as the original.
func (c *WALConsumeCheckpoint) Clone() *WALConsumeCheckpoint {
	if c == nil {
		return nil
	}
	return &WALConsumeCheckpoint{
		MessageID: c.MessageID,
		TimeTick:  c.TimeTick,
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
