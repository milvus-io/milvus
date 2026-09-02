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
		MessageID:           message.MustUnmarshalMessageID(cp.MessageId),
		TimeTick:            cp.TimeTick,
		Magic:               cp.RecoveryMagic,
		Term:                cp.Term,
		ReplicateConfig:     cp.ReplicateConfig,
		ReplicateCheckpoint: cp.ReplicateCheckpoint,
		AlterWalState:       cp.AlterWalState,
	}
}

// WALCheckpoint represents a consume checkpoint in the Write-Ahead Log (WAL).
type WALCheckpoint struct {
	MessageID message.MessageID // should always be not nil.
	TimeTick  uint64
	Magic     int64
	// Term of the publisher that advanced this checkpoint. It fences the
	// checkpoint advancement across term changes: a publisher whose term is
	// older than the recorded one must never advance it (its takeover has been
	// superseded), or WAL truncation would outrun the successor's inherited
	// manifest coverage.
	Term int64
	// ReplicateConfig, ReplicateCheckpoint and AlterWalState are the
	// pchannel-scoped recovery control state. They advance atomically with
	// the checkpoint: the checkpoint is the single source of truth for the
	// control state after a crash, and a crash never loses an applied control
	// effect because the control messages are replayed after the checkpoint.
	ReplicateConfig     *commonpb.ReplicateConfiguration
	ReplicateCheckpoint *commonpb.ReplicateCheckpoint
	AlterWalState       *streamingpb.AlterWALState
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
		Term:                c.Term,
		ReplicateConfig:     c.ReplicateConfig,
		ReplicateCheckpoint: c.ReplicateCheckpoint,
		AlterWalState:       c.AlterWalState,
	}
}

// Clone creates a new WALCheckpoint with the same values as the original.
func (c *WALCheckpoint) Clone() *WALCheckpoint {
	if c == nil {
		return nil
	}
	return &WALCheckpoint{
		MessageID:           c.MessageID,
		TimeTick:            c.TimeTick,
		Magic:               c.Magic,
		Term:                c.Term,
		ReplicateConfig:     proto.Clone(c.ReplicateConfig).(*commonpb.ReplicateConfiguration),
		ReplicateCheckpoint: proto.Clone(c.ReplicateCheckpoint).(*commonpb.ReplicateCheckpoint),
		AlterWalState:       proto.Clone(c.AlterWalState).(*streamingpb.AlterWALState),
	}
}

// ApplyControl freezes the pchannel control state into the checkpoint so it
// advances atomically with the checkpoint publication. Callers must hold the
// same ordering guarantees as the checkpoint itself: the control effects
// applied through this call are covered by the checkpoint position.
func (c *WALCheckpoint) ApplyControl(control *streamingpb.PChannelRecoveryControlMeta) {
	if c == nil || control == nil {
		return
	}
	c.ReplicateConfig = proto.Clone(control.ReplicateConfig).(*commonpb.ReplicateConfiguration)
	c.ReplicateCheckpoint = proto.Clone(control.ReplicateCheckpoint).(*commonpb.ReplicateCheckpoint)
	c.AlterWalState = proto.Clone(control.AlterWalState).(*streamingpb.AlterWALState)
}

// PChannelControlFromCheckpoint decodes the pchannel-scoped control state
// embedded in the WAL checkpoint. The control fields advance atomically with
// the checkpoint, so the checkpoint is the single source of truth for the
// control state after a crash; the decoded frontier is the lower bound for
// the control messages replayed during recovery.
func PChannelControlFromCheckpoint(cp *WALCheckpoint) *streamingpb.PChannelRecoveryControlMeta {
	control := &streamingpb.PChannelRecoveryControlMeta{}
	if cp == nil {
		return control
	}
	control.CheckpointTimeTick = cp.TimeTick
	control.ReplicateConfig = proto.Clone(cp.ReplicateConfig).(*commonpb.ReplicateConfiguration)
	control.ReplicateCheckpoint = proto.Clone(cp.ReplicateCheckpoint).(*commonpb.ReplicateCheckpoint)
	control.AlterWalState = proto.Clone(cp.AlterWalState).(*streamingpb.AlterWALState)
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
