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
	wcp := &WALCheckpoint{
		MessageID:       message.MustUnmarshalMessageID(cp.MessageId),
		TimeTick:        cp.TimeTick,
		Magic:           cp.RecoveryMagic,
		ReplicateConfig: cp.ReplicateConfig,
	}
	if cp.ReplicateCheckpoint != nil {
		var messageID message.MessageID
		if cp.ReplicateCheckpoint.MessageId != nil {
			messageID = message.MustUnmarshalMessageID(cp.ReplicateCheckpoint.MessageId)
		}
		wcp.ReplicateCheckpoint = &ReplicateCheckpoint{
			ClusterID: cp.ReplicateCheckpoint.ClusterId,
			PChannel:  cp.ReplicateCheckpoint.Pchannel,
			MessageID: messageID,
			TimeTick:  cp.ReplicateCheckpoint.TimeTick,
		}
	}
	return wcp
}

// WALCheckpoint represents a consume checkpoint in the Write-Ahead Log (WAL).
type WALCheckpoint struct {
	MessageID           message.MessageID
	TimeTick            uint64
	Magic               int64
	ReplicateCheckpoint *ReplicateCheckpoint
	ReplicateConfig     *commonpb.ReplicateConfiguration
}

// IntoProto converts the WALCheckpoint to a protobuf message.
func (c *WALCheckpoint) IntoProto() *streamingpb.WALCheckpoint {
	cp := &streamingpb.WALCheckpoint{
		MessageId:     c.MessageID.IntoProto(),
		TimeTick:      c.TimeTick,
		RecoveryMagic: c.Magic,
	}
	if c.ReplicateCheckpoint != nil {
		cp.ReplicateCheckpoint = c.ReplicateCheckpoint.IntoProto()
	}
	return cp
}

// Clone creates a new WALCheckpoint with the same values as the original.
func (c *WALCheckpoint) Clone() *WALCheckpoint {
	return &WALCheckpoint{
		MessageID:           c.MessageID,
		TimeTick:            c.TimeTick,
		Magic:               c.Magic,
		ReplicateCheckpoint: c.ReplicateCheckpoint.Clone(),
	}
}

// NewReplicateCheckpointFromProto creates a new ReplicateCheckpoint from a protobuf message.
func NewReplicateCheckpointFromProto(cp *commonpb.ReplicateCheckpoint) *ReplicateCheckpoint {
	return &ReplicateCheckpoint{
		ClusterID: cp.ClusterId,
		PChannel:  cp.Pchannel,
		MessageID: message.MustUnmarshalMessageID(cp.MessageId),
		TimeTick:  cp.TimeTick,
	}
}

// ReplicateCheckpoint represents a source milvus cluster checkpoint.
// It's used to recover the replication state for remote source cluster.
type ReplicateCheckpoint struct {
	ClusterID string            // the cluster id of the source cluster.
	PChannel  string            // the pchannel of the source cluster.
	MessageID message.MessageID // the last confirmed message id of the last replicated message.
	TimeTick  uint64            // the time tick of the last replicated message.
}

// IntoProto converts the ReplicateCheckpoint to a protobuf message.
func (c *ReplicateCheckpoint) IntoProto() *commonpb.ReplicateCheckpoint {
	if c == nil {
		return nil
	}
	var messageID *commonpb.MessageID
	if c.MessageID != nil {
		messageID = c.MessageID.IntoProto()
	}
	return &commonpb.ReplicateCheckpoint{
		ClusterId: c.ClusterID,
		Pchannel:  c.PChannel,
		MessageId: messageID,
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
