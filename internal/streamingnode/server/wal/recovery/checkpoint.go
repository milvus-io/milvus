package recovery

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

const (
	recoveryMagicStreamingInitialized int64 = 1 // the vchannel info is set into the catalog.
	// the checkpoint is set into the catalog.
)

// newWALCheckpointFromProto creates a new WALCheckpoint from a protobuf message.
func newWALCheckpointFromProto(walName string, cp *streamingpb.WALCheckpoint) *WALCheckpoint {
	return &WALCheckpoint{
		MessageID: message.MustUnmarshalMessageID(walName, cp.MessageId.Id),
		TimeTick:  cp.TimeTick,
		Magic:     cp.RecoveryMagic,
	}
}

// WALCheckpoint represents a consume checkpoint in the Write-Ahead Log (WAL).
type WALCheckpoint struct {
	MessageID message.MessageID
	TimeTick  uint64
	Magic     int64
}

// IntoProto converts the WALCheckpoint to a protobuf message.
func (c *WALCheckpoint) IntoProto() *streamingpb.WALCheckpoint {
	cp := &streamingpb.WALCheckpoint{
		MessageId: &messagespb.MessageID{
			Id: c.MessageID.Marshal(),
		},
		TimeTick:      c.TimeTick,
		RecoveryMagic: c.Magic,
	}
	return cp
}

// Clone creates a new WALCheckpoint with the same values as the original.
func (c *WALCheckpoint) Clone() *WALCheckpoint {
	return &WALCheckpoint{
		MessageID: c.MessageID,
		TimeTick:  c.TimeTick,
		Magic:     c.Magic,
	}
}
