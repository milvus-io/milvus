package recovery

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

const (
	recoveryMagicStreamingInitialized int64 = iota // the vchannel info is set into the catalog.
	// the checkpoint is set into the catalog.
)

// newWALCheckpointFromProto creates a new WALCheckpoint from a protobuf message.
func newWALCheckpointFromProto(walName string, cp *streamingpb.WALCheckpoint) *WALCheckpoint {
	return &WALCheckpoint{
		WriteAheadCheckpoint:         message.MustUnmarshalMessageID(walName, cp.WriteAheadCheckpoint.Id),
		WriteAheadCheckpointTimeTick: cp.WriteAheadCheckpointTimeTick,
		Magic:                        cp.RecoveryMagic,
	}
}

type WALCheckpoint struct {
	WriteAheadCheckpoint         message.MessageID
	WriteAheadCheckpointTimeTick uint64
	Magic                        int64
}

// IntoProto converts the WALCheckpoint to a protobuf message.
func (c *WALCheckpoint) IntoProto() *streamingpb.WALCheckpoint {
	cp := &streamingpb.WALCheckpoint{
		WriteAheadCheckpoint: &messagespb.MessageID{
			Id: c.WriteAheadCheckpoint.Marshal(),
		},
		WriteAheadCheckpointTimeTick: c.WriteAheadCheckpointTimeTick,
		RecoveryMagic:                c.Magic,
	}
	return cp
}

// Clone creates a new WALCheckpoint with the same values as the original.
func (c *WALCheckpoint) Clone() *WALCheckpoint {
	return &WALCheckpoint{
		WriteAheadCheckpoint:         c.WriteAheadCheckpoint,
		WriteAheadCheckpointTimeTick: c.WriteAheadCheckpointTimeTick,
		Magic:                        c.Magic,
	}
}
