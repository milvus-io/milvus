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
	var flushCheckpoint message.MessageID
	if cp.FlushCheckpoint != nil {
		flushCheckpoint = message.MustUnmarshalMessageID(walName, cp.FlushCheckpoint.Id)
	}
	return &WALCheckpoint{
		FlushCheckpoint:              flushCheckpoint,
		WriteAheadCheckpoint:         message.MustUnmarshalMessageID(walName, cp.WriteAheadCheckpoint.Id),
		WriteAheadCheckpointTimeTick: cp.WriteAheadCheckpointTimeTick,
		Magic:                        cp.RecoveryMagic,
	}
}

type WALCheckpoint struct {
	FlushCheckpoint              message.MessageID
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
	if c.FlushCheckpoint != nil {
		cp.FlushCheckpoint = &messagespb.MessageID{
			Id: c.FlushCheckpoint.Marshal(),
		}
	}
	return cp
}

// Clone creates a new WALCheckpoint with the same values as the original.
func (c *WALCheckpoint) Clone() *WALCheckpoint {
	return &WALCheckpoint{
		FlushCheckpoint:              c.FlushCheckpoint,
		WriteAheadCheckpoint:         c.WriteAheadCheckpoint,
		WriteAheadCheckpointTimeTick: c.WriteAheadCheckpointTimeTick,
		Magic:                        c.Magic,
	}
}
