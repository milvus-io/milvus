package recovery

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// RecoverySnapshot is the snapshot of the recovery info.
type RecoverySnapshot struct {
	VChannels          map[string]*streamingpb.VChannelMeta
	SegmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta
	Checkpoint         *WALCheckpoint
	TxnBuffer          *utility.TxnBuffer
}

type BuildRecoveryStreamParam struct {
	StartCheckpoint message.MessageID
	EndTimeTick     uint64
}

type RecoveryStreamBuilder interface {
	// WALName returns the name of the WAL.
	WALName() string

	// Channel returns the channel info of wal.
	Channel() types.PChannelInfo

	// Build builds a recovery stream from the given channel info.
	// The recovery stream will return the messages from the start checkpoint to the end time tick.
	Build(param BuildRecoveryStreamParam) RecoveryStream
}

// RecoveryStream is an interface that is used to recover the recovery storage from the WAL.
type RecoveryStream interface {
	// Chan returns the channel of the recovery stream.
	// The channel is closed when the recovery stream is done.
	Chan() <-chan message.ImmutableMessage

	// Error should be called after the stream `Chan()` is consumed.
	// It returns the error if the stream is not done.
	// If the stream is full consumed, it returns nil.
	Error() error

	// TxnBuffer returns the uncommitted txn buffer after recovery stream is done.
	// Can be only called the stream is consumed, and Error() return nil.
	TxnBuffer() *utility.TxnBuffer

	// Close closes the recovery stream.
	Close() error
}
