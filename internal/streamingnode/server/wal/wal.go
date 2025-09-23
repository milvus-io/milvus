package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type (
	AppendResult        = types.AppendResult
	ReplicateCheckpoint = utility.ReplicateCheckpoint
)

// WAL is the WAL framework interface.
// !!! Don't implement it directly, implement walimpls.WAL instead.
type WAL interface {
	ROWAL

	// GetLatestMVCCTimestamp get the latest mvcc timestamp of the wal at vchannel.
	GetLatestMVCCTimestamp(ctx context.Context, vchannel string) (uint64, error)

	// GetReplicateCheckpoint returns the replicate checkpoint of the wal.
	// If the wal is not on replicating mode, it will return ReplicateViolationError.
	// If the wal is on replicating mode, it will return the replicate checkpoint of the wal.
	// If the wal is initialized into replica mode, not replicate any message,
	// the message id of the replicate checkpoint will be 0.
	GetReplicateCheckpoint() (*ReplicateCheckpoint, error)

	// Append writes a record to the log.
	Append(ctx context.Context, msg message.MutableMessage) (*AppendResult, error)

	// Append a record to the log asynchronously.
	AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(*AppendResult, error))
}

// ROWAL is the read-only WAL interface.
// ROWAL only supports reading records from the wal but not writing.
// !!! Don't implement it directly, implement walimpls.WAL instead.
type ROWAL interface {
	// WALName returns the name of the wal.
	WALName() message.WALName

	// Metrics returns the metrics of the wal.
	Metrics() types.WALMetrics

	// Channel returns the channel assignment info of the wal.
	Channel() types.PChannelInfo

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, deliverPolicy ReadOption) (Scanner, error)

	// Available return a channel that will be closed when the wal is available.
	Available() <-chan struct{}

	// IsAvailable returns if the wal is available.
	IsAvailable() bool

	// Close closes the wal instance.
	Close()
}
