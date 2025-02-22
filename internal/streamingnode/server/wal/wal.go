package wal

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type AppendResult = types.AppendResult

// WAL is the WAL framework interface.
// !!! Don't implement it directly, implement walimpls.WAL instead.
type WAL interface {
	WALName() string

	// Channel returns the channel assignment info of the wal.
	Channel() types.PChannelInfo

	// Append writes a record to the log.
	Append(ctx context.Context, msg message.MutableMessage) (*AppendResult, error)

	// Append a record to the log asynchronously.
	AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(*AppendResult, error))

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, deliverPolicy ReadOption) (Scanner, error)

	// Available return a channel that will be closed when the wal is available.
	Available() <-chan struct{}

	// IsAvailable returns if the wal is available.
	IsAvailable() bool

	// Close closes the wal instance.
	Close()
}
