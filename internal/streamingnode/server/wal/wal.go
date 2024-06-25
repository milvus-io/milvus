package wal

import (
	"context"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

// WAL is the WAL framework interface.
// !!! Don't implement it directly, implement walimpls.WAL instead.
type WAL interface {
	WALName() string

	// Channel returns the channel assignment info of the wal.
	Channel() types.PChannelInfo

	// Append writes a record to the log.
	Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Append a record to the log asynchronously.
	AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(message.MessageID, error))

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, deliverPolicy ReadOption) (Scanner, error)

	// Close closes the wal instance.
	Close()
}
