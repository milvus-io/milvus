package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/message"
)

// WAL is the WAL framework interface.
// !!! Don't implement it directly, implement walimpls.WAL instead.
type WAL interface {
	// Channel returns the channel assignment info of the wal.
	// Should be read-only.
	Channel() *streamingpb.PChannelInfo

	// Append writes a record to the log.
	Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Append a record to the log asynchronously.
	AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(message.MessageID, error))

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, deliverPolicy ReadOption) (Scanner, error)

	// Close closes the wal instance.
	Close()
}
