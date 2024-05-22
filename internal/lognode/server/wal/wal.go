package wal

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// builders is a map of registered wal builders.
var builders typeutil.ConcurrentMap[string, OpenerBuilder]

// Register registers the wal builder.
//
// NOTE: this function must only be called during initialization time (i.e. in
// an init() function), name of builder is lowercase. If multiple Builder are
// registered with the same name, panic will occur.
func RegisterBuilder(b OpenerBuilder) {
	_, loaded := builders.GetOrInsert(strings.ToLower(b.Name()), b)
	if loaded {
		panic("wal builder already registered: " + b.Name())
	}
}

// MustGetBuilder returns the wal builder by name.
func MustGetBuilder(name string) OpenerBuilder {
	b, ok := builders.Get(name)
	if !ok {
		panic("wal builder not found: " + name)
	}
	return b
}

// BasicWAL is basic wal interface.
type BasicWAL interface {
	// Channel returns the channel assignment info of the wal.
	// Should be read-only.
	Channel() *logpb.PChannelInfo

	// Append writes a record to the log.
	Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, deliverPolicy ReadOption) (Scanner, error)

	// GetLatestMessageID returns the latest message id of the channel.
	GetLatestMessageID(ctx context.Context) (message.MessageID, error)

	// Close closes the wal instance.
	Close()
}

// WAL is the extend version interface of wal.
// Use extends.NewWALExtend to convert a BasicWAL into WAL.
// !!! Don't implement it directly.
type WAL interface {
	BasicWAL

	// Append a record to the log asynchronously.
	AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(message.MessageID, error))
}
