package walimpls

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/message"
)

type WALImpls interface {
	// Channel returns the channel assignment info of the wal.
	// Should be read-only.
	Channel() *streamingpb.PChannelInfo

	// Append writes a record to the log.
	Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, opts ReadOption) (ScannerImpls, error)

	// Close closes the wal instance.
	Close()
}
