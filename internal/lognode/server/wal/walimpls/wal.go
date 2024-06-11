package walimpls

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

type WALImpls interface {
	// Channel returns the channel assignment info of the wal.
	// Should be read-only.
	Channel() *logpb.PChannelInfo

	// Append writes a record to the log.
	Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, opts ReadOption) (ScannerImpls, error)

	// Close closes the wal instance.
	Close()
}
