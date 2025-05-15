package walimpls

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

var ErrFenced = errors.New("fenced")

// ROWALImpls is the underlying implementation for a read-only wal.
type ROWALImpls interface {
	// WALName returns the name of the wal.
	WALName() string

	// Channel returns the channel assignment info of the wal.
	// Should be read-only.
	Channel() types.PChannelInfo

	// Close closes the wal instance.
	Close()

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, opts ReadOption) (ScannerImpls, error)
}

// WALImpls is the underlying implementation for a wal that supports read and write operations.
type WALImpls interface {
	ROWALImpls

	// Append writes a record to the log.
	// Can be only called when the wal is in read-write mode.
	// If ErrFenced is returned, the wal write operation is fenced forever and cannot recover.
	// The caller should close the wal and recreate a new wal instance to recover the write operation.
	Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Truncate truncates the wal to the given id (inclusive).
	Truncate(ctx context.Context, id message.MessageID) error
}
