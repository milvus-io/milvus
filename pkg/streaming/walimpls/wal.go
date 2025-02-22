package walimpls

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type WALImpls interface {
	// WALName returns the name of the wal.
	WALName() string

	// Channel returns the channel assignment info of the wal.
	// Should be read-only.
	Channel() types.PChannelInfo

	// Append writes a record to the log.
	Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, opts ReadOption) (ScannerImpls, error)

	// Close closes the wal instance.
	Close()
}
