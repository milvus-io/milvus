package walimpls

import (
	"github.com/milvus-io/milvus/internal/util/streamingutil/message"
	"github.com/milvus-io/milvus/internal/util/streamingutil/options"
)

type ReadOption struct {
	Name          string
	DeliverPolicy options.DeliverPolicy
}

// ScannerImpls is the interface for reading records from the wal.
type ScannerImpls interface {
	// Name returns the name of scanner.
	Name() string

	// Chan returns the channel of message.
	Chan() <-chan message.ImmutableMessage

	// Error returns the error of scanner failed.
	// Will block until scanner is closed or Chan is dry out.
	Error() error

	// Done returns a channel which will be closed when scanner is finished or closed.
	Done() <-chan struct{}

	// Close the scanner, release the underlying resources.
	// Return the error same with `Error`
	Close() error
}
