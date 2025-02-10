package walimpls

import (
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
)

type ReadOption struct {
	// The name of the reader.
	Name string
	// ReadAheadBufferSize sets the size of scanner read ahead queue size.
	// Control how many messages can be read ahead by the scanner.
	// Higher value could potentially increase the scanner throughput but bigger memory utilization.
	// 0 is the default value determined by the underlying wal implementation.
	ReadAheadBufferSize int
	// DeliverPolicy sets the deliver policy of the reader.
	DeliverPolicy options.DeliverPolicy
}

// ScannerImpls is the interface for reading records from the wal.
type ScannerImpls interface {
	// Name returns the name of scanner.
	Name() string

	// Chan returns the channel of message.
	// If the scanner is failure, the channel will be closed.
	// And an error will be returned by Error().
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
