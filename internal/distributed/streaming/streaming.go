package streaming

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
)

var singleton *walAccesserImpl = nil

// Init initializes the wal accesser with the given etcd client.
// should be called before any other operations.
func Init(c *clientv3.Client) {
	singleton = newWALAccesser(c)
}

// Release releases the resources of the wal accesser.
func Release() {
	if singleton != nil {
		singleton.Close()
	}
}

// WAL is the entrance to interact with the milvus write ahead log.
func WAL() WALAccesser {
	return singleton
}

type ReadOption struct {
	// VChannel is the target vchannel to read.
	VChannel string

	// DeliverPolicy is the deliver policy of the consumer.
	DeliverPolicy options.DeliverPolicy

	// DeliverFilters is the deliver filters of the consumer.
	DeliverFilters []options.DeliverFilter

	// Handler is the message handler used to handle message after recv from consumer.
	MessageHandler message.Handler
}

// Scanner is the interface for reading records from the wal.
type Scanner interface {
	// Done returns a channel which will be closed when scanner is finished or closed.
	Done() <-chan struct{}

	// Error returns the error of the scanner.
	Error() error

	// Close the scanner, release the underlying resources.
	Close()
}

// WALAccesser is the interfaces to interact with the milvus write ahead log.
type WALAccesser interface {
	// Append writes a record to the log.
	// !!! Append didn't promise the order of the message and atomic write.
	Append(ctx context.Context, msgs ...message.MutableMessage) AppendResponses

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, opts ReadOption) Scanner
}
