package streaming

import (
	"context"
	"time"

	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

var singleton *walAccesserImpl = nil

// Init initializes the wal accesser with the given etcd client.
// should be called before any other operations.
func Init() {
	c, _ := kvfactory.GetEtcdAndPath()
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

type TxnOption struct {
	// VChannel is the target vchannel to write.
	// TODO: support cross-wal txn in future.
	VChannel string

	// TTL is the time to live of the transaction.
	// Only make sense when ttl is greater than 1ms.
	TTL time.Duration
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
	// Txn returns a transaction for writing records to the log.
	// Once the txn is returned, the Commit or Rollback operation must be called once, otherwise resource leak on wal.
	Txn(ctx context.Context, opts TxnOption) (Txn, error)

	// Append writes a batch records to the log.
	// !!! Append didn't promise the order of the message and atomic write.
	Append(ctx context.Context, msgs ...message.MutableMessage) AppendResponses

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, opts ReadOption) Scanner
}

// Txn is the interface for writing transaction into the wal.
type Txn interface {
	// Append writes a record to the log.
	// Once the error is returned, the transaction is invalid and should be rollback.
	Append(ctx context.Context, msgs ...message.MutableMessage) error

	// Commit commits the transaction.
	// Commit and Rollback can be only call once, and not concurrent safe with append operation.
	Commit(ctx context.Context) (*types.AppendResult, error)

	// Rollback rollbacks the transaction.
	// Commit and Rollback can be only call once, and not concurrent safe with append operation.
	// TODO: Manually rollback is make no sense for current single wal txn.
	// It is preserved for future cross-wal txn.
	Rollback(ctx context.Context) error
}
