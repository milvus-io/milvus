package streaming

import (
	"context"
	"time"

	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

var singleton WALAccesser = nil

// Init initializes the wal accesser with the given etcd client.
// should be called before any other operations.
func Init() {
	c, _ := kvfactory.GetEtcdAndPath()
	singleton = newWALAccesser(c)
}

// Release releases the resources of the wal accesser.
func Release() {
	if w, ok := singleton.(*walAccesserImpl); ok && w != nil {
		w.Close()
	}
}

// WAL is the entrance to interact with the milvus write ahead log.
func WAL() WALAccesser {
	return singleton
}

// AppendOption is the option for append operation.
type AppendOption struct {
	BarrierTimeTick uint64 // BarrierTimeTick is the barrier time tick of the message.
	// Must be allocated from tso, otherwise undetermined behavior.
}

type TxnOption struct {
	// VChannel is the target vchannel to write.
	// TODO: support cross-wal txn in future.
	VChannel string

	// Keepalive is the time to keepalive of the transaction.
	// If the txn don't append message in the keepalive time, the txn will be expired.
	// Only make sense when ttl is greater than 1ms.
	Keepalive time.Duration
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

	// RawAppend writes a records to the log.
	RawAppend(ctx context.Context, msgs message.MutableMessage, opts ...AppendOption) (*types.AppendResult, error)

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, opts ReadOption) Scanner

	// AppendMessages appends messages to the wal.
	// It it a helper utility function to append messages to the wal.
	// If the messages is belong to one vchannel, it will be sent as a transaction.
	// Otherwise, it will be sent as individual messages.
	// !!! This function do not promise the atomicity and deliver order of the messages appending.
	// TODO: Remove after we support cross-wal txn.
	AppendMessages(ctx context.Context, msgs ...message.MutableMessage) AppendResponses

	// AppendMessagesWithOption appends messages to the wal with the given option.
	// Same with AppendMessages, but with the given option.
	// TODO: Remove after we support cross-wal txn.
	AppendMessagesWithOption(ctx context.Context, opts AppendOption, msgs ...message.MutableMessage) AppendResponses
}

// Txn is the interface for writing transaction into the wal.
type Txn interface {
	// Append writes a record to the log.
	Append(ctx context.Context, msg message.MutableMessage, opts ...AppendOption) error

	// Commit commits the transaction.
	// Commit and Rollback can be only call once, and not concurrent safe with append operation.
	Commit(ctx context.Context) (*types.AppendResult, error)

	// Rollback rollbacks the transaction.
	// Commit and Rollback can be only call once, and not concurrent safe with append operation.
	// TODO: Manually rollback is make no sense for current single wal txn.
	// It is preserved for future cross-wal txn.
	Rollback(ctx context.Context) error
}
