package streaming

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

var singleton WALAccesser = nil

// Init initializes the wal accesser with the given etcd client.
// should be called before any other operations.
func Init() {
	c, _ := kvfactory.GetEtcdAndPath()
	singleton = newWALAccesser(c)
	// Add the wal accesser to the broadcaster registry for making broadcast operation.
	registry.Register(registry.AppendOperatorTypeStreaming, singleton)
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
	// Only make sense when keepalive is greater than 1ms.
	// The default value is 0, which means the keepalive is setted by the wal at streaming node.
	Keepalive time.Duration
}

type ReadOption struct {
	// PChannel is the target pchannel to read, if the pchannel is not set.
	// It will be parsed from setted `VChannel`.
	PChannel string

	// VChannel is the target vchannel to read.
	// It must be set to read message from a vchannel.
	// If VChannel is empty, the PChannel must be set, and all message of pchannel will be read.
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
	// WALName returns the name of the wal.
	WALName() string

	// Local returns the local services.
	Local() Local

	// Txn returns a transaction for writing records to one vchannel.
	// It promises the atomicity written of the messages.
	// Once the txn is returned, the Commit or Rollback operation must be called once, otherwise resource leak on wal.
	Txn(ctx context.Context, opts TxnOption) (Txn, error)

	// RawAppend writes a records to the log.
	RawAppend(ctx context.Context, msgs message.MutableMessage, opts ...AppendOption) (*types.AppendResult, error)

	// Broadcast returns a broadcast service of wal.
	// Broadcast support cross-vchannel message broadcast.
	// It promises the atomicity written of the messages and eventual consistency.
	// And the broadcasted message must be acked cat consuming-side, otherwise resource leak on broadcast.
	// Broadcast also support the resource-key to achieve a resource-exclusive acquirsion.
	Broadcast() Broadcast

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, opts ReadOption) Scanner

	// AppendMessages appends messages to the wal.
	// It it a helper utility function to append messages to the wal.
	// If the messages is belong to one vchannel, it will be sent as a transaction.
	// Otherwise, it will be sent as individual messages.
	// !!! This function do not promise the atomicity and deliver order of the messages appending.
	AppendMessages(ctx context.Context, msgs ...message.MutableMessage) AppendResponses

	// AppendMessagesWithOption appends messages to the wal with the given option.
	// Same with AppendMessages, but with the given option.
	AppendMessagesWithOption(ctx context.Context, opts AppendOption, msgs ...message.MutableMessage) AppendResponses
}

type Local interface {
	// GetLatestMVCCTimestampIfLocal gets the latest mvcc timestamp of the vchannel.
	// If the wal is located at remote, it will return 0, error.
	GetLatestMVCCTimestampIfLocal(ctx context.Context, vchannel string) (uint64, error)

	// GetMetricsIfLocal gets the metrics of the local wal.
	// It will only return the metrics of the local wal but not the remote wal.
	GetMetricsIfLocal(ctx context.Context) (*types.StreamingNodeMetrics, error)
}

// Broadcast is the interface for writing broadcast message into the wal.
type Broadcast interface {
	// Append of Broadcast sends a broadcast message to all target vchannels.
	// Guarantees the atomicity written of the messages and eventual consistency.
	// The resource-key bound at the message will be held as a mutex until the message is broadcasted to all vchannels,
	// so the other append operation with the same resource-key will be searialized with a deterministic order on every vchannel.
	// The Append operation will be blocked until the message is consumed and acknowledged by the flusher at streamingnode.
	Append(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error)

	// Ack acknowledges a broadcast message at the specified vchannel.
	// It must be called after the message is comsumed by the unique-consumer.
	// It will only return error when the ctx is canceled.
	Ack(ctx context.Context, req types.BroadcastAckRequest) error
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
