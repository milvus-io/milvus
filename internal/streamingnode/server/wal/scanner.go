package wal

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type MessageFilter = func(message.ImmutableMessage) bool

var ErrUpstreamClosed = errors.New("upstream closed")

// ReadOption is the option for reading records from the wal.
type ReadOption struct {
	VChannel string // vchannel is a optional field to select a vchannel to consume.
	// If the vchannel is setup, the message that is not belong to these vchannel will be dropped by scanner.
	// Otherwise all message on WAL will be sent.
	DeliverPolicy  options.DeliverPolicy
	MessageFilter  []options.DeliverFilter
	MesasgeHandler message.Handler // message handler for message processing.
	// If the message handler is nil (no redundant operation need to apply),
	// the default message handler will be used, and the receiver will be returned from Chan.
	// Otherwise, Chan will panic.
	// vaild every message will be passed to this handler before being delivered to the consumer.

	// IgnorePauseConsumption is the flag to ignore the consumption pause of the scanner.
	IgnorePauseConsumption bool

	// RateLimitControl is the rate limit controller to enable the rate limit control of the scanner.
	// If the scanner is working with catchup mode, the rate limit slowdown mode will be triggered to protect the wal from being overloaded.
	// And the rate limit recovery mode will be triggered if the scanner is working with tailing mode.
	RateLimitControl *ratelimit.AdaptiveRateLimitController

	// AppendRateCounter is a reference to the WAL's append rate counter.
	// Used by the scanner to compare read rate vs append rate for slowdown control.
	AppendRateCounter *utility.RateCounter
}

// Scanner is the interface for reading records from the wal.
type Scanner interface {
	// Chan returns the channel of message if Option.MessageHandler is nil.
	Chan() <-chan message.ImmutableMessage

	// Channel returns the channel assignment info of the wal.
	Channel() types.PChannelInfo

	// Error returns the error of scanner failed.
	// Will block until scanner is closed or Chan is dry out.
	Error() error

	// Done returns a channel which will be closed when scanner is finished or closed.
	Done() <-chan struct{}

	// Close the scanner, release the underlying resources.
	// Return the error same with `Error`
	Close() error
}
