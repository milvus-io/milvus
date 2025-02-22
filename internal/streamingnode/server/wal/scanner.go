package wal

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type MessageFilter = func(message.ImmutableMessage) bool

var ErrUpstreamClosed = errors.New("upstream closed")

// ReadOption is the option for reading records from the wal.
type ReadOption struct {
	VChannel       string // vchannel name
	DeliverPolicy  options.DeliverPolicy
	MessageFilter  []options.DeliverFilter
	MesasgeHandler MessageHandler // message handler for message processing.
	// If the message handler is nil (no redundant operation need to apply),
	// the default message handler will be used, and the receiver will be returned from Chan.
	// Otherwise, Chan will panic.
	// vaild every message will be passed to this handler before being delivered to the consumer.
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

type HandleParam struct {
	Ctx          context.Context
	Upstream     <-chan message.ImmutableMessage
	Message      message.ImmutableMessage
	TimeTickChan <-chan struct{}
}

type HandleResult struct {
	Incoming        message.ImmutableMessage // Not nil if upstream return new message.
	MessageHandled  bool                     // True if Message is handled successfully.
	TimeTickUpdated bool                     // True if TimeTickChan is triggered.
	Error           error                    // Error is context is canceled.
}

// MessageHandler is used to handle message read from log.
// TODO: should be removed in future after msgstream is removed.
type MessageHandler interface {
	// Handle is the callback for handling message.
	Handle(param HandleParam) HandleResult

	// Close is called after all messages are handled or handling is interrupted.
	Close()
}
