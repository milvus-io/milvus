package wal

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

type MessageFilter = func(message.ImmutableMessage) bool

var ErrUpstreamClosed = errors.New("upstream closed")

// ReadOption is the option for reading records from the wal.
type ReadOption struct {
	DeliverPolicy  options.DeliverPolicy
	MessageFilter  MessageFilter
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

// MessageHandler is used to handle message read from log.
// TODO: should be removed in future after msgstream is removed.
type MessageHandler interface {
	// Handle is the callback for handling message.
	// The message will be passed to the handler for processing.
	// Handle operation can be blocked, but should listen to the context.Done() and upstream.
	// If the context is canceled, the handler should return immediately with ctx.Err.
	// If the upstream is closed, the handler should return immediately with ErrUpstreamClosed.
	// If the upstream recv a message, the handler should return the incoming message.
	// If the handler handle the message successfully, it should return the ok=true.
	Handle(ctx context.Context, upstream <-chan message.ImmutableMessage, msg message.ImmutableMessage) (incoming message.ImmutableMessage, ok bool, err error)

	// Close is called after all messages are handled or handling is interrupted.
	Close()
}
