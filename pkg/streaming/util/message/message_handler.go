package message

import (
	"context"
)

// HandleParam is the parameter for handler.
type HandleParam struct {
	Ctx      context.Context
	Upstream <-chan ImmutableMessage
	Message  ImmutableMessage
}

// HandleResult is the result of handler.
type HandleResult struct {
	Incoming       ImmutableMessage // Not nil if upstream return new message.
	MessageHandled bool             // True if Message is handled successfully.
	Error          error            // Error is context is canceled.
}

// Handler is used to handle message read from log.
type Handler interface {
	// Handle is the callback for handling message.
	// Return true if the message is consumed, false if the message is not consumed.
	// Should return error if and only if ctx is done.
	// !!! It's a bad implementation for compatibility for msgstream,
	// will be removed in the future.
	Handle(param HandleParam) HandleResult

	// Close is called after all messages are handled or handling is interrupted.
	Close()
}
