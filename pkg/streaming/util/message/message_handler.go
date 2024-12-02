package message

import "context"

// Handler is used to handle message read from log.
type Handler interface {
	// Handle is the callback for handling message.
	// Return true if the message is consumed, false if the message is not consumed.
	// Should return error if and only if ctx is done.
	// !!! It's a bad implementation for compatibility for msgstream,
	// should be removed in the future.
	Handle(ctx context.Context, msg ImmutableMessage) (bool, error)

	// Close is called after all messages are handled or handling is interrupted.
	Close()
}

var _ Handler = ChanMessageHandler(nil)

// ChanMessageHandler is a handler just forward the message into a channel.
type ChanMessageHandler chan ImmutableMessage

// Handle is the callback for handling message.
func (cmh ChanMessageHandler) Handle(ctx context.Context, msg ImmutableMessage) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case cmh <- msg:
		return true, nil
	}
}

// Close is called after all messages are handled or handling is interrupted.
func (cmh ChanMessageHandler) Close() {
	close(cmh)
}
