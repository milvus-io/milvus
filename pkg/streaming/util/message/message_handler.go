package message

// Handler is used to handle message read from log.
type Handler interface {
	// Handle is the callback for handling message.
	Handle(msg ImmutableMessage)

	// Close is called after all messages are handled or handling is interrupted.
	Close()
}

var _ Handler = ChanMessageHandler(nil)

// ChanMessageHandler is a handler just forward the message into a channel.
type ChanMessageHandler chan ImmutableMessage

// Handle is the callback for handling message.
func (cmh ChanMessageHandler) Handle(msg ImmutableMessage) {
	cmh <- msg
}

// Close is called after all messages are handled or handling is interrupted.
func (cmh ChanMessageHandler) Close() {
	close(cmh)
}

// NopCloseHandler is a handler that do nothing when close.
type NopCloseHandler struct {
	Handler
}

// Close is called after all messages are handled or handling is interrupted.
func (nch NopCloseHandler) Close() {
}
