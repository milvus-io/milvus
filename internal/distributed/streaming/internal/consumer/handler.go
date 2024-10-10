package consumer

import "github.com/milvus-io/milvus/pkg/streaming/util/message"

// nopCloseHandler is a handler that do nothing when close.
type nopCloseHandler struct {
	message.Handler
	HandleInterceptor func(msg message.ImmutableMessage, handle func(message.ImmutableMessage))
}

// Handle is the callback for handling message.
func (nch nopCloseHandler) Handle(msg message.ImmutableMessage) {
	if nch.HandleInterceptor != nil {
		nch.HandleInterceptor(msg, nch.Handler.Handle)
		return
	}
	nch.Handler.Handle(msg)
}

// Close is called after all messages are handled or handling is interrupted.
func (nch nopCloseHandler) Close() {
}
