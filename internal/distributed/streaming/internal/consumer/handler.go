package consumer

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// nopCloseHandler is a handler that do nothing when close.
type nopCloseHandler struct {
	message.Handler
	HandleInterceptor func(handleParam message.HandleParam, h message.Handler) message.HandleResult
}

// Handle is the callback for handling message.
func (nch nopCloseHandler) Handle(handleParam message.HandleParam) message.HandleResult {
	if nch.HandleInterceptor != nil {
		return nch.HandleInterceptor(handleParam, nch.Handler)
	}
	return nch.Handler.Handle(handleParam)
}

// Close is called after all messages are handled or handling is interrupted.
func (nch nopCloseHandler) Close() {
}
