package consumer

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

type handleFunc func(ctx context.Context, msg message.ImmutableMessage) (bool, error)

// nopCloseHandler is a handler that do nothing when close.
type nopCloseHandler struct {
	message.Handler
	HandleInterceptor func(ctx context.Context, msg message.ImmutableMessage, handle handleFunc) (bool, error)
}

// Handle is the callback for handling message.
func (nch nopCloseHandler) Handle(ctx context.Context, msg message.ImmutableMessage) (bool, error) {
	if nch.HandleInterceptor != nil {
		return nch.HandleInterceptor(ctx, msg, nch.Handler.Handle)
	}
	return nch.Handler.Handle(ctx, msg)
}

// Close is called after all messages are handled or handling is interrupted.
func (nch nopCloseHandler) Close() {
}
