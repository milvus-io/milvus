package consumer

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// timeTickOrderMessageHandler is a message handler that will record the last sent message id.
type timeTickOrderMessageHandler struct {
	inner                  message.Handler
	lastConfirmedMessageID message.MessageID
	lastTimeTick           uint64
}

func (mh *timeTickOrderMessageHandler) Handle(ctx context.Context, msg message.ImmutableMessage) (bool, error) {
	lastConfirmedMessageID := msg.LastConfirmedMessageID()
	timetick := msg.TimeTick()

	ok, err := mh.inner.Handle(ctx, msg)
	if ok {
		mh.lastConfirmedMessageID = lastConfirmedMessageID
		mh.lastTimeTick = timetick
	}
	return ok, err
}

func (mh *timeTickOrderMessageHandler) Close() {
	mh.inner.Close()
}
