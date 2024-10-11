package consumer

import (
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// timeTickOrderMessageHandler is a message handler that will do metrics and record the last sent message id.
type timeTickOrderMessageHandler struct {
	inner                  message.Handler
	lastConfirmedMessageID message.MessageID
	lastTimeTick           uint64
}

func (mh *timeTickOrderMessageHandler) Handle(msg message.ImmutableMessage) {
	lastConfirmedMessageID := msg.LastConfirmedMessageID()
	timetick := msg.TimeTick()

	mh.inner.Handle(msg)

	mh.lastConfirmedMessageID = lastConfirmedMessageID
	mh.lastTimeTick = timetick
}

func (mh *timeTickOrderMessageHandler) Close() {
	mh.inner.Close()
}
