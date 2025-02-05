package consumer

import (
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// timeTickOrderMessageHandler is a message handler that will record the last sent message id.
type timeTickOrderMessageHandler struct {
	inner                  message.Handler
	lastConfirmedMessageID message.MessageID
	lastTimeTick           uint64
}

func (mh *timeTickOrderMessageHandler) Handle(handleParam message.HandleParam) message.HandleResult {
	var lastConfirmedMessageID message.MessageID
	var lastTimeTick uint64
	if handleParam.Message != nil {
		lastConfirmedMessageID = handleParam.Message.LastConfirmedMessageID()
		lastTimeTick = handleParam.Message.TimeTick()
	}

	result := mh.inner.Handle(handleParam)
	if result.MessageHandled {
		mh.lastConfirmedMessageID = lastConfirmedMessageID
		mh.lastTimeTick = lastTimeTick
	}
	return result
}

func (mh *timeTickOrderMessageHandler) Close() {
	mh.inner.Close()
}
