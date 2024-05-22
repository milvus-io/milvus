package logservice

import (
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

type mqMessageWrapper struct {
	inner   message.ImmutableMessage
	channel string
}

func (w *mqMessageWrapper) Topic() string {
	return w.channel
}

func (w *mqMessageWrapper) Payload() []byte {
	return w.inner.Payload()
}

func (w *mqMessageWrapper) Properties() map[string]string {
	return w.inner.Properties().ToRawMap()
}

func (w *mqMessageWrapper) ID() mqwrapper.MessageID {
	return w.inner.MessageID()
}

type msgHandler struct {
	ch      chan mqwrapper.Message
	channel string
}

func (h *msgHandler) Handle(msg message.ImmutableMessage) {
	h.ch <- &mqMessageWrapper{
		inner:   msg,
		channel: h.channel,
	}
}

func (h *msgHandler) Close() {
	close(h.ch)
}
