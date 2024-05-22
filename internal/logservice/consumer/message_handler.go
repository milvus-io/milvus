package consumer

import (
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// messageHandler is a message handler that will do metrics and record the last sent message id.
type messageHandler struct {
	inner         message.Handler
	lastMessageID message.MessageID
}

func (mh *messageHandler) Handle(msg message.ImmutableMessage) {
	id := msg.MessageID()
	messageSize := msg.EstimateSize()

	mh.inner.Handle(msg)

	mh.lastMessageID = id
	// Do a metric here.
	metrics.LogServiceClientConsumeBytes.WithLabelValues(paramtable.GetStringNodeID()).Observe(float64(messageSize))
}

func (mh *messageHandler) Close() {
	mh.inner.Close()
}
