package consumer

import (
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	messageSize := msg.EstimateSize()

	mh.inner.Handle(msg)

	mh.lastConfirmedMessageID = lastConfirmedMessageID
	mh.lastTimeTick = timetick
	// Do a metric here.
	metrics.StreamingServiceClientConsumeBytes.WithLabelValues(paramtable.GetStringNodeID()).Observe(float64(messageSize))
}

func (mh *timeTickOrderMessageHandler) Close() {
	mh.inner.Close()
}
