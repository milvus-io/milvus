package options

import (
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/message"
)

const (
	deliverOrderTimetick DeliverOrder = 1
)

// DeliverOrder is the order of delivering messages.
type (
	DeliverOrder  int
	DeliverPolicy *streamingpb.DeliverPolicy
)

// DeliverPolicyAll delivers all messages.
func DeliverPolicyAll() DeliverPolicy {
	return streamingpb.NewDeliverAll()
}

// DeliverLatest delivers the latest message.
func DeliverPolicyLatest() DeliverPolicy {
	return streamingpb.NewDeliverLatest()
}

// DeliverEarliest delivers the earliest message.
func DeliverPolicyStartFrom(messageID message.MessageID) DeliverPolicy {
	return streamingpb.NewDeliverStartFrom(&streamingpb.MessageID{
		Id: messageID.Marshal(),
	})
}

// DeliverPolicyStartAfter delivers the message after the specified message.
func DeliverPolicyStartAfter(messageID message.MessageID) DeliverPolicy {
	return streamingpb.NewDeliverStartAfter(&streamingpb.MessageID{
		Id: messageID.Marshal(),
	})
}

// DeliverOrderTimeTick delivers messages by time tick.
func DeliverOrderTimeTick() DeliverOrder {
	return deliverOrderTimetick
}
