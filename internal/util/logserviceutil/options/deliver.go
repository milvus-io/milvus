package options

import (
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

const (
	deliverOrderTimetick DeliverOrder = 1
)

// DeliverOrder is the order of delivering messages.
type (
	DeliverOrder  int
	DeliverPolicy *logpb.DeliverPolicy
)

// DeliverPolicyAll delivers all messages.
func DeliverPolicyAll() DeliverPolicy {
	return logpb.NewDeliverAll()
}

// DeliverLatest delivers the latest message.
func DeliverPolicyLatest() DeliverPolicy {
	return logpb.NewDeliverLatest()
}

// DeliverEarliest delivers the earliest message.
func DeliverPolicyStartFrom(messageID message.MessageID) DeliverPolicy {
	return logpb.NewDeliverStartFrom(&logpb.MessageID{
		Id: messageID.Marshal(),
	})
}

// DeliverPolicyStartAfter delivers the message after the specified message.
func DeliverPolicyStartAfter(messageID message.MessageID) DeliverPolicy {
	return logpb.NewDeliverStartAfter(&logpb.MessageID{
		Id: messageID.Marshal(),
	})
}

// DeliverOrderTimeTick delivers messages by time tick.
func DeliverOrderTimeTick() DeliverOrder {
	return deliverOrderTimetick
}
