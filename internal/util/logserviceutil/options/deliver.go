package options

import (
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

type DeliverPolicy *logpb.DeliverPolicy

func DeliverAll() DeliverPolicy {
	return logpb.NewDeliverAll()
}

func DeliverLatest() DeliverPolicy {
	return logpb.NewDeliverLatest()
}

func DeliverStartFrom(messageID message.MessageID) DeliverPolicy {
	return logpb.NewDeliverStartFrom(
		message.NewPBMessageIDFromMessageID(messageID),
	)
}

func DeliverStartAfter(messageID message.MessageID) DeliverPolicy {
	return logpb.NewDeliverStartAfter(
		message.NewPBMessageIDFromMessageID(messageID),
	)
}
