package adaptor

import (
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/mqimpl/rocksmq/server"
	mqpulsar "github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/pulsar"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	msgpulsar "github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/rmq"
)

// MustGetMQWrapperIDFromMessage converts message.MessageID to common.MessageID
// TODO: should be removed in future after common.MessageID is removed
func MustGetMQWrapperIDFromMessage(messageID message.MessageID) common.MessageID {
	if id, ok := messageID.(interface{ PulsarID() pulsar.MessageID }); ok {
		return mqpulsar.NewPulsarID(id.PulsarID())
	} else if id, ok := messageID.(interface{ RmqID() int64 }); ok {
		return &server.RmqID{MessageID: id.RmqID()}
	}
	panic("unsupported now")
}

// MustGetMessageIDFromMQWrapperID converts common.MessageID to message.MessageID
// TODO: should be removed in future after common.MessageID is removed
func MustGetMessageIDFromMQWrapperID(commonMessageID common.MessageID) message.MessageID {
	if id, ok := commonMessageID.(interface{ PulsarID() pulsar.MessageID }); ok {
		return msgpulsar.NewPulsarID(id.PulsarID())
	} else if id, ok := commonMessageID.(*server.RmqID); ok {
		return rmq.NewRmqID(id.MessageID)
	}
	return nil
}
