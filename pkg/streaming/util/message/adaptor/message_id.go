package adaptor

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/mqimpl/rocksmq/server"
	mqpulsar "github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	msgpulsar "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
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

// DeserializeToMQWrapperID deserializes messageID bytes to common.MessageID
// TODO: should be removed in future after common.MessageID is removed
func DeserializeToMQWrapperID(msgID []byte, walName string) (common.MessageID, error) {
	switch walName {
	case "pulsar":
		pulsarID, err := mqpulsar.DeserializePulsarMsgID(msgID)
		if err != nil {
			return nil, err
		}
		return mqpulsar.NewPulsarID(pulsarID), nil
	case "rocksmq":
		rID := server.DeserializeRmqID(msgID)
		return &server.RmqID{MessageID: rID}, nil
	default:
		return nil, fmt.Errorf("unsupported mq type %s", walName)
	}
}

func MustGetMessageIDFromMQWrapperIDBytes(walName string, msgIDBytes []byte) message.MessageID {
	var commonMsgID common.MessageID
	switch walName {
	case "rocksmq":
		id := server.DeserializeRmqID(msgIDBytes)
		commonMsgID = &server.RmqID{MessageID: id}
	case "pulsar":
		msgID, err := mqpulsar.DeserializePulsarMsgID(msgIDBytes)
		if err != nil {
			panic(err)
		}
		commonMsgID = mqpulsar.NewPulsarID(msgID)
	default:
		panic("unsupported now")
	}
	return MustGetMessageIDFromMQWrapperID(commonMsgID)
}
