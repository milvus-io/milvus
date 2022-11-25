package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
)

type kafkaMessage struct {
	msg *kafka.Message
}

func (km *kafkaMessage) Topic() string {
	return *km.msg.TopicPartition.Topic
}

func (km *kafkaMessage) Properties() map[string]string {
	if len(km.msg.Headers) == 0 {
		return nil
	}
	var properties map[string]string
	for i := 0; i < len(km.msg.Headers); i++ {
		if _, ok := properties[km.msg.Headers[i].Key]; ok {
			log.Info("Repeated key in kafka message headers")
		}
		properties[km.msg.Headers[i].Key] = string(km.msg.Headers[i].Value)
	}
	return properties
}

func (km *kafkaMessage) Payload() []byte {
	return km.msg.Value
}

func (km *kafkaMessage) ID() mqwrapper.MessageID {
	kid := &kafkaID{messageID: int64(km.msg.TopicPartition.Offset)}
	return kid
}
