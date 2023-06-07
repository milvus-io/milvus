package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

type kafkaMessage struct {
	msg *kafka.Message
}

func (km *kafkaMessage) Topic() string {
	return *km.msg.TopicPartition.Topic
}

func (km *kafkaMessage) Properties() map[string]string {
	properties := make(map[string]string)
	for _, header := range km.msg.Headers {
		properties[header.Key] = string(header.Value)
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
