package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
)

type kafkaMessage struct {
	msg *kafka.Message
}

func (km *kafkaMessage) Topic() string {
	return *km.msg.TopicPartition.Topic
}

func (km *kafkaMessage) Properties() map[string]string {
	return nil
}

func (km *kafkaMessage) Payload() []byte {
	return km.msg.Value
}

func (km *kafkaMessage) ID() mqwrapper.MessageID {
	kid := &kafkaID{messageID: int64(km.msg.TopicPartition.Offset)}
	return kid
}
