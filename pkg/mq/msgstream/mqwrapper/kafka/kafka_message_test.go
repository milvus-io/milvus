package kafka

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestKafkaMessage_All(t *testing.T) {
	topic := "t"
	msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0}, Value: nil, Headers: nil}
	km := &kafkaMessage{msg: msg}
	properties := make(map[string]string)
	assert.Equal(t, topic, km.Topic())
	assert.Equal(t, int64(0), km.ID().(*KafkaID).MessageID)
	assert.Nil(t, km.Payload())
	assert.Equal(t, properties, km.Properties())
}
