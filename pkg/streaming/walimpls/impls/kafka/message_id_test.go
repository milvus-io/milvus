package kafka

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

func TestMessageID(t *testing.T) {
	assert.Equal(t, kafka.Offset(1), message.MessageID(kafkaID(1)).(interface{ KafkaID() kafka.Offset }).KafkaID())

	assert.Equal(t, walName, kafkaID(1).WALName())

	assert.True(t, kafkaID(1).LT(kafkaID(2)))
	assert.True(t, kafkaID(1).EQ(kafkaID(1)))
	assert.True(t, kafkaID(1).LTE(kafkaID(1)))
	assert.True(t, kafkaID(1).LTE(kafkaID(2)))
	assert.False(t, kafkaID(2).LT(kafkaID(1)))
	assert.False(t, kafkaID(2).EQ(kafkaID(1)))
	assert.False(t, kafkaID(2).LTE(kafkaID(1)))
	assert.True(t, kafkaID(2).LTE(kafkaID(2)))

	msgID, err := UnmarshalMessageID(kafkaID(1).Marshal())
	assert.NoError(t, err)
	assert.Equal(t, kafkaID(1), msgID)

	_, err = UnmarshalMessageID(string([]byte{0x01, 0x02, 0x03, 0x04}))
	assert.Error(t, err)
}
