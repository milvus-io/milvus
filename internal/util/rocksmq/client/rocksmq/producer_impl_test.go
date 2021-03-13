package rocksmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	// invalid client
	producer, err := newProducer(nil, ProducerOptions{
		Topic: newTopicName(),
	})
	assert.Nil(t, producer)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())

	// invalid produceroptions
	producer, err = newProducer(newMockClient(), ProducerOptions{})
	assert.Nil(t, producer)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())
}

func TestProducerTopic(t *testing.T) {
	topicName := newTopicName()
	producer, err := newProducer(newMockClient(), ProducerOptions{
		Topic: topicName,
	})
	assert.NotNil(t, producer)
	assert.Nil(t, err)
	assert.Equal(t, topicName, producer.Topic())
}
