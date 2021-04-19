package rocksmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsumer(t *testing.T) {
	consumer, err := newConsumer(nil, ConsumerOptions{
		Topic:                       newTopicName(),
		SubscriptionName:            newConsumerName(),
		SubscriptionInitialPosition: SubscriptionPositionLatest,
	})
	assert.Nil(t, consumer)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())

	consumer, err = newConsumer(newMockClient(), ConsumerOptions{})
	assert.Nil(t, consumer)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())

	consumer, err = newConsumer(newMockClient(), ConsumerOptions{
		Topic: newTopicName(),
	})
	assert.Nil(t, consumer)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())
}

func TestSubscription(t *testing.T) {
	topicName := newTopicName()
	consumerName := newConsumerName()
	consumer, err := newConsumer(newMockClient(), ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: consumerName,
	})
	assert.Nil(t, consumer)
	assert.NotNil(t, err)
	//assert.Equal(t, consumerName, consumer.Subscription())
}
