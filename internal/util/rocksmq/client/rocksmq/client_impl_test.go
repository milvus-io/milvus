package rocksmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	client, err := NewClient(ClientOptions{})
	assert.NotNil(t, client)
	assert.Nil(t, err)
}

func TestCreateProducer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		Server: newMockRocksMQ(),
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})
	assert.Error(t, err)
	assert.Nil(t, producer)

	client.Close()
}

func TestSubscribe(t *testing.T) {
	client, err := NewClient(ClientOptions{
		Server: newMockRocksMQ(),
	})
	assert.NoError(t, err)

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            newTopicName(),
		SubscriptionName: newConsumerName(),
	})
	assert.Error(t, err)
	assert.Nil(t, consumer)

	client.Close()
}
