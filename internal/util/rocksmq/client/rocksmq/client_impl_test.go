package rocksmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	client, err := NewClient(ClientOptions{})
	assert.Nil(t, client)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())
}

func TestCreateProducer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		server: newMockRocksMQ(),
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	client.Close()
}

func TestSubscribe(t *testing.T) {
	client, err := NewClient(ClientOptions{
		server: newMockRocksMQ(),
	})
	assert.NoError(t, err)

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            newTopicName(),
		SubscriptionName: newConsumerName(),
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	client.Close()
}
