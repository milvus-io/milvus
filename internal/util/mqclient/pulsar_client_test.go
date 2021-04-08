package mqclient

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestNewPulsarClient(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)
}

func TestPulsarCreateProducer(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)

	topic := "test_CreateProducer"
	producer, err := pc.CreateProducer(ProducerOptions{Topic: topic})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
}

func TestPulsarSubscribe(t *testing.T) {
	pulsarAddress, _ := Params.Load("_PulsarAddress")
	pc, err := NewPulsarClient(pulsar.ClientOptions{URL: pulsarAddress})
	defer pc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, pc)

	topic := "test_Subscribe"
	subName := "subName"
	consumer, err := pc.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		Type:                        KeyShared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
}
