package nmq

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

func TestNatsConsumer_Subscription(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	proOpts := mqwrapper.ProducerOptions{Topic: topic}
	_, err = client.CreateProducer(proOpts)
	assert.NoError(t, err)

	consumer, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            topic,
		SubscriptionInitialPosition: mqwrapper.SubscriptionPositionEarliest,
		BufSize:                     1024,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	str := consumer.Subscription()
	assert.NotNil(t, str)
}

func Test_PatchEarliestMessageID(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()
	mid := client.EarliestMessageID()

	assert.NotNil(t, mid)
	assert.Equal(t, mid.(*nmqID).messageID, uint64(1))
}

func TestComsumeMessage(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	p, err := client.CreateProducer(mqwrapper.ProducerOptions{Topic: topic})
	assert.NoError(t, err)

	c, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: topic,
	})
	assert.NoError(t, err)
	defer c.Close()

	msg := []byte("test the first message")
	_, err = p.Send(context.Background(), &mqwrapper.ProducerMessage{
		Payload:    msg,
		Properties: map[string]string{},
	})
	assert.NoError(t, err)
	recvMsg, err := c.(*Consumer).sub.NextMsg(1 * time.Second)
	assert.NoError(t, err)
	recvMsg.Ack()
	assert.Equal(t, msg, recvMsg.Data)

	msg2 := []byte("test the second message")
	_, err = p.Send(context.Background(), &mqwrapper.ProducerMessage{
		Payload: msg2,
	})
	assert.NoError(t, err)
	recvMsg, err = c.(*Consumer).sub.NextMsg(1 * time.Second)
	assert.NoError(t, err)
	recvMsg.Ack()
	assert.Equal(t, msg2, recvMsg.Data)

	assert.Nil(t, err)
	assert.NotNil(t, c)
}

func TestNatsConsumer_Close(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	c, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: topic,
	})
	assert.Nil(t, err)
	assert.NotNil(t, c)

	str := c.Subscription()
	assert.NotNil(t, str)

	c.Close()

	// test double close
	assert.Panics(t, func() { c.Close() }, "Should panic on consumer double close")
}

func TestNatsClientUnsubscribeTwice(t *testing.T) {
	topic := t.Name()
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	consumer, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: topic,
	})
	defer consumer.Close()
	assert.NoError(t, err)

	err = consumer.(*Consumer).sub.Unsubscribe()
	assert.NoError(t, err)
	err = consumer.(*Consumer).sub.Unsubscribe()
	assert.True(t, strings.Contains(err.Error(), "invalid subscription"))
	t.Log(err)
}

func TestCheckPreTopicValid(t *testing.T) {
	client, err := createNmqClient()
	assert.NoError(t, err)
	defer client.Close()

	topic := t.Name()
	consumer, err := client.Subscribe(mqwrapper.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: topic,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	str := consumer.Subscription()
	assert.NotNil(t, str)

	err = consumer.CheckTopicValid(topic)
	assert.NoError(t, err)
}
