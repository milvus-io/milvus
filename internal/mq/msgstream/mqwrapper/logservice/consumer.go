package logservice

import (
	"context"
	"errors"
	"sync"

	"github.com/milvus-io/milvus/internal/logservice"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
)

var _ mqwrapper.Consumer = (*consumer)(nil)

type consumer struct {
	client           logservice.Client // TODO: client shouldn't assign here, remove in future after disable seek.
	channelName      string
	subscriptionName string // TODO: subscriptionName is a fake name, remove in future. all consume operation is stateless, so subscriptionName is not needed.
	// Stateful subscription like pulsar will handled by logcoord to clean up.
	deliverPolicy options.DeliverPolicy
	bufSize       int

	// runtime field.
	consumer        logservice.Consumer
	latestMessageID mqwrapper.MessageID
	ch              chan mqwrapper.Message
	once            sync.Once
}

// returns the subscription for the consumer
func (c *consumer) Subscription() string {
	return c.subscriptionName
}

// Get Message channel, once you chan you can not seek again
func (c *consumer) Chan() <-chan mqwrapper.Message {
	c.once.Do(func() {
		c.ch = make(chan mqwrapper.Message, c.bufSize)
		c.consumer = c.client.CreateConsumer(&options.ConsumerOptions{
			Channel:       c.channelName,
			DeliverPolicy: c.deliverPolicy,
			MessageHandler: &msgHandler{
				ch:      c.ch,
				channel: c.channelName,
			},
		})
	})
	return c.ch
}

// Seek to the uniqueID position
func (c *consumer) Seek(msgID mqwrapper.MessageID, inclusive bool) error {
	if c.ch != nil {
		return errors.New("Seek should be called before Chan")
	}

	if inclusive {
		c.deliverPolicy = options.DeliverStartFrom(msgID)
		return nil
	}
	c.deliverPolicy = options.DeliverStartAfter(msgID)
	return nil
}

// Ack make sure that msg is received
func (c *consumer) Ack(mqwrapper.Message) {
	// ACK is done by logservice consumer on server side.
	// do nothing here.
}

// Close consumer
func (c *consumer) Close() {
	c.consumer.Close()
}

// GetLatestMsgID return the latest message ID
func (c *consumer) GetLatestMsgID() (mqwrapper.MessageID, error) {
	msgID, err := c.client.GetLatestMessageID(context.Background(), c.channelName)
	if err != nil {
		return nil, err
	}
	return msgID, nil
}

// check created topic whether vaild or not
func (c *consumer) CheckTopicValid(channel string) error {
	// TODO: implement it.
	return nil
}
