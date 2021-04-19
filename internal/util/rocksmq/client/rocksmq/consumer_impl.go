package rocksmq

import (
	"context"
)

type consumer struct {
	topic        string
	client       *client
	consumerName string
	options      ConsumerOptions

	messageCh chan ConsumerMessage
}

func newConsumer(c *client, options ConsumerOptions) (*consumer, error) {
	if c == nil {
		return nil, newError(InvalidConfiguration, "client is nil")
	}

	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is empty")
	}

	if options.SubscriptionName == "" {
		return nil, newError(InvalidConfiguration, "SubscriptionName is empty")
	}

	messageCh := options.MessageChannel
	if options.MessageChannel == nil {
		messageCh = make(chan ConsumerMessage, 10)
	}

	return &consumer{
		topic:        options.Topic,
		client:       c,
		consumerName: options.SubscriptionName,
		options:      options,
		messageCh:    messageCh,
	}, nil
}

func (c *consumer) Subscription() string {
	return c.consumerName
}

func (c *consumer) Receive(ctx context.Context) (ConsumerMessage, error) {
	msgs, err := c.client.server.Consume(c.consumerName, c.topic, 1)
	if err != nil {
		return ConsumerMessage{}, err
	}

	if len(msgs) == 0 {
		return ConsumerMessage{}, nil
	}

	return ConsumerMessage{
		Payload: msgs[0].Payload,
	}, nil
}
