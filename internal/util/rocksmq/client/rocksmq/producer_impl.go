package rocksmq

import (
	server "github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

type producer struct {
	// client which the producer belong to
	c     *client
	topic string
}

func newProducer(c *client, options ProducerOptions) (*producer, error) {
	if c == nil {
		return nil, newError(InvalidConfiguration, "client is nil")
	}

	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is empty")
	}

	return &producer{
		c:     c,
		topic: options.Topic,
	}, nil
}

func (p *producer) Topic() string {
	return p.topic
}

func (p *producer) Send(message *ProducerMessage) error {
	return p.c.server.Produce(p.topic, []server.ProducerMessage{
		{
			Payload: message.Payload,
		},
	})
}
