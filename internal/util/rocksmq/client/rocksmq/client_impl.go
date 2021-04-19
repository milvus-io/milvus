package rocksmq

import (
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/log"
	server "github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

type client struct {
	server          RocksMQ
	producerOptions []ProducerOptions
	consumerOptions []ConsumerOptions
}

func newClient(options ClientOptions) (*client, error) {
	if options.Server == nil {
		return nil, newError(InvalidConfiguration, "Server is nil")
	}

	c := &client{
		server:          options.Server,
		producerOptions: []ProducerOptions{},
	}
	return c, nil
}

func (c *client) CreateProducer(options ProducerOptions) (Producer, error) {
	// Create a producer
	producer, err := newProducer(c, options)
	if err != nil {
		return nil, err
	}

	// Create a topic in rocksmq, ignore if topic exists
	err = c.server.CreateTopic(options.Topic)
	if err != nil {
		return nil, err
	}
	c.producerOptions = append(c.producerOptions, options)

	return producer, nil
}

func (c *client) Subscribe(options ConsumerOptions) (Consumer, error) {
	// Create a consumer
	consumer, err := newConsumer(c, options)
	if err != nil {
		return nil, err
	}

	// Create a consumergroup in rocksmq, raise error if consumergroup exists
	err = c.server.CreateConsumerGroup(options.Topic, options.SubscriptionName)
	if err != nil {
		return nil, err
	}

	// Register self in rocksmq server
	cons := &server.Consumer{
		Topic:     consumer.topic,
		GroupName: consumer.consumerName,
		MsgMutex:  consumer.msgMutex,
	}
	c.server.RegisterConsumer(cons)

	// Take messages from RocksDB and put it into consumer.Chan(),
	// trigger by consumer.MsgMutex which trigger by producer
	go func() {
		for { //nolint:gosimple
			select {
			case _, ok := <-consumer.MsgMutex():
				if !ok {
					// consumer MsgMutex closed, goroutine exit
					return
				}

				for {
					msg, err := consumer.client.server.Consume(consumer.topic, consumer.consumerName, 1)
					if err != nil {
						log.Debug("Consumer's goroutine cannot consume from (" + consumer.topic +
							"," + consumer.consumerName + "): " + err.Error())
						break
					}

					if len(msg) != 1 {
						log.Debug("Consumer's goroutine cannot consume from (" + consumer.topic +
							"," + consumer.consumerName + "): message len(" + strconv.Itoa(len(msg)) +
							") is not 1")
						break
					}

					consumer.messageCh <- ConsumerMessage{
						MsgID:   msg[0].MsgID,
						Payload: msg[0].Payload,
					}
				}
			}
		}
	}()

	return consumer, nil
}

func (c *client) Close() {
	// TODO: free resources
}
