package rocksmq

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/log"
	server "github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

type client struct {
	server          RocksMQ
	producerOptions []ProducerOptions
	consumerOptions []ConsumerOptions
	context         context.Context
	cancel          context.CancelFunc
}

func newClient(options ClientOptions) (*client, error) {
	if options.Server == nil {
		return nil, newError(InvalidConfiguration, "Server is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		server:          options.Server,
		producerOptions: []ProducerOptions{},
		context:         ctx,
		cancel:          cancel,
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
	//for _, con := range c.consumers {
	//	log.Debug(con.Topic() + "---------------" + con.Subscription())
	//	if con.Topic() == options.Topic && con.Subscription() == options.SubscriptionName {
	//		log.Debug("consumer existed")
	//		return con, nil
	//	}
	//}
	if exist, con := c.server.ExistConsumerGroup(options.Topic, options.SubscriptionName); exist {
		log.Debug("EXISTED")
		consumer, err := newConsumer1(c, options, con.MsgMutex)
		if err != nil {
			return nil, err
		}
		go consume(c.context, consumer)
		return consumer, nil
	}
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
	go consume(c.context, consumer)
	c.consumerOptions = append(c.consumerOptions, options)

	return consumer, nil
}

func consume(ctx context.Context, consumer *consumer) {
	for {
		select {
		case <-ctx.Done():
			log.Debug("client finished")
			return
		case _, ok := <-consumer.MsgMutex():
			log.Debug("Before consume")
			if !ok {
				// consumer MsgMutex closed, goroutine exit
				log.Debug("consumer MsgMutex closed")
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
					//log.Debug("Consumer's goroutine cannot consume from (" + consumer.topic +
					//	"," + consumer.consumerName + "): message len(" + strconv.Itoa(len(msg)) +
					//	") is not 1")
					break
				}

				consumer.messageCh <- ConsumerMessage{
					MsgID:   msg[0].MsgID,
					Payload: msg[0].Payload,
				}
			}
		}
	}
}

func (c *client) Close() {
	// TODO: free resources
	for _, opt := range c.consumerOptions {
		log.Debug("Close" + opt.Topic + "+" + opt.SubscriptionName)
		_ = c.server.DestroyConsumerGroup(opt.Topic, opt.SubscriptionName)
		//TODO(yukun): Should topic be closed?
		//_ = c.server.DestroyTopic(opt.Topic)
	}
	c.cancel()
}
