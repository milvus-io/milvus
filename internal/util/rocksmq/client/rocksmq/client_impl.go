package rocksmq

type client struct {
	server *RocksMQ
}

func newClient(options ClientOptions) (*client, error) {
	if options.server == nil {
		return nil, newError(InvalidConfiguration, "Server is nil")
	}

	c := &client{
		server: options.server,
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
	err = c.server.CreateChannel(options.Topic)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func (c *client) Subscribe(options ConsumerOptions) (Consumer, error) {
	// Create a consumer
	consumer, err := newConsumer(c, options)
	if err != nil {
		return nil, err
	}

	// Create a consumergroup in rocksmq, raise error if consumergroup exists
	_, err = c.server.CreateConsumerGroup(options.SubscriptionName, options.Topic)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func (c *client) Close() {
	// TODO: free resources
}
