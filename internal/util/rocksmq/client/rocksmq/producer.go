package rocksmq

type ProducerOptions struct {
	Topic string
}

type ProducerMessage struct {
	Payload []byte
}

type Producer interface {
	// return the topic which producer is publishing to
	Topic() string

	// publish a message
	Send(message *ProducerMessage) error
}
