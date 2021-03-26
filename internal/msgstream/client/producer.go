package client

import "context"

type ProducerOptions struct {
	Topic string
}

type ProducerMessage struct {
	Payload    []byte
	Properties map[string]string
}

type Producer interface {
	// return the topic which producer is publishing to
	//Topic() string

	// publish a message
	Send(ctx context.Context, message *ProducerMessage) error

	Close()
}
