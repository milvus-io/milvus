package rocksmq

import (
	server "github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
)

type RocksMQ = server.RocksMQ

func NewClient(options ClientOptions) (Client, error) {
	return newClient(options)
}

type ClientOptions struct {
	server *RocksMQ
}

type Client interface {
	// Create a producer instance
	CreateProducer(options ProducerOptions) (Producer, error)

	// Create a consumer instance and subscribe a topic
	Subscribe(options ConsumerOptions) (Consumer, error)

	// Close the client and free associated resources
	Close()
}
