package options

import (
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

// ProducerOptions is the options for creating a producer.
type ProducerOptions struct {
	Channel string
}

// ConsumerOptions is the options for creating a consumer.
type ConsumerOptions struct {
	// Channel is the channel name of the consumer.
	Channel string

	// DeliverPolicy is the deliver policy of the consumer.
	DeliverPolicy DeliverPolicy

	// Handler is the message handler used to handle message after recv from consumer.
	MessageHandler message.Handler
}
