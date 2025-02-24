package consumer

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
)

var _ ResumableConsumer = (*resumableConsumerImpl)(nil)

// ConsumerOptions is the options for creating a consumer.
type ConsumerOptions struct {
	// PChannel is the pchannel of the consumer.
	PChannel string

	// VChannel is the vchannel of the consumer.
	VChannel string

	// DeliverPolicy is the deliver policy of the consumer.
	DeliverPolicy options.DeliverPolicy

	// DeliverFilters is the deliver filters of the consumer.
	DeliverFilters []options.DeliverFilter

	// Handler is the message handler used to handle message after recv from consumer.
	MessageHandler message.Handler
}

// ResumableConsumer is the interface for consuming message to log service.
// ResumableConsumer select a right log node to consume automatically.
// ResumableConsumer will do automatic resume from stream broken and log node re-balance.
// All error in these package should be marked by streamingservice/errs package.
type ResumableConsumer interface {
	// Done returns a channel which will be closed when scanner is finished or closed.
	Done() <-chan struct{}

	// Error returns the error of the Consumer.
	Error() error

	// Close the scanner, release the underlying resources.
	Close()
}
