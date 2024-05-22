package producer

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

var _ ResumableProducer = (*resumableProducerImpl)(nil)

// ResumableProducer is a interface for producing message to log service.
// ResumableProducer select a right log node to produce automatically.
// ResumableProducer will do automatic resume from stream broken and log node re-balance.
// All error in these package should be marked by logservice/errs package.
type ResumableProducer interface {
	// Channel returns the channel of producer.
	Channel() string

	// Produce produce a new message to log service.
	Produce(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Close close the producer client.
	Close()
}
