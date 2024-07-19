package producer

import (
	"context"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

var _ Producer = (*producerImpl)(nil)

// Producer is the interface that wraps the basic produce method on grpc stream.
// Producer is work on a single stream on grpc,
// so Producer cannot recover from failure because of the stream is broken.
type Producer interface {
	// Assignment returns the assignment of the producer.
	Assignment() types.PChannelInfoAssigned

	// Produce sends the produce message to server.
	Produce(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Check if a producer is available.
	IsAvailable() bool

	// Available returns a channel that will be closed when the producer is unavailable.
	Available() <-chan struct{}

	// Close close the producer client.
	Close()
}
