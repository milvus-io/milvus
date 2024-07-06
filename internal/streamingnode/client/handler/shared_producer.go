package handler

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ producer.Producer = (*sharedProducer)(nil)

// newSharedProducer creates a shared producer.
func newSharedProducer(ref *typeutil.SharedReference[Producer]) sharedProducer {
	return sharedProducer{
		SharedReference: ref,
	}
}

// sharedProducer is a shared producer.
type sharedProducer struct {
	*typeutil.SharedReference[Producer]
}

// Clone clones the shared producer.
func (sp sharedProducer) Clone() *sharedProducer {
	return &sharedProducer{
		SharedReference: sp.SharedReference.Clone(),
	}
}

// Assignment returns the assignment of the producer.
func (sp sharedProducer) Assignment() types.PChannelInfoAssigned {
	return sp.Deref().Assignment()
}

// Produce sends the produce message to server.
func (sp sharedProducer) Produce(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	return sp.Deref().Produce(ctx, msg)
}

// Check if a producer is available.
func (sp sharedProducer) IsAvailable() bool {
	return sp.Deref().IsAvailable()
}

// Available returns a channel that will be closed when the producer is unavailable.
func (sp sharedProducer) Available() <-chan struct{} {
	return sp.Deref().Available()
}
