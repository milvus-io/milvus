package streaming

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ Broadcast = broadcast{}

type broadcast struct {
	*walAccesserImpl
}

func (b broadcast) Ack(ctx context.Context, msg message.ImmutableMessage) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		// should be an unreachable error.
		return ErrWALAccesserClosed
	}
	defer b.lifetime.Done()

	// Retry belongs to the caller's scheduler, so an unavailable coordinator
	// cannot occupy a worker indefinitely.
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	return b.streamingCoordClient.Broadcast().Ack(ctx, msg)
}
