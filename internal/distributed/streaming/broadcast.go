package streaming

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	// retry until the ctx is canceled.
	return retry.Do(ctx, func() error { return b.streamingCoordClient.Broadcast().Ack(ctx, msg) }, retry.AttemptAlways())
}
