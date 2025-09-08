package streaming

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ Broadcast = broadcast{}

type broadcast struct {
	*walAccesserImpl
}

func (b broadcast) Append(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	assertValidBroadcastMessage(msg)
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer b.lifetime.Done()

	// The broadcast operation will be sent to the coordinator.
	// The coordinator will dispatch the message to all the vchannels with an eventual consistency guarantee.
	return b.streamingCoordClient.Broadcast().Broadcast(ctx, msg)
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
