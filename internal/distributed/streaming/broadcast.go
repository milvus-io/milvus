package streaming

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
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

func (b broadcast) Ack(ctx context.Context, req types.BroadcastAckRequest) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return ErrWALAccesserClosed
	}
	defer b.lifetime.Done()

	return b.streamingCoordClient.Broadcast().Ack(ctx, req)
}

func (b broadcast) BlockUntilResourceKeyAckOnce(ctx context.Context, rk message.ResourceKey) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return ErrWALAccesserClosed
	}
	defer b.lifetime.Done()
	return b.streamingCoordClient.Broadcast().BlockUntilEvent(ctx, message.NewResourceKeyAckOneBroadcastEvent(rk))
}

func (b broadcast) BlockUntilResourceKeyAckAll(ctx context.Context, rk message.ResourceKey) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return ErrWALAccesserClosed
	}
	defer b.lifetime.Done()
	return b.streamingCoordClient.Broadcast().BlockUntilEvent(ctx, message.NewResourceKeyAckAllBroadcastEvent(rk))
}
