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

// Append is deprecated and should not be used.
// Import operations now call DataCoord directly, which handles broadcasting internally.
// This method is kept for backward compatibility but will be removed in future versions.
func (b broadcast) Append(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	// Cross-RPC broadcast has been removed as part of import refactoring.
	// All broadcast operations should now go through the local broadcaster in coordinators.
	panic("broadcast.Append() has been deprecated - use coordinator's internal broadcaster instead")
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
