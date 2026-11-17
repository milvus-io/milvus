package broadcaster

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type waitForAcknowledgement struct{}

// WithWaitForAcknowledgement returns after durable channel acknowledgements.
// The broadcaster retains the resource guards until the callback completes.
func WithWaitForAcknowledgement(ctx context.Context) context.Context {
	return context.WithValue(ctx, waitForAcknowledgement{}, true)
}

func (b *broadcastTask) waitForResponse(ctx context.Context) (*types.BroadcastAppendResult, error) {
	if ctx.Value(waitForAcknowledgement{}) != true {
		return b.BlockUntilDone(ctx)
	}
	if err := b.BlockUntilAllAck(ctx); err != nil {
		return nil, err
	}
	_, result := b.BroadcastResult()
	return &types.BroadcastAppendResult{BroadcastID: b.Header().BroadcastID, AppendResults: result}, nil
}
