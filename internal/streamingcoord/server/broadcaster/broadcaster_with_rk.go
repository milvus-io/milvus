package broadcaster

import (
	"context"

	"go.opentelemetry.io/otel/codes"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type broadcasterWithRK struct {
	broadcaster *broadcastTaskManager
	broadcastID uint64
	guards      *lockGuards
}

func (b *broadcasterWithRK) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	// The idempotency lookup lives here rather than in an exported check method so
	// it cannot be called before the resource keys are held: this object only
	// exists once StartBroadcastWithResourceKeys acquired them. Without that
	// ordering two concurrent same-key requests would both miss and create two
	// tasks. The guards are deliberately NOT consumed on a hit, so the caller's
	// deferred Close() releases the locks.
	if key := message.IdempotencyKeyOf(msg); key != "" {
		if dup, ok := b.broadcaster.getDuplicatedBroadcastMessage(key); ok {
			return &types.BroadcastAppendResult{
				BroadcastID: dup.BroadcastHeader().BroadcastID,
				Duplicated:  dup,
			}, nil
		}
	}

	// Consume the guards before handing them to broadcast to avoid double unlock.
	guards := b.guards
	b.guards = nil
	msg = msg.OverwriteBroadcastHeader(b.broadcastID, guards.ResourceKeys()...)
	ctx, span := message.StartSpanForMessage(ctx, msg, message.SpanNameWALBroadcast)
	defer span.End()

	// Keep a trace context in the broadcast message so that the DDL ack callback
	// can still extract it after the original caller span is long gone.
	message.InjectTraceContext(ctx, msg)

	result, err := b.broadcaster.broadcast(ctx, msg, b.broadcastID, guards)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return result, err
}

func (b *broadcasterWithRK) Close() {
	if b.guards != nil {
		b.guards.Unlock()
	}
}
