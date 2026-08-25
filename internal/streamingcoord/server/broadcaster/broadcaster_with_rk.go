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
	// exists once StartBroadcastWithResourceKeys acquired them. Two concurrent
	// same-key requests are then serialized by the resource lock rather than both
	// missing -- but only if the lock they hold is an exclusive lock on the object
	// the key is scoped to. Under a shared lock both requests hold a read lock, both
	// can miss, and each creates a task; the index keeps the first broadcastID while
	// the second has already reached the WAL. The only keyed caller today (import)
	// scopes to a collection and holds that collection's exclusive key.
	//
	// The guards are deliberately NOT consumed on a hit, so the caller's deferred
	// Close() releases the locks.
	if scope := idempotencyScopeOfMessage(msg); scope != "" {
		if dup, results, ok := b.broadcaster.getOriginalBroadcast(scope); ok {
			return &types.BroadcastAppendResult{
				BroadcastID:   dup.BroadcastHeader().BroadcastID,
				AppendResults: results,
				Duplicated:    dup,
			}, nil
		}
		// Checked after the lookup, not before: the bound is admission control over new
		// index entries, so it has nothing to decide about a key that is already indexed.
		//
		// This ordering is local to the broadcaster. It is NOT a cluster guarantee that
		// lowering the bound leaves an open idempotency window alone: both doors a client
		// key enters through -- the REST middleware and the propagation interceptor -- run
		// interceptor.ValidateIdempotencyKey against the same refreshable parameter before
		// a request can reach here, so a lowered limit does reject in-window retries at the
		// edge. What this order still buys is that the broadcaster adds no second rejection
		// of its own once a key has been admitted.
		if err := validateIdempotencyKeyLength(message.IdempotencyKeyOf(msg)); err != nil {
			return nil, err
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
