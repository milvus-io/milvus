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
	ctx, span := message.StartSpan(ctx, message.SpanNameWALBroadcast)
	defer span.End()

	// Keep a trace context in the broadcast message so that the DDL ack callback
	// can still extract it after the original caller span is long gone.
	message.InjectTraceContext(ctx, msg)

	// consume the guards after the broadcast is called to avoid double unlock.
	guards := b.guards
	b.guards = nil
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
