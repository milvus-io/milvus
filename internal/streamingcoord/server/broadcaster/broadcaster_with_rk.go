package broadcaster

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type broadcasterWithRK struct {
	broadcaster *broadcastTaskManager
	broadcastID uint64
	guards      *lockGuards
}

func (b *broadcasterWithRK) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	// Inject the caller's trace context into the broadcast message so that
	// the DDL ack callback (which may run post-restart after the original
	// caller span is long gone) can still extract and parent its span.
	message.InjectTraceContext(ctx, msg)

	// consume the guards after the broadcast is called to avoid double unlock.
	guards := b.guards
	b.guards = nil
	return b.broadcaster.broadcast(ctx, msg, b.broadcastID, guards)
}

func (b *broadcasterWithRK) Close() {
	if b.guards != nil {
		b.guards.Unlock()
	}
}
