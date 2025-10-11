package broadcaster

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type broadcasterWithRK struct {
	broadcaster *broadcastTaskManager
	broadcastID uint64
	guards      *lockGuards
}

func (b *broadcasterWithRK) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
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
