package broadcaster

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type broadcasterWithRK struct {
	broadcaster *broadcastTaskManager
	guards      *lockGuards
}

func (b *broadcasterWithRK) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	guards := b.guards
	b.guards = nil
	return b.broadcaster.broadcast(ctx, msg, guards)
}

func (b *broadcasterWithRK) Close() {
	if b.guards != nil {
		b.guards.Unlock()
	}
}
