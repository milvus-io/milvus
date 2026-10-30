package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (c *Core) broadcastCollectionAlter(ctx context.Context, api broadcaster.BroadcastAPI, msg message.BroadcastMutableMessage) error {
	_, err := api.Broadcast(ctx, msg)
	return err
}
