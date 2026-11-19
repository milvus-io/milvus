package shard

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (impl *shardInterceptor) handleRetireSplitShard(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	header := message.MustAsMutableRetireSplitShardMessageV2(msg).Header()
	if !msg.IsUnreplicable() || msg.ReplicateHeader() != nil || header.GetSplitTaskId() == 0 ||
		!shards.IsSplitFenced(impl.shardManager, msg.VChannel()) {
		return nil, status.NewUnrecoverableError("split retirement must be local and name a fenced source")
	}
	// FenceForSplit checks collection, vchannel and task identity. The source
	// is fenced already, so this changes nothing and seals no further segment.
	if _, _, err := impl.shardManager.FenceForSplit(header.GetCollectionId(), msg.VChannel(), header.GetSplitTaskId(), msg.TimeTick()); err != nil {
		return nil, status.NewUnrecoverableError("split retirement does not match the fenced source collection and task")
	}
	return appendOp(ctx, msg)
}
