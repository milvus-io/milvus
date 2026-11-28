package balancer

import (
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type TriggerScope = api.TriggerScope

// parseShardCollection extracts the collection ID encoded in a shard vchannel.
// Planning falls back to the replica index when the channel cannot be parsed.
func parseShardCollection(shardID qviews.ShardID) (int64, bool) {
	_, id, _, err := funcutil.ParseVChannel(shardID.VChannel)
	return id, err == nil
}
