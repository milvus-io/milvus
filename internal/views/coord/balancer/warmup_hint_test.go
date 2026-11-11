package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestAutomaticPlanningNeverAddsWarmupHint(t *testing.T) {
	cfg := cfgFor(1, 10, []int64{1}, nil)
	c := newTestCache(cfg)
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	version := c.GetCollection(1).ConfigVersion()
	stats := testShardStats(ver(1, 0, 1), version, placement(1000, 1, 1, coordview.SegmentStateUp))
	c.PublishShard(id, stats)
	reader := newPlanningContext(c)
	reader.schemaVersions = map[int64]int32{1: 0}
	require.Equal(t, actionMayOptimize, classifyShard(reader, id))
	// New data (flush/import/compaction) still requires a View, without warming it.
	next := cacheData(1, id.VChannel, 100, 200)
	next.DataVersion = qviews.DataVersion{StreamingVersion: 2}
	c.PublishDataView(1, next)
	reader = newPlanningContext(c)
	reader.schemaVersions = map[int64]int32{1: 0}
	plan := NewDefaultBalancePolicy().Plan(reader, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	require.Nil(t, plan.Prepares[id].Build().GetMeta().ProtoReflect().Descriptor().Fields().ByName("warmup_epoch"))
}
