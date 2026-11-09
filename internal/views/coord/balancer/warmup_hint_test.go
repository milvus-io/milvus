package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestWarmupEpochRebuildsUpViewWithTheEpoch(t *testing.T) {
	cfg := cfgFor(1, 10, []int64{1}, nil)
	cfg.WarmupEpoch = 42
	c := newTestCache(cfg)
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	config := *c.GetBalanceConfig()
	config.OptimizationEnabled = false
	c.UpdateBalanceConfig(&config)
	version := c.GetCollection(1).ConfigVersion()
	reader := func(upWarmupEpoch int64) *planningContext {
		stats := testShardStats(ver(1, 0, 1), version, placement(1000, 1, 1, coordview.SegmentStateUp))
		stats.UpWarmupEpoch = upWarmupEpoch
		c.PublishShard(id, stats)
		reader := newPlanningContext(c)
		reader.schemaVersions = map[int64]int32{1: 0}
		return reader
	}

	require.Equal(t, actionMust, classifyShard(reader(0), id), "QueryNodes learn a new epoch from a rebuilt view")
	require.Equal(t, actionMust, classifyShard(reader(41), id))
	require.Equal(t, actionMayOptimize, classifyShard(reader(42), id))

	plan := NewDefaultBalancePolicy().Plan(reader(41), []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	meta := plan.Prepares[id].Build().GetMeta()
	require.EqualValues(t, 42, meta.GetWarmupEpoch())
	require.Equal(t, version, meta.GetLoadInfoVersion(), "the epoch is not part of the load identity")
}
