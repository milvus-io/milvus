package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func optionalBalanceFixture() (*balancercache.Cache, qviews.ShardID) {
	c := replicaCache(1, 2)
	id := cacheShard(1, 10)
	c.PublishShard(id, upStats(qviews.DataVersion{StreamingVersion: 1}, placement(1000, 1, 1, coordview.SegmentStateUp)))
	publishBackgroundRows(c, map[int64]int64{1: 10000})
	value := DefaultBalanceConfig()
	value.AutoBalance = false
	value.StickinessWeight, value.FanoutWeight = 0, 0
	c.UpdateBalanceConfig(value)
	return c, id
}

func TestAutoBalanceOnlyGatesOptionalOptimization(t *testing.T) {
	c, id := optionalBalanceFixture()
	policy := NewDefaultBalancePolicy()
	plan := policy.Plan(c, []qviews.ShardID{id})
	require.Empty(t, plan.Prepares)
	require.Empty(t, plan.Retries)
	value := *c.GetBalanceConfig()
	value.AutoBalance = true
	c.UpdateBalanceConfig(&value)
	plan = policy.Plan(c, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	require.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[id])[1000])
}

func TestAutoBalanceDisabledStillMaintainsViews(t *testing.T) {
	for _, scenario := range []string{"initial load", "node loss", "data update", "config update", "release"} {
		t.Run(scenario, func(t *testing.T) {
			c, id := optionalBalanceFixture()
			switch scenario {
			case "initial load":
				c.PublishShard(id, nil)
			case "node loss":
				c.PublishNode(1, nil)
			case "data update":
				data := cacheData(1, id.VChannel, 100)
				data.DataVersion = qviews.DataVersion{StreamingVersion: 2}
				c.PublishDataView(1, data)
			case "config update":
				cfg := cfgFor(1, 10, nil, nil)
				c.PublishLoadConfig(1, cfg, 2)
			case "release":
				c.PublishLoadConfig(1, nil, 2)
			}
			plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{id})
			if scenario == "release" {
				require.Contains(t, plan.Releases, id)
			} else {
				require.Contains(t, plan.Prepares, id)
			}
			require.Empty(t, plan.Retries)
		})
	}
}

func TestAutoBalanceDisabledStillConvergesReplicaIsolation(t *testing.T) {
	c := replicaCache(2, 2)
	value := DefaultBalanceConfig()
	value.AutoBalance = false
	c.UpdateBalanceConfig(value)
	a, b := cacheShard(1, 10), cacheShard(1, 11)
	for _, id := range []qviews.ShardID{a, b} {
		c.PublishShard(id, upStats(qviews.DataVersion{StreamingVersion: 1}, placement(1000, 1, 1, coordview.SegmentStateUp)))
	}
	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{a, b})
	require.Len(t, plan.Prepares, 1)
	for _, builder := range plan.Prepares {
		require.Equal(t, int64(2), assignmentsFromBuilder(builder)[1000])
	}
}
