package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestPlanningWaitsForMinimumDataVersion(t *testing.T) {
	id := cacheShard(1, 10)
	cfg := cfgFor(1, 10, []int64{1}, nil)
	cfg.MinimumDataVersion = qviews.DataVersion{StreamingVersion: 2}
	c := newTestCache(cfg)
	data := cacheData(1, id.VChannel, 100)
	data.DataVersion = qviews.DataVersion{StreamingVersion: 1}
	c.PublishDataView(1, data)
	publishTestNodes(c, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	policy := NewDefaultBalancePolicy()
	require.Empty(t, policy.Plan(c, []qviews.ShardID{id}).Prepares)
	next := cacheData(1, id.VChannel, 100)
	next.DataVersion = cfg.MinimumDataVersion
	c.PublishDataView(1, next)
	plan := policy.Plan(c, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	require.Equal(t, cfg.MinimumDataVersion, qviews.FromProtoDataVersion(plan.Prepares[id].Build().Meta.Version.DataVersion))
}
