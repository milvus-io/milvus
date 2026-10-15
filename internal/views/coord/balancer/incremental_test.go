package balancer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func publishSuccessfulPlan(c *balancercache.Cache, id qviews.ShardID, builder *qviews.QueryViewAtCoordBuilder) map[int64]int64 {
	assignments := assignmentsFromBuilder(builder)
	var placements []testSegmentPlacement
	for segment, node := range assignments {
		placements = append(placements, placement(segment, 1, node, coordview.SegmentStateUp))
	}
	c.PublishShard(id, upStats(builder.DataVersion(), placements...))
	return assignments
}

func TestIncrementalFailureRejoinConvergesAndStaysStable(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 1e6, 1e6, 1e6, 1e6, 1e6, 1e6))
	for _, node := range []int64{1, 2, 3} {
		c.PublishNode(node, &NodeInfo{NodeID: node, Alive: true, ResourceGroup: "rg1"})
	}
	policy := NewDefaultBalancePolicy()
	run := func() map[int64]int64 {
		plan := policy.Plan(c, []qviews.ShardID{id})
		require.Contains(t, plan.Prepares, id)
		return publishSuccessfulPlan(c, id, plan.Prepares[id])
	}
	run()
	for _, node := range []int64{1, 2, 3} {
		require.Equal(t, int64(2e6), c.GetNode(node).TargetRows())
	}
	c.PublishNode(1, nil)
	run()
	require.Equal(t, int64(3e6), c.GetNode(2).TargetRows())
	require.Equal(t, int64(3e6), c.GetNode(3).TargetRows())
	c.PublishNode(4, &NodeInfo{NodeID: 4, Alive: true, ResourceGroup: "rg1"})
	run()
	for _, node := range []int64{2, 3, 4} {
		require.Equal(t, int64(2e6), c.GetNode(node).TargetRows())
	}
	for i := 0; i < 3; i++ {
		require.Empty(t, policy.Plan(c, []qviews.ShardID{id}).Prepares)
	}
}

func TestIncrementalPartialFailureReusesReadySegments(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	id := cacheShard(1, 10)
	data := cacheData(1, id.VChannel, 1e6, 1e6, 1e6)
	c.PublishDataView(1, data)
	for _, node := range []int64{1, 2, 3} {
		c.PublishNode(node, &NodeInfo{NodeID: node, Alive: true, ResourceGroup: "rg1"})
	}
	failed := testShardStats(nil, 0,
		placement(1000, 1, 1, coordview.SegmentStateReady),
		placement(1001, 1, 2, coordview.SegmentStateReady),
		placement(1002, 1, 3, coordview.SegmentStateUnrecoverable))
	for segment, node := range map[int64]int64{1000: 1, 1001: 2} {
		failed.Resources[node] = map[coordview.ResourceKey]struct{}{{PartitionID: 1, SegmentID: segment, DataVersion: data.DataVersion, LoadInfoVersion: 1}: {}}
	}
	c.PublishShard(id, failed)
	for _, node := range []int64{1, 2, 3} {
		require.Zero(t, c.GetNode(node).TargetRows(), "failed target contributes no intended load")
	}
	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	assignments := assignmentsFromBuilder(plan.Prepares[id])
	require.Equal(t, int64(1), assignments[1000])
	require.Equal(t, int64(2), assignments[1001])
	require.Equal(t, int64(3), assignments[1002])
	// This synthetic test intentionally leaves only node 3 free: failed loading
	// can be retried, while successful resources remain reusable independently.
}

func TestIncrementalVersionAdvancePreservesLegalPlacements(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100, 200))
	for _, node := range []int64{1, 2} {
		c.PublishNode(node, &NodeInfo{NodeID: node, Alive: true, ResourceGroup: "rg1"})
	}
	c.PublishShard(id, upStats(qviews.DataVersion{}, placement(1000, 1, 2, coordview.SegmentStateUp), placement(1001, 1, 1, coordview.SegmentStateUp)))
	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{id})
	require.Equal(t, map[int64]int64{1000: 2, 1001: 1}, assignmentsFromBuilder(plan.Prepares[id]), "metadata advancement alone does not reshuffle healthy placements")
}

func TestIncrementalBoundedSearchContinuesPastUnhelpfulPrefix(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 1e6, 1e6, 1e6, 1e6, 1e6, 1e6))
	for _, node := range []int64{1, 2, 3} {
		c.PublishNode(node, &NodeInfo{NodeID: node, Alive: true, ResourceGroup: "rg1"})
	}
	c.PublishShard(id, upStats(qviews.DataVersion{StreamingVersion: 1},
		placement(1000, 1, 1, coordview.SegmentStateUp), placement(1001, 1, 1, coordview.SegmentStateUp), placement(1002, 1, 1, coordview.SegmentStateUp),
		placement(1003, 1, 2, coordview.SegmentStateUp), placement(1004, 1, 2, coordview.SegmentStateUp), placement(1005, 1, 2, coordview.SegmentStateUp)))
	profile := DefaultBalanceConfig()
	profile.MaxCandidateEvaluations = 1
	c.UpdateBalanceConfig(profile)
	policy := NewDefaultBalancePolicy()
	var moves int
	for i := 0; i < 100; i++ {
		plan := policy.Plan(c, []qviews.ShardID{id})
		require.Empty(t, plan.Retries, "a search budget is not an availability failure")
		if builder := plan.Prepares[id]; builder != nil {
			publishSuccessfulPlan(c, id, builder)
			moves++
		}
		if len(plan.Prepares) == 0 && len(plan.Continues) == 0 {
			break
		}
	}
	require.Positive(t, moves)
	for _, node := range []int64{1, 2, 3} {
		require.Equal(t, int64(2e6), c.GetNode(node).TargetRows())
	}
}

func TestIncrementalWholeTinyShardMoveWithExternalLoad(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 20_000, 20_000, 20_000, 20_000))
	for _, node := range []int64{1, 2} {
		c.PublishNode(node, &NodeInfo{NodeID: node, Alive: true, ResourceGroup: "rg1"})
	}
	c.PublishShard(id, upStats(qviews.DataVersion{StreamingVersion: 1}, placement(1000, 1, 1, coordview.SegmentStateUp), placement(1001, 1, 1, coordview.SegmentStateUp), placement(1002, 1, 1, coordview.SegmentStateUp), placement(1003, 1, 1, coordview.SegmentStateUp)))
	publishBackgroundRows(c, map[int64]int64{1: 10e6})
	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	for _, node := range assignmentsFromBuilder(plan.Prepares[id]) {
		require.Equal(t, int64(2), node)
	}
	require.Equal(t, int64(10e6), c.GetNode(1).TargetContribution(cacheShard(999, 9990)), "unselected collection remains background, not a candidate")
}

func TestIncrementalPreparingPublishesTargetBeforeReconcileReturns(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	registry := emptyRegistry(t)
	t.Cleanup(registry.RegisterPublicationListener(c.PublishShard))
	c.MarkReady()
	controller := NewDefaultBalancer(c, registry, nil)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Equal(t, int64(100), c.GetNode(1).TargetRows())
	require.Equal(t, int64(100), c.GetCollection(1).ReplicaRows(10, 1))
	require.NotNil(t, c.GetCollection(1).GetShard(id).Stats().PreparingPlacement)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Equal(t, int64(100), c.GetNode(1).TargetRows(), "next reconcile must not double count or see the node as empty")
}

func TestIncrementalTinyShardStaysConcentratedOnManyNodes(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 20_000, 20_000, 20_000, 20_000))
	for node := int64(1); node <= 100; node++ {
		c.PublishNode(node, &NodeInfo{NodeID: node, Alive: true, ResourceGroup: "rg1"})
	}
	policy := NewDefaultBalancePolicy()
	plan := policy.Plan(c, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	assignments := publishSuccessfulPlan(c, id, plan.Prepares[id])
	for _, node := range assignments {
		require.Equal(t, assignments[1000], node)
	}
	require.Empty(t, policy.Plan(c, []qviews.ShardID{id}).Prepares)
}

func TestIncrementalLargeCollectionOfTinyShardsUsesLocalBalanceWithinRGBand(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	segments := make(map[int64]*SegmentDataView)
	var shards []*viewpb.DataViewOfShard
	var ids []qviews.ShardID
	version := qviews.DataVersion{StreamingVersion: 1}
	for i := int64(0); i < 9; i++ {
		id := qviews.ShardID{ReplicaID: 10, VChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_1v%d", i)}
		ids = append(ids, id)
		segments[1000+i] = &SegmentDataView{SegmentID: 1000 + i, PartitionID: 1, RowNum: 100_000}
		shards = append(shards, shardDataView(id.VChannel, 1, 1000+i))
	}
	publishTestData(c, 1, version, segments, shards...)
	for _, node := range []int64{1, 2, 3} {
		c.PublishNode(node, &NodeInfo{NodeID: node, Alive: true, ResourceGroup: "rg1"})
	}
	for i, id := range ids {
		c.PublishShard(id, upStats(version, placement(1000+int64(i), 1, 1, coordview.SegmentStateUp)))
	}
	publishBackgroundRows(c, map[int64]int64{2: 900_000, 3: 900_000})
	policy := NewDefaultBalancePolicy()
	plan := policy.Plan(c, ids)
	require.Len(t, plan.Prepares, 1, "one tiny shard can move for collection balance without leaving the RG band")
	for id, builder := range plan.Prepares {
		assignments := publishSuccessfulPlan(c, id, builder)
		for _, node := range assignments {
			require.NotEqual(t, int64(1), node)
		}
	}
	require.Empty(t, policy.Plan(c, ids).Prepares, "further local improvement must not worsen the RG penalty")
	for _, node := range []int64{1, 2, 3} {
		require.InDelta(t, 900_000, c.GetNode(node).TargetRows(), 100_000)
	}
}
