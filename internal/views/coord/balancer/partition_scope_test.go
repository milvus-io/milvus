package balancer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestBalancePartitionScope(t *testing.T) {
	id := cacheShard(1, 10)
	dv := qviews.DataVersion{StreamingVersion: 1}
	for _, partitions := range [][]int64{{1}, {2}, {1, 2}, nil} {
		cfg := cfgFor(1, 10, partitions, nil)
		c := newTestCache(cfg)
		publishTestData(c, 1, dv, map[int64]*SegmentDataView{
			101: {SegmentID: 101, PartitionID: 1, LogicalSize: 100, ManifestVersion: 8},
			201: {SegmentID: 201, PartitionID: 2, LogicalSize: 900, ManifestVersion: 9},
		}, &viewpb.DataViewOfShard{Vchannel: id.VChannel, Partitions: []*viewpb.DataViewOfPartition{
			{PartitionId: 1, SegmentIds: []int64{101}}, {PartitionId: 2, SegmentIds: []int64{201}},
		}})
		publishTestNodes(c, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
		policy := NewDefaultBalancePolicy()
		plan := policy.Plan(c, []qviews.ShardID{id})
		require.Contains(t, plan.Prepares, id)
		want := map[int64]int64{}
		for _, partition := range partitions {
			segment := partition*100 + 1
			want[segment] = 1
		}
		require.Equal(t, want, assignmentsFromBuilder(plan.Prepares[id]))
		require.Equal(t, int64(1000), c.GetCollection(1).DataView().LogicalSize)

		// A serving all-partition view must be replaced after a partial release.
		c.PublishShard(id, upStats(dv, placement(101, 1, 1, coordview.SegmentStateUp), placement(201, 2, 1, coordview.SegmentStateUp)))
		c.PublishLoadConfig(1, cfg.Clone(), 2)
		plan = policy.Plan(c, []qviews.ShardID{id})
		require.Contains(t, plan.Prepares, id)
		require.Equal(t, want, assignmentsFromBuilder(plan.Prepares[id]))
		require.Equal(t, uint64(2), plan.Prepares[id].Build().GetMeta().GetLoadInfoVersion())
	}
}

func TestUnloadedPartitionsDoNotAffectBalance(t *testing.T) {
	id := cacheShard(1, 10)
	other := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_1v1"}
	makeCache := func(extra bool) *balancercache.Cache {
		c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
		for _, node := range []int64{1, 2, 3} {
			c.PublishNode(node, &NodeInfo{NodeID: node, Alive: true, ResourceGroup: "rg1"})
		}
		shards := []*ShardDataView{
			{VChannel: id.VChannel, Partitions: []*PartitionDataView{{PartitionID: 1, Segments: []*SegmentDataView{
				{SegmentID: 101, LogicalSize: 40_000}, {SegmentID: 102, LogicalSize: 40_000},
			}}}},
			{VChannel: other.VChannel, Partitions: []*PartitionDataView{{PartitionID: 1, Segments: []*SegmentDataView{
				{SegmentID: 103, LogicalSize: 1_000_000}, {SegmentID: 104, LogicalSize: 1_000_000},
			}}}},
		}
		if extra {
			shards[0].Partitions = append(shards[0].Partitions, &PartitionDataView{PartitionID: 2, Segments: []*SegmentDataView{
				{SegmentID: 201, LogicalSize: 1_000_000_000},
			}})
		}
		c.PublishDataView(1, api.PrepareCollectionDataView(&CollectionDataView{CollectionID: 1, DataVersion: qviews.DataVersion{StreamingVersion: 1}, Shards: shards}))
		return c
	}
	baseline, extra := makeCache(false), makeCache(true)
	for _, c := range []*balancercache.Cache{baseline, extra} {
		p := newPlanningContext(c)
		require.Equal(t, int64(80_000), shardTotalLoad(p, id))
		require.Equal(t, int64(2_000_000), shardTotalLoad(p, other))
		require.Equal(t, int64(2_080_000), c.GetResourceGroup("rg1").Demand(3))
		p.targets[id.ReplicaID] = []int64{1, 2, 3}
		p.fanouts = make(map[qviews.ShardID]int)
		ctx := newAllocationContext(p, id, map[int64]int64{}, c.GetCollection(1).DataView().Shard(id.VChannel).Partitions[0].Segments)
		require.InDelta(t, 2_080_000.0/3, ctx.mean, 1e-6)
		require.InDelta(t, 88_000, ctx.shardLimit, 1e-6)
		require.InDelta(t, 1.1*2_080_000/3, ctx.collectionLimit, 1e-6)
	}
	policy, extraPolicy := NewDefaultBalancePolicy(), NewDefaultBalancePolicy()
	compare := func() {
		a := policy.Plan(baseline, []qviews.ShardID{id, other})
		b := extraPolicy.Plan(extra, []qviews.ShardID{id, other})
		require.Equal(t, a.Releases, b.Releases)
		require.Equal(t, a.Retries, b.Retries)
		require.Equal(t, a.Continues, b.Continues)
		require.Len(t, b.Prepares, len(a.Prepares))
		for id, builder := range a.Prepares {
			require.Contains(t, b.Prepares, id)
			require.Equal(t, builder.Build(), b.Prepares[id].Build(), "unloaded data must not change candidate order, scores or assignments")
		}
		require.Equal(t, policy.fanouts, extraPolicy.fanouts)
		require.Equal(t, 1, policy.fanouts[id], "the loaded small shard retains a compact fanout")
	}
	compare()
	// Repeat with a skewed serving placement to exercise optional optimization.
	for _, c := range []*balancercache.Cache{baseline, extra} {
		c.PublishShard(id, upStats(qviews.DataVersion{StreamingVersion: 1},
			placement(101, 1, 1, coordview.SegmentStateUp), placement(102, 1, 1, coordview.SegmentStateUp)))
		c.PublishShard(other, upStats(qviews.DataVersion{StreamingVersion: 1},
			placement(103, 1, 1, coordview.SegmentStateUp), placement(104, 1, 1, coordview.SegmentStateUp)))
	}
	compare()
	// Resource reuse must not credit unloaded data during replica layout repair.
	c := extra.GetCollection(1)
	require.Equal(t, int64(-1), resourceRows(c, coordview.ResourceKey{
		PartitionID: 2, SegmentID: 201, DataVersion: c.DataView().DataVersion, LoadInfoVersion: c.ConfigVersion(),
	}))
}

func TestPublishedRecoveryRetainsHistoricalPartitions(t *testing.T) {
	id := cacheShard(1, 10)
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	publishTestNodes(c, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	p := newPlanningContext(c)
	p.fanouts = make(map[qviews.ShardID]int)
	p.targets[id.ReplicaID] = []int64{2}
	p.recovery = newPublishedRecoveryBase(id, &coordview.PublishedRecovery{View: &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 1, ReplicaId: 10, Vchannel: id.VChannel, SchemaVersion: 3, LoadInfoVersion: 77,
			Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}},
		},
		QueryNode: []*viewpb.QueryViewOfQueryNode{{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{
			{PartitionId: 2, SegmentIds: []int64{201}, SegmentManifestVersions: []int64{9}},
		}}},
	}, Sizes: map[int64]int64{201: 900}})
	result := allocate(p, id, map[int64]int64{}, false)
	require.NotNil(t, result)
	require.Equal(t, map[int64]int64{201: 2}, result.assignments, "recovery must not use the newer partition scope")
	view := result.builder.Build()
	require.Equal(t, uint64(77), view.Meta.LoadInfoVersion)
	require.Equal(t, int32(3), view.Meta.SchemaVersion)
	require.Equal(t, []int64{9}, view.QueryNode[0].Partitions[0].SegmentManifestVersions)
}
