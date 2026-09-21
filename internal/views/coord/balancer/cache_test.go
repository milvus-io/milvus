package balancer

import (
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func cacheShard(collection, replica int64) qviews.ShardID {
	return qviews.ShardID{ReplicaID: replica, VChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collection)}
}

func cacheData(collection int64, channel string, rows ...int64) *CollectionDataView {
	segments := make([]*SegmentDataView, 0, len(rows))
	for i, row := range rows {
		segments = append(segments, &SegmentDataView{SegmentID: collection*1000 + int64(i), PartitionID: 1, RowNum: row})
	}
	return api.PrepareCollectionDataView(&CollectionDataView{CollectionID: collection, DataVersion: qviews.DataVersion{StreamingVersion: 1}, Shards: []*ShardDataView{{VChannel: channel, Partitions: []*PartitionDataView{{PartitionID: 1, Segments: segments}}}}})
}

func cacheStats(segment, node, rows int64, state coordview.SegmentState, known bool) *coordview.ShardStats {
	return &coordview.ShardStats{Segments: map[int64]*coordview.SegmentStats{segment: {SegmentID: segment, PartitionID: 1, RowNum: rows, HasRowNum: known, Nodes: map[int64]coordview.SegmentState{node: state}}}}
}

func TestPlanningPinsNodeContributionAlongsideTotal(t *testing.T) {
	c := balancercache.New(nil)
	shard := cacheShard(1, 10)
	c.PublishLoadConfig(1, cfgFor(1, 10, nil, nil), 1)
	c.PublishDataView(1, cacheData(1, shard.VChannel, 100))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishShard(shard, cacheStats(1000, 1, 100, coordview.SegmentStateUp, true))
	p := newPlanningContext(c)
	c.PublishShard(shard, cacheStats(1000, 1, 40, coordview.SegmentStateReady, true))
	// The collection is first read after the node baseline was acquired.
	require.Equal(t, int64(40), p.GetShardStats(shard).Segments[1000].RowNum)
	require.Equal(t, int64(100), p.CurrentRows(shard)[1])
	require.Zero(t, withoutRows(initialProjectedRows(p.nodes), p.CurrentRows(shard))[1])
	old := p.GetCollection(1)
	c.PublishDataView(1, cacheData(1, shard.VChannel, 200))
	require.Same(t, old, p.GetCollection(1))
	require.Equal(t, int64(100), p.DataViewForShard(shard).TotalRows)
	require.Equal(t, []int64{1}, p.CandidateNodes("rg1"))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg2"})
	require.Equal(t, []int64{1}, p.CandidateNodes("rg1"), "candidate inputs are fixed within a batch")
	require.Empty(t, newPlanningContext(c).CandidateNodes("rg1"))
}

func TestCacheNodeAndResourceGroupScopes(t *testing.T) {
	c := balancercache.New(nil)
	a, b := cacheShard(1, 10), cacheShard(2, 20)
	cfgA, cfgB := cfgFor(1, 10, nil, nil), cfgFor(2, 20, nil, nil)
	cfgB.Replicas[0].ResourceGroup = "rg2"
	c.PublishLoadConfig(1, cfgA, 1)
	c.PublishLoadConfig(2, cfgB, 1)
	c.PublishDataView(1, cacheData(1, a.VChannel, 0))
	c.PublishDataView(2, cacheData(2, b.VChannel, 10))
	var events []TriggerScope
	c.SetNotifier(func(event TriggerScope) { events = append(events, event) })
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	require.Contains(t, events[len(events)-1].DirtyCollections, int64(1), "scale-out must reach collections without placements")
	c.PublishShard(a, cacheStats(1000, 1, 0, coordview.SegmentStateUp, true))
	require.Contains(t, resolveCacheScope(c, triggerBatch{dirtyNodes: map[int64]struct{}{1: {}}}), a)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg2"})
	require.ElementsMatch(t, []int64{1, 2}, events[len(events)-1].DirtyCollections)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, Stopping: true, ResourceGroup: "rg2"})
	require.Contains(t, events[len(events)-1].DirtyNodes, int64(1))
	require.Empty(t, newPlanningContext(c).CandidateNodes("rg2"))
	require.ElementsMatch(t, []qviews.ShardID{a, b}, resolveCacheScope(c, triggerBatch{full: true}))
}

func TestCacheControllerReadinessAndRetry(t *testing.T) {
	c := balancercache.New(nil)
	registry := emptyRegistry(t)
	controller := NewDefaultBalancer(c, registry, nil)
	controller.Trigger()
	require.ErrorIs(t, controller.Reconcile(t.Context()), merr.ErrServiceNotReady)
	c.MarkReady()
	require.NoError(t, controller.Reconcile(t.Context()))
	shard := cacheShard(1, 10)
	c.PublishLoadConfig(1, cfgFor(1, 10, nil, nil), 1)
	// Desired with missing DataView is not release; retry allocation once available.
	controller.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shard}})
	require.Error(t, controller.Reconcile(t.Context()))
	require.Contains(t, controller.queue.takePending().dirtyShards, shard)
	c.PublishDataView(1, cacheData(1, shard.VChannel, 10))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	failure := mockey.Mock((*coordview.ShardViewManager).AddPreparing).Return(merr.WrapErrServiceUnavailableMsg("test unavailable version")).Build()
	require.Error(t, controller.Reconcile(t.Context()))
	failure.UnPatch()
	require.Contains(t, controller.queue.takePending().dirtyShards, shard)
	controller.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shard}})
	require.NoError(t, controller.Reconcile(t.Context()))
}

func BenchmarkCacheFullPlan(b *testing.B) {
	for _, scenario := range []struct {
		name                  string
		collections, segments int
	}{{"large_collection", 1, 10000}, {"many_collections", 1000, 10}} {
		b.Run(scenario.name, func(b *testing.B) {
			c := balancercache.New(policyTestConfig())
			for id := int64(1); id <= 8; id++ {
				c.PublishNode(id, &NodeInfo{NodeID: id, Alive: true, ResourceGroup: "rg1"})
			}
			rows := make([]int64, scenario.segments)
			for i := range rows {
				rows[i] = 1000
			}
			for id := int64(1); id <= int64(scenario.collections); id++ {
				shard := cacheShard(id, id*10)
				c.PublishLoadConfig(id, cfgFor(id, id*10, nil, nil), 1)
				c.PublishDataView(id, cacheData(id, shard.VChannel, rows...))
			}
			p := NewDefaultBalancePolicy()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := newPlanningContext(c)
				p.Plan(reader, resolveCacheScope(reader, triggerBatch{full: true}))
			}
		})
	}
}

func TestPlanningContextPinsAbsenceAndResolvesReplicaBinding(t *testing.T) {
	c := balancercache.New(nil)
	p := newPlanningContext(c)
	shard := cacheShard(1, 10)
	require.Nil(t, p.GetCollection(1))
	require.Zero(t, p.ConfigVersion(1))
	c.PublishLoadConfig(1, cfgFor(1, 10, nil, nil), 1)
	require.Nil(t, p.ConfigForShard(shard), "an absent collection is also pinned for this batch")
	fresh := newPlanningContext(c)
	require.Equal(t, uint64(1), fresh.ConfigVersion(1))
	// A channel without an encoded collection ID resolves through its replica.
	short := qviews.ShardID{ReplicaID: 10, VChannel: "short-channel"}
	c.PublishShard(short, cacheStats(100, 1, 42, coordview.SegmentStateUp, true))
	next := newPlanningContext(c)
	require.Equal(t, int64(1), next.ConfigForShard(short).CollectionID)
	require.Equal(t, int64(42), next.CurrentRows(short)[1])
	require.NotNil(t, next.GetShardStats(short))
	short.ReplicaID = 999
	require.Nil(t, next.ConfigForShard(short))
	require.Nil(t, next.GetShardStats(short))
}
