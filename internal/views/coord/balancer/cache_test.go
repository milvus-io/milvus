package balancer

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
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

func TestCacheImmutablePublicationAndFallback(t *testing.T) {
	c := NewCache(nil)
	shard := cacheShard(1, 10)
	cfg := cfgFor(1, 10, nil, nil)
	c.PublishLoadConfig(1, cfg, 1)
	data := cacheData(1, shard.VChannel, 100, 0)
	c.PublishDataView(1, data)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: cfg.Replicas[0].ResourceGroup})
	stats := cacheStats(1000, 1, 0, coordview.SegmentStateUp, false)
	c.PublishShard(shard, stats)
	before := c.GetCollection(1)
	node := c.GetNode(1)
	require.Equal(t, int64(100), node.Info().UpRowCount)
	require.Equal(t, int64(100), node.Contribution(shard).UpRowCount)
	require.Equal(t, int64(100), data.Shards[0].TotalRows)
	require.Equal(t, 2, data.Shards[0].SegmentCount)
	c.PublishShard(shard, stats)
	require.Same(t, node, c.GetNode(1), "duplicate publication preserves the immutable object")
	c.PublishShard(shard, cacheStats(1000, 1, 0, coordview.SegmentStateReady, true))
	require.Zero(t, c.GetNode(1).Info().PendingRowCount, "published zero must not use historical fallback")
	require.Equal(t, int64(100), node.Info().UpRowCount, "retained readers remain immutable")
	require.Same(t, data, c.GetCollection(1).DataView())
	require.Same(t, stats, before.GetShard(shard).Stats())
	c.PublishShard(shard, cacheStats(1000, 1, 0, coordview.SegmentStatePreparing, false))
	c.PublishDataView(1, cacheData(1, shard.VChannel))
	require.Equal(t, int64(100), c.GetNode(1).Info().PendingRowCount, "resident old membership retains fallback")
	c.PublishLoadConfig(1, nil, 2)
	require.NotNil(t, c.GetCollection(1).GetShard(shard), "desired removal preserves cleanup scope")
	scope := resolveCacheScope(c, triggerBatch{full: true})
	require.Contains(t, scope, shard)
	c.PublishDataView(1, nil)
	c.PublishNode(1, nil)
	require.NotNil(t, c.GetNode(1), "lost node retains placement accounting")
	c.PublishShard(shard, nil)
	require.Nil(t, c.GetCollection(1))
	require.Nil(t, c.GetNode(1))
	_, ok := c.CollectionForReplica(10)
	require.False(t, ok)
}

func TestCacheLateDataViewRefreshesOnlyUnknownFootprints(t *testing.T) {
	c := NewCache(nil)
	shard := cacheShard(1, 10)
	c.PublishShard(shard, cacheStats(1000, 1, 0, coordview.SegmentStateReady, false))
	require.Zero(t, c.GetNode(1).Info().PendingRowCount)
	c.PublishDataView(1, cacheData(1, shard.VChannel, 42))
	require.Equal(t, int64(42), c.GetNode(1).Info().PendingRowCount)
	c.PublishShard(shard, cacheStats(1000, 1, 7, coordview.SegmentStateUp, true))
	c.PublishDataView(1, cacheData(1, shard.VChannel, 99))
	require.Equal(t, int64(7), c.GetNode(1).Info().UpRowCount)
}

func TestPlanningPinsNodeContributionAlongsideTotal(t *testing.T) {
	c := NewCache(nil)
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
	c := NewCache(nil)
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

func TestCacheConcurrentFieldsAndContributions(t *testing.T) {
	c := NewCache(nil)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	var wg sync.WaitGroup
	for i := int64(1); i <= 16; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			shard := cacheShard(id, id*10)
			for j := 0; j < 30; j++ {
				c.PublishDataView(id, cacheData(id, shard.VChannel, id))
				c.PublishLoadConfig(id, cfgFor(id, id*10, nil, nil), uint64(j+1))
				c.PublishShard(shard, cacheStats(id*1000, 1, id, coordview.SegmentStateUp, true))
				node := c.GetNode(1)
				var total int64
				node.RangeShards(func(shard qviews.ShardID) bool { total += node.Contribution(shard).UpRowCount; return true })
				if total != node.Info().UpRowCount {
					t.Errorf("inconsistent published node: %d != %d", total, node.Info().UpRowCount)
				}
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, Stopping: i%2 == 0, ResourceGroup: "rg1"})
		}
	}()
	wg.Wait()
	require.Equal(t, int64(136), c.GetNode(1).Info().UpRowCount)
	for i := int64(1); i <= 16; i++ {
		entry := c.GetCollection(i)
		require.NotNil(t, entry.LoadConfig())
		require.NotNil(t, entry.DataView())
		require.NotNil(t, entry.GetShard(cacheShard(i, i*10)))
	}
	// Concurrent writes to different fields of the same collection must merge.
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			c.PublishDataView(1, cacheData(1, cacheShard(1, 10).VChannel, int64(i)))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			c.PublishLoadConfig(1, cfgFor(1, 10, nil, nil), uint64(i+100))
		}
	}()
	wg.Wait()
	require.Equal(t, uint64(199), c.GetCollection(1).ConfigVersion())
	require.Equal(t, int64(99), c.GetCollection(1).DataView().Shards[0].TotalRows)
}

func TestCacheControllerReadinessAndRetry(t *testing.T) {
	c := NewCache(nil)
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

func snapshotOfCache(c *Cache) *BalancerSnapshot {
	s := &BalancerSnapshot{Config: c.GetBalanceConfig(), Nodes: make(map[int64]*BalanceNode), ShardRowStatsSnapshot: make(map[qviews.ShardID]ShardRowStats)}
	configs := make(map[int64]*loadmgr.LoadConfig)
	versions := make(map[int64]uint64)
	stats := make(map[qviews.ShardID]*coordview.ShardStats)
	var data []*CollectionDataView
	c.RangeCollectionIDs(func(id int64) bool {
		entry := c.GetCollection(id)
		if entry.config != nil {
			configs[id] = entry.config
			versions[id] = entry.configVersion
		}
		if entry.data != nil {
			data = append(data, entry.data)
		}
		entry.RangeShards(func(shard *ShardEntry) bool {
			stats[shard.id] = shard.stats
			s.ShardRowStatsSnapshot[shard.id] = shard.rows
			return true
		})
		return true
	})
	c.RangeNodeIDs(func(id int64) bool { s.Nodes[id] = c.GetNode(id).Info(); return true })
	s.LoadConfigSnapshot = loadmgr.NewLoadConfigSnapshotWithVersions(1, configs, versions)
	s.ShardViewSnapshot = coordview.NewShardViewSnapshot(1, stats)
	s.DataViewSnapshot = NewDataViewSnapshot(1, data)
	return s
}

func TestCachePolicyMatchesLegacyBatchPlanning(t *testing.T) {
	random := rand.New(rand.NewSource(17))
	for scenario := 0; scenario < 80; scenario++ {
		c := NewCache(policyTestConfig())
		for id := int64(1); id <= 4; id++ {
			c.PublishNode(id, &NodeInfo{NodeID: id, Alive: true, Stopping: id == 4 && scenario%3 == 0, ResourceGroup: "rg1"})
		}
		for id := int64(1); id <= 5; id++ {
			shard := cacheShard(id, id*10)
			c.PublishLoadConfig(id, cfgFor(id, id*10, nil, nil), 1)
			rows := []int64{int64(random.Intn(1_000_000)), int64(random.Intn(1_000_000)), 0}
			c.PublishDataView(id, cacheData(id, shard.VChannel, rows...))
			if (scenario+int(id))%3 != 0 {
				version := qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 1}, QueryVersion: 1}
				stats := &coordview.ShardStats{UpVersion: &version, UpLoadInfoVersion: 1, Segments: make(map[int64]*coordview.SegmentStats)}
				for i, row := range rows {
					segment := id*1000 + int64(i)
					stats.Segments[segment] = &coordview.SegmentStats{SegmentID: segment, PartitionID: 1, RowNum: row, HasRowNum: true, Nodes: map[int64]coordview.SegmentState{int64(random.Intn(4) + 1): coordview.SegmentStateUp}}
				}
				c.PublishShard(shard, stats)
			}
			if id == 5 && scenario%2 == 0 {
				c.PublishLoadConfig(id, nil, 2)
			}
		}
		dirty := resolveCacheScope(c, triggerBatch{full: true})
		old := legacyPlan(snapshotOfCache(c), dirty)
		next := NewDefaultBalancePolicy().Plan(c, dirty)
		require.Equal(t, old.Releases, next.Releases, "scenario %d", scenario)
		require.Len(t, next.Prepares, len(old.Prepares), "scenario %d", scenario)
		for shard, builder := range old.Prepares {
			require.Contains(t, next.Prepares, shard)
			require.Equal(t, flattenAssignments(builder.Build()), flattenAssignments(next.Prepares[shard].Build()), "scenario %d, shard %v", scenario, shard)
		}
	}
}

func BenchmarkCacheGetCollection(b *testing.B) {
	c := NewCache(nil)
	shard := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, shard.VChannel, 1, 2, 3))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.GetCollection(1)
	}
}

func TestCacheRetainsReplicaUntilLastResidentShard(t *testing.T) {
	c := NewCache(nil)
	a, b := cacheShard(1, 10), cacheShard(1, 10)
	b.VChannel = "by-dev-rootcoord-dml_0_1v1"
	c.PublishLoadConfig(1, cfgFor(1, 10, nil, nil), 1)
	c.PublishShard(a, cacheStats(1000, 1, 10, coordview.SegmentStateUp, true))
	c.PublishShard(b, cacheStats(1001, 1, 20, coordview.SegmentStateUp, true))
	c.PublishLoadConfig(1, nil, 2)
	c.PublishShard(a, nil)
	id, ok := c.CollectionForReplica(10)
	require.True(t, ok)
	require.Equal(t, int64(1), id)
	c.PublishShard(b, nil)
	_, ok = c.CollectionForReplica(10)
	require.False(t, ok)
	require.Nil(t, c.GetCollection(1))
	require.Nil(t, c.GetNode(1))
	require.Empty(t, c.groups)
}

func BenchmarkCachePublishShard(b *testing.B) {
	for _, size := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprintf("resident_shards_%d", size), func(b *testing.B) {
			c := NewCache(nil)
			for i := 0; i < size; i++ {
				c.PublishShard(cacheShard(1, int64(i+1)), cacheStats(int64(i), 1, 1, coordview.SegmentStateUp, true))
			}
			id := cacheShard(1, 1)
			a, bStats := cacheStats(0, 1, 1, coordview.SegmentStateUp, true), cacheStats(0, 1, 2, coordview.SegmentStateUp, true)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%2 == 0 {
					c.PublishShard(id, a)
				} else {
					c.PublishShard(id, bStats)
				}
			}
		})
	}
}

func BenchmarkCacheFullPlan(b *testing.B) {
	for _, scenario := range []struct {
		name                  string
		collections, segments int
	}{{"large_collection", 1, 10000}, {"many_collections", 1000, 10}} {
		b.Run(scenario.name, func(b *testing.B) {
			c := NewCache(policyTestConfig())
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

func TestRetainedCacheVersionsShareUnchangedChildren(t *testing.T) {
	c := NewCache(nil)
	for i := int64(1); i <= 10000; i++ {
		c.PublishShard(cacheShard(1, i), cacheStats(i, 1, 1, coordview.SegmentStateUp, true))
	}
	untouched := cacheShard(1, 9999)
	original := c.GetCollection(1).GetShard(untouched)
	retained := make([]*CollectionEntry, 0, 128)
	for i := int64(1); i <= 128; i++ {
		retained = append(retained, c.GetCollection(1))
		c.PublishShard(cacheShard(1, 1), cacheStats(1, 1, i, coordview.SegmentStateUp, true))
	}
	for _, entry := range retained {
		require.Same(t, original, entry.GetShard(untouched))
	}
	require.Equal(t, int64(1), retained[0].GetShard(cacheShard(1, 1)).Stats().Segments[1].RowNum)
	require.Equal(t, int64(128), c.GetNode(1).Contribution(cacheShard(1, 1)).UpRowCount)
}

func BenchmarkCacheRetainedVersions(b *testing.B) {
	c := NewCache(nil)
	for i := int64(1); i <= 10000; i++ {
		c.PublishShard(cacheShard(1, i), cacheStats(i, 1, 1, coordview.SegmentStateUp, true))
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	retained := make([]*CollectionEntry, 128)
	nodes := make([]*NodeEntry, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for v := range retained {
			retained[v] = c.GetCollection(1)
			nodes[v] = c.GetNode(1)
			c.PublishShard(cacheShard(1, 1), cacheStats(1, 1, int64(v), coordview.SegmentStateUp, true))
		}
	}
	b.StopTimer()
	runtime.GC()
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "retained_heap_B")
	runtime.KeepAlive(retained)
	runtime.KeepAlive(nodes)
	runtime.KeepAlive(c)
}
