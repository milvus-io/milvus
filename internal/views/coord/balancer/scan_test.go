package balancer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func scanTestCache(sizes ...int) *balancercache.Cache {
	c := balancercache.New(nil)
	for i, size := range sizes {
		id := int64(i + 1)
		c.PublishLoadConfig(id, cfgFor(id, id*10, nil, nil), 1)
		data := &CollectionDataView{CollectionID: id, DataVersion: qviews.DataVersion{StreamingVersion: 1}}
		for channel := 0; channel < size; channel++ {
			data.Shards = append(data.Shards, &ShardDataView{VChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv%d", id, channel)})
		}
		c.PublishDataView(id, api.PrepareCollectionDataView(data))
	}
	c.MarkReady()
	return c
}

func TestDiscoveryLargeCollectionAndLocalScope(t *testing.T) {
	c := scanTestCache(10000, 1)
	t.Cleanup(c.Close)
	b := NewDefaultBalancer(c, nil, nil, nil)
	b.Trigger()
	require.Equal(t, discoveryShardLimit, b.dispatch())
	b.Trigger(TriggerScope{DirtyCollections: []int64{2}})
	require.Equal(t, 1, b.dispatch())
	require.Contains(t, b.ready.items, cacheShard(2, 20))
	require.Len(t, b.ready.items, discoveryShardLimit+1)
	for b.dispatch() >= 0 {
	}
	require.Len(t, b.ready.items, 10001)
}

func TestDiscoveryEmptyCollectionsYieldAndPruneLayout(t *testing.T) {
	c := scanTestCache(make([]int, scanSlotBudget+1)...)
	t.Cleanup(c.Close)
	b := NewDefaultBalancer(c, nil, nil, nil)
	policy := b.policy.(*DefaultBalancePolicy)
	policy.layouts.domains = map[int64]map[string]*replicaLayout{1: {}, 2: {}}
	b.Trigger()
	require.Zero(t, b.dispatch())
	c.PublishLoadConfig(1, nil, 2)
	c.PublishDataView(1, nil)
	require.Zero(t, b.dispatch())
	require.NotContains(t, policy.layouts.domains, int64(1))
	require.Contains(t, policy.layouts.domains, int64(2))
	for b.dispatch() >= 0 {
	}
}

func TestFaultDiscoveryAdmitsColdCollectionBeforeRangeCompletes(t *testing.T) {
	for _, manyCollections := range []bool{false, true} {
		t.Run(fmt.Sprintf("many_collections_%t", manyCollections), func(t *testing.T) {
			sizes := []int{10000}
			if manyCollections {
				sizes = make([]int, 10000)
				for i := range sizes {
					sizes[i] = 1
				}
			}
			c := scanTestCache(sizes...)
			t.Cleanup(c.Close)
			cfg := DefaultBalanceConfig()
			cfg.ShardBatchSize = 8
			c.UpdateBalanceConfig(cfg)
			var batches [][]qviews.ShardID
			b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, completionTestPolicy(func(_ balancercache.Reader, batch []qviews.ShardID) *BalancePlan {
				batches = append(batches, batch)
				return nil
			}), nil)
			// Drive the producer and worker separately to inject the request at
			// the same point regardless of CPU speed or goroutine scheduling.
			b.running.Store(true)
			b.Trigger(TriggerScope{DirtyResourceGroups: []string{"rg1"}})
			require.Equal(t, discoveryShardLimit, b.dispatch())
			require.NoError(t, b.Reconcile(t.Context()))
			cold := cacheShard(10001, 100010)
			auto := cfgFor(10001, 100010, nil, nil)
			auto.AutoLoad = true
			c.PublishLoadConfig(10001, auto, 1)
			c.PublishDataView(10001, cacheData(10001, cold.VChannel, 1))
			require.Equal(t, 1, b.dispatch())
			require.NoError(t, b.Reconcile(t.Context()))
			require.Equal(t, cold, batches[1][0])
			require.Less(t, len(b.ready.items), 100)
			// Unfinished discovery must release its cursor on stop; the next
			// run can replay the range without losing a successor publication.
			b.discovery.suspend()
		})
	}
}

func TestReleaseCompletionSpansDiscoverySlicesAndWorkerBatches(t *testing.T) {
	c := balancercache.New(nil)
	t.Cleanup(c.Close)
	count := 2*discoveryShardLimit + 1
	for i := 0; i < count; i++ {
		c.PublishShard(qviews.ShardID{ReplicaID: 10, VChannel: fmt.Sprintf("p_1v%d", i)}, &coordview.ShardStats{})
	}
	c.MarkReady()
	cfg := DefaultBalanceConfig()
	cfg.ShardBatchSize = 7
	c.UpdateBalanceConfig(cfg)
	seen := make(map[qviews.ShardID]bool)
	policy := completionTestPolicy(func(_ balancercache.Reader, dirty []qviews.ShardID) *BalancePlan {
		require.LessOrEqual(t, len(dirty), 7)
		for _, id := range dirty {
			require.False(t, seen[id])
			seen[id] = true
		}
		return nil
	})
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, policy, nil)
	f := b.TriggerWithCompletion(TriggerScope{DirtyCollections: []int64{1}})
	for range count {
		require.NoError(t, b.Reconcile(t.Context()))
		if f.Ready() {
			break
		}
	}
	require.True(t, f.Ready())
	require.NoError(t, f.Get())
	require.Len(t, seen, count)
}

func TestActiveCollectionRediscoversSupersedingPublication(t *testing.T) {
	c := scanTestCache(discoveryShardLimit + 1)
	t.Cleanup(c.Close)
	b := NewDefaultBalancer(c, nil, nil, nil)
	b.Trigger(TriggerScope{DirtyCollections: []int64{1}})
	require.Equal(t, discoveryShardLimit, b.dispatch())
	first := cacheShard(1, 10)
	require.EqualValues(t, 2, b.ready.items[first].priority)
	cfg := c.GetCollection(1).LoadConfig().Clone()
	cfg.AutoLoad = true
	c.PublishLoadConfig(1, cfg, 2)
	b.Trigger(TriggerScope{DirtyCollections: []int64{1}})
	for b.dispatch() >= 0 {
	}
	require.EqualValues(t, 12, b.ready.items[first].priority)
	require.Len(t, b.ready.items, discoveryShardLimit+1)
}
