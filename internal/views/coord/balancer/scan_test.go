package balancer

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

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
	require.Equal(t, discoveryShardLimit, b.dispatch(anySource))
	b.Trigger(TriggerScope{DirtyCollections: []int64{2}})
	require.Equal(t, 1, b.dispatch(anySource))
	require.Contains(t, b.ready.items, cacheShard(2, 20))
	require.Len(t, b.ready.items, discoveryShardLimit+1)
	for b.dispatch(anySource) >= 0 {
	}
	require.Len(t, b.ready.items, 10001)
}

// A node failure can publish one local notification per affected shard before
// the producer sees a cold collection's load. Exercise the cache callbacks so
// the ordering guarantee also covers the path that runs under publisher locks.
func TestLocalCollectionDuringShardPublicationBacklog(t *testing.T) {
	const backlog = 10000
	c := scanTestCache(backlog)
	t.Cleanup(c.Close)
	shards := c.GetCollection(1).DataView().Shards
	for _, shard := range shards {
		id := qviews.ShardID{ReplicaID: 10, VChannel: shard.VChannel}
		c.PublishShard(id, &coordview.ShardStats{PreparingVersion: ver(1, 1, 1), MaybeUped: true})
	}
	cold := cacheShard(2, 20)
	c.PublishDataView(2, cacheData(2, cold.VChannel, 1))
	b := NewDefaultBalancer(c, nil, nil, nil)
	for _, shard := range shards {
		id := qviews.ShardID{ReplicaID: 10, VChannel: shard.VChannel}
		c.PublishShard(id, &coordview.ShardStats{MaybeUped: true})
	}
	select {
	case <-b.discovery.queues[localChanges].signal:
	default:
		t.Fatal("shard publications did not wake the local producer")
	}
	require.Equal(t, 1, b.dispatch(localChanges))
	cfg := cfgFor(2, 20, nil, nil)
	cfg.AutoLoad = true
	c.PublishLoadConfig(2, cfg, 1)
	require.Equal(t, 1, b.dispatch(localChanges))
	require.Equal(t, []qviews.ShardID{cold}, b.ready.take(1))
	for b.dispatch(localChanges) >= 0 {
	}
	require.Len(t, b.ready.items, backlog)
}

func TestLocalDiscoveryAlternatesWideScopes(t *testing.T) {
	const size = 2*discoveryShardLimit + 1
	c := scanTestCache(size, size)
	t.Cleanup(c.Close)
	b := NewDefaultBalancer(c, nil, nil, nil)
	ids := make([]qviews.ShardID, 0, size)
	for _, shard := range c.GetCollection(1).DataView().Shards {
		ids = append(ids, qviews.ShardID{ReplicaID: 10, VChannel: shard.VChannel})
	}
	b.Trigger(TriggerScope{DirtyCollections: []int64{2}})
	require.Equal(t, discoveryShardLimit, b.dispatch(localChanges))
	b.Trigger(TriggerScope{DirtyShards: ids})
	require.Equal(t, discoveryShardLimit, b.dispatch(localChanges))
	require.Contains(t, b.ready.items, ids[0])
	require.Equal(t, discoveryShardLimit, b.dispatch(localChanges))
	require.NotContains(t, b.ready.items, ids[discoveryShardLimit])
	require.Equal(t, discoveryShardLimit, b.dispatch(localChanges))
	require.Contains(t, b.ready.items, ids[discoveryShardLimit])
	b.discovery.suspend()
	for b.dispatch(anySource) >= 0 {
	}
	require.Len(t, b.ready.items, 2*size)
}

func TestDiscoveryEmptyCollectionsYieldAndPruneLayout(t *testing.T) {
	c := scanTestCache(make([]int, scanSlotBudget+1)...)
	t.Cleanup(c.Close)
	b := NewDefaultBalancer(c, nil, nil, nil)
	policy := b.policy.(*DefaultBalancePolicy)
	policy.layouts.domains = map[int64]map[string]*replicaLayout{1: {}, 2: {}}
	b.Trigger()
	require.Zero(t, b.dispatch(anySource))
	c.PublishLoadConfig(1, nil, 2)
	c.PublishDataView(1, nil)
	require.Zero(t, b.dispatch(anySource))
	require.NotContains(t, policy.layouts.domains, int64(1))
	require.Contains(t, policy.layouts.domains, int64(2))
	for b.dispatch(anySource) >= 0 {
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
			b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, balancePolicyFunc(func(_ balancercache.Reader, batch []qviews.ShardID) *BalancePlan {
				batches = append(batches, batch)
				return nil
			}), nil)
			// Drive the producer and worker separately to inject the request at
			// the same point regardless of CPU speed or goroutine scheduling.
			b.running.Store(true)
			b.Trigger(TriggerScope{DirtyResourceGroups: []string{"rg1"}})
			require.Equal(t, discoveryShardLimit, b.dispatch(anySource))
			require.NoError(t, b.Reconcile(t.Context()))
			cold := cacheShard(10001, 100010)
			auto := cfgFor(10001, 100010, nil, nil)
			auto.AutoLoad = true
			c.PublishLoadConfig(10001, auto, 1)
			c.PublishDataView(10001, cacheData(10001, cold.VChannel, 1))
			require.Equal(t, 1, b.dispatch(anySource))
			require.NoError(t, b.Reconcile(t.Context()))
			require.Equal(t, cold, batches[1][0])
			require.Less(t, len(b.ready.items), 100)
			// Unfinished discovery must release its cursor on stop; the next
			// run can replay the range without losing a successor publication.
			b.discovery.suspend()
		})
	}
}

func TestActiveCollectionRediscoversSupersedingPublication(t *testing.T) {
	c := scanTestCache(discoveryShardLimit + 1)
	t.Cleanup(c.Close)
	b := NewDefaultBalancer(c, nil, nil, nil)
	b.Trigger(TriggerScope{DirtyCollections: []int64{1}})
	require.Equal(t, discoveryShardLimit, b.dispatch(anySource))
	first := cacheShard(1, 10)
	require.EqualValues(t, 2, b.ready.items[first].priority)
	cfg := c.GetCollection(1).LoadConfig().Clone()
	cfg.AutoLoad = true
	c.PublishLoadConfig(1, cfg, 2)
	b.Trigger(TriggerScope{DirtyCollections: []int64{1}})
	for b.dispatch(anySource) >= 0 {
	}
	require.EqualValues(t, 12, b.ready.items[first].priority)
	require.Len(t, b.ready.items, discoveryShardLimit+1)
}

// A blocked resident refresh must hold neither the node publisher nor the
// collection producer. Wait for startup discovery before publishing node loss.
func TestNodeRefreshDoesNotBlockColdCollection(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, nil, nil))
	t.Cleanup(c.Close)
	c.MarkReady()
	hot := cacheShard(1, 10)
	cold := cacheShard(2, 20)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishShard(hot, cacheStats(1, 1, 1, coordview.SegmentStateUp, true))
	startup, admitted := make(chan struct{}), make(chan struct{})
	started := sync.OnceFunc(func() { close(startup) })
	accepted := sync.OnceFunc(func() { close(admitted) })
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, balancePolicyFunc(func(_ balancercache.Reader, ids []qviews.ShardID) *BalancePlan {
		if slices.Contains(ids, hot) {
			started()
		}
		if slices.Contains(ids, cold) {
			accepted()
		}
		return nil
	}), nil)
	locked, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c.SetScheduler(func(scope TriggerScope) {
		if slices.Contains(scope.DirtyCollections, int64(1)) {
			close(locked)
			<-resume // PublishLoadConfig still owns collection 1's writer lock.
		}
		b.publishScope(scope)
	})
	b.Start(t.Context())
	defer b.Stop()
	select {
	case <-startup:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	go func() { defer close(done); c.PublishLoadConfig(1, cfgFor(1, 10, nil, nil), 2) }()
	defer func() { close(resume); <-done }()
	<-locked
	notified := make(chan struct{})
	go func() { c.PublishNode(1, nil); close(notified) }()
	select {
	case <-notified:
	case <-time.After(5 * time.Second):
		t.Fatal("node publisher waited for resident refresh")
	}
	require.Eventually(t, func() bool {
		b.discovery.mu.Lock()
		defer b.discovery.mu.Unlock()
		job := b.discovery.pending[scopeKey{kind: "node", id: 1}]
		return job != nil && job.active
	}, 5*time.Second, time.Millisecond)
	c.PublishLoadConfig(2, cfgFor(2, 20, nil, nil), 1)
	c.PublishDataView(2, cacheData(2, cold.VChannel, 1))
	select {
	case <-admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("cold collection waited for topology producer")
	}
}

// Force notifications both before dequeue and after Plan has selected a
// Preparing but before it is applied. The second pass must re-read the cache.
func TestOverlappingSourcesReplanWithoutDuplicatePreparing(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, nil, nil))
	t.Cleanup(c.Close)
	id := cacheShard(1, 10)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	c.MarkReady()
	reg := emptyRegistry(t)
	t.Cleanup(reg.RegisterPublicationListener(c.PublishShard))
	planned, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	policy := NewDefaultBalancePolicy()
	passes, prepares := 0, 0
	b := NewDefaultBalancer(c, reg, balancePolicyFunc(func(r balancercache.Reader, ids []qviews.ShardID) *BalancePlan {
		plan := policy.Plan(r, ids)
		passes++
		prepares += len(plan.Prepares)
		if passes == 1 {
			close(planned)
			<-resume
		}
		return plan
	}), nil)
	b.running.Store(true)
	overlap := func() {
		b.Trigger(TriggerScope{DirtyCollections: []int64{1}}, TriggerScope{DirtyResourceGroups: []string{"rg1"}})
		b.Trigger()
		var wg sync.WaitGroup
		for source := range producerCount {
			wg.Go(func() { b.dispatch(source) })
		}
		wg.Wait()
		require.Len(t, b.ready.items, 1)
	}
	overlap()
	result := make(chan error, 1)
	go func() { result <- b.Reconcile(t.Context()) }()
	select {
	case <-planned:
	case <-time.After(5 * time.Second):
		t.Fatal("first pass did not select a plan")
	}
	overlap()
	unblock()
	require.NoError(t, <-result)
	version := *reg.Get(id).Stats().PreparingVersion
	require.NoError(t, b.Reconcile(t.Context()))
	require.Equal(t, 2, passes)
	require.Equal(t, 1, prepares)
	require.Equal(t, version, *reg.Get(id).Stats().PreparingVersion)
}

// If a local notification wins the race with topology discovery, the worker
// must repair this shard's target before the policy sees the batch.
func TestLocalBatchRepairsTargetDuringNodeLoss(t *testing.T) {
	c := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	t.Cleanup(c.Close)
	id := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, id.VChannel, 100))
	publishTestNodes(c, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"}, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	stats := cacheStats(1000, 1, 0, coordview.SegmentStateUp, false)
	stats.UpNodes, stats.PreparingNodes = []int64{1}, []int64{2}
	stats.UpPlacement = &coordview.ViewPlacement{Assignments: map[int64]int64{1000: 1}, LostStats: []int64{1000}}
	stats.PreparingPlacement = &coordview.ViewPlacement{Assignments: map[int64]int64{1000: 2}, LostStats: []int64{1000}}
	c.PublishShard(id, stats)
	c.MarkReady()
	require.EqualValues(t, 100, c.GetNode(2).TargetContribution(id))
	b := NewDefaultBalancer(c, &coordview.ShardViewRegistry{}, balancePolicyFunc(func(r balancercache.Reader, _ []qviews.ShardID) *BalancePlan {
		require.True(t, r.(*planningContext).deferOptimization)
		require.EqualValues(t, 100, r.GetNode(1).TargetContribution(id))
		require.Zero(t, r.GetNode(2).TargetContribution(id))
		return nil
	}), nil)
	b.running.Store(true)
	c.PublishNode(2, nil)
	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{id}})
	require.NoError(t, b.Reconcile(t.Context()))
	require.Equal(t, 1, b.dispatch(topologyChanges))
	require.Zero(t, b.discovery.refreshingNodes.Load())
}
