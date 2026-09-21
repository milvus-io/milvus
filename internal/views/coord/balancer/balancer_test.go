package balancer

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestBalancer_ReconcileDirtyShardAppliesPrepare(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}

	reg := emptyRegistry(t)
	reg.Ensure(shardID)

	cache := newTestCache(cfgFor(collID, replicaID, []int64{100}, nil))
	cache.PublishDataView(collID, cacheData(collID, shardID.VChannel, 600, 200))
	t.Cleanup(reg.RegisterPublicationListener(cache.PublishShard))
	cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	cache.PublishNode(2, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	cache.MarkReady()
	b := NewDefaultBalancer(cache, reg, nil)

	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
	require.NoError(t, b.Reconcile(context.Background()))

	stats := reg.Get(shardID).Stats()
	require.NotNil(t, stats)
	assert.NotNil(t, stats.PreparingVersion)
	assert.NotEmpty(t, stats.Segments)
}

func TestBalancer_ReconcileDirtyCollectionCreatesDataViewShards(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}

	reg := emptyRegistry(t)
	cache := newTestCache(cfgFor(collID, replicaID, []int64{100}, nil))
	cache.PublishDataView(collID, cacheData(collID, shardID.VChannel, 100))
	t.Cleanup(reg.RegisterPublicationListener(cache.PublishShard))
	cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	cache.MarkReady()
	b := NewDefaultBalancer(cache, reg, nil)

	b.Trigger(TriggerScope{DirtyCollections: []int64{collID}})
	require.NoError(t, b.Reconcile(context.Background()))

	mgr := reg.Get(shardID)
	require.NotNil(t, mgr)
	stats := mgr.Stats()
	assert.NotNil(t, stats.PreparingVersion)
}

func TestBalancer_ReconcilePreservesTriggerArrivingDuringCacheRead(t *testing.T) {
	const collectionID, replicaID int64 = 1, 10
	shard := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	registry := emptyRegistry(t)
	addShardWithPreparingView(t, registry, shard, map[int64]map[int64][]int64{1: {100: {101}}})
	cache := balancercache.New(policyTestConfig())
	cache.PublishLoadConfig(collectionID, cfgFor(collectionID, replicaID, nil, nil), 1)
	cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	t.Cleanup(registry.RegisterPublicationListener(cache.PublishShard))
	cache.MarkReady()
	controller := NewDefaultBalancer(cache, registry, nil)
	var original func(*balancercache.Cache, int64) *balancercache.CollectionEntry
	once := true
	patch := mockey.Mock((*balancercache.Cache).GetCollection).Origin(&original).To(func(c *balancercache.Cache, id int64) *balancercache.CollectionEntry {
		entry := original(c, id)
		if once {
			once = false
			cache.PublishLoadConfig(collectionID, nil, 2)
		}
		return entry
	}).Build()
	defer patch.UnPatch()
	controller.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shard}})
	require.NoError(t, controller.Reconcile(t.Context()))
	require.NotNil(t, registry.Get(shard).Stats().PreparingVersion)
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Nil(t, registry.Get(shard).Stats().PreparingVersion)
}

func TestBalancer_NodePublicationTriggersCollection(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}

	reg := emptyRegistry(t)
	reg.Ensure(shardID)
	cache := newTestCache(cfgFor(collID, replicaID, []int64{100}, nil))
	cache.PublishDataView(collID, cacheData(collID, shardID.VChannel, 100))
	t.Cleanup(reg.RegisterPublicationListener(cache.PublishShard))
	cache.MarkReady()
	b := NewDefaultBalancer(cache, reg, nil)

	cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	require.NoError(t, b.Reconcile(context.Background()))

	stats := reg.Get(shardID).Stats()
	assert.NotNil(t, stats.PreparingVersion)
}

func TestBalancer_ReconcileFullScanDoesNotRestackPreparing(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}

	reg := emptyRegistry(t)
	reg.Ensure(shardID)
	cache := newTestCache(cfgFor(collID, replicaID, []int64{100}, nil))
	cache.PublishDataView(collID, cacheData(collID, shardID.VChannel, 100))
	t.Cleanup(reg.RegisterPublicationListener(cache.PublishShard))
	cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	cache.MarkReady()
	b := NewDefaultBalancer(cache, reg, nil)

	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
	require.NoError(t, b.Reconcile(context.Background()))
	stats := reg.Get(shardID).Stats()
	require.NotNil(t, stats.PreparingVersion)
	before := stats.Segments

	b.Trigger()
	require.NoError(t, b.Reconcile(context.Background()))
	after := reg.Get(shardID).Stats().Segments
	assert.Equal(t, before, after)
}

func TestBalancer_StartStop(t *testing.T) {
	reg := emptyRegistry(t)
	b := NewDefaultBalancer(nil, reg, nil)
	assert.Equal(t, 10*time.Second, b.tickerInterval)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	b.Trigger()
	time.Sleep(10 * time.Millisecond)
	b.Stop()
	b.Stop()
}

func TestBalancer_UsesConfiguredTickerInterval(t *testing.T) {
	reg := emptyRegistry(t)
	c := balancercache.New(&BalanceConfig{TickerInterval: 5 * time.Minute})
	b := NewDefaultBalancer(c, reg, nil)

	assert.Equal(t, 5*time.Minute, b.tickerInterval)
}

func TestBalancerLoopInitialAndPeriodicFullReconcile(t *testing.T) {
	reg := emptyRegistry(t)
	c := newTestCache(cfgFor(1, 10, nil, nil))
	config := policyTestConfig()
	config.TickerInterval = 10 * time.Millisecond
	c.UpdateBalanceConfig(config)
	first := cacheShard(1, 10)
	c.PublishDataView(1, cacheData(1, first.VChannel, 100))
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	t.Cleanup(reg.RegisterPublicationListener(c.PublishShard))
	c.MarkReady()
	b := NewDefaultBalancer(c, reg, nil)
	b.Start(t.Context())
	b.Start(t.Context()) // A second Start must not create another loop.
	t.Cleanup(b.Stop)
	require.Eventually(t, func() bool {
		manager := reg.Get(first)
		return manager != nil && manager.Stats().PreparingVersion != nil
	}, 5*time.Second, time.Millisecond)

	// Publish work without a notifier: only a periodic full pass can discover
	// this second desired shard. Preparing notifications target only the first.
	c.SetNotifier(nil)
	second := first
	second.VChannel = "by-dev-rootcoord-dml_0_1v1"
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1}, nil,
		shardDataView(first.VChannel, 1, 1000), shardDataView(second.VChannel, 1, 1001))
	require.Eventually(t, func() bool {
		manager := reg.Get(second)
		return manager != nil && manager.Stats().PreparingVersion != nil
	}, 5*time.Second, time.Millisecond)
}

func TestBalancerLoopRetriesUnavailableAllocation(t *testing.T) {
	reg := emptyRegistry(t)
	c := newTestCache(cfgFor(1, 10, nil, nil))
	shard := cacheShard(1, 10)
	c.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishShard(shard, &coordview.ShardStats{})
	t.Cleanup(reg.RegisterPublicationListener(c.PublishShard))
	c.MarkReady()
	failed := make(chan struct{}, 1)
	var original func(*DefaultBalancePolicy, balancercache.Reader, []qviews.ShardID) *BalancePlan
	patch := mockey.Mock((*DefaultBalancePolicy).Plan).Origin(&original).To(func(p *DefaultBalancePolicy, reader balancercache.Reader, dirty []qviews.ShardID) *BalancePlan {
		plan := original(p, reader, dirty)
		if len(plan.Retries) > 0 {
			select {
			case failed <- struct{}{}:
			default:
			}
		}
		return plan
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	b := NewDefaultBalancer(c, reg, nil)
	b.Start(t.Context())
	t.Cleanup(b.Stop)
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("initial allocation did not retry missing DataView")
	}
	// Disable publication notifications so recovery must consume the queued retry.
	c.SetNotifier(nil)
	c.PublishDataView(1, cacheData(1, shard.VChannel, 100))
	require.Eventually(t, func() bool {
		manager := reg.Get(shard)
		return manager != nil && manager.Stats().PreparingVersion != nil
	}, 5*time.Second, time.Millisecond)
}
