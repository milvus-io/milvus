package balancer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	b.Trigger()
	time.Sleep(10 * time.Millisecond)
	b.Stop()
	b.Stop()
}

func TestBalancerLoopInitialAndPeriodicFullReconcile(t *testing.T) {
	reg := emptyRegistry(t)
	c := newTestCache(cfgFor(1, 10, nil, nil))
	params := paramtable.Get()
	setBalanceParam(t, params, params.QueryViewCfg.BalancerReconcileInterval.Key, "10ms")
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

func TestBalancerLoopRetriesDetachedManager(t *testing.T) {
	registry := emptyRegistry(t)
	shard := cacheShard(1, 10)
	cache := newTestCache(cfgFor(1, 10, []int64{100}, nil))
	cache.PublishDataView(1, cacheData(1, shard.VChannel, 100))
	cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	t.Cleanup(registry.RegisterPublicationListener(cache.PublishShard))
	config := *cache.GetBalanceConfig()
	config.TickerInterval = time.Hour
	cache.UpdateBalanceConfig(&config)
	cache.MarkReady()

	// Retire the first manager between Ensure and AddPreparing, reproducing
	// the stale-pointer interleaving without replacing either implementation.
	var ensure func(*coordview.ShardViewRegistry, qviews.ShardID) *coordview.ShardViewManager
	var retired *coordview.ShardViewManager
	var releaseErr error
	ensurePatch := mockey.Mock((*coordview.ShardViewRegistry).Ensure).Origin(&ensure).To(func(r *coordview.ShardViewRegistry, id qviews.ShardID) *coordview.ShardViewManager {
		manager := ensure(r, id)
		if retired == nil {
			retired = manager
			releaseErr = manager.RequestRelease(t.Context())
		}
		return manager
	}).Build()
	t.Cleanup(func() { ensurePatch.UnPatch() })

	type attempt struct {
		manager *coordview.ShardViewManager
		err     error
	}
	attempts := make(chan attempt, 4)
	var prepare func(*coordview.ShardViewManager, context.Context, *qviews.QueryViewAtCoordBuilder) error
	preparePatch := mockey.Mock((*coordview.ShardViewManager).AddPreparing).Origin(&prepare).To(func(manager *coordview.ShardViewManager, ctx context.Context, builder *qviews.QueryViewAtCoordBuilder) error {
		err := prepare(manager, ctx, builder)
		select {
		case attempts <- attempt{manager: manager, err: err}:
		case <-ctx.Done():
		}
		return err
	}).Build()
	t.Cleanup(func() { preparePatch.UnPatch() })

	balancer := NewDefaultBalancer(cache, registry, nil)
	// Only the initial scan and apply's own retry can drive this test:
	// neither cache notifications nor the periodic timer may rescue it.
	cache.SetNotifier(nil)
	balancer.Start(t.Context())
	t.Cleanup(balancer.Stop)
	nextAttempt := func() attempt {
		t.Helper()
		select {
		case result := <-attempts:
			return result
		case <-time.After(5 * time.Second):
			t.Fatal("balancer did not retry preparation through the current manager")
			return attempt{}
		}
	}
	first, second := nextAttempt(), nextAttempt()
	balancer.Stop()
	require.NoError(t, releaseErr)
	require.Same(t, retired, first.manager)
	require.ErrorIs(t, first.err, merr.ErrServiceUnavailable)
	require.NoError(t, second.err)
	require.NotSame(t, first.manager, second.manager)
	require.Same(t, registry.Get(shard), second.manager)
	require.Nil(t, first.manager.Stats().PreparingVersion)
	require.NotNil(t, second.manager.Stats().PreparingVersion)
	require.Empty(t, attempts)
}

func TestBalancerApplyDoesNotHoldOtherShardEventsDuringReferenceWait(t *testing.T) {
	flushed := make(chan struct{}, 1)
	registry := emptyRegistry(t, func(context.Context, syncer.SyncGroup) error {
		select {
		case flushed <- struct{}{}:
		default:
		}
		return nil
	})
	blockedShard, otherShard := cacheShard(1, 10), cacheShard(1, 11)
	blockedManager := registry.Ensure(blockedShard)
	started, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	defer release()
	var prepare func(*coordview.ShardViewManager, context.Context, *qviews.QueryViewAtCoordBuilder) error
	patch := mockey.Mock((*coordview.ShardViewManager).AddPreparing).Origin(&prepare).To(func(manager *coordview.ShardViewManager, ctx context.Context, builder *qviews.QueryViewAtCoordBuilder) error {
		if manager == blockedManager {
			// Emulate the provider wait inside AddPreparing. The real provider
			// and manager lock behavior are covered in coordview's tests.
			close(started)
			<-unblock
		}
		return prepare(manager, ctx, builder)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	controller := NewDefaultBalancer(nil, registry, nil)
	// Use a DataVersion retained by the real registry fixture.
	builder := qviews.NewQueryViewAtCoordBuilder(blockedShard.ReplicaID, &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
		Shards:       []*viewpb.DataViewOfShard{{Vchannel: blockedShard.VChannel}},
	}, blockedShard.VChannel)
	done := make(chan error, 1)
	var workers sync.WaitGroup
	workers.Go(func() {
		done <- controller.apply(t.Context(), &BalancePlan{Prepares: map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder{blockedShard: builder}})
	})
	defer func() {
		release()
		workers.Wait()
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("apply did not reach reference acquisition")
	}
	addShardWithPreparingView(t, registry, otherShard, map[int64]map[int64][]int64{1: {100: {101}}})
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("one blocked acquisition held another shard's flush")
	}
	release()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("apply did not finish after reference acquisition resumed")
	}
}
