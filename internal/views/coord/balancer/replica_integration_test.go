package balancer

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/qviews"
	qvobserve "github.com/milvus-io/milvus/internal/views/qviews/observe"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestReplicaControllerZeroCapacityHandoff(t *testing.T) {
	var releases, handoffs atomic.Int64
	observer := mockey.Mock(qvobserve.Observe).To(func(_ context.Context, event qvobserve.Event) {
		switch event.(type) {
		case qvobserve.CoordViewReleaseRequestedEvent:
			releases.Add(1)
		case qvobserve.CoordViewHandoffToNewUpEvent:
			handoffs.Add(1)
		}
	}).Build()
	t.Cleanup(func() { observer.UnPatch() })
	responses := make(chan syncer.SyncView, 1)
	registry := emptyRegistry(t, func(_ context.Context, group syncer.SyncGroup) error {
		for _, views := range group.ViewsByNode {
			for _, view := range views {
				if node, ok := view.View.WorkNode().(qviews.QueryNode); ok && node.ID == 2 && view.View.State() == qviews.QueryViewStatePreparing {
					responses <- view
					continue
				}
				response := view.View.IntoProto()
				if response.Meta.State == viewpb.QueryViewState_QueryViewStatePreparing {
					response.Meta.State = viewpb.QueryViewState_QueryViewStateReady
				}
				view.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(response))
			}
		}
		return nil
	})
	c := replicaCache(1, 1)
	t.Cleanup(registry.RegisterPublicationListener(c.PublishShard))
	c.MarkReady()
	controller := NewDefaultBalancer(c, registry, nil, nil)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	id := cacheShard(1, 10)
	mgr := registry.Get(id)
	require.NotNil(t, mgr)
	require.Eventually(t, func() bool { return mgr.Stats().UpVersion != nil }, 5*time.Second, time.Millisecond)
	old := *mgr.Stats().UpVersion
	c.PublishNode(1, &NodeInfo{NodeID: 1, ResourceGroup: "rg1", Alive: true, Stopping: true})
	for range 3 {
		controller.Trigger()
		require.NoError(t, controller.Reconcile(t.Context()))
		require.Same(t, mgr, registry.Get(id))
		require.Equal(t, &old, mgr.Stats().UpVersion)
		require.Nil(t, mgr.Stats().PreparingVersion)
	}
	require.Zero(t, releases.Load())
	c.PublishNode(2, &NodeInfo{NodeID: 2, ResourceGroup: "rg1", Alive: true})
	require.NoError(t, controller.Reconcile(t.Context()))
	var pending syncer.SyncView
	select {
	case pending = <-responses:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement was not prepared on the new QN")
	}
	require.Equal(t, &old, mgr.Stats().UpVersion, "old Up survives until replacement is ready")
	require.NotNil(t, mgr.Stats().PreparingVersion)
	require.Zero(t, handoffs.Load())
	response := pending.View.IntoProto()
	response.Meta.State = viewpb.QueryViewState_QueryViewStateReady
	pending.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(response))
	require.Eventually(t, func() bool {
		stats := mgr.Stats()
		return stats.UpVersion != nil && stats.UpVersion.QueryVersion > old.QueryVersion && stats.PreparingVersion == nil
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, []int64{2}, mgr.Stats().UpNodes)
	require.EqualValues(t, 1, handoffs.Load())
	require.Zero(t, releases.Load())
}

func TestReplicaControllerSuspensionAndRestoration(t *testing.T) {
	store, err := loadmgr.RecoverLoadConfigStore(t.Context(), loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	require.NoError(t, store.Put(t.Context(), &loadmgr.LoadConfig{
		CollectionID: 1, PartitionIDs: []int64{1},
		Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10, ResourceGroup: "rg1"}, {ReplicaID: 11, ResourceGroup: "rg1"}},
	}))
	var lost atomic.Int64
	// Only catalog/transport boundaries are mocked. Registry, lifecycle, cache,
	// policy, controller and load config store run their real implementations.
	registry := emptyRegistry(t, func(_ context.Context, group syncer.SyncGroup) error {
		for _, views := range group.ViewsByNode {
			for _, view := range views {
				if node, ok := view.View.WorkNode().(qviews.QueryNode); ok && node.ID == lost.Load() {
					if view.OnQueryNodeLost != nil {
						view.OnQueryNodeLost(node)
					}
					continue
				}
				response := view.View.IntoProto()
				if response.Meta.State == viewpb.QueryViewState_QueryViewStatePreparing {
					response.Meta.State = viewpb.QueryViewState_QueryViewStateReady
				}
				for _, node := range response.QueryNode {
					for _, part := range node.Partitions {
						part.ReadySegmentIds = part.SegmentIds
					}
				}
				view.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(response))
			}
		}
		return nil
	})
	cache := replicaCache(2, 2)
	t.Cleanup(store.RegisterLoadConfigListener(cache.PublishLoadConfig))
	t.Cleanup(registry.RegisterPublicationListener(cache.PublishShard))
	cache.MarkReady()
	controller := NewDefaultBalancer(cache, registry, nil, nil)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Eventually(t, func() bool {
		for _, id := range []qviews.ShardID{cacheShard(1, 10), cacheShard(1, 11)} {
			if mgr := registry.Get(id); mgr == nil || mgr.Stats().UpVersion == nil {
				return false
			}
		}
		return true
	}, 5*time.Second, time.Millisecond)
	nodeOneReplica := int64(0)
	for _, id := range []qviews.ShardID{cacheShard(1, 10), cacheShard(1, 11)} {
		if registry.Get(id).Stats().UpNodes[0] == 1 {
			nodeOneReplica = id.ReplicaID
		}
	}
	require.NotZero(t, nodeOneReplica)
	id := cacheShard(1, nodeOneReplica)
	manager := registry.Get(id)
	up := manager.Stats()
	key := coordview.ResourceKey{PartitionID: 1, SegmentID: 1000, DataVersion: up.UpVersion.DataVersion, LoadInfoVersion: up.UpLoadInfoVersion}
	lost.Store(1)
	cache.PublishNode(1, nil)
	for range 3 {
		controller.Trigger()
		require.NoError(t, controller.Reconcile(t.Context()))
		require.Same(t, manager, registry.Get(id))
		stats := manager.Stats()
		require.Equal(t, up.UpVersion, stats.UpVersion)
		require.Nil(t, stats.PreparingVersion)
		require.Contains(t, stats.Resources[int64(1)], key)
	}
	require.Len(t, store.Snapshot().ConfigsMap()[1].Replicas, 2)

	cache.PublishNode(3, &NodeInfo{NodeID: 3, ResourceGroup: "rg1", Alive: true})
	require.Eventually(t, func() bool {
		if controller.Reconcile(t.Context()) != nil {
			return false
		}
		stats := manager.Stats()
		return stats.UpVersion != nil && stats.UpVersion.GT(*up.UpVersion) && slices.Equal(stats.UpNodes, []int64{3})
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, up.UpVersion.DataVersion, manager.Stats().UpVersion.DataVersion)

	registry.PublishCollectionSchema(1, 0)
	// Removing this replica from the desired config still retires its manager.
	cfg := store.Snapshot().ConfigsMap()[1].Clone()
	cfg.Replicas = slices.DeleteFunc(cfg.Replicas, func(r *loadmgr.ReplicaAssignment) bool { return r.ReplicaID == id.ReplicaID })
	require.NoError(t, store.Put(t.Context(), cfg))
	require.Eventually(t, func() bool {
		return controller.Reconcile(t.Context()) == nil && registry.Get(id) == nil
	}, 5*time.Second, time.Millisecond)
	require.NotNil(t, registry.Get(cacheShard(1, cfg.Replicas[0].ReplicaID)))
}

// Recovery may replay desired state, DataViews, and resident old/new managers
// in different orders. Only the desired replica x vchannel product counts.
func TestRetirementWaitsForWholeTarget(t *testing.T) {
	c := replicaCache(2, 2)
	defer c.Close()
	a, b, removed := cacheShard(1, 10), cacheShard(1, 11), cacheShard(1, 12)
	channel2 := "by-dev-rootcoord-dml_1_1v1"
	c.PublishShard(removed, testShardStats(ver(1, 0, 1), 1))
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 2}, nil, shardDataView(a.VChannel, 1), shardDataView(channel2, 1))
	replacement := func(node int64) *coordview.ShardStats {
		return testShardStats(ver(2, 0, 1), 1, placement(1000, 1, node, coordview.SegmentStateUp))
	}
	check := func(ready bool) {
		t.Helper()
		plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{removed})
		require.Equal(t, ready, slices.Contains(plan.Releases, removed))
	}
	for _, id := range []qviews.ShardID{a, b, {ReplicaID: a.ReplicaID, VChannel: channel2}} {
		c.PublishShard(id, replacement(1))
	}
	last := qviews.ShardID{ReplicaID: b.ReplicaID, VChannel: channel2}
	c.PublishShard(qviews.ShardID{ReplicaID: b.ReplicaID, VChannel: "extra_1v2"}, replacement(2))
	check(false)
	c.PublishShard(last, testShardStats(ver(1, 0, 1), 1))
	check(false)
	c.PublishShard(last, testShardStats(ver(2, 0, 1), 9))
	check(false)
	stale := replacement(2)
	stale.UpSchemaVersion = 3
	c.PublishShard(last, stale)
	check(false)

	var rescanned []int64
	c.SetScheduler(func(scope TriggerScope) { rescanned = append(rescanned, scope.DirtyCollections...) })
	c.PublishShard(last, replacement(2))
	require.Equal(t, []int64{1}, rescanned)
	check(true)
	for _, info := range []*NodeInfo{nil, {NodeID: 1, ResourceGroup: "rg1", Alive: true, Stopping: true}, {NodeID: 1, ResourceGroup: "other", Alive: true}} {
		c.PublishNode(1, info)
		check(false)
	}
	c.PublishNode(1, &NodeInfo{NodeID: 1, ResourceGroup: "rg1", Alive: true})
	check(true)
	c.PublishShard(last, nil)
	check(false)
}

func TestRetirementRechecksApplyInputs(t *testing.T) {
	c := replicaCache(1, 1)
	defer c.Close()
	target, old := cacheShard(1, 10), cacheShard(1, 11)
	registry := emptyRegistry(t)
	t.Cleanup(registry.RegisterPublicationListener(c.PublishShard))
	addShardWithPreparingView(t, registry, old, nil)
	c.PublishShard(target, testShardStats(ver(1, 0, 1), 1, placement(1000, 1, 1, coordview.SegmentStateUp)))
	b := NewDefaultBalancer(c, registry, nil, nil)
	pinned := newPlanningContext(c)
	plan := b.policy.Plan(pinned, []qviews.ShardID{old})
	require.Contains(t, plan.Releases, old)
	manager := registry.Get(old)
	require.NoError(t, manager.RequestReplicaRetirement(t.Context(), 0, func() bool { return true }))
	require.NotNil(t, manager.Stats().PreparingVersion, "schema zero also needs a recovered public fence")
	manager.PublishSchema(0)
	require.NoError(t, manager.RequestReplicaRetirement(t.Context(), 1, func() bool { return true }))
	require.NotNil(t, manager.Stats().PreparingVersion, "a replacement on an unpublished schema cannot take over reads")
	// Keep the same desired config, but lose the destination after planning.
	c.PublishNode(1, nil)
	require.NoError(t, b.apply(t.Context(), pinned, plan, nil))
	require.NotNil(t, registry.Get(old).Stats().PreparingVersion)
	c.PublishNode(1, &NodeInfo{NodeID: 1, ResourceGroup: "rg1", Alive: true})
	cfg := c.GetCollection(1).LoadConfig().Clone()
	cfg.Replicas[0].ReplicaID = 13
	c.PublishLoadConfig(1, cfg, 2)
	require.NoError(t, b.apply(t.Context(), pinned, plan, nil))
	require.NotNil(t, registry.Get(old).Stats().PreparingVersion)
}
