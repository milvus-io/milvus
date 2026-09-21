package balancer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestReplicaControllerLifecycleAndDiscovery(t *testing.T) {
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*querycoord.Catalog).GetCollections).Return([]*querypb.CollectionLoadInfo{{CollectionID: 1}}, nil).Build(),
		mockey.Mock((*querycoord.Catalog).GetPartitions).Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Build(),
		mockey.Mock((*querycoord.Catalog).GetReplicas).Return([]*querypb.Replica{{ID: 10, CollectionID: 1, ResourceGroup: "rg1"}, {ID: 11, CollectionID: 1, ResourceGroup: "rg1"}}, nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	store, err := loadmgr.RecoverLoadConfigStore(t.Context(), querycoord.NewCatalog(nil))
	require.NoError(t, err)
	discovery := loadmgr.NewCollectionLoadManager(store, nil)
	var lost atomic.Int64
	var holdDrops atomic.Bool
	var dropMu sync.Mutex
	var drops []syncer.SyncView
	// Only catalog/transport boundaries are mocked. Registry, lifecycle, cache,
	// policy, controller and discovery manager run their real implementations.
	registry := emptyRegistry(t, func(_ context.Context, group syncer.SyncGroup) error {
		for _, views := range group.ViewsByNode {
			for _, view := range views {
				if holdDrops.Load() && view.View.State() == qviews.QueryViewStateDropped {
					dropMu.Lock()
					drops = append(drops, view)
					dropMu.Unlock()
					continue
				}
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
	controller := NewDefaultBalancer(cache, registry, nil)
	controller.SetDiscoveryPublisher(discovery)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	awaitServing := func(count int) {
		t.Helper()
		require.Eventually(t, func() bool {
			if controller.Reconcile(t.Context()) != nil {
				return false
			}
			return len(discovery.ShardAssignmentsByPChannel()["by-dev-rootcoord-dml_0"]) == count
		}, 5*time.Second, time.Millisecond)
	}
	awaitServing(2)
	nodeOneReplica := int64(0)
	for _, id := range []qviews.ShardID{cacheShard(1, 10), cacheShard(1, 11)} {
		if registry.Get(id).Stats().UpNodes[0] == 1 {
			nodeOneReplica = id.ReplicaID
		}
	}
	require.NotZero(t, nodeOneReplica)
	retired := cacheShard(1, nodeOneReplica)
	up := registry.Get(retired).Stats()
	key := coordview.ResourceKey{PartitionID: 1, SegmentID: 1000, DataVersion: up.UpVersion.DataVersion, LoadInfoVersion: up.UpLoadInfoVersion}
	require.Contains(t, up.Resources[int64(1)], key)
	holdDrops.Store(true)
	lost.Store(1)
	cache.PublishNode(1, nil)
	require.NoError(t, controller.Reconcile(t.Context()))
	awaitServing(1)
	require.Eventually(t, func() bool {
		dropMu.Lock()
		defer dropMu.Unlock()
		return len(drops) == 2
	}, 5*time.Second, time.Millisecond)
	dropping := registry.Get(retired).Stats()
	require.Empty(t, dropping.Segments)
	require.Empty(t, dropping.Resources)
	require.Equal(t, []int64{1}, dropping.ResidentNodes)
	require.Equal(t, []int64{1}, cache.GetCollection(1).GetShard(retired).ResidentNodes())
	holdDrops.Store(false)
	dropMu.Lock()
	responses := append([]syncer.SyncView(nil), drops...)
	dropMu.Unlock()
	for _, view := range responses {
		view.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(view.View.IntoProto()))
	}
	require.Eventually(t, func() bool { return registry.Get(retired) == nil }, 5*time.Second, time.Millisecond)
	require.Equal(t, []int64{1}, dropping.ResidentNodes, "published facts remain immutable after durable cleanup")
	require.Contains(t, up.Resources[int64(1)], key)
	discovery.ObserveShardUp(retired)
	require.Len(t, discovery.ShardAssignmentsByPChannel()["by-dev-rootcoord-dml_0"], 1)
	cache.PublishNode(3, &NodeInfo{NodeID: 3, ResourceGroup: "rg1", Alive: true})
	require.NoError(t, controller.Reconcile(t.Context()))
	awaitServing(2)
	require.Equal(t, []int64{3}, registry.Get(retired).Stats().UpNodes)
	require.Len(t, store.Snapshot().ConfigsMap()[1].Replicas, 2)
}

func TestDiscoveryRejectionPreservesViewsAndRequeues(t *testing.T) {
	registry := emptyRegistry(t)
	cache := replicaCache(1, 1)
	id := cacheShard(1, 10)
	addShardWithPreparingView(t, registry, id, map[int64]map[int64][]int64{1: {1: {1000}}})
	controller := NewDefaultBalancer(cache, registry, nil)
	publisher := loadmgr.NewCollectionLoadManager(nil, nil)
	patch := mockey.Mock((*loadmgr.CollectionLoadManager).UpdateCollectionDiscovery).Return(false).Build()
	t.Cleanup(func() { patch.UnPatch() })
	controller.SetDiscoveryPublisher(publisher)
	plan := &BalancePlan{Releases: []qviews.ShardID{id}, Discovery: []loadmgr.CollectionDiscoveryUpdate{{CollectionID: 1, Revision: 1}}}
	require.Error(t, controller.apply(t.Context(), plan))
	require.NotNil(t, registry.Get(id).Stats().PreparingVersion)
	require.Contains(t, controller.queue.takePending().dirtyColls, int64(1))
}
