package balancer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestBalancerRepairsPublishedSchemaWhileTargetRemainsPending(t *testing.T) {
	var latest atomic.Int32
	patch := mockey.Mock(testLatestSchema).To(func(context.Context, int64) (int32, error) { return latest.Load(), nil }).Build()
	defer patch.UnPatch()
	registry := emptyRegistry(t, func(_ context.Context, group syncer.SyncGroup) error {
		for _, views := range group.ViewsByNode {
			for _, view := range views {
				response := view.View.IntoProto()
				if response.GetMeta().GetSchemaVersion() == 1 {
					continue // Historical function output is still being materialized.
				}
				if response.Meta.State == viewpb.QueryViewState_QueryViewStatePreparing {
					response.Meta.State = viewpb.QueryViewState_QueryViewStateReady
				}
				view.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(response))
			}
		}
		return nil
	})
	cache := newTestCache(cfgFor(1, 10, []int64{1}, nil))
	id := cacheShard(1, 10)
	data := cacheData(1, id.VChannel, 100)
	cache.PublishDataView(1, data)
	cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	t.Cleanup(registry.RegisterPublicationListener(cache.PublishShard))
	cache.MarkReady()
	controller := NewDefaultBalancer(cache, registry, nil, testLatestSchema)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	manager := registry.Get(id)
	require.Eventually(t, func() bool { return manager.Stats().UpVersion != nil }, 5*time.Second, time.Millisecond)
	old := *manager.Stats().UpVersion
	manager.PublishSchema(0)
	latest.Store(1)
	data.DataVersion.CompactVersion = 1
	cache.PublishDataView(1, data)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.EqualValues(t, 1, manager.Stats().PreparingSchemaVersion)
	// A capacity-degraded layout can ask a suspended replica to drain while
	// its LoadConfig remains present. That must not erase the only old-schema
	// base before the pending target is materialized and published.
	drain := mockey.Mock((*DefaultBalancePolicy).Plan).Return(&BalancePlan{Releases: []qviews.ShardID{id}}).Build()
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	drain.UnPatch()
	require.NotNil(t, manager.PublishedRecovery(1))
	require.Contains(t, manager.Stats().UpBySchema, int32(0))
	cache.PublishNode(1, nil)
	cache.PublishNode(2, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Eventually(t, func() bool {
		up, ok := manager.Stats().UpBySchema[0]
		return ok && up.Version.GT(old)
	}, 5*time.Second, time.Millisecond)
	recovered := manager.Stats().UpBySchema[0].Version
	require.Equal(t, old.DataVersion, recovered.DataVersion, "repair must retain the published data base")
	require.Equal(t, []int64{2}, manager.Stats().UpNodes)
	for range 3 {
		controller.Trigger()
		require.NoError(t, controller.Reconcile(t.Context()))
		require.Equal(t, recovered, manager.Stats().UpBySchema[0].Version, "healthy recovery must not be recreated on each reconcile")
		require.EqualValues(t, 1, manager.Stats().PreparingSchemaVersion)
	}
	cache.PublishLoadConfig(1, nil, 2)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Nil(t, manager.PublishedRecovery(1), "explicit release must still remove the recovery obligation")
}
