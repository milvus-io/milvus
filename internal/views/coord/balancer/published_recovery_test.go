package balancer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestBalancerRecoversOnlyLatestView(t *testing.T) {
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
	history := cache.GetCollection(1).Planning().Replica(id.ReplicaID)
	require.Positive(t, history.Fanout(id.VChannel))
	old := *manager.Stats().UpVersion
	manager.PublishSchema(0)
	latest.Store(1)
	// The target is known, but DataView still certifies schema 0. Losing its
	// QN must recover the published base despite target admission being gated.
	cache.PublishNode(1, nil)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Equal(t, old, manager.Stats().UpBySchema[0].Version, "waiting for a target must retain the published base")
	require.Equal(t, 1, history.Fanout(id.VChannel), "waiting for a target preserves the previous allocation")
	cache.PublishNode(2, &NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	// Once a target exists, a failed allocation must surface for retry.
	mockey.PatchConvey("allocation failure after topology recovery", t, func() {
		mockey.Mock(allocate).Return(nil).Build()
		controller.Trigger()
		require.Error(t, controller.Reconcile(t.Context()))
		require.Equal(t, old, manager.Stats().UpBySchema[0].Version)
	})
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Eventually(t, func() bool {
		up, ok := manager.Stats().UpBySchema[0]
		return ok && up.Version.GT(old)
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, old.DataVersion, manager.Stats().UpBySchema[0].Version.DataVersion)
	require.EqualValues(t, 0, manager.Stats().PreparingSchemaVersion)
	require.Equal(t, 1, history.Fanout(id.VChannel), "published recovery updated the same replica history")
	old = manager.Stats().UpBySchema[0].Version
	data = cacheData(1, id.VChannel, 100)
	data.SchemaVersion = 1
	data.DataVersion.CompactVersion = 1
	cache.PublishDataView(1, data)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.EqualValues(t, 1, manager.Stats().PreparingSchemaVersion)
	target := *manager.Stats().PreparingVersion
	require.Nil(t, manager.PublishedRecovery(1), "newer data preparation supersedes published-schema recovery")
	cache.PublishNode(2, nil)
	cache.PublishNode(3, &NodeInfo{NodeID: 3, Alive: true, ResourceGroup: "rg1"})
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Eventually(t, func() bool {
		preparing := manager.Stats().PreparingVersion
		return preparing != nil && preparing.GT(target)
	}, 5*time.Second, time.Millisecond)
	recovered := *manager.Stats().PreparingVersion
	require.Equal(t, target.DataVersion, recovered.DataVersion, "recover the target rather than the old published data base")
	for range 3 {
		controller.Trigger()
		require.NoError(t, controller.Reconcile(t.Context()))
		require.Equal(t, old, manager.Stats().UpBySchema[0].Version, "no replacement may be generated for the older schema")
		require.Equal(t, recovered, *manager.Stats().PreparingVersion, "healthy preparation must not be recreated on each reconcile")
		require.EqualValues(t, 1, manager.Stats().PreparingSchemaVersion)
	}
	cache.PublishLoadConfig(1, nil, 2)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(t.Context()))
	require.Nil(t, manager.PublishedRecovery(1), "explicit release must still remove the recovery obligation")
}

func TestPublishedRecoveryPreservesPartitionStats(t *testing.T) {
	id := qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"}
	original := &viewpb.QueryViewOfShard{
		Meta:          &viewpb.QueryViewMeta{CollectionId: 1, RequiredStreamingVersion: 7, Vchannel: id.VChannel, ReplicaId: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 10}}},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{PartitionStats: []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}},
		QueryNode:     []*viewpb.QueryViewOfQueryNode{{NodeId: 10, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 2, SegmentIds: []int64{101}, SegmentManifestVersions: []int64{4}}}}},
	}
	base := newPublishedRecoveryBase(id, &coordview.PublishedRecovery{View: original})
	data := &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: original.Meta.Version.DataVersion, Shards: []*viewpb.DataViewOfShard{shardDataViewToProto(base.shard)}}
	replacement := qviews.NewQueryViewAtCoordBuilder(1, data, id.VChannel).SetAssignments(map[int64]map[int64][]int64{20: {2: {101}}}).Build()
	require.EqualValues(t, 7, replacement.Meta.RequiredStreamingVersion)
	require.EqualValues(t, 7, qviews.NewQueryViewAtCoordBuilderFromView(replacement).Build().Meta.RequiredStreamingVersion)
	require.Equal(t, original.StreamingNode.PartitionStats, replacement.StreamingNode.PartitionStats)
	require.Equal(t, []int64{4}, replacement.QueryNode[0].Partitions[0].SegmentManifestVersions)
}
