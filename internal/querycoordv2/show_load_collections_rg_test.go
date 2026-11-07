package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestShowLoadCollectionsQueryViewResourceGroups(t *testing.T) {
	paramtable.Init()
	if meta.GlobalFailedLoadCache == nil {
		meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	}
	for _, patch := range []*mockey.Mocker{
		mockey.Mock(catalogkv.Catalog.GetCollections).Return([]*querypb.CollectionLoadInfo(nil), nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetPartitions).Return(map[int64][]*querypb.PartitionLoadInfo(nil), nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetReplicas).Return([]*querypb.Replica(nil), nil).Build(),
		mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.ReleaseCollection).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.ReleaseReplicas).Return(nil).Build(),
		mockey.Mock((*meta.ResourceManager).ContainResourceGroup).To(func(_ *meta.ResourceManager, _ context.Context, rg string) bool {
			return rg == "a" || rg == "b" || rg == "empty"
		}).Build(),
		mockey.Mock((*meta.CoordinatorBroker).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{VirtualChannelNames: []string{"v0", "v1"}}, nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	ctx := context.Background()
	store, err := loadmgr.RecoverLoadConfigStore(ctx, loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	cfg := &loadmgr.LoadConfig{CollectionID: 100, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10, ResourceGroup: "a"}, {ReplicaID: 20, ResourceGroup: "b"}}}
	require.NoError(t, store.Put(ctx, cfg))
	stats := map[qviews.ShardID]*coordview.ShardStats{}
	var duringRead func()
	patch := mockey.Mock((*coordview.ShardViewRegistry).SnapshotForShards).To(func(_ *coordview.ShardViewRegistry, ids []qviews.ShardID) *coordview.ShardViewSnapshot {
		copy := make(map[qviews.ShardID]*coordview.ShardStats)
		for _, id := range ids {
			if stat := stats[id]; stat != nil {
				copy[id] = stat
			}
		}
		if duringRead != nil {
			duringRead()
		}
		return coordview.NewShardViewSnapshot(1, copy)
	}).Build()
	defer patch.UnPatch()
	s := &Server{meta: &meta.Meta{ResourceManager: &meta.ResourceManager{}}, broker: &meta.CoordinatorBroker{}, qviewsRuntime: &qviewsRuntime{loadConfigStore: store, shardViewRegistry: &coordview.ShardViewRegistry{}}}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	check := func(rg string, want int64) {
		response, err := s.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{CollectionIDs: []int64{100}, ResourceGroup: rg})
		require.NoError(t, merr.CheckRPCCall(response, err))
		require.Equal(t, []int64{want}, response.InMemoryPercentages)
		require.Equal(t, []bool{want == 100}, response.QueryServiceAvailable)
		percent, err := s.GetLoadPercentageByResourceGroup(ctx, 100, rg)
		require.NoError(t, err)
		require.EqualValues(t, want, percent)
	}
	up := func(replica int64, channel string) {
		stats[qviews.ShardID{ReplicaID: replica, VChannel: channel}] = &coordview.ShardStats{UpVersion: &qviews.QueryViewVersion{}, UpLoadInfoVersion: store.GetConfigVersion(100)}
	}
	check("a", 0)
	check("empty", -1)
	up(10, "v0")
	check("a", 50) // The absent expected shard still counts.
	up(10, "v1")
	check("a", 100)
	check("b", 0)
	check("", 50)
	for _, collection := range []int64{100, 777} {
		_, err := s.GetLoadPercentageByResourceGroup(ctx, collection, "typo")
		require.ErrorIs(t, err, merr.ErrResourceGroupNotFound)
	}
	percent, err := s.GetLoadPercentageByResourceGroup(ctx, 777, "a")
	require.NoError(t, err)
	require.EqualValues(t, -1, percent)
	up(20, "v0")
	up(20, "v1")
	check("", 100)
	cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 30, ResourceGroup: "a"})
	require.NoError(t, store.Put(ctx, cfg))
	check("a", 0) // Old Up versions cannot acknowledge the new configuration.
	up(10, "v0")
	up(10, "v1")
	check("a", 50)
	up(30, "v0")
	up(30, "v1")
	check("a", 100)
	duringRead = func() {
		cfg.LoadFields = nil
		cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: int64(100 + len(cfg.Replicas)), ResourceGroup: "b"})
		require.NoError(t, store.Put(ctx, cfg))
	}
	check("a", 0) // Configuration changes invalidate even a fully Up snapshot.
	duringRead = nil
	store.StopPlanning(100)
	require.NoError(t, store.Remove(ctx, 100))
	check("a", -1)
	require.NoError(t, store.Put(ctx, cfg))
	check("a", 0) // Release/reload cannot use the former Up configuration.
}
