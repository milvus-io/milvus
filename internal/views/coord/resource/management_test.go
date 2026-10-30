package resource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func managementResources(t *testing.T) *Resources {
	t.Helper()
	for _, patch := range []*mockey.Mocker{
		mockey.Mock(catalogkv.Catalog.GetCollections).Return([]*querypb.CollectionLoadInfo(nil), nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetPartitions).Return(map[int64][]*querypb.PartitionLoadInfo(nil), nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetReplicas).Return([]*querypb.Replica(nil), nil).Build(),
		mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.ReleaseCollection).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.ReleaseReplicas).Return(nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	store, err := loadmgr.RecoverLoadConfigStore(t.Context(), catalogkv.Catalog{})
	require.NoError(t, err)
	r := &Resources{
		LoadConfigs: store, ShardViews: &coordview.ShardViewRegistry{}, Readiness: readiness.NewNotifications(),
		Balancer: &balancer.DefaultBalancer{}, BalanceCache: cache.New(nil),
		VChannels: func(context.Context, int64) ([]string, error) { return []string{"v0", "v1"}, nil },
	}
	store.RegisterObserver(func(id int64, released bool) {
		if released {
			r.Readiness.Release(id)
		} else {
			r.Readiness.Notify(id)
		}
	})
	t.Cleanup(r.Readiness.Close)
	require.NoError(t, store.Put(t.Context(), &loadmgr.LoadConfig{CollectionID: 1, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}, {ReplicaID: 11}}}))
	return r
}

func TestQueryViewReplicas(t *testing.T) {
	r := managementResources(t)
	cfg, version := r.LoadConfigs.GetConfigWithVersion(1)
	pl := &balancer.CollectionPlacement{
		Config: cfg, ConfigVersion: version, Nodes: map[int64][]int64{10: {1, 2}, 11: {3}},
		Shards: map[qviews.ShardID]*coordview.ShardStats{
			{ReplicaID: 10, VChannel: "v0"}: {UpVersion: &qviews.QueryViewVersion{}, UpNodes: []int64{2}},
			{ReplicaID: 11, VChannel: "v0"}: {PreparingVersion: &qviews.QueryViewVersion{}, PreparingNodes: []int64{3}},
		},
	}
	patch := mockey.Mock((*balancer.DefaultBalancer).Placement).Return(pl, nil).Build()
	defer patch.UnPatch()
	replicas, err := r.Replicas(t.Context(), 1, true)
	require.NoError(t, err)
	require.Len(t, replicas, 2)
	require.Equal(t, []int64{1, 2}, replicas[0].NodeIds)
	require.Len(t, replicas[0].ShardReplicas, 1)
	require.Equal(t, []int64{2}, replicas[0].ShardReplicas[0].NodeIds)
	require.Empty(t, replicas[1].ShardReplicas, "desired replica must not fabricate serving shards")
	replicas, err = r.Replicas(t.Context(), 1, false)
	require.NoError(t, err)
	require.Empty(t, replicas[0].ShardReplicas[0].NodeIds)
	// Identical content has the same config version but a new lifecycle identity.
	r.VChannels = func(ctx context.Context, _ int64) ([]string, error) {
		require.NoError(t, r.LoadConfigs.Remove(ctx, 1))
		require.NoError(t, r.LoadConfigs.Put(ctx, cfg))
		return []string{"v0", "v1"}, nil
	}
	_, err = r.Replicas(t.Context(), 1, true)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}
