package shardstats

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestReplicaStatsLoadLifecycle(t *testing.T) {
	mockey.PatchConvey("real config listener replays, updates and withdraws replicas", t, func() {
		paramtable.Init()
		catalog := queryview.NewCollectionLoadInfoCatalog(nil)
		mockey.Mock(mockey.GetMethod(catalog, "List")).Return([]*viewpb.CollectionLoadConfig{{CollectionID: 1, Version: 1, Replicas: []*viewpb.LoadReplicaConfig{{ReplicaId: 10, ResourceGroupName: "a"}}}}, nil).Build()
		mockey.Mock(mockey.GetMethod(catalog, "Save")).Return(nil).Build()
		version := uint64(1)
		configs, err := loadmgr.RecoverLoadConfigStore(t.Context(), loadmgr.LoadConfigStoreDependencies{Catalog: catalog, AllocateVersion: func(context.Context) (uint64, error) { version++; return version, nil }})
		require.NoError(t, err)
		store := New(configs)
		defer store.Close()
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		_, ok := store.GetShardStats(id)
		require.False(t, ok)
		sample := qviews.ShardStats{ShardID: id, RequestCount: 7, LastAccessUnixNanos: 100}
		store.OnShardStats([]qviews.ShardStats{sample, sample})
		got, ok := store.GetShardStats(id)
		require.True(t, ok)
		require.Equal(t, sample, got)
		cfg := configs.GetConfig(1).Clone()
		cfg.Replicas[0].ResourceGroup = "b"
		cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 11, ResourceGroup: "b"})
		require.NoError(t, configs.Put(t.Context(), cfg))
		got, ok = store.GetShardStats(id)
		require.True(t, ok)
		require.Equal(t, sample, got)
		reset := sample
		reset.RequestCount = 2
		reset.LastAccessUnixNanos = 90
		store.OnShardStats([]qviews.ShardStats{reset, {ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "p_2v0"}}, {ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "bad"}}, {ShardID: qviews.ShardID{ReplicaID: 999, VChannel: "p_1v0"}}})
		got, _ = store.GetShardStats(id)
		require.EqualValues(t, 2, got.RequestCount)
		require.EqualValues(t, 100, got.LastAccessUnixNanos)
		require.Len(t, store.replicas[10].shards, 1)
		cfg.Replicas = cfg.Replicas[1:]
		require.NoError(t, configs.Put(t.Context(), cfg))
		store.OnShardStats([]qviews.ShardStats{sample})
		_, ok = store.GetShardStats(id)
		require.False(t, ok)
		second := sample
		second.ShardID.ReplicaID = 11
		store.OnShardStats([]qviews.ShardStats{second})
		configs.StopPlanning(1)
		store.OnShardStats([]qviews.ShardStats{second})
		require.Empty(t, store.replicas)
		store.Close()
		require.Empty(t, store.collections)
		require.NoError(t, configs.Put(t.Context(), cfg))
		require.Empty(t, store.replicas, "closed listener must not recreate stats")
	})
}

func TestReplicaStatsConcurrentWithdrawal(t *testing.T) {
	store := New(&loadmgr.LoadConfigStore{})
	defer store.Close()
	cfg := &loadmgr.LoadConfig{CollectionID: 1, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}
	sample := qviews.ShardStats{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}, RequestCount: 1}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				store.OnShardStats([]qviews.ShardStats{sample})
				store.GetShardStats(sample.ShardID)
			}
		}()
	}
	for range 100 {
		store.onLoadConfig(1, cfg, 1)
		store.onLoadConfig(1, nil, 0)
	}
	wg.Wait()
	_, ok := store.GetShardStats(sample.ShardID)
	require.False(t, ok)
}
