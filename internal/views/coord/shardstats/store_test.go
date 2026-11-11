package shardstats

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func testWindow(samples ...qviews.ShardStats) qviews.ShardStatsWindow {
	now := time.Now().UnixNano()
	w := qviews.ShardStatsWindow{StartUnixNanos: now - int64(5*time.Second), EndUnixNanos: now, Shards: make(map[qviews.ShardID]qviews.ShardStats)}
	for _, s := range samples {
		w.Shards[s.ShardID] = s
	}
	return w
}

func TestReplicaStatsLoadLifecycle(t *testing.T) {
	mockey.PatchConvey("configuration owns stats independently of windows", t, func() {
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
		store.OnShardStats(testWindow(sample))
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
		next := sample
		next.RequestCount = 2
		next.LastAccessUnixNanos = 90
		store.OnShardStats(testWindow(next, qviews.ShardStats{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "p_2v0"}}, qviews.ShardStats{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "bad"}}, qviews.ShardStats{ShardID: qviews.ShardID{ReplicaID: 999, VChannel: "p_1v0"}}))
		got, _ = store.GetShardStats(id)
		require.EqualValues(t, 9, got.RequestCount)
		require.EqualValues(t, 100, got.LastAccessUnixNanos)
		require.Len(t, store.replicas[10].shards, 1)
		cfg.Replicas = cfg.Replicas[1:]
		require.NoError(t, configs.Put(t.Context(), cfg))
		store.OnShardStats(testWindow(sample))
		_, ok = store.GetShardStats(id)
		require.False(t, ok)
		next.ShardID.ReplicaID = 11
		store.OnShardStats(testWindow(next))
		configs.StopPlanning(1)
		store.OnShardStats(testWindow(next))
		require.Empty(t, store.replicas)
		store.Close()
		require.Empty(t, store.collections)
		require.NoError(t, configs.Put(t.Context(), cfg))
		require.Empty(t, store.replicas)
	})
}

func TestReplicaStatsExpiresWithoutReportsOrReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		sample := qviews.ShardStats{ShardID: id, RequestCount: 4, LastAccessUnixNanos: 10}
		store.OnShardStats(testWindow(sample))
		time.Sleep(20 * time.Second)
		sample.RequestCount = 2
		sample.LastAccessUnixNanos = 20
		store.OnShardStats(testWindow(sample))
		time.Sleep(99 * time.Second)
		synctest.Wait()
		got, ok := store.GetShardStats(id)
		require.True(t, ok)
		require.EqualValues(t, 6, got.RequestCount, "retain both windows before two minutes")
		time.Sleep(2 * time.Second)
		synctest.Wait()
		store.mu.RLock()
		require.Len(t, store.replicas[10].shards[id], 1)
		store.mu.RUnlock()
		got, ok = store.GetShardStats(id)
		require.True(t, ok)
		require.EqualValues(t, 2, got.RequestCount)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		store.mu.RLock()
		require.Empty(t, store.replicas[10].shards, "no report or read triggered cleanup")
		store.mu.RUnlock()
		_, ok = store.GetShardStats(id)
		require.False(t, ok)
		store.Close()
		store.Close()
	})
}

func TestReplicaStatsWindowValidationAndReadExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		sample := qviews.ShardStats{ShardID: id, RequestCount: 3}
		w := testWindow(sample)
		legacy := w
		legacy.StartUnixNanos = 0
		legacy.EndUnixNanos = 0
		store.OnShardStats(legacy)
		bad := w
		bad.StartUnixNanos = bad.EndUnixNanos
		store.OnShardStats(bad)
		long := w
		long.StartUnixNanos -= int64(3 * time.Minute)
		store.OnShardStats(long)
		old := w
		old.StartUnixNanos -= int64(3 * time.Minute)
		old.EndUnixNanos -= int64(3 * time.Minute)
		store.OnShardStats(old)
		_, ok := store.GetShardStats(id)
		require.False(t, ok)
		// Expire between cleanup ticks: reads must not expose the stale window.
		w.StartUnixNanos -= int64(119*time.Second + 500*time.Millisecond)
		w.EndUnixNanos -= int64(119*time.Second + 500*time.Millisecond)
		store.OnShardStats(w)
		time.Sleep(600 * time.Millisecond)
		_, ok = store.GetShardStats(id)
		require.False(t, ok)
		// Future sender timestamps still expire two minutes after receipt.
		future := testWindow(sample)
		future.StartUnixNanos += int64(time.Hour)
		future.EndUnixNanos += int64(time.Hour)
		store.OnShardStats(future)
		got, ok := store.GetShardStats(id)
		require.True(t, ok)
		require.EqualValues(t, 3, got.RequestCount)
		time.Sleep(121 * time.Second)
		synctest.Wait()
		store.mu.RLock()
		require.Empty(t, store.replicas[10].shards)
		store.mu.RUnlock()
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
				store.OnShardStats(testWindow(sample))
				store.GetShardStats(sample.ShardID)
			}
		}()
	}
	for range 100 {
		store.onLoadConfig(1, cfg, 1)
		store.expire(time.Now().UnixNano())
		store.onLoadConfig(1, nil, 0)
	}
	wg.Wait()
	_, ok := store.GetShardStats(sample.ShardID)
	require.False(t, ok)
}
