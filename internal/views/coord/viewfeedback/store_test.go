package viewfeedback

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

func testWindow(samples ...qviews.ViewFeedback) qviews.ViewFeedbackWindow {
	now := time.Now().UnixNano()
	w := qviews.ViewFeedbackWindow{StartUnixNanos: now - int64(5*time.Second), EndUnixNanos: now, Shards: make(map[qviews.ShardID]qviews.ViewFeedback)}
	for _, s := range samples {
		w.Shards[s.ShardID] = s
	}
	return w
}

func TestReplicaFeedbackLoadLifecycle(t *testing.T) {
	mockey.PatchConvey("configuration owns feedback independently of windows", t, func() {
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
		_, ok := store.GetViewFeedback(id)
		require.False(t, ok)
		sample := qviews.ViewFeedback{ShardID: id, RequestCount: 7, LastAccessUnixNanos: 100}
		store.OnViewFeedback(testWindow(sample))
		got, ok := store.GetViewFeedback(id)
		require.True(t, ok)
		require.Equal(t, sample, got)
		cfg := configs.GetConfig(1).Clone()
		cfg.Replicas[0].ResourceGroup = "b"
		cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 11, ResourceGroup: "b"})
		require.NoError(t, configs.Put(t.Context(), cfg))
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.Equal(t, sample, got)
		next := sample
		next.RequestCount = 2
		next.LastAccessUnixNanos = 90
		store.OnViewFeedback(testWindow(next, qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "p_2v0"}}, qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "bad"}}, qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 999, VChannel: "p_1v0"}}))
		got, _ = store.GetViewFeedback(id)
		require.EqualValues(t, 9, got.RequestCount)
		require.EqualValues(t, 100, got.LastAccessUnixNanos)
		require.Len(t, store.replicas[10].shards, 1)
		cfg.Replicas = cfg.Replicas[1:]
		require.NoError(t, configs.Put(t.Context(), cfg))
		store.OnViewFeedback(testWindow(sample))
		_, ok = store.GetViewFeedback(id)
		require.False(t, ok)
		next.ShardID.ReplicaID = 11
		store.OnViewFeedback(testWindow(next))
		configs.StopPlanning(1)
		store.OnViewFeedback(testWindow(next))
		require.Empty(t, store.replicas)
		store.Close()
		require.Empty(t, store.collections)
		require.NoError(t, configs.Put(t.Context(), cfg))
		require.Empty(t, store.replicas)
	})
}

func TestReplicaFeedbackExpiresWithoutReportsOrReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		sample := qviews.ViewFeedback{ShardID: id, RequestCount: 4, LastAccessUnixNanos: 10}
		store.OnViewFeedback(testWindow(sample))
		time.Sleep(20 * time.Second)
		sample.RequestCount = 2
		sample.LastAccessUnixNanos = 20
		store.OnViewFeedback(testWindow(sample))
		time.Sleep(99 * time.Second)
		synctest.Wait()
		got, ok := store.GetViewFeedback(id)
		require.True(t, ok)
		require.EqualValues(t, 6, got.RequestCount, "retain both windows before two minutes")
		time.Sleep(2 * time.Second)
		synctest.Wait()
		store.mu.RLock()
		require.Len(t, store.replicas[10].shards[id], 1)
		store.mu.RUnlock()
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.EqualValues(t, 2, got.RequestCount)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		store.mu.RLock()
		require.Empty(t, store.replicas[10].shards, "no report or read triggered cleanup")
		store.mu.RUnlock()
		_, ok = store.GetViewFeedback(id)
		require.False(t, ok)
		store.Close()
		store.Close()
	})
}

func TestReplicaFeedbackWindowValidationAndReadExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		sample := qviews.ViewFeedback{ShardID: id, RequestCount: 3}
		w := testWindow(sample)
		legacy := w
		legacy.StartUnixNanos = 0
		legacy.EndUnixNanos = 0
		store.OnViewFeedback(legacy)
		bad := w
		bad.StartUnixNanos = bad.EndUnixNanos
		store.OnViewFeedback(bad)
		long := w
		long.StartUnixNanos -= int64(3 * time.Minute)
		store.OnViewFeedback(long)
		old := w
		old.StartUnixNanos -= int64(3 * time.Minute)
		old.EndUnixNanos -= int64(3 * time.Minute)
		store.OnViewFeedback(old)
		_, ok := store.GetViewFeedback(id)
		require.False(t, ok)
		// Expire between cleanup ticks: reads must not expose the stale window.
		w.StartUnixNanos -= int64(119*time.Second + 500*time.Millisecond)
		w.EndUnixNanos -= int64(119*time.Second + 500*time.Millisecond)
		store.OnViewFeedback(w)
		time.Sleep(600 * time.Millisecond)
		_, ok = store.GetViewFeedback(id)
		require.False(t, ok)
		// Future sender timestamps still expire two minutes after receipt.
		future := testWindow(sample)
		future.StartUnixNanos += int64(time.Hour)
		future.EndUnixNanos += int64(time.Hour)
		store.OnViewFeedback(future)
		got, ok := store.GetViewFeedback(id)
		require.True(t, ok)
		require.EqualValues(t, 3, got.RequestCount)
		time.Sleep(121 * time.Second)
		synctest.Wait()
		store.mu.RLock()
		require.Empty(t, store.replicas[10].shards)
		store.mu.RUnlock()
	})
}

func TestReplicaFeedbackConcurrentWithdrawal(t *testing.T) {
	store := New(&loadmgr.LoadConfigStore{})
	defer store.Close()
	cfg := &loadmgr.LoadConfig{CollectionID: 1, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}
	sample := qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}, RequestCount: 1}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				store.OnViewFeedback(testWindow(sample))
				store.GetViewFeedback(sample.ShardID)
			}
		}()
	}
	for range 100 {
		store.onLoadConfig(1, cfg, 1)
		store.expire(time.Now().UnixNano())
		store.onLoadConfig(1, nil, 0)
	}
	wg.Wait()
	_, ok := store.GetViewFeedback(sample.ShardID)
	require.False(t, ok)
}
