package viewfeedback

import (
	"context"
	"sort"
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
		require.Equal(t, sample, got.ViewFeedback)
		cfg := configs.GetConfig(1).Clone()
		cfg.Replicas[0].ResourceGroup = "b"
		cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 11, ResourceGroup: "b"})
		require.NoError(t, configs.Put(t.Context(), cfg))
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.Equal(t, sample, got.ViewFeedback)
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
		require.Len(t, store.replicas[10].shards[id].windows, 1)
		store.mu.RUnlock()
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.EqualValues(t, 2, got.RequestCount)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		store.mu.RLock()
		require.Empty(t, store.replicas[10].shards[id].windows, "no report or read triggered cleanup")
		store.mu.RUnlock()
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.Zero(t, got.RequestCount)
		require.EqualValues(t, 20, got.LastAccessUnixNanos)
		store.Close()
		store.Close()
	})
}

func TestReplicaFeedbackWindowValidationAndNodeTime(t *testing.T) {
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
		_, ok := store.GetViewFeedback(id)
		require.False(t, ok)
		// Reads return the cached value until the independent cleanup tick.
		w.StartUnixNanos -= int64(119*time.Second + 500*time.Millisecond)
		w.EndUnixNanos -= int64(119*time.Second + 500*time.Millisecond)
		store.OnViewFeedback(w)
		time.Sleep(600 * time.Millisecond)
		got, ok := store.GetViewFeedback(id)
		require.True(t, ok)
		require.EqualValues(t, 3, got.RequestCount)
		time.Sleep(400 * time.Millisecond)
		synctest.Wait()
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.Zero(t, got.RequestCount)
		// Future sender timestamps remain unchanged; receipt time is not used.
		future := testWindow(sample)
		future.StartUnixNanos += int64(time.Hour)
		future.EndUnixNanos += int64(time.Hour)
		store.OnViewFeedback(future)
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.EqualValues(t, 3, got.RequestCount)
		time.Sleep(121 * time.Second)
		synctest.Wait()
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.EqualValues(t, 3, got.RequestCount)
		time.Sleep(time.Hour)
		synctest.Wait()
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.Zero(t, got.RequestCount)
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

func TestReplicaFeedbackRevertsOrderedContributions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		end := time.Now().UnixNano()
		report := func(offset time.Duration, count uint64, last int64) {
			w := testWindow(qviews.ViewFeedback{ShardID: id, RequestCount: count, LastAccessUnixNanos: last})
			w.EndUnixNanos = end + int64(offset)
			w.StartUnixNanos = w.EndUnixNanos - int64(5*time.Second)
			store.OnViewFeedback(w)
		}
		// Receive newer first, then older, then another batch with the same time.
		report(20*time.Second, 3, 90)
		report(0, 5, 100)
		report(0, 2, 80)
		store.mu.RLock()
		require.Equal(t, []feedbackContribution{{end - int64(5*time.Second), end, 5}, {end - int64(5*time.Second), end, 2}, {end + int64(15*time.Second), end + int64(20*time.Second), 3}}, store.replicas[10].shards[id].windows)
		require.EqualValues(t, 10, store.replicas[10].shards[id].RequestCount, "aggregate is updated on write")
		store.mu.RUnlock()
		store.expire(end + int64(retention))
		got, ok := store.GetViewFeedback(id)
		require.True(t, ok)
		require.EqualValues(t, 3, got.RequestCount)
		require.EqualValues(t, 100, got.LastAccessUnixNanos, "revert does not roll back LastAccess")
		store.expire(end + int64(retention))
		got, _ = store.GetViewFeedback(id)
		require.EqualValues(t, 3, got.RequestCount, "each contribution is reverted once")
		store.expire(end + int64(retention+20*time.Second))
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.Zero(t, got.RequestCount)
		require.EqualValues(t, 100, got.LastAccessUnixNanos)
		// A late old report is inserted by Node time and removed on the next sweep.
		report(-3*time.Minute, 7, 50)
		store.expire(end)
		got, _ = store.GetViewFeedback(id)
		require.Zero(t, got.RequestCount)
		require.EqualValues(t, 100, got.LastAccessUnixNanos)
		store.mu.RLock()
		require.Nil(t, store.replicas[10].shards[id].windows)
		store.mu.RUnlock()
	})
}

func TestReplicaFeedbackRatesUseActualWindowSpan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		base := time.Now().UnixNano()
		for _, end := range []time.Duration{5 * time.Second, 20 * time.Second} {
			store.OnViewFeedback(qviews.ViewFeedbackWindow{
				StartUnixNanos: base + int64(end-5*time.Second), EndUnixNanos: base + int64(end),
				Shards: map[qviews.ShardID]qviews.ViewFeedback{id: {RequestCount: 50}},
			})
		}
		got, ok := store.GetViewFeedback(id)
		require.True(t, ok)
		require.Equal(t, RequestRates{5, 5, 5, 5}, got.RequestRates, "100 attempts / 20 seconds including the gap; not 100 / 10")
		store.expire(base + int64(retention+5*time.Second))
		got, _ = store.GetViewFeedback(id)
		require.EqualValues(t, 50, got.RequestCount)
		require.Equal(t, RequestRates{10, 10, 10, 10}, got.RequestRates, "fewer than N windows uses the actual remaining span")
		store.expire(base + int64(retention+20*time.Second))
		got, ok = store.GetViewFeedback(id)
		require.True(t, ok)
		require.Zero(t, got.RequestCount)
		require.Equal(t, RequestRates{}, got.RequestRates)
	})
}

func TestReplicaFeedbackRecentGroupsMatchReceivedWindows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		base := time.Now().UnixNano() - int64(100*time.Second)
		var received []qviews.ViewFeedbackWindow
		check := func(cutoff int64) {
			var live []qviews.ViewFeedbackWindow
			var total uint64
			for _, w := range received {
				if w.EndUnixNanos > cutoff {
					live = append(live, w)
					total += w.Shards[id].RequestCount
				}
			}
			sort.SliceStable(live, func(i, j int) bool { return live[i].EndUnixNanos < live[j].EndUnixNanos })
			got, ok := store.GetViewFeedback(id)
			require.True(t, ok)
			require.Equal(t, total, got.RequestCount)
			rates := []float64{got.RequestRates.Last2Windows, got.RequestRates.Last6Windows, got.RequestRates.Last12Windows, got.RequestRates.Last24Windows}
			for index, count := range []int{2, 6, 12, 24} {
				var expected float64
				if len(live) > 0 {
					selected := live[max(0, len(live)-count):]
					start := selected[0].StartUnixNanos
					var attempts uint64
					for _, w := range selected {
						start = min(start, w.StartUnixNanos)
						attempts += w.Shards[id].RequestCount
					}
					expected = float64(attempts) / time.Duration(selected[len(selected)-1].EndUnixNanos-start).Seconds()
				}
				require.InDelta(t, expected, rates[index], 1e-10, "recent %d windows", count)
			}
		}
		// Alternate old/new arrivals; varying durations also create overlapping
		// windows whose Start order differs from their End order.
		for step := range 32 {
			i := step / 2
			if step%2 == 0 {
				i = 31 - i
			}
			end := base + int64(time.Duration(i*3)*time.Second)
			w := qviews.ViewFeedbackWindow{
				StartUnixNanos: end - int64(time.Duration(1+i%7)*time.Second), EndUnixNanos: end,
				Shards: map[qviews.ShardID]qviews.ViewFeedback{id: {RequestCount: uint64(i + 1), LastAccessUnixNanos: end}},
			}
			received = append(received, w)
			store.OnViewFeedback(w)
			check(0)
		}
		for _, seconds := range []int{0, 24, 60, 84, 93} {
			cutoff := base + int64(time.Duration(seconds)*time.Second)
			store.expire(cutoff + int64(retention))
			check(cutoff)
		}
	})
}
