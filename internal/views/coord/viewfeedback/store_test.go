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
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Publishes the collection topology independently of observed feedback.
type testDataViews struct{}

func (testDataViews) RegisterDataViewListener(listener api.DataViewListener) func() {
	listener(1, &api.CollectionDataView{Shards: []*api.ShardDataView{{VChannel: "p_1v0"}}})
	return func() {}
}

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
		store := New(configs, testDataViews{})
		defer store.Close()
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		_, ok := store.GetViewFeedback(id.ReplicaID)
		require.False(t, ok)
		sample := qviews.ViewFeedback{ShardID: id, RequestCount: 7, LastAccessUnixNanos: 100}
		firstWindow := testWindow(sample)
		store.OnViewFeedback(firstWindow)
		got, ok := store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, sample.LastAccessUnixNanos, got.LastAccessUnixNanos)
		require.Equal(t, RequestRates{1.4, 1.4, 1.4, 1.4}, got.RequestRates)
		cfg := configs.GetConfig(1).Clone()
		cfg.Replicas[0].ResourceGroup = "b"
		cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 11, ResourceGroup: "b"})
		require.NoError(t, configs.Put(t.Context(), cfg))
		got, ok = store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, sample.LastAccessUnixNanos, got.LastAccessUnixNanos)
		require.Equal(t, RequestRates{1.4, 1.4, 1.4, 1.4}, got.RequestRates)
		next := sample
		next.RequestCount = 2
		next.LastAccessUnixNanos = 90
		nextWindow := testWindow(next, qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "p_2v0"}}, qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "bad"}}, qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 999, VChannel: "p_1v0"}})
		nextWindow.StartUnixNanos = firstWindow.EndUnixNanos + 1
		nextWindow.EndUnixNanos = nextWindow.StartUnixNanos + int64(5*time.Second)
		store.OnViewFeedback(nextWindow)
		got, _ = store.GetViewFeedback(id.ReplicaID)
		require.Equal(t, 9/time.Duration(nextWindow.EndUnixNanos-firstWindow.StartUnixNanos).Seconds(), got.RequestRates.Last24Windows)
		require.EqualValues(t, 100, got.LastAccessUnixNanos)
		require.Len(t, store.replicas[10].shards, 1)
		cfg.Replicas = cfg.Replicas[1:]
		require.NoError(t, configs.Put(t.Context(), cfg))
		store.OnViewFeedback(testWindow(sample))
		_, ok = store.GetViewFeedback(id.ReplicaID)
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
		store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
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
		got, ok := store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, 6.0/25, got.RequestRates.Last24Windows, "retain both windows before two minutes")
		time.Sleep(2 * time.Second)
		synctest.Wait()
		store.mu.RLock()
		require.Len(t, store.replicas[10].shards[id].windows, 1)
		store.mu.RUnlock()
		got, ok = store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, 2.0/5, got.RequestRates.Last24Windows)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		store.mu.RLock()
		require.Empty(t, store.replicas[10].shards[id].windows, "no report or read triggered cleanup")
		store.mu.RUnlock()
		got, ok = store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, RequestRates{}, got.RequestRates)
		require.EqualValues(t, 20, got.LastAccessUnixNanos)
		store.Close()
		store.Close()
	})
}

func TestReplicaFeedbackWindowValidationAndNodeTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
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
		_, ok := store.GetViewFeedback(id.ReplicaID)
		require.False(t, ok)
		// Reads return the cached value until the independent cleanup tick.
		w.StartUnixNanos -= int64(119*time.Second + 500*time.Millisecond)
		w.EndUnixNanos -= int64(119*time.Second + 500*time.Millisecond)
		store.OnViewFeedback(w)
		time.Sleep(600 * time.Millisecond)
		got, ok := store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, 3.0/5, got.RequestRates.Last24Windows)
		time.Sleep(400 * time.Millisecond)
		synctest.Wait()
		got, ok = store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, RequestRates{}, got.RequestRates)
		// Future sender timestamps remain unchanged; receipt time is not used.
		future := testWindow(sample)
		future.StartUnixNanos += int64(time.Hour)
		future.EndUnixNanos += int64(time.Hour)
		store.OnViewFeedback(future)
		got, ok = store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, 3.0/5, got.RequestRates.Last24Windows)
		time.Sleep(121 * time.Second)
		synctest.Wait()
		got, ok = store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, 3.0/5, got.RequestRates.Last24Windows)
		time.Sleep(time.Hour)
		synctest.Wait()
		got, ok = store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, RequestRates{}, got.RequestRates)
	})
}

func TestReplicaFeedbackConcurrentWithdrawal(t *testing.T) {
	store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
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
				if snapshot, found := store.GetViewFeedback(sample.ShardID.ReplicaID); found {
					// Readers inspect the immutable result after releasing the store lock.
					require.Zero(t, snapshot.LastAccessUnixNanos)
					require.GreaterOrEqual(t, snapshot.RequestRates.Last24Windows, 0.0)
				}
				rates := store.GetGlobalRequestRates()
				require.GreaterOrEqual(t, rates.Last24Windows, 0.0)
			}
		}()
	}
	for range 100 {
		store.onLoadConfig(1, cfg, 1)
		store.expire(time.Now().UnixNano())
		store.onLoadConfig(1, nil, 0)
	}
	wg.Wait()
	_, ok := store.GetViewFeedback(sample.ShardID.ReplicaID)
	require.False(t, ok)
}

func TestReplicaFeedbackRevertsOrderedContributions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
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
		// Accepted windows append in order; duplicate and late reports are discarded.
		report(0, 5, 100)
		report(20*time.Second, 3, 90)
		report(0, 2, 800)
		store.mu.RLock()
		require.Equal(t, []feedbackContribution{{end - int64(5*time.Second), end, 5}, {end + int64(15*time.Second), end + int64(20*time.Second), 3}}, store.replicas[10].shards[id].windows)
		require.EqualValues(t, 8, store.replicas[10].shards[id].recent[3], "aggregate is updated on write")
		store.mu.RUnlock()
		store.expire(end + int64(retention))
		got, ok := store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, 3.0/5, got.RequestRates.Last24Windows)
		require.EqualValues(t, 100, got.LastAccessUnixNanos, "revert does not roll back LastAccess")
		store.expire(end + int64(retention))
		got, _ = store.GetViewFeedback(id.ReplicaID)
		require.Equal(t, 3.0/5, got.RequestRates.Last24Windows, "each contribution is reverted once")
		store.expire(end + int64(retention+20*time.Second))
		got, ok = store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, RequestRates{}, got.RequestRates)
		require.EqualValues(t, 100, got.LastAccessUnixNanos)
		// Expiring every window must not admit a late report again.
		report(-3*time.Minute, 7, 500)
		got, _ = store.GetViewFeedback(id.ReplicaID)
		require.Equal(t, RequestRates{}, got.RequestRates)
		require.EqualValues(t, 100, got.LastAccessUnixNanos)
		store.mu.RLock()
		require.Nil(t, store.replicas[10].shards[id].windows)
		store.mu.RUnlock()
	})
}

func TestReplicaFeedbackRatesUseActualWindowSpan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
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
		got, ok := store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, RequestRates{5, 5, 5, 5}, got.RequestRates, "100 attempts / 20 seconds including the gap; not 100 / 10")
		store.expire(base + int64(retention+5*time.Second))
		got, _ = store.GetViewFeedback(id.ReplicaID)
		require.Equal(t, RequestRates{10, 10, 10, 10}, got.RequestRates, "fewer than N windows uses the actual remaining span")
		store.expire(base + int64(retention+20*time.Second))
		got, ok = store.GetViewFeedback(id.ReplicaID)
		require.True(t, ok)
		require.Equal(t, RequestRates{}, got.RequestRates)
	})
}

func TestReplicaFeedbackRejectsOverlappingWindows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		first := testWindow(qviews.ViewFeedback{ShardID: id, RequestCount: 5, LastAccessUnixNanos: 100})
		store.OnViewFeedback(first)
		before, _ := store.GetViewFeedback(10)
		global := store.GetGlobalRequestRates()
		for _, bounds := range [][2]int64{
			{first.StartUnixNanos - 2, first.StartUnixNanos - 1}, // Older window.
			{first.StartUnixNanos, first.EndUnixNanos},           // Replay.
			{first.EndUnixNanos - 1, first.EndUnixNanos + 2},     // Later End still overlaps.
			{first.EndUnixNanos, first.EndUnixNanos + 2},         // Equal boundary is rejected.
		} {
			store.OnViewFeedback(qviews.ViewFeedbackWindow{
				StartUnixNanos: bounds[0], EndUnixNanos: bounds[1],
				Shards: map[qviews.ShardID]qviews.ViewFeedback{id: {RequestCount: 1000, LastAccessUnixNanos: 999}},
			})
			got, _ := store.GetViewFeedback(10)
			require.Same(t, before, got, "discarded windows cannot update rates or LastAccess")
			require.Same(t, global, store.GetGlobalRequestRates())
		}
		store.expire(first.EndUnixNanos + int64(retention))
		empty, _ := store.GetViewFeedback(10)
		store.OnViewFeedback(first)
		got, _ := store.GetViewFeedback(10)
		require.Same(t, empty, got, "expiration retains the last accepted End")

		next := qviews.ViewFeedbackWindow{
			StartUnixNanos: first.EndUnixNanos + 1,
			EndUnixNanos:   first.EndUnixNanos + 1 + int64(time.Second),
			Shards:         map[qviews.ShardID]qviews.ViewFeedback{id: {RequestCount: 7, LastAccessUnixNanos: 200}},
		}
		store.OnViewFeedback(next)
		got, _ = store.GetViewFeedback(10)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 200, RequestRates: RequestRates{7, 7, 7, 7}}, got)
		store.onLoadConfig(1, nil, 0)
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		store.OnViewFeedback(first)
		got, _ = store.GetViewFeedback(10)
		require.Equal(t, before, got, "withdrawing the Replica clears its window watermark")
	})
}

func TestReplicaFeedbackRecentGroupsMatchReceivedWindows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		base := time.Now().UnixNano() - int64(100*time.Second)
		var received []qviews.ViewFeedbackWindow
		check := func(cutoff int64) {
			var live []qviews.ViewFeedbackWindow
			for _, w := range received {
				if w.EndUnixNanos > cutoff {
					live = append(live, w)
				}
			}
			got, ok := store.GetViewFeedback(id.ReplicaID)
			require.True(t, ok)
			rates := []float64{got.RequestRates.Last2Windows, got.RequestRates.Last6Windows, got.RequestRates.Last12Windows, got.RequestRates.Last24Windows}
			for index, count := range []int{2, 6, 12, 24} {
				var expected float64
				if len(live) > 0 {
					selected := live[max(0, len(live)-count):]
					start := selected[0].StartUnixNanos
					var attempts uint64
					for _, w := range selected {
						attempts += w.Shards[id].RequestCount
					}
					expected = float64(attempts) / time.Duration(selected[len(selected)-1].EndUnixNanos-start).Seconds()
				}
				require.InDelta(t, expected, rates[index], 1e-10, "recent %d windows", count)
				global := store.GetGlobalRequestRates()
				globalRates := []float64{global.Last2Windows, global.Last6Windows, global.Last12Windows, global.Last24Windows}
				require.InDelta(t, expected, globalRates[index], 1e-10, "global recent %d windows", count)
			}
		}
		// Vary window lengths and gaps while keeping each accepted window disjoint.
		end := base
		for i := range 32 {
			start := end + int64(time.Duration(1+i%3)*time.Second)
			end = start + int64(time.Duration(1+i%7)*time.Second)
			w := qviews.ViewFeedbackWindow{
				StartUnixNanos: start, EndUnixNanos: end,
				Shards: map[qviews.ShardID]qviews.ViewFeedback{id: {RequestCount: uint64(i + 1), LastAccessUnixNanos: end}},
			}
			received = append(received, w)
			store.OnViewFeedback(w)
			// Replayed earlier windows cannot change any group's count or span.
			store.OnViewFeedback(received[i/2])
			check(0)
		}
		for _, seconds := range []int{0, 24, 60, 120, 240} {
			cutoff := base + int64(time.Duration(seconds)*time.Second)
			store.expire(cutoff + int64(retention))
			check(cutoff)
		}
	})
}

func TestReplicaFeedbackSnapshotsAreCopyOnWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		missing, found := store.GetViewFeedback(id.ReplicaID)
		require.False(t, found)
		require.Nil(t, missing)
		firstWindow := testWindow(qviews.ViewFeedback{ShardID: id, RequestCount: 5, LastAccessUnixNanos: 100})
		store.OnViewFeedback(firstWindow)
		first, found := store.GetViewFeedback(id.ReplicaID)
		require.True(t, found)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 100, RequestRates: RequestRates{1, 1, 1, 1}}, first)
		again, _ := store.GetViewFeedback(id.ReplicaID)
		require.Same(t, first, again, "reads return the published pointer")
		require.Zero(t, testing.AllocsPerRun(100, func() {
			got, ok := store.GetViewFeedback(id.ReplicaID)
			if !ok || got != first {
				panic("read did not return the published snapshot")
			}
		}))

		time.Sleep(20 * time.Second)
		store.OnViewFeedback(testWindow(qviews.ViewFeedback{ShardID: id, RequestCount: 5, LastAccessUnixNanos: 200}))
		second, _ := store.GetViewFeedback(id.ReplicaID)
		require.NotSame(t, first, second)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 200, RequestRates: RequestRates{0.4, 0.4, 0.4, 0.4}}, second)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 100, RequestRates: RequestRates{1, 1, 1, 1}}, first)
		store.expire(firstWindow.EndUnixNanos + int64(retention))
		third, _ := store.GetViewFeedback(id.ReplicaID)
		require.NotSame(t, second, third)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 200, RequestRates: RequestRates{1, 1, 1, 1}}, third)
		require.Equal(t, RequestRates{0.4, 0.4, 0.4, 0.4}, second.RequestRates)
		store.expire(firstWindow.EndUnixNanos + int64(retention))
		unchanged, _ := store.GetViewFeedback(id.ReplicaID)
		require.Same(t, third, unchanged, "an expiration pass with no removed contribution does not publish")
		store.expire(firstWindow.EndUnixNanos + int64(retention+20*time.Second))
		empty, _ := store.GetViewFeedback(id.ReplicaID)
		require.NotSame(t, third, empty)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 200}, empty)
		require.Equal(t, RequestRates{1, 1, 1, 1}, third.RequestRates)
		store.onLoadConfig(1, nil, 0)
		missing, found = store.GetViewFeedback(id.ReplicaID)
		require.False(t, found)
		require.Nil(t, missing)
		store.Close()
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 100, RequestRates: RequestRates{1, 1, 1, 1}}, first, "retained snapshots survive withdrawal and Close unchanged")
	})
}

func TestReplicaFeedbackAveragesAllShards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}, {ReplicaID: 11}}}, 1)
		data := &api.CollectionDataView{Shards: []*api.ShardDataView{{VChannel: "p_1v0"}, {VChannel: "q_1v1"}}}
		store.onDataView(1, data)
		_, found := store.GetViewFeedback(10)
		require.False(t, found, "topology alone does not create feedback")
		first := testWindow(qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}, RequestCount: 20, LastAccessUnixNanos: 100})
		store.OnViewFeedback(first)
		snapshot, found := store.GetViewFeedback(10)
		require.True(t, found)
		require.Equal(t, RequestRates{2, 2, 2, 2}, snapshot.RequestRates, "4/s and an unreported shard: divide by two")
		time.Sleep(10 * time.Second)
		second := testWindow(
			qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "q_1v1"}, RequestCount: 30, LastAccessUnixNanos: 90},
			qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 11, VChannel: "p_1v0"}, RequestCount: 80, LastAccessUnixNanos: 200},
		)
		// Distinct Node windows are not pooled: 4/s and 3/s average to 3.5/s.
		second.StartUnixNanos -= int64(5 * time.Second)
		store.OnViewFeedback(second)
		combined, found := store.GetViewFeedback(10)
		require.True(t, found)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 100, RequestRates: RequestRates{3.5, 3.5, 3.5, 3.5}}, combined)
		require.Equal(t, RequestRates{2, 2, 2, 2}, snapshot.RequestRates)
		other, _ := store.GetViewFeedback(11)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 200, RequestRates: RequestRates{4, 4, 4, 4}}, other)
		store.onDataView(1, data)
		unchanged, _ := store.GetViewFeedback(10)
		require.Same(t, combined, unchanged, "ordinary DataVersion updates do not republish feedback")
		store.expire(first.EndUnixNanos + int64(retention))
		expired, _ := store.GetViewFeedback(10)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 100, RequestRates: RequestRates{1.5, 1.5, 1.5, 1.5}}, expired)
		store.expire(second.EndUnixNanos + int64(retention))
		empty, _ := store.GetViewFeedback(10)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: 100}, empty)
	})
}

func TestReplicaFeedbackTopologyPublication(t *testing.T) {
	store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
	defer store.Close()
	store.onLoadConfig(2, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 20}}}, 1)
	store.OnViewFeedback(testWindow(qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 20, VChannel: "p_2v0"}, RequestCount: 10, LastAccessUnixNanos: 99}))
	missing, found := store.GetViewFeedback(20)
	require.False(t, found, "do not guess the divisor before DataView is available")
	require.Nil(t, missing)
	store.onDataView(2, &api.CollectionDataView{Shards: []*api.ShardDataView{{VChannel: "p_2v0"}, {VChannel: "q_2v1"}}})
	first, found := store.GetViewFeedback(20)
	require.True(t, found)
	require.Equal(t, RequestRates{1, 1, 1, 1}, first.RequestRates)
	require.Equal(t, &first.RequestRates, store.GetGlobalRequestRates())
	store.onDataView(2, &api.CollectionDataView{Shards: []*api.ShardDataView{{VChannel: "p_2v0"}, {VChannel: "q_2v1"}, {VChannel: "r_2v2"}, {VChannel: "s_2v3"}}})
	second, found := store.GetViewFeedback(20)
	require.True(t, found)
	require.NotSame(t, first, second)
	require.Equal(t, RequestRates{0.5, 0.5, 0.5, 0.5}, second.RequestRates)
	require.Equal(t, &second.RequestRates, store.GetGlobalRequestRates())
	store.onDataView(2, nil)
	require.Equal(t, &RequestRates{}, store.GetGlobalRequestRates())
	missing, found = store.GetViewFeedback(20)
	require.False(t, found)
	require.Nil(t, missing)
	require.Equal(t, &Snapshot{LastAccessUnixNanos: 99, RequestRates: RequestRates{1, 1, 1, 1}}, first)
}

func TestGlobalRequestRatesFollowReplicaContributions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
		defer store.Close()
		initial := store.GetGlobalRequestRates()
		require.Equal(t, &RequestRates{}, initial)
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}, {ReplicaID: 11}}}, 1)
		store.onDataView(1, &api.CollectionDataView{Shards: make([]*api.ShardDataView, 2)})
		store.onLoadConfig(2, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 20}}}, 1)
		store.onDataView(2, &api.CollectionDataView{Shards: make([]*api.ShardDataView, 1)})
		firstWindow := testWindow(
			qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}, RequestCount: 100, LastAccessUnixNanos: 100},
			qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 11, VChannel: "q_1v1"}, RequestCount: 40, LastAccessUnixNanos: 200},
		)
		store.OnViewFeedback(firstWindow)
		first := store.GetGlobalRequestRates()
		require.Equal(t, &RequestRates{14, 14, 14, 14}, first, "sum shard-normalized Replica rates, without averaging Replicas")
		require.Equal(t, &RequestRates{}, initial)
		require.Zero(t, testing.AllocsPerRun(100, func() {
			if store.GetGlobalRequestRates() != first {
				panic("global read did not return the published pointer")
			}
		}))
		time.Sleep(10 * time.Second)
		secondWindow := testWindow(qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 20, VChannel: "p_2v0"}, RequestCount: 30})
		store.OnViewFeedback(secondWindow)
		second := store.GetGlobalRequestRates()
		require.Equal(t, &RequestRates{20, 20, 20, 20}, second, "include another collection's normalized rate")
		require.NotSame(t, first, second)
		require.Equal(t, &RequestRates{14, 14, 14, 14}, first)
		store.expire(firstWindow.EndUnixNanos)
		require.Same(t, second, store.GetGlobalRequestRates(), "no expiry keeps the snapshot")
		store.onDataView(1, &api.CollectionDataView{Shards: make([]*api.ShardDataView, 4)})
		require.Equal(t, &RequestRates{13, 13, 13, 13}, store.GetGlobalRequestRates(), "new divisor replaces the old contribution")
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 2)
		require.Equal(t, &RequestRates{11, 11, 11, 11}, store.GetGlobalRequestRates(), "withdraw just Replica 11")
		store.expire(firstWindow.EndUnixNanos + int64(retention))
		require.Equal(t, &RequestRates{6, 6, 6, 6}, store.GetGlobalRequestRates(), "expiry retracts Replica 10 while retaining collection 2")
		store.expire(secondWindow.EndUnixNanos + int64(retention))
		require.Equal(t, &RequestRates{}, store.GetGlobalRequestRates(), "all windows expired")
		newWindow := testWindow(qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 20, VChannel: "p_2v0"}, RequestCount: 7})
		newWindow.StartUnixNanos = secondWindow.EndUnixNanos + 1
		newWindow.EndUnixNanos = newWindow.StartUnixNanos + int64(5*time.Second)
		store.OnViewFeedback(newWindow)
		beforeClose := store.GetGlobalRequestRates()
		require.Equal(t, &RequestRates{1.4, 1.4, 1.4, 1.4}, beforeClose)
		store.Close()
		require.Equal(t, &RequestRates{}, store.GetGlobalRequestRates())
		require.Equal(t, &RequestRates{1.4, 1.4, 1.4, 1.4}, beforeClose, "Close leaves retained snapshots immutable")
	})
}
