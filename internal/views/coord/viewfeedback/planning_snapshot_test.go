package viewfeedback

import (
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestPlanningSnapshotTracksResourceGroups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(&loadmgr.LoadConfigStore{}, testDataViews{})
		defer s.Close()
		cfg := &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10, ResourceGroup: "a"}, {ReplicaID: 11, ResourceGroup: "a"}}}
		s.onLoadConfig(1, cfg, 1)
		s.onLoadConfig(2, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 20, ResourceGroup: "b"}}}, 1)
		s.onDataView(2, &api.CollectionDataView{Shards: []*api.ShardDataView{{}}})
		empty := s.GetPlanningSnapshot()
		sample, rates := empty.GetReplicaFeedback(10, "a")
		require.Nil(t, sample)
		require.Zero(t, rates)
		w := testWindow(qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}, RequestCount: 100, LastAccessUnixNanos: 100}, qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 11, VChannel: "p_1v0"}, RequestCount: 50}, qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 20, VChannel: "p_2v0"}, RequestCount: 500})
		s.OnViewFeedback(w)
		first := s.GetPlanningSnapshot()
		sample, rates = first.GetReplicaFeedback(10, "a")
		require.Equal(t, RequestRates{20, 20, 20, 20}, sample.RequestRates)
		require.Equal(t, RequestRates{30, 30, 30, 30}, rates)
		_, rates = first.GetReplicaFeedback(20, "b")
		require.Equal(t, RequestRates{100, 100, 100, 100}, rates)
		sample, rates = first.GetReplicaFeedback(10, "b")
		require.Nil(t, sample)
		require.Zero(t, rates)
		sample, rates = first.GetReplicaFeedback(999, "a")
		require.Nil(t, sample)
		require.Zero(t, rates)
		require.Zero(t, testing.AllocsPerRun(100, func() { s.GetPlanningSnapshot().GetReplicaFeedback(10, "a") }))
		s.onLoadConfig(1, cfg, 1)
		s.expire(w.EndUnixNanos)
		require.Same(t, first, s.GetPlanningSnapshot(), "no-op publications reuse the version")
		s.onDataView(1, &api.CollectionDataView{Shards: []*api.ShardDataView{{}, {}}})
		_, rates = s.GetPlanningSnapshot().GetReplicaFeedback(10, "a")
		require.Equal(t, RequestRates{15, 15, 15, 15}, rates)
		cfg = cfg.Clone()
		cfg.Replicas[0].ResourceGroup = "b"
		s.onLoadConfig(1, cfg, 2)
		migrated := s.GetPlanningSnapshot()
		sample, rates = migrated.GetReplicaFeedback(10, "b")
		require.EqualValues(t, 10, sample.RequestRates.Last6Windows)
		require.EqualValues(t, 110, rates.Last6Windows)
		_, rates = migrated.GetReplicaFeedback(11, "a")
		require.EqualValues(t, 5, rates.Last6Windows)
		sample, rates = migrated.GetReplicaFeedback(10, "a")
		require.Nil(t, sample)
		require.Zero(t, rates)
		_, rates = first.GetReplicaFeedback(10, "a")
		require.EqualValues(t, 30, rates.Last6Windows)
		s.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: cfg.Replicas[:1]}, 3)
		require.Equal(t, 1, s.GetPlanningSnapshot().groups.Len(), "empty RG is removed")
		s.onDataView(2, nil)
		_, rates = s.GetPlanningSnapshot().GetReplicaFeedback(10, "b")
		require.EqualValues(t, 10, rates.Last6Windows)
		s.expire(w.EndUnixNanos + int64(retention))
		sample, rates = s.GetPlanningSnapshot().GetReplicaFeedback(10, "b")
		require.EqualValues(t, 100, sample.LastAccessUnixNanos)
		require.Zero(t, rates)
		require.Zero(t, s.GetPlanningSnapshot().groups.Len())
		s.onLoadConfig(1, nil, 4)
		sample, _ = s.GetPlanningSnapshot().GetReplicaFeedback(10, "b")
		require.Nil(t, sample)
		s.onLoadConfig(1, cfg, 5)
		s.OnViewFeedback(w)
		require.NotNil(t, s.replicas[10].snapshot, "reload accepts a new lifetime")
		s.Close()
		sample, _ = s.GetPlanningSnapshot().GetReplicaFeedback(10, "b")
		require.Nil(t, sample)
		_, rates = migrated.GetReplicaFeedback(10, "b")
		require.EqualValues(t, 110, rates.Last6Windows)
	})
}

func TestPlanningSnapshotConcurrentPublication(t *testing.T) {
	s := New(&loadmgr.LoadConfigStore{}, testDataViews{})
	defer s.Close()
	cfg := &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10, ResourceGroup: "a"}}}
	s.onLoadConfig(1, cfg, 1)
	w := testWindow(qviews.ViewFeedback{ShardID: qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}, RequestCount: 100})
	s.OnViewFeedback(w)
	retained := s.GetPlanningSnapshot()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			next := cfg.Clone()
			if i%2 == 0 {
				next.Replicas[0].ResourceGroup = "b"
			}
			s.onLoadConfig(1, next, uint64(i+2))
			w.StartUnixNanos = w.EndUnixNanos + 1
			w.EndUnixNanos = w.StartUnixNanos + int64(time.Second)
			s.OnViewFeedback(w)
		}
	}()
	for i := 0; i < 200; i++ {
		current := s.GetPlanningSnapshot()
		for _, group := range []string{"a", "b"} {
			if replica, total := current.GetReplicaFeedback(10, group); replica != nil {
				require.Equal(t, replica.RequestRates, total)
			}
		}
		replica, total := retained.GetReplicaFeedback(10, "a")
		require.EqualValues(t, 20, replica.RequestRates.Last6Windows)
		require.Equal(t, replica.RequestRates, total)
	}
	wg.Wait()
}

// Includes writer-side cost so the read optimization cannot hide a full-map
// copy on every update. The retained window bounds historical root ownership.
func BenchmarkPlanningSnapshotPublication(b *testing.B) {
	for _, count := range []int{100, 10000} {
		b.Run(fmt.Sprintf("replicas%d", count), func(b *testing.B) {
			snapshot := &PlanningSnapshot{}
			for i := 0; i < count; i++ {
				snapshot = snapshot.replaceReplica(int64(i), "rg", &Snapshot{RequestRates: RequestRates{Last6Windows: 1}})
			}
			retained := make([]*PlanningSnapshot, 32)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				retained[i%len(retained)] = snapshot
				snapshot = snapshot.replaceReplica(int64(i%count), "rg", &Snapshot{LastAccessUnixNanos: int64(i + 1), RequestRates: RequestRates{Last6Windows: float64(i%2 + 1)}})
			}
			b.StopTimer()
			require.NotNil(b, retained[0])
		})
	}
}
