package snview

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestShardStatsIndependentOfViews(t *testing.T) {
	h := &SNQueryViewHandler{shards: make(map[qviews.ShardID]*snShardView)}
	id := qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"}
	shard := &snShardView{}
	h.shards[id] = shard
	h.RecordShardAccess(id)
	first := h.CollectShardStats()[0]
	require.EqualValues(t, 1, first.RequestCount)
	require.Positive(t, first.LastAccessUnixNanos)
	h.makeOnEmpty(id)(shard)
	require.Empty(t, h.shards)
	require.Equal(t, []qviews.ShardStats{first}, h.CollectShardStats())
	h.mu.Lock()
	shard.mu.Lock()
	done := make(chan struct{})
	go func() { h.RecordShardAccess(id); _ = h.CollectShardStats(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("stats acquired a View lock")
	}
	shard.mu.Unlock()
	h.mu.Unlock()
	require.EqualValues(t, 2, h.CollectShardStats()[0].RequestCount)
	require.Empty(t, (&SNQueryViewHandler{}).CollectShardStats(), "new WAL starts fresh")
}

func TestShardStatsConcurrentRecordAndCollect(t *testing.T) {
	var stats shardStats
	var wg sync.WaitGroup
	for replica := int64(1); replica <= 4; replica++ {
		for range 4 {
			wg.Add(1)
			go func(id int64) {
				defer wg.Done()
				for n := int64(1); n <= 1000; n++ {
					stats.record(qviews.ShardID{ReplicaID: id, VChannel: "p_1v0"}, n)
					if n%100 == 0 {
						_ = stats.collect()
					}
				}
			}(replica)
		}
	}
	wg.Wait()
	require.Len(t, stats.collect(), 4)
	for _, sample := range stats.collect() {
		require.EqualValues(t, 4000, sample.RequestCount)
		require.EqualValues(t, 1000, sample.LastAccessUnixNanos)
	}
	stats.record(qviews.ShardID{ReplicaID: 1, VChannel: "p_1v1"}, 1)
	require.Len(t, stats.collect(), 5)
}

func BenchmarkShardStats(b *testing.B) {
	for _, n := range []int{1, 1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			var stats shardStats
			for i := 0; i < n; i++ {
				stats.record(qviews.ShardID{ReplicaID: int64(i), VChannel: "p_1v0"}, 1)
			}
			b.Run("record", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						stats.record(qviews.ShardID{ReplicaID: 0, VChannel: "p_1v0"}, 2)
					}
				})
			})
			b.Run("collect", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					stats.collect()
				}
			})
		})
	}
}
