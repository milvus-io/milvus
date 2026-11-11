package snview

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestShardStatsWindowOwnershipAndViewIndependence(t *testing.T) {
	h := &SNQueryViewHandler{shards: make(map[qviews.ShardID]*snShardView)}
	id := qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"}
	shard := &snShardView{}
	h.shards[id] = shard
	h.RecordShardAccess(id)
	h.makeOnEmpty(id)(shard)
	require.Empty(t, h.shards)
	h.mu.Lock()
	shard.mu.Lock()
	done := make(chan qviews.ShardStatsWindow, 1)
	go func() { h.RecordShardAccess(id); done <- h.TakeShardStats() }()
	var first qviews.ShardStatsWindow
	select {
	case first = <-done:
	case <-time.After(time.Second):
		t.Error("stats acquired a View lock")
	}
	shard.mu.Unlock()
	h.mu.Unlock()
	require.EqualValues(t, 2, first.Shards[id].RequestCount)
	require.Positive(t, first.StartUnixNanos)
	require.Greater(t, first.EndUnixNanos, first.StartUnixNanos)
	h.RecordShardAccess(id)
	second := h.TakeShardStats()
	require.Equal(t, first.EndUnixNanos, second.StartUnixNanos)
	require.EqualValues(t, 1, second.Shards[id].RequestCount)
	require.EqualValues(t, 2, first.Shards[id].RequestCount, "detached window must never change")
	require.Empty(t, h.TakeShardStats().Shards, "taking twice cannot replay a window")
}

func TestShardStatsSizeNotification(t *testing.T) {
	for _, readyBeforeRecord := range []bool{false, true} {
		var stats shardStats
		if readyBeforeRecord {
			stats.ready()
		}
		id := qviews.ShardID{VChannel: "p_1v0"}
		for range qviews.ShardStatsMaxEntries * 2 {
			stats.record(id, 1)
		}
		ready := stats.ready()
		require.Empty(t, ready, "query count is not the window size")
		for i := 1; i < qviews.ShardStatsMaxEntries; i++ {
			stats.record(qviews.ShardID{ReplicaID: int64(i), VChannel: "p_1v0"}, 2)
		}
		require.Len(t, ready, 1)
		require.Equal(t, ready, stats.ready())
		window := stats.take()
		require.Len(t, window.Shards, qviews.ShardStatsMaxEntries)
		require.Empty(t, ready)
		require.Empty(t, stats.shards)
	}
	var late shardStats
	for i := range qviews.ShardStatsMaxEntries {
		late.record(qviews.ShardID{ReplicaID: int64(i)}, 1)
	}
	require.Len(t, late.ready(), 1, "discovery after the threshold must still wake the sender")
}

func TestShardStatsConcurrentRecordAndTake(t *testing.T) {
	var stats shardStats
	var wg sync.WaitGroup
	for replica := int64(1); replica <= 4; replica++ {
		for range 4 {
			wg.Add(1)
			go func(id int64) {
				defer wg.Done()
				for n := int64(1); n <= 1000; n++ {
					stats.record(qviews.ShardID{ReplicaID: id, VChannel: "p_1v0"}, n)
				}
			}(replica)
		}
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	totals := make(map[qviews.ShardID]uint64)
	add := func() {
		for id, sample := range stats.take().Shards {
			totals[id] += sample.RequestCount
		}
	}
	for {
		select {
		case <-finished:
			add()
			require.Len(t, totals, 4)
			for _, count := range totals {
				require.EqualValues(t, 4000, count)
			}
			return
		default:
			add()
		}
	}
}

func BenchmarkShardStats(b *testing.B) {
	for _, n := range []int{1, 1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			var stats shardStats
			for i := range n {
				stats.record(qviews.ShardID{ReplicaID: int64(i), VChannel: "p_1v0"}, 1)
			}
			b.Run("record", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						stats.record(qviews.ShardID{VChannel: "p_1v0"}, 2)
					}
				})
			})
			b.Run("take", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					window := stats.take()
					stats.shards = window.Shards // Reuse the detached map; no sender mutates it in this benchmark.
				}
			})
		})
	}
}
