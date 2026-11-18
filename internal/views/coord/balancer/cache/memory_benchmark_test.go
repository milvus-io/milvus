package cache

import (
	"fmt"
	"runtime"
	"runtime/metrics"
	"testing"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func BenchmarkMemoryTopology(b *testing.B) {
	for _, shards := range []int{100, 1000} {
		b.Run(fmt.Sprintf("shards%d", shards), func(b *testing.B) {
			c := New(nil)
			c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
			c.PublishNode(2, &api.NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
			for s := 1; s <= shards; s++ {
				id := int64(s)
				shard := cacheShard(id, id*10)
				c.PublishLoadConfig(id, cacheConfig(id, id*10), 1)
				c.PublishDataView(id, cacheData(id, shard.VChannel, 100))
				stats := cacheStats(id*1000, 1, 100, coordview.SegmentStateUp, true)
				stats.UpNodes = []int64{1}
				stats.UpPlacement = &coordview.ViewPlacement{Assignments: map[int64]int64{id * 1000: 1}, PartitionSizes: map[int64]map[int64]int64{1: {1: 100}}}
				c.PublishShard(shard, stats)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.PublishNode(2, &api.NodeInfo{NodeID: 2, Alive: true, Stopping: i%2 == 0, ResourceGroup: "rg1"})
			}
		})
	}
}

// Diagnostic benchmark: retains a bounded reader window across topology
// publications and samples the Go heap once per pass. Timing includes sampling.
func BenchmarkMemoryRetainedTopology(b *testing.B) {
	c := New(nil)
	c.PublishNode(1, &api.NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	c.PublishNode(2, &api.NodeInfo{NodeID: 2, Alive: true, ResourceGroup: "rg1"})
	c.PublishLoadConfig(1, cacheConfig(1, 10), 1)
	for i := 0; i < 1000; i++ {
		id := qviews.ShardID{ReplicaID: 10, VChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_1v%d", i)}
		stats := cacheStats(int64(i), 1, 100, coordview.SegmentStateUp, true)
		stats.UpNodes = []int64{1}
		stats.UpPlacement = &coordview.ViewPlacement{Assignments: map[int64]int64{int64(i): 1}, PartitionSizes: map[int64]map[int64]int64{1: {1: 100}}}
		c.PublishShard(id, stats)
	}
	retained := make([]*CollectionEntry, 32)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
	metrics.Read(samples)
	gcCPU := samples[0].Value.Float64()
	var peak uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		retained[i%len(retained)] = c.GetCollection(1)
		c.PublishNode(2, &api.NodeInfo{NodeID: 2, Alive: true, Stopping: i%2 == 0, ResourceGroup: "rg1"})
		runtime.ReadMemStats(&after)
		peak = max(peak, after.HeapAlloc)
	}
	b.StopTimer()
	metrics.Read(samples)
	b.ReportMetric(samples[0].Value.Float64()-gcCPU, "gc_cpu_s")
	b.ReportMetric(float64(after.NumGC-before.NumGC), "gc_cycles")
	b.ReportMetric(float64(peak), "sampled_peak_heap_B")
	runtime.GC()
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "retained_heap_delta_B")
	runtime.KeepAlive(retained)
	runtime.KeepAlive(c)
}
