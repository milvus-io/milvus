package balancer

import (
	"fmt"
	"testing"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// Keep cache publication outside the measured loop and reuse identical inputs.
// These cases cover both retained placements and the segment-by-node search.
func BenchmarkCapacityAllocation(b *testing.B) {
	for _, mode := range []string{"initial", "retained", "failed", "optional"} {
		b.Run(mode, func(b *testing.B) {
			c := balancercache.New(DefaultBalanceConfig())
			id := cacheShard(1, 10)
			c.PublishLoadConfig(1, cfgFor(1, 10, []int64{1}, nil), 1)
			nodes := []int64{1, 2, 3, 4, 5, 6, 7, 8}
			for _, n := range nodes {
				c.PublishNode(n, &NodeInfo{NodeID: n, Alive: true, ResourceGroup: "rg1"})
			}
			rows := make([]int64, 1000)
			placements := make([]testSegmentPlacement, len(rows))
			for i := range rows {
				rows[i] = 1
				placements[i] = placement(1000+int64(i), 1, 1, coordview.SegmentStateUp)
			}
			c.PublishDataView(1, cacheData(1, id.VChannel, rows...))
			stats := &coordview.ShardStats{}
			if mode != "initial" {
				stats = testShardStats(ver(1, 0, 1), 1, placements...)
			}
			if mode == "failed" {
				failed := make(map[coordview.FailedResourceKey]coordview.ResourceFailure, len(rows))
				for i := range rows {
					failed[coordview.FailedResourceKey{PartitionID: 1, SegmentID: 1000 + int64(i), DataVersion: ver(1, 0, 1).DataVersion, LoadInfoVersion: 1}] = coordview.ResourceFailure{Version: *ver(1, 0, 1)}
				}
				stats.FailedResources = map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure{1: failed}
			}
			c.PublishShard(id, stats)
			p := newPlanningContext(c)
			p.targets[10] = nodes
			p.fanouts = make(map[qviews.ShardID]int)
			p.search = make(map[qviews.ShardID]*searchCursor)
			base := make(map[int64]int64)
			allocate(p, id, base, mode == "optional")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result := allocate(p, id, base, mode == "optional")
				if result == nil || len(result.assignments) != len(rows) {
					b.Fatal("incomplete allocation")
				}
			}
		})
	}
}

func BenchmarkCapacityManyCollections(b *testing.B) {
	for _, count := range []int{100, 10000} {
		for _, mode := range []string{"stable", "refresh"} {
			b.Run(fmt.Sprintf("%s/collections%d", mode, count), func(b *testing.B) {
				c := balancercache.New(DefaultBalanceConfig())
				for n := int64(1); n <= 8; n++ {
					c.PublishNode(n, &NodeInfo{NodeID: n, Alive: true, ResourceGroup: "rg1"})
				}
				dirty := make([]qviews.ShardID, 0, count)
				for collection := int64(1); collection <= int64(count); collection++ {
					id := cacheShard(collection, collection*10)
					dirty = append(dirty, id)
					c.PublishLoadConfig(collection, cfgFor(collection, collection*10, []int64{1}, nil), 1)
					data := cacheData(collection, id.VChannel, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
					if mode == "refresh" {
						data.DataVersion.StreamingVersion++
					}
					c.PublishDataView(collection, data)
					var placements []testSegmentPlacement
					for i := int64(0); i < 10; i++ {
						placements = append(placements, placement(collection*1000+i, 1, (collection-1)%8+1, coordview.SegmentStateUp))
					}
					c.PublishShard(id, testShardStats(ver(1, 0, 1), 1, placements...))
				}
				policy := NewDefaultBalancePolicy()
				policy.Plan(c, dirty) // warm layouts without applying the plan
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					plan := policy.Plan(c, dirty)
					if mode == "refresh" && len(plan.Prepares) != count {
						b.Fatal("missing mandatory refresh")
					}
					if mode == "stable" && len(plan.Prepares) != 0 {
						b.Fatal("unexpected placement change")
					}
				}
			})
		}
	}
}
