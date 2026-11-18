package balancer

import (
	"fmt"
	"testing"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// Every iteration plans against the same published input: accepted plans are
// intentionally not applied, keeping the before/after workload identical.
func BenchmarkMemoryPlan(b *testing.B) {
	for _, tc := range []struct{ nodes, shards, segments int }{{8, 100, 10}, {1000, 100, 10}, {8, 1000, 10}, {8, 1, 10000}} {
		for _, mode := range []string{"unchanged", "move", "loss", "release", "continuation"} {
			b.Run(fmt.Sprintf("%s/n%d/s%d/seg%d", mode, tc.nodes, tc.shards, tc.segments), func(b *testing.B) {
				cfg := DefaultBalanceConfig()
				if mode == "continuation" {
					cfg.MaxCandidateEvaluationsPerShard = 1
				}
				c := balancercache.New(cfg)
				for n := 1; n <= tc.nodes; n++ {
					c.PublishNode(int64(n), &NodeInfo{NodeID: int64(n), Alive: true, ResourceGroup: "rg1"})
				}
				dirty := make([]qviews.ShardID, 0, tc.shards)
				for s := 1; s <= tc.shards; s++ {
					id := int64(s)
					shard := cacheShard(id, id*10)
					dirty = append(dirty, shard)
					c.PublishLoadConfig(id, cfgFor(id, id*10, []int64{1}, nil), 1)
					rows := make([]int64, tc.segments)
					sizes := make(map[int64]int64)
					placements := make([]testSegmentPlacement, 0, tc.segments)
					for j := range rows {
						rows[j] = 1
						if mode == "move" {
							rows[j] = 1000000
						}
						segment := id*1000 + int64(j)
						sizes[segment] = rows[j]
						node := int64((s-1)%tc.nodes + 1)
						if mode == "move" || mode == "loss" {
							node = 1
						}
						placements = append(placements, placement(segment, 1, node, coordview.SegmentStateUp))
					}
					c.PublishDataView(id, cacheData(id, shard.VChannel, rows...))
					c.PublishShard(shard, withSegmentRows(testShardStats(ver(1, 0, 1), 1, placements...), sizes))
				}
				if mode == "loss" {
					c.PublishNode(1, nil)
				}
				if mode == "release" {
					for s := 1; s <= tc.shards; s++ {
						c.PublishLoadConfig(int64(s), nil, 2)
					}
				}
				p := NewDefaultBalancePolicy()
				p.Plan(c, dirty) // warm retained layouts
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					plan := p.Plan(c, dirty)
					if mode == "unchanged" && len(plan.Prepares) != 0 {
						b.Fatal("unexpected placement change")
					}
					if (mode == "move" || mode == "loss") && len(plan.Prepares) == 0 {
						b.Fatal("expected preparation")
					}
					if mode == "release" && len(plan.Releases) != len(dirty) {
						b.Fatal("missing release")
					}
				}
			})
		}
	}
}
