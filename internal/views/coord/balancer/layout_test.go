package balancer

import (
	"math/bits"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func replicaCache(replicas, nodes int) *balancercache.Cache {
	cfg := cfgFor(1, 10, nil, nil)
	cfg.Replicas = nil
	for i := 0; i < replicas; i++ {
		cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: int64(10 + i), ResourceGroup: "rg1"})
	}
	c := newTestCache(cfg)
	for i := 1; i <= nodes; i++ {
		c.PublishNode(int64(i), &NodeInfo{NodeID: int64(i), Alive: true, ResourceGroup: "rg1"})
	}
	c.PublishDataView(1, cacheData(1, cacheShard(1, 10).VChannel, 100))
	return c
}

func TestReplicaLayoutMinimumTransfers(t *testing.T) {
	// Compare every admissible quota, independently of the gain implementation.
	for n := 0; n <= 7; n++ {
		for r := 1; r <= 4; r++ {
			c := replicaCache(r, n).GetCollection(1)
			nodes, replicas := make([]int64, n), make([]int64, r)
			for i := range nodes {
				nodes[i] = int64(i + 1)
			}
			for i := range replicas {
				replicas[i] = int64(i + 10)
			}
			var visit func([]int, int)
			visit = func(counts []int, left int) {
				if len(counts) < r {
					for count := 0; count <= left; count++ {
						visit(append(slices.Clone(counts), count), left-count)
					}
					return
				}
				previous := &replicaLayout{replicas: replicas, nodes: nodes, owners: map[int64]int64{}, targets: map[int64][]int64{}}
				node := int64(1)
				for i, count := range counts {
					for j := 0; j < count; j++ {
						previous.owners[node] = replicas[i]
						previous.targets[replicas[i]] = append(previous.targets[replicas[i]], node)
						node++
					}
				}
				got := buildReplicaLayout(c, previous, replicas, nodes)
				require.Len(t, got.owners, n)
				active := 0
				for _, target := range got.targets {
					require.True(t, len(target) == n/r || len(target) == (n+r-1)/r)
					if len(target) > 0 {
						active++
					}
				}
				require.Equal(t, min(n, r), active)
				moved := 0
				for node, owner := range previous.owners {
					if got.owners[node] != owner {
						moved++
					}
				}
				best := n
				for mask := 0; mask < (1 << r); mask++ {
					if bits.OnesCount(uint(mask)) != n%r {
						continue
					}
					moves := 0
					for i, count := range counts {
						moves += max(0, count-n/r-((mask>>i)&1))
					}
					best = min(best, moves)
				}
				require.Equal(t, best, moved, "N=%d R=%d counts=%v", n, r, counts)
				require.Equal(t, got.owners, buildReplicaLayout(c, got, replicas, nodes).owners)
			}
			visit(nil, n)
		}
	}
}

func TestReplicaLayoutIsolationSuspensionAndRestore(t *testing.T) {
	c := replicaCache(3, 3)
	policy := NewDefaultBalancePolicy()
	shard := cacheShard(1, 10)
	plan := policy.Plan(c, []qviews.ShardID{shard})
	require.Len(t, plan.Prepares, 3)
	owners := make(map[int64]int64)
	for id, builder := range plan.Prepares {
		assignments := assignmentsFromBuilder(builder)
		require.Len(t, assignments, 1)
		node := assignments[1000]
		require.NotContains(t, owners, node)
		owners[node] = id.ReplicaID
		c.PublishShard(id, upStats(qviews.DataVersion{StreamingVersion: 1}, placement(1000, 1, node, coordview.SegmentStateUp)))
	}
	c.PublishNode(1, nil)
	lost := qviews.ShardID{ReplicaID: owners[1], VChannel: shard.VChannel}
	plan = policy.Plan(c, []qviews.ShardID{lost})
	require.Contains(t, plan.Releases, lost)
	require.Empty(t, plan.Retries)
	require.NotContains(t, plan.Prepares, lost)
	c.PublishShard(lost, nil)
	require.Empty(t, policy.Plan(c, []qviews.ShardID{lost}).Prepares)
	c.PublishNode(4, &NodeInfo{NodeID: 4, Alive: true, ResourceGroup: "rg1"})
	plan = policy.Plan(c, []qviews.ShardID{lost})
	require.Contains(t, plan.Prepares, lost)
	require.Equal(t, int64(4), assignmentsFromBuilder(plan.Prepares[lost])[1000])
	require.Len(t, c.GetCollection(1).LoadConfig().Replicas, 3)
}

func TestReplicaSuspensionPreservesLastServingCover(t *testing.T) {
	c := replicaCache(2, 1)
	a, b := cacheShard(1, 10), cacheShard(1, 11)
	// Freeze A as active before B's old view is reported.
	p := NewDefaultBalancePolicy()
	initial := p.Plan(c, []qviews.ShardID{a})
	for id := range initial.Prepares {
		a = id
	}
	if a.ReplicaID == 10 {
		b.ReplicaID = 11
	} else {
		b.ReplicaID = 10
	}
	c.PublishShard(b, upStats(qviews.DataVersion{StreamingVersion: 1}, placement(1000, 1, 1, coordview.SegmentStateUp)))
	plan := p.Plan(c, []qviews.ShardID{b})
	require.Empty(t, plan.Releases)
	require.Empty(t, plan.Retries)
	c.PublishShard(a, upStats(qviews.DataVersion{StreamingVersion: 1}, placement(1000, 1, 1, coordview.SegmentStateUp)))
	plan = p.Plan(c, []qviews.ShardID{a})
	require.Contains(t, plan.Releases, b)
	require.NotContains(t, plan.Prepares, b)
	require.Empty(t, plan.Retries)
}

func TestReplicaLayoutLoadingCostAndCompatibility(t *testing.T) {
	c := replicaCache(2, 4)
	version := qviews.DataVersion{StreamingVersion: 1}
	// A owns three nodes. Its sole copy on node 1 is useful to both replicas;
	// moving it saves B's load but would lose A's only copy. Node 2 also holds it,
	// making node 1 or 2 strictly preferable to empty node 3.
	c.PublishShard(cacheShard(1, 10), upStats(version,
		placement(1000, 1, 1, coordview.SegmentStateUp), placement(1000, 1, 2, coordview.SegmentStateUp)))
	previous := &replicaLayout{replicas: []int64{10, 11}, nodes: []int64{1, 2, 3, 4}, owners: map[int64]int64{1: 10, 2: 10, 3: 10, 4: 11}, targets: map[int64][]int64{10: {1, 2, 3}, 11: {4}}}
	layout := buildReplicaLayout(c.GetCollection(1), previous, previous.replicas, previous.nodes)
	require.Equal(t, int64(10), layout.owners[3])
	require.True(t, layout.owners[1] == 11 || layout.owners[2] == 11)
	p := newPlanningContext(c)
	states := reusableResources(p, cacheShard(1, 11), &SegmentDataView{SegmentID: 1000, PartitionID: 1}, []int64{1, 2, 3}, nil)
	require.Len(t, states, 2)
	c.PublishLoadConfig(1, c.GetCollection(1).LoadConfig(), 2)
	require.Empty(t, reusableResources(newPlanningContext(c), cacheShard(1, 11), &SegmentDataView{SegmentID: 1000, PartitionID: 1}, []int64{1, 2}, nil))
	c.PublishLoadConfig(1, c.GetCollection(1).LoadConfig(), 1)
	c.PublishDataView(1, cacheData(1, cacheShard(1, 10).VChannel, 100))
	data := c.GetCollection(1).DataView()
	next := *data
	next.DataVersion = qviews.DataVersion{StreamingVersion: 2}
	c.PublishDataView(1, &next)
	require.Empty(t, reusableResources(newPlanningContext(c), cacheShard(1, 11), &SegmentDataView{SegmentID: 1000, PartitionID: 1}, []int64{1, 2}, nil))
}

func TestReplicaLayoutBudgetFallback(t *testing.T) {
	c := replicaCache(260, 260)
	replicas, nodes := make([]int64, 260), make([]int64, 260)
	for i := range nodes {
		nodes[i] = int64(i + 1)
		replicas[i] = int64(i + 10)
	}
	got := buildReplicaLayout(c.GetCollection(1), nil, replicas, nodes)
	require.Len(t, got.owners, 260)
	for _, target := range got.targets {
		require.Len(t, target, 1)
	}
}

func TestLostPreparingIsReplaced(t *testing.T) {
	c := replicaCache(1, 2)
	id := cacheShard(1, 10)
	stats := withPreparingVersion(testShardStats(nil, 0, placement(1000, 1, 1, coordview.SegmentStateReady)), ver(1, 0, 1))
	stats.PreparingNodes = []int64{1}
	c.PublishShard(id, stats)
	c.PublishNode(1, nil)
	plan := NewDefaultBalancePolicy().Plan(c, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	require.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[id])[1000])
}

func TestReplicaLayoutSharedAcrossChannelsAndRGChange(t *testing.T) {
	c := replicaCache(2, 4)
	a := cacheShard(1, 10)
	other := a
	other.VChannel = "by-dev-rootcoord-dml_0_1v1"
	publishTestData(c, 1, qviews.DataVersion{StreamingVersion: 1}, map[int64]*SegmentDataView{1000: {SegmentID: 1000, PartitionID: 1, RowNum: 100}, 1001: {SegmentID: 1001, PartitionID: 1, RowNum: 100}},
		shardDataView(a.VChannel, 1, 1000), shardDataView(other.VChannel, 1, 1001))
	policy := NewDefaultBalancePolicy()
	plan := policy.Plan(c, []qviews.ShardID{a})
	require.Len(t, plan.Prepares, 4)
	layout := policy.layouts.domains[1]["rg1"]
	for id, builder := range plan.Prepares {
		for _, node := range assignmentsFromBuilder(builder) {
			require.Equal(t, id.ReplicaID, layout.owners[node])
		}
	}
	scoped := policy.Plan(c, []qviews.ShardID{other})
	require.Len(t, scoped.Prepares, 1)
	require.Same(t, layout, policy.layouts.domains[1]["rg1"], "progress reuses a stable target")
	cfg := c.GetCollection(1).LoadConfig().Clone()
	cfg.Replicas[1].ResourceGroup = "rg2"
	c.PublishLoadConfig(1, cfg, 2)
	c.PublishNode(5, &NodeInfo{NodeID: 5, Alive: true, ResourceGroup: "rg2"})
	plan = policy.Plan(c, []qviews.ShardID{a})
	require.Len(t, plan.Prepares, 4)
	for id, builder := range plan.Prepares {
		for _, node := range assignmentsFromBuilder(builder) {
			if id.ReplicaID == 11 {
				require.Equal(t, int64(5), node)
			} else {
				require.NotEqual(t, int64(5), node)
			}
		}
	}
	c.PublishLoadConfig(1, nil, 3)
	policy.Plan(c, []qviews.ShardID{a})
	require.Empty(t, policy.layouts.domains)
}

func BenchmarkReplicaLayoutStable(b *testing.B) {
	c := replicaCache(3, 30)
	policy := NewDefaultBalancePolicy()
	id := cacheShard(1, 10)
	// Establish the target, then mark all replicas in flight to isolate layout cost.
	first := policy.Plan(c, []qviews.ShardID{id})
	for shard, builder := range first.Prepares {
		stats := testShardStats(nil, 0)
		stats.PreparingVersion = ver(1, 0, 1)
		for _, node := range assignmentsFromBuilder(builder) {
			stats.PreparingNodes = append(stats.PreparingNodes, node)
		}
		c.PublishShard(shard, stats)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		policy.Plan(c, []qviews.ShardID{id})
	}
}

func BenchmarkReplicaLayoutRepair(b *testing.B) {
	c := replicaCache(3, 1000).GetCollection(1)
	nodes := make([]int64, 1000)
	for i := range nodes {
		nodes[i] = int64(i + 1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildReplicaLayout(c, nil, []int64{10, 11, 12}, nodes)
	}
}
