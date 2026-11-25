package balancer

import (
	"math"
	"math/rand"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	balancerapi "github.com/milvus-io/milvus/internal/views/coord/balancer/api"
)

func TestBandPenaltyAndMigrationExample(t *testing.T) {
	penalty := func(loads ...float64) float64 {
		var total float64
		for _, rows := range loads {
			total += bandPenalty(rows, 2e6, 2e5)
		}
		return total
	}
	require.InDelta(t, 1.13e6, penalty(3e6, 3e6, 0), 1e-6)
	require.InDelta(t, .32e6, penalty(2e6, 3e6, 1e6), 1e-6)
	require.Zero(t, penalty(2e6, 2e6, 2e6))
	require.InDelta(t, .71e6, penalty(3e6, 3e6, 0)-penalty(2e6, 3e6, 1e6)-.1e6, 1e-6)
	require.Zero(t, bandPenalty(0, 0, 1))
	require.Zero(t, bandPenalty(80_000, 800, 100_000), "a tiny shard on many nodes has no RG spreading incentive")
	require.Zero(t, concentrationPenalty(100, 0))
	require.Zero(t, concentrationPenalty(100, 110))
	require.InDelta(t, 50, concentrationPenalty(200, 100), 1e-9)
	require.False(t, math.IsInf(bandPenalty(float64(math.MaxInt64), 1, 0), 0))
}

func TestPreferredFanoutHysteresis(t *testing.T) {
	cfg := DefaultBalanceConfig()
	for _, tc := range []struct {
		previous, nodes, segments int
		rows                      int64
		want                      int
	}{
		{0, 4, 10, 100_000, 1},
		{1, 4, 10, 105_000, 1},
		{1, 4, 10, 120_000, 2},
		{2, 4, 10, 95_000, 2},
		{2, 4, 10, 80_000, 1},
		{3, 1, 10, 1_000_000, 1},
		{3, 4, 1, 1_000_000, 1},
		{1, 0, 10, 100, 0},
		{1, 4, 0, 100, 0},
		{0, 4, 1, 0, 1},
	} {
		require.Equal(t, tc.want, preferredFanout(tc.previous, tc.nodes, tc.segments, tc.rows, cfg), "%+v", tc)
	}
}

// An indivisible large segment would overshoot the mean; the smaller tail can
// still improve placement. Compare pruning with a full greedy sweep over many
// placements, including empty nodes, zero-size segments and fanout penalties.
func TestSizeOrderedOptimizationMatchesFullSweep(t *testing.T) {
	rng := rand.New(rand.NewSource(55701))
	for trial := range 201 {
		n := 2 + rng.Intn(4)
		count := 1 + rng.Intn(24)
		var entries []*SegmentDataView
		owners := make(map[int64]int64)
		base := make(map[int64]int64)
		for i := range count {
			s := &SegmentDataView{SegmentID: int64(i + 1), LogicalSize: int64(rng.Intn(25))}
			entries = append(entries, s)
			owners[s.SegmentID] = int64(1 + rng.Intn(n))
		}
		cfg := DefaultBalanceConfig()
		cfg.TargetLogicalSizePerNode = 10
		if trial == 0 {
			n = 2
			entries = []*SegmentDataView{{SegmentID: 1, LogicalSize: 9}, {SegmentID: 2, LogicalSize: 7}, {SegmentID: 3, LogicalSize: 2}}
			owners = map[int64]int64{1: 1, 2: 2, 3: 1}
			cfg.ShardConcentrationWeight, cfg.CollectionConcentrationWeight, cfg.FanoutWeight = 0, 0, 0
			cfg.MoveCostPerUnit, cfg.LoadCostPerUnit, cfg.MinScoreGain = 0, 0, 0.01
		} else {
			for i := range n {
				base[int64(i+1)] = int64(rng.Intn(30))
			}
		}
		view := &ShardDataView{Partitions: []*PartitionDataView{{Segments: entries}}}
		balancerapi.PrepareCollectionDataView(&CollectionDataView{Shards: []*ShardDataView{view}})
		entries = view.SegmentsBySize
		mean := 9 + float64(trial%35)
		makeContext := func() *allocationContext {
			ctx := &allocationContext{base: base, mean: mean, shardLimit: 25, collectionLimit: 40, config: cfg, fanout: 1, assignments: map[int64]int64{}, counts: map[int64]int{}, rows: map[int64]int64{}}
			for i := range n {
				ctx.nodes = append(ctx.nodes, int64(i+1))
			}
			for i, segment := range entries {
				ctx.assign(entries[i:i+1], owners[segment.SegmentID])
			}
			return ctx
		}
		expected, actual := makeContext(), makeContext()
		byNode := map[int64][]*SegmentDataView{}
		for _, segment := range entries {
			node := owners[segment.SegmentID]
			byNode[node] = append(byNode[node], segment)
		}
		expected.tryMoves(entries)
		for _, node := range expected.nodes {
			if group := byNode[node]; len(group) > 0 {
				expected.tryMoves(group)
			}
		}
		for i := range entries {
			expected.tryMoves(entries[i : i+1])
		}
		actual.optimize(entries)
		require.Equal(t, expected.assignments, actual.assignments, "trial %d", trial)
		if trial == 0 {
			require.Equal(t, map[int64]int64{1: 1, 2: 2, 3: 2}, actual.assignments)
		}
	}
}

func TestSizeCutoffSkipsBalancedSingleMoves(t *testing.T) {
	ctx := &allocationContext{
		config: DefaultBalanceConfig(), nodes: []int64{1, 2}, mean: 2, fanout: 2,
		rows: map[int64]int64{}, counts: map[int64]int{}, assignments: map[int64]int64{},
	}
	var entries []*SegmentDataView
	for i := range 4 {
		segment := &SegmentDataView{SegmentID: int64(i + 1), LogicalSize: 1}
		entries = append(entries, segment)
		ctx.assign(entries[i:i+1], int64(i%2+1))
	}
	var tryMoves func(*allocationContext, []*SegmentDataView)
	var groupSizes []int
	patch := mockey.Mock((*allocationContext).tryMoves).Origin(&tryMoves).To(func(c *allocationContext, segments []*SegmentDataView) {
		groupSizes = append(groupSizes, len(segments))
		tryMoves(c, segments)
	}).Build()
	defer patch.UnPatch()
	ctx.optimize(entries)
	require.Equal(t, []int{4, 2, 2}, groupSizes)
}
