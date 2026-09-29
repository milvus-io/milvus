package balancer

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
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
