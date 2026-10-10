package api

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIncrementalProfileValidation(t *testing.T) {
	require.True(t, DefaultBalanceConfig().Valid())
	for _, change := range []func(*BalanceConfig){
		func(c *BalanceConfig) { c.MovePrice = -1 },
		func(c *BalanceConfig) { c.LoadPrice = math.NaN() },
		func(c *BalanceConfig) { c.ShardWeight = math.Inf(1) },
		func(c *BalanceConfig) {
			c.GlobalWeight, c.ShardWeight, c.CollectionWeight, c.FanoutPenaltyWeight = 0, 0, 0, 0
		},
		func(c *BalanceConfig) { c.GlobalWeight, c.ShardWeight = math.MaxFloat64, math.MaxFloat64 },
		func(c *BalanceConfig) { c.RelativeTolerance = 1 },
		func(c *BalanceConfig) { c.FanoutHysteresis = 1 },
		func(c *BalanceConfig) { c.AbsoluteToleranceRows = 0 },
		func(c *BalanceConfig) { c.TargetRowsPerShardNode = 0 },
		func(c *BalanceConfig) { c.MinGainRows = 0 },
		func(c *BalanceConfig) { c.MaxCandidateEvaluations = 0 },
		func(c *BalanceConfig) { c.TickerInterval = 0 },
	} {
		cfg := DefaultBalanceConfig()
		change(cfg)
		require.False(t, cfg.Valid(), "%+v", cfg)
	}
}
