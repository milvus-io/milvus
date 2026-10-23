package api

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIncrementalProfileValidation(t *testing.T) {
	require.True(t, DefaultBalanceConfig().Valid())
	for _, change := range []func(*BalanceConfig){
		func(c *BalanceConfig) { c.MoveCostPerUnit = -1 },
		func(c *BalanceConfig) { c.LoadCostPerUnit = math.NaN() },
		func(c *BalanceConfig) { c.ShardConcentrationWeight = math.Inf(1) },
		func(c *BalanceConfig) {
			c.ResourceGroupWeight, c.ShardConcentrationWeight, c.CollectionConcentrationWeight, c.FanoutWeight = 0, 0, 0, 0
		},
		func(c *BalanceConfig) {
			c.ResourceGroupWeight, c.ShardConcentrationWeight = math.MaxFloat64, math.MaxFloat64
		},
		func(c *BalanceConfig) { c.ResourceGroupRelativeTolerance = 1 },
		func(c *BalanceConfig) { c.FanoutHysteresis = 1 },
		func(c *BalanceConfig) { c.ResourceGroupAbsoluteTolerance = 0 },
		func(c *BalanceConfig) { c.TargetLogicalSizePerNode = 0 },
		func(c *BalanceConfig) { c.MinScoreGain = 0 },
		func(c *BalanceConfig) { c.MaxCandidateEvaluationsPerShard = 0 },
		func(c *BalanceConfig) { c.ReconcileInterval = 0 },
	} {
		cfg := DefaultBalanceConfig()
		change(cfg)
		require.False(t, cfg.Valid(), "%+v", cfg)
	}
}
