package api

import (
	"math"
	"time"
)

// BalanceNode combines a QueryNode's identity and health with cross-shard
// aggregated logical size derived from the ShardViewRegistry.
//
// UpLogicalSize and PendingLogicalSize describe merged lifecycle progress. Scoring
// starts from TargetLogicalSize and tracks within-batch effects in a private map.
type BalanceNode struct {
	// Identity & health (Node Manager).
	NodeID        int64
	Alive         bool
	Stopping      bool
	ResourceGroup string

	// UpLogicalSize is the sum of LogicalSize across all Up-view segments on this node,
	// aggregated across all shards.
	UpLogicalSize int64
	// TargetLogicalSize counts one selected intended placement per shard.
	TargetLogicalSize int64
	// PendingLogicalSize is the sum of LogicalSize across all Preparing/Ready-view
	// segments on this node (in-flight loads).
	PendingLogicalSize int64
}

// BalanceConfig pins one complete incremental-scoring profile for a batch.
type BalanceConfig struct {
	ReconcileInterval time.Duration

	OptimizationEnabled             bool
	MinScoreGain                    float64
	MaxCandidateEvaluationsPerShard int64

	TargetLogicalSizePerNode       int64
	ResourceGroupWeight            float64
	ResourceGroupRelativeTolerance float64
	ResourceGroupAbsoluteTolerance int64
	ShardConcentrationWeight       float64
	CollectionConcentrationWeight  float64
	ConcentrationRelativeTolerance float64
	FanoutWeight                   float64
	FanoutHysteresis               float64
	MoveCostPerUnit                float64
	LoadCostPerUnit                float64
}

func DefaultBalanceConfig() *BalanceConfig {
	return &BalanceConfig{
		OptimizationEnabled: true, ReconcileInterval: time.Minute,
		ResourceGroupWeight: 1, ShardConcentrationWeight: 1, CollectionConcentrationWeight: 1, FanoutWeight: 1,
		MoveCostPerUnit: 0.02, LoadCostPerUnit: 0.08,
		ResourceGroupRelativeTolerance: 0.1, ConcentrationRelativeTolerance: 0.1, FanoutHysteresis: 0.1,
		ResourceGroupAbsoluteTolerance: 100_000, TargetLogicalSizePerNode: 100_000,
		MinScoreGain: 1, MaxCandidateEvaluationsPerShard: 100_000,
	}
}

// Valid rejects an entire malformed refresh, preserving the last valid profile.
func (c *BalanceConfig) Valid() bool {
	for _, v := range []float64{
		c.ResourceGroupWeight, c.ShardConcentrationWeight, c.CollectionConcentrationWeight, c.FanoutWeight,
		c.MoveCostPerUnit, c.LoadCostPerUnit, c.ResourceGroupRelativeTolerance, c.ConcentrationRelativeTolerance, c.FanoutHysteresis, c.MinScoreGain,
	} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return false
		}
	}
	sum := c.ResourceGroupWeight + c.ShardConcentrationWeight + c.CollectionConcentrationWeight + c.FanoutWeight
	return sum > 0 && !math.IsInf(sum, 0) && c.ResourceGroupRelativeTolerance < 1 && c.FanoutHysteresis < 1 &&
		c.ResourceGroupAbsoluteTolerance > 0 && c.TargetLogicalSizePerNode > 0 && c.MinScoreGain > 0 &&
		c.MaxCandidateEvaluationsPerShard > 0 && c.ReconcileInterval > 0
}
