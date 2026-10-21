package api

import (
	"math"
	"time"
)

// BalanceNode combines a QueryNode's identity and health with cross-shard
// aggregated row load derived from the ShardViewRegistry.
//
// UpRowCount and PendingRowCount describe merged lifecycle progress. Scoring
// starts from TargetRowCount and tracks within-batch effects in a private map.
type BalanceNode struct {
	// Identity & health (Node Manager).
	NodeID        int64
	Alive         bool
	Stopping      bool
	ResourceGroup string

	// UpRowCount is the sum of RowNum across all Up-view segments on this node,
	// aggregated across all shards.
	UpRowCount int64
	// TargetRowCount counts one selected intended placement per shard.
	TargetRowCount int64
	// PendingRowCount is the sum of RowNum across all Preparing/Ready-view
	// segments on this node (in-flight loads).
	PendingRowCount int64
}

// BalanceConfig pins one complete incremental-scoring profile for a batch.
type BalanceConfig struct {
	AutoBalance                                                      bool
	TickerInterval                                                   time.Duration
	GlobalWeight, ShardWeight, CollectionWeight, FanoutPenaltyWeight float64
	MovePrice, LoadPrice                                             float64
	RelativeTolerance, LocalTolerance, FanoutHysteresis              float64
	AbsoluteToleranceRows, TargetRowsPerShardNode                    int64
	MinGainRows                                                      float64
	MaxCandidateEvaluations                                          int64
}

func DefaultBalanceConfig() *BalanceConfig {
	return &BalanceConfig{
		AutoBalance: true, TickerInterval: time.Minute,
		GlobalWeight: 1, ShardWeight: 1, CollectionWeight: 1, FanoutPenaltyWeight: 1,
		MovePrice: 0.02, LoadPrice: 0.08,
		RelativeTolerance: 0.1, LocalTolerance: 0.1, FanoutHysteresis: 0.1,
		AbsoluteToleranceRows: 100_000, TargetRowsPerShardNode: 100_000,
		MinGainRows: 1, MaxCandidateEvaluations: 100_000,
	}
}

// Valid rejects an entire malformed refresh, preserving the last valid profile.
func (c *BalanceConfig) Valid() bool {
	for _, v := range []float64{
		c.GlobalWeight, c.ShardWeight, c.CollectionWeight, c.FanoutPenaltyWeight,
		c.MovePrice, c.LoadPrice, c.RelativeTolerance, c.LocalTolerance, c.FanoutHysteresis, c.MinGainRows,
	} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return false
		}
	}
	sum := c.GlobalWeight + c.ShardWeight + c.CollectionWeight + c.FanoutPenaltyWeight
	return sum > 0 && !math.IsInf(sum, 0) && c.RelativeTolerance < 1 && c.FanoutHysteresis < 1 &&
		c.AbsoluteToleranceRows > 0 && c.TargetRowsPerShardNode > 0 && c.MinGainRows > 0 &&
		c.MaxCandidateEvaluations > 0 && c.TickerInterval > 0
}
