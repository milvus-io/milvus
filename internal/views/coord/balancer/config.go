package balancer

import "time"

// BalanceNode combines a QueryNode's identity and health with cross-shard
// aggregated row load derived from the ShardViewRegistry.
//
// UpRowCount and PendingRowCount are snapshotted values; the Policy tracks
// within-batch effects in a separate steady-state row map.
type BalanceNode struct {
	// Identity & health (Node Manager).
	NodeID        int64
	Alive         bool
	Stopping      bool
	ResourceGroup string

	// UpRowCount is the sum of RowNum across all Up-view segments on this node,
	// aggregated across all shards.
	UpRowCount int64
	// PendingRowCount is the sum of RowNum across all Preparing/Ready-view
	// segments on this node (in-flight loads).
	PendingRowCount int64
}

// BalanceConfig is the tunable parameter set for the allocation algorithm.
type BalanceConfig struct {
	// Normalized scoring weights. Each component is bounded in [0, 1] before
	// its weight is applied.
	StickinessWeight float64
	NodeLoadWeight   float64
	FanoutWeight     float64

	// StickyRowsScale controls the row-proportional movement penalty.
	StickyRowsScale int64
	// TargetRowsPerShardNode controls the data-derived free fanout budget.
	TargetRowsPerShardNode int64

	// Full-scan interval for the reconcile loop (ticker fallback).
	TickerInterval time.Duration
}

// DefaultBalanceConfig returns the production scoring configuration for
// homogeneous QueryNodes. RowNum is the sole load metric.
func DefaultBalanceConfig() *BalanceConfig {
	return &BalanceConfig{
		StickinessWeight:       1,
		NodeLoadWeight:         1,
		FanoutWeight:           1,
		StickyRowsScale:        1_000_000,
		TargetRowsPerShardNode: 100_000,
	}
}
