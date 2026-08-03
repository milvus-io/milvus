package balancer

import (
	"time"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// BalancerSnapshot is the world view consumed by BalancePolicy.Plan.
// Built once at the start of each reconcile cycle and reused for every
// dirty shard in that batch, so cross-shard decisions are consistent.
//
// Fields are not mutated during Plan; a separate predictedLoad tracker
// (outside the snapshot) captures within-batch allocation effects.
type BalancerSnapshot struct {
	// Provider-owned immutable snapshots composed for this reconcile cycle.
	LoadConfigSnapshot *loadmgr.LoadConfigSnapshot
	ShardViewSnapshot  *coordview.ShardViewSnapshot
	DataViewSnapshot   *DataViewSnapshot
	NodeSnapshot       *NodeSnapshot

	// Per-node info with cross-shard aggregates embedded.
	Nodes map[int64]*BalanceNode
	// ShardRowStatsSnapshot contains the target shards' exact row counts already
	// included in Nodes. Policy subtracts them before replacing or releasing ashard
	ShardRowStatsSnapshot map[qviews.ShardID]ShardRowStats

	// Tunable parameters for the allocation algorithm.
	Config *BalanceConfig
}

// ConfigForShard returns the LoadConfig owning the given shard, or nil if
// no such config exists (e.g., the collection is not loaded). A shard is
// owned by the config whose Replicas list contains ShardID.ReplicaID.
func (s *BalancerSnapshot) ConfigForShard(shardID qviews.ShardID) *loadmgr.LoadConfig {
	if s == nil || s.LoadConfigSnapshot == nil {
		return nil
	}
	return s.LoadConfigSnapshot.ReplicaToConfigMap()[shardID.ReplicaID]
}

func (s *BalancerSnapshot) SegmentInfo(segmentID int64) (*SegmentInfo, bool) {
	if s == nil || s.DataViewSnapshot == nil {
		return nil, false
	}
	return s.DataViewSnapshot.SegmentInfo(segmentID)
}

func (s *BalancerSnapshot) DataVersionForCollection(collectionID int64) (qviews.DataVersion, bool) {
	if s == nil || s.DataViewSnapshot == nil {
		return qviews.DataVersion{}, false
	}
	return s.DataViewSnapshot.DataVersion(collectionID)
}

func (s *BalancerSnapshot) DataViewForShard(shardID qviews.ShardID) *viewpb.DataViewOfShard {
	cfg := s.ConfigForShard(shardID)
	if cfg == nil || s.DataViewSnapshot == nil {
		return nil
	}
	shard, _ := s.DataViewSnapshot.ShardView(cfg.CollectionID, shardID.VChannel)
	return shard
}

func (s *BalancerSnapshot) RangeDataShards(collectionID int64, fn func(qviews.ShardID) bool) {
	cfgs := s.ConfigsMap()
	cfg := cfgs[collectionID]
	if cfg == nil || s.DataViewSnapshot == nil {
		return
	}
	s.DataViewSnapshot.RangeShards(collectionID, func(shard *viewpb.DataViewOfShard) bool {
		if shard == nil {
			return true
		}
		for _, replica := range cfg.Replicas {
			if !fn(qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: shard.GetVchannel()}) {
				return false
			}
		}
		return true
	})
}

func (s *BalancerSnapshot) ConfigsMap() map[int64]*loadmgr.LoadConfig {
	if s == nil || s.LoadConfigSnapshot == nil {
		return nil
	}
	return s.LoadConfigSnapshot.ConfigsMap()
}

func (s *BalancerSnapshot) ShardStatsMap() map[qviews.ShardID]*coordview.ShardStats {
	if s == nil || s.ShardViewSnapshot == nil {
		return nil
	}
	return s.ShardViewSnapshot.StatsMap()
}

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
