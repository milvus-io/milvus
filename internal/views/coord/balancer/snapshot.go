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

// BalanceNode combines a QueryNode's identity, health and capacity with
// cross-shard aggregated load derived from the ShardViewRegistry.
//
// UpMemLoad / PendingMemLoad / SegmentCount are snapshotted values; the Policy
// tracks within-batch effects in a separate predictedLoad map cloned from
// these values.
type BalanceNode struct {
	// Identity & health (Node Manager).
	NodeID        int64
	Alive         bool
	Stopping      bool
	ResourceGroup string

	// Capacity (node registration / config).
	MemoryCapacity int64
	// MemoryUsage is the most recent actual memory usage reported by the node
	// via SyncResponse. Not used for hard constraints (we use MemLoad + pending
	// instead) but useful for anomaly detection.
	MemoryUsage int64

	// UpMemLoad is the sum of MemSize across all Up-view segments on this
	// node, aggregated across all shards.
	UpMemLoad int64
	// PendingMemLoad is the sum of MemSize across all Preparing/Ready-view
	// segments on this node (in-flight loads).
	PendingMemLoad int64
	// SegmentCount is the total number of Up-view segments on this node
	// (for count-based balance metrics).
	SegmentCount int
}

// BalanceConfig is the tunable parameter set for the allocation algorithm.
type BalanceConfig struct {
	// Scoring weights. Weights differ by orders of magnitude to enforce
	// strict priority ordering (Stickiness >> Memory Balance >> Count Balance).
	StickinessBaseWeight float64
	MemoryWeight         float64
	SegmentCountWeight   float64

	// BaselineSegmentSize normalizes size-proportional stickiness so the
	// stickiness bonus of a typical segment equals StickinessBaseWeight.
	BaselineSegmentSize int64

	// Optional-balance thresholds (Phase 3).
	BalanceThreshold        float64 // minimum absolute score improvement
	CostEfficiencyThreshold float64 // minimum score gain per byte migrated

	// Full-scan interval for the reconcile loop (ticker fallback).
	TickerInterval time.Duration
}
