package balancer

import (
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
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

func (s *BalancerSnapshot) SegmentInfo(segmentID int64) (*SegmentDataView, bool) {
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

func (s *BalancerSnapshot) DataViewForShard(shardID qviews.ShardID) *ShardDataView {
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
	s.DataViewSnapshot.RangeShards(collectionID, func(shard *ShardDataView) bool {
		if shard == nil {
			return true
		}
		for _, replica := range cfg.Replicas {
			if !fn(qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: shard.VChannel}) {
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

// Compatibility accessors for callers of allocation helpers. The controller
// uses planningContext and never materializes this legacy snapshot.
func (s *BalancerSnapshot) GetBalanceConfig() *BalanceConfig { return s.Config }
func (s *BalancerSnapshot) NodesMap() map[int64]*BalanceNode { return s.Nodes }
func (s *BalancerSnapshot) GetShardStats(id qviews.ShardID) *coordview.ShardStats {
	return s.ShardStatsMap()[id]
}

func (s *BalancerSnapshot) ConfigVersion(id int64) uint64 {
	return s.LoadConfigSnapshot.ConfigVersion(id)
}
func (s *BalancerSnapshot) CandidateNodes(rg string) []int64 { return candidateNodeIDs(s.Nodes, rg) }
func (s *BalancerSnapshot) CurrentRows(id qviews.ShardID) map[int64]int64 {
	rows := make(map[int64]int64)
	for nodeID, row := range s.ShardRowStatsSnapshot[id] {
		rows[nodeID] = row.UpRowCount + row.PendingRowCount
	}
	return rows
}

func segmentInfoFor(snap *BalancerSnapshot, segmentID, partitionID int64) *SegmentDataView {
	if info, ok := snap.SegmentInfo(segmentID); ok && info != nil {
		return info
	}
	return &SegmentDataView{SegmentID: segmentID, PartitionID: partitionID}
}
