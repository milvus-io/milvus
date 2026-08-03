package balancer

import (
	"sort"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// DefaultBalancePolicy is the built-in three-phase policy described in
// balancer_design.md. It is intentionally stateless; all mutable planning
// state is scoped to a single Plan call.
type DefaultBalancePolicy struct{}

// NewDefaultBalancePolicy creates the standard balance policy.
func NewDefaultBalancePolicy() *DefaultBalancePolicy {
	return &DefaultBalancePolicy{}
}

type balanceCandidate struct {
	shardID qviews.ShardID
	size    int64
}

// Plan classifies dirty shards, orders mandatory work before optional
// optimization, and allocates each accepted shard against a shared
// steady-state row tracker.
func (p *DefaultBalancePolicy) Plan(snap *BalancerSnapshot, dirty []qviews.ShardID) *BalancePlan {
	plan := &BalancePlan{
		Prepares: make(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder),
	}
	if snap == nil || len(dirty) == 0 {
		return plan
	}
	if snap.Config == nil {
		snapCopy := *snap
		snapCopy.Config = DefaultBalanceConfig()
		snap = &snapCopy
	}

	var mandatory, optional []balanceCandidate
	seen := make(map[qviews.ShardID]struct{}, len(dirty))
	for _, shardID := range dirty {
		if _, ok := seen[shardID]; ok {
			continue
		}
		seen[shardID] = struct{}{}

		action := classifyShard(snap, shardID)
		switch action {
		case actionRelease:
			plan.Releases = append(plan.Releases, shardID)
		case actionMust:
			mandatory = append(mandatory, balanceCandidate{
				shardID: shardID,
				size:    shardTotalLoad(snap, shardID),
			})
		case actionMayOptimize:
			optional = append(optional, balanceCandidate{
				shardID: shardID,
				size:    shardTotalLoad(snap, shardID),
			})
		}
	}

	sortCandidates(mandatory)
	sortCandidates(optional)

	projectedRows := initialProjectedRows(snap.Nodes)
	for _, shardID := range plan.Releases {
		projectedRows = withoutRows(projectedRows, currentShardRows(snap, shardID))
	}

	for _, candidate := range mandatory {
		baseRows := withoutRows(projectedRows, currentShardRows(snap, candidate.shardID))
		result := allocate(snap, candidate.shardID, baseRows)
		if result == nil {
			continue
		}
		plan.Prepares[candidate.shardID] = result.builder
		projectedRows = withRows(baseRows, result.rowsByNode)
	}

	for _, candidate := range optional {
		baseRows := withoutRows(projectedRows, currentShardRows(snap, candidate.shardID))
		result := allocate(snap, candidate.shardID, baseRows)
		if result == nil {
			continue
		}
		if assignmentsEqual(currentSegmentNodes(snap, candidate.shardID), result.assignments) {
			continue
		}
		plan.Prepares[candidate.shardID] = result.builder
		projectedRows = withRows(baseRows, result.rowsByNode)
	}

	sort.Slice(plan.Releases, func(i, j int) bool {
		return shardLess(plan.Releases[i], plan.Releases[j])
	})
	return plan
}

func initialProjectedRows(nodes map[int64]*BalanceNode) map[int64]int64 {
	projected := make(map[int64]int64, len(nodes))
	for nodeID, node := range nodes {
		if node == nil {
			continue
		}
		rows := node.UpRowCount + node.PendingRowCount
		if rows < 0 {
			rows = 0
		}
		projected[nodeID] = rows
	}
	return projected
}

func currentShardRows(snap *BalancerSnapshot, shardID qviews.ShardID) map[int64]int64 {
	rowsByNode := make(map[int64]int64)
	if snap == nil {
		return rowsByNode
	}
	rowStats := snap.ShardRowStatsSnapshot[shardID]
	for nodeID, rows := range rowStats {
		rowsByNode[nodeID] = rows.UpRowCount + rows.PendingRowCount
	}
	return rowsByNode
}

func withoutRows(projected, remove map[int64]int64) map[int64]int64 {
	result := cloneRows(projected)
	for nodeID, rows := range remove {
		if _, ok := result[nodeID]; !ok {
			continue
		}
		result[nodeID] -= rows
		if result[nodeID] < 0 {
			result[nodeID] = 0
		}
	}
	return result
}

func withRows(base, add map[int64]int64) map[int64]int64 {
	result := cloneRows(base)
	for nodeID, rows := range add {
		result[nodeID] += rows
	}
	return result
}

func cloneRows(rows map[int64]int64) map[int64]int64 {
	result := make(map[int64]int64, len(rows))
	for nodeID, rowCount := range rows {
		result[nodeID] = rowCount
	}
	return result
}

func assignmentsEqual(current, candidate map[int64]int64) bool {
	if len(current) != len(candidate) {
		return false
	}
	for segmentID, nodeID := range current {
		if candidate[segmentID] != nodeID {
			return false
		}
	}
	return true
}

func sortCandidates(candidates []balanceCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].size != candidates[j].size {
			return candidates[i].size > candidates[j].size
		}
		return shardLess(candidates[i].shardID, candidates[j].shardID)
	})
}

func shardLess(a, b qviews.ShardID) bool {
	if a.ReplicaID != b.ReplicaID {
		return a.ReplicaID < b.ReplicaID
	}
	return a.VChannel < b.VChannel
}

func shardTotalLoad(snap *BalancerSnapshot, shardID qviews.ShardID) int64 {
	shard := snap.DataViewForShard(shardID)
	if shard == nil {
		return 0
	}
	var total int64
	for _, p := range shard.GetPartitions() {
		for _, segmentID := range p.GetSegmentIds() {
			total += segmentRows(segmentInfoFor(snap, segmentID, p.GetPartitionId()))
		}
	}
	return total
}

func flattenAssignments(view *viewpb.QueryViewOfShard) map[int64]int64 {
	out := make(map[int64]int64)
	for _, qn := range view.GetQueryNode() {
		for _, p := range qn.GetPartitions() {
			for _, segmentID := range p.GetSegmentIds() {
				out[segmentID] = qn.GetNodeId()
			}
		}
	}
	return out
}
