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
// optimization, and allocates each accepted shard against a shared predicted
// load tracker.
func (p *DefaultBalancePolicy) Plan(snap *BalancerSnapshot, dirty []qviews.ShardID) *BalancePlan {
	plan := &BalancePlan{
		Prepares: make(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder),
	}
	if snap == nil || len(dirty) == 0 {
		return plan
	}
	if snap.Config == nil {
		snapCopy := *snap
		snapCopy.Config = &BalanceConfig{}
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

	predicted := clonePredictedLoad(snap.Nodes)
	for _, candidate := range mandatory {
		nextPredicted := clonePredictedLoad(predicted)
		builder := allocate(snap, candidate.shardID, nextPredicted)
		if builder == nil {
			continue
		}
		plan.Prepares[candidate.shardID] = builder
		predicted = nextPredicted
	}

	for _, candidate := range optional {
		nextPredicted := clonePredictedLoad(predicted)
		builder := allocate(snap, candidate.shardID, nextPredicted)
		if builder == nil {
			continue
		}
		gain, migrationCost := placementImprovement(snap, candidate.shardID, predicted, nextPredicted, builder)
		if !worthOptionalMigration(gain, migrationCost, snap.Config) {
			continue
		}
		plan.Prepares[candidate.shardID] = builder
		predicted = nextPredicted
	}

	sort.Slice(plan.Releases, func(i, j int) bool {
		return shardLess(plan.Releases[i], plan.Releases[j])
	})
	return plan
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
			total += segmentLoad(segmentInfoFor(snap, segmentID, p.GetPartitionId()))
		}
	}
	return total
}

func placementImprovement(
	snap *BalancerSnapshot,
	shardID qviews.ShardID,
	currentNodes map[int64]*BalanceNode,
	candidateNodes map[int64]*BalanceNode,
	builder *qviews.QueryViewAtCoordBuilder,
) (float64, int64) {
	currentAssignments := currentSegmentNodes(snap, shardID)
	nextAssignments := flattenAssignments(builder.Build())

	currentScore := placementScore(snap, shardID, currentNodes, currentAssignments)
	nextScore := placementScore(snap, shardID, candidateNodes, nextAssignments)
	migrationCost := movedSegmentLoad(snap, currentAssignments, nextAssignments)
	return nextScore - currentScore, migrationCost
}

func placementScore(
	snap *BalancerSnapshot,
	shardID qviews.ShardID,
	nodes map[int64]*BalanceNode,
	assignments map[int64]int64,
) float64 {
	stickyStates := currentSegmentStates(snap, shardID)
	shard := snap.DataViewForShard(shardID)
	if shard == nil {
		return 0
	}

	var total float64
	for _, p := range shard.GetPartitions() {
		for _, segmentID := range p.GetSegmentIds() {
			nodeID, ok := assignments[segmentID]
			if !ok {
				continue
			}
			node := nodes[nodeID]
			if node == nil {
				continue
			}
			total += score(node, segmentInfoFor(snap, segmentID, p.GetPartitionId()), stickyStates[segmentID], snap.Config)
		}
	}
	return total
}

func movedSegmentLoad(snap *BalancerSnapshot, current, next map[int64]int64) int64 {
	var total int64
	for segmentID, nextNode := range next {
		if current[segmentID] == nextNode {
			continue
		}
		total += segmentLoad(segmentInfoFor(snap, segmentID, 0))
	}
	return total
}

func worthOptionalMigration(gain float64, migrationCost int64, cfg *BalanceConfig) bool {
	if migrationCost <= 0 {
		return false
	}
	if gain <= cfg.BalanceThreshold {
		return false
	}
	return gain/float64(migrationCost) > cfg.CostEfficiencyThreshold
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
