package balancer

import (
	"sort"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// Frozen pre-refactor entry point for static-input equivalence tests.
func legacyPlan(snap *BalancerSnapshot, dirty []qviews.ShardID) *BalancePlan {
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
