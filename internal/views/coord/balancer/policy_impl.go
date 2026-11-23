package balancer

import (
	"sort"
	"sync"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// DefaultBalancePolicy retains target layouts across batches. Actual facts and
// row predictions remain in the cache and call-local planning context respectively.
type DefaultBalancePolicy struct {
	feedback ViewFeedbackReader
	mu       sync.Mutex
	layouts  layoutManager
	fanouts  map[qviews.ShardID]int
	search   map[qviews.ShardID]*searchCursor
}

// NewDefaultBalancePolicy creates the standard balance policy.
func NewDefaultBalancePolicy(options ...PolicyOption) *DefaultBalancePolicy {
	p := &DefaultBalancePolicy{}
	for _, option := range options {
		option(p)
	}
	return p
}

type balanceCandidate struct {
	shardID  qviews.ShardID
	size     int64
	optional bool
}

// Plan classifies dirty shards and allocates eligible placements against one
// projected row tracker. Controller batches arrive in scheduling order.
func (p *DefaultBalancePolicy) Plan(reader balancercache.Reader, dirty []qviews.ShardID) *BalancePlan {
	p.mu.Lock()
	defer p.mu.Unlock()
	plan := &BalancePlan{
		Prepares: make(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder),
	}
	if reader == nil || len(dirty) == 0 {
		return plan
	}
	snap := newPlanningContext(reader)
	snap.feedback = p.feedback
	selected := len(dirty)
	dirty = p.layouts.prepare(snap, dirty)
	plan.RescanCollections = snap.changedLayouts
	if p.fanouts == nil {
		p.fanouts = make(map[qviews.ShardID]int)
		p.search = make(map[qviews.ShardID]*searchCursor)
	}
	snap.fanouts, snap.search = p.fanouts, p.search

	var candidates []balanceCandidate
	seen := make(map[qviews.ShardID]struct{}, len(dirty))
	for i, shardID := range dirty {
		if _, ok := seen[shardID]; ok {
			continue
		}
		seen[shardID] = struct{}{}
		if snap.boundedBatch && i >= selected {
			plan.Continues = append(plan.Continues, shardID)
			continue
		}

		// Layout publication can refresh cached inputs. A concurrently loaded
		// collection may not have been included in this pass's metadata read.
		if cfg := snap.ConfigForShard(shardID); cfg != nil && snap.schemaVersions != nil {
			if _, ok := snap.schemaVersions[cfg.CollectionID]; !ok {
				plan.Retries = append(plan.Retries, shardID)
				continue
			}
			if snap.schemaVersions[cfg.CollectionID] != snap.SchemaVersion(cfg.CollectionID) {
				continue // DataView publication will wake planning when materialization is complete.
			}
		}
		action := classifyShard(snap, shardID)
		switch action {
		case actionRelease:
			plan.Releases = append(plan.Releases, shardID)
			// A missing shard config can also mean replica removal. Only
			// collection-wide load intent withdrawal preempts serving leases.
			collection := snap.collectionForShard(shardID)
			if collection == nil || collection.LoadConfig() == nil {
				if plan.ReleaseControls == nil {
					plan.ReleaseControls = make(map[qviews.ShardID]*viewpb.QueryViewControl)
				}
				plan.ReleaseControls[shardID] = &viewpb.QueryViewControl{PreemptServingLease: true}
			}
		case actionMust, actionMayOptimize:
			optional := action == actionMayOptimize
			if optional && (snap.deferOptimization || !snap.GetBalanceConfig().OptimizationEnabled) {
				continue
			}
			candidate := balanceCandidate{shardID: shardID, optional: optional}
			if !snap.boundedBatch {
				candidate.size = shardTotalLoad(snap, shardID)
			}
			candidates = append(candidates, candidate)
		}
	}
	if !snap.boundedBatch {
		sortCandidates(candidates)
	}

	projectedRows := initialProjectedRows(snap.NodesMap())
	for _, shardID := range plan.Releases {
		subtractRows(projectedRows, currentShardRows(snap, shardID))
		snap.acceptRows(shardID, nil)
		delete(p.fanouts, shardID)
		delete(p.search, shardID)
	}

	for _, candidate := range candidates {
		oldRows := currentShardRows(snap, candidate.shardID)
		subtractRows(projectedRows, oldRows)
		result := allocate(snap, candidate.shardID, projectedRows, candidate.optional)
		if result == nil {
			addRows(projectedRows, oldRows)
			plan.Retries = append(plan.Retries, candidate.shardID)
			continue
		}
		if result.more {
			plan.Continues = append(plan.Continues, candidate.shardID)
		}
		if candidate.optional && assignmentsEqual(currentSegmentNodes(snap, candidate.shardID), result.assignments) {
			addRows(projectedRows, oldRows)
			continue
		}
		snap.acceptRows(candidate.shardID, result.rowsByNode)
		plan.Prepares[candidate.shardID] = result.materialize(snap, candidate.shardID)
		addRows(projectedRows, result.rowsByNode)
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
		rows := node.TargetLogicalSize
		if rows < 0 {
			rows = 0
		}
		projected[nodeID] = rows
	}
	return projected
}

func currentShardRows(snap balanceInput, shardID qviews.ShardID) map[int64]int64 {
	if snap == nil {
		return nil
	}
	return snap.CurrentSize(shardID)
}

// subtractRows mutates only the batch-owned prediction. Each shard is processed
// once, and remove comes from the same pinned node versions as the baselines.
// Rejected/failed allocations restore remove before planning the next shard.
func subtractRows(projected, remove map[int64]int64) {
	for node, rows := range remove {
		projected[node] -= rows
	}
}

func addRows(projected, add map[int64]int64) {
	for node, rows := range add {
		projected[node] += rows
	}
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
		if candidates[i].optional != candidates[j].optional {
			return !candidates[i].optional
		}
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

func shardTotalLoad(snap *planningContext, shardID qviews.ShardID) int64 {
	collection := snap.collectionForShard(shardID)
	if collection == nil {
		return 0
	}
	return collection.ShardLoadedSize(shardID.VChannel)
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
