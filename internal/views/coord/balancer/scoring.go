package balancer

import (
	"math"
	"sort"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
)

func reusableState(state coordview.SegmentState) bool {
	switch state {
	case coordview.SegmentStateUp, coordview.SegmentStateReady, coordview.SegmentStatePreparing:
		return true
	default:
		return false
	}
}

func stickinessScore(
	nodeID int64,
	rows int64,
	currentStates map[int64]coordview.SegmentState,
	eligible map[int64]struct{},
	cfg *BalanceConfig,
) float64 {
	if len(currentStates) == 0 {
		return 1
	}

	hasEligibleReusableCopy := false
	for currentNodeID, state := range currentStates {
		if _, ok := eligible[currentNodeID]; ok && reusableState(state) {
			hasEligibleReusableCopy = true
			break
		}
	}
	if !hasEligibleReusableCopy {
		return 1
	}

	if state, ok := currentStates[nodeID]; ok && reusableState(state) {
		return 1
	}

	scale := int64(0)
	if cfg != nil {
		scale = cfg.StickyRowsScale
	}
	if scale <= 0 {
		return 0
	}
	if rows < 0 {
		rows = 0
	}
	penalty := math.Min(float64(rows)/float64(scale), 1)
	return 1 - penalty
}

func nodeLoadScore(referenceRows, projectedRows float64) float64 {
	if referenceRows <= 0 {
		return 1
	}
	if projectedRows < 0 {
		projectedRows = 0
	}
	return referenceRows / (referenceRows + projectedRows)
}

func fanoutScore(nodeID int64, opened map[int64]struct{}, budget int) float64 {
	if _, ok := opened[nodeID]; ok {
		return 1
	}
	if len(opened) < budget {
		return 1
	}
	return 0
}

func placementIntent(stickiness, nodeLoad, fanout float64, cfg *BalanceConfig) float64 {
	if cfg == nil {
		return 0
	}
	weightSum := cfg.StickinessWeight + cfg.NodeLoadWeight + cfg.FanoutWeight
	if weightSum <= 0 {
		return 0
	}
	return (cfg.StickinessWeight*stickiness +
		cfg.NodeLoadWeight*nodeLoad +
		cfg.FanoutWeight*fanout) / weightSum
}

// This file implements Phase 2 node selection: given a segment and a set of
// candidate nodes, filter by hard constraints then pick the highest-scoring
// node via weighted soft constraints.

const scoreEpsilon = 1e-12

type allocationContext struct {
	nodes         map[int64]*BalanceNode
	eligible      []int64
	eligibleSet   map[int64]struct{}
	baseRows      map[int64]int64
	assignedRows  map[int64]int64
	openedNodes   map[int64]struct{}
	referenceRows float64
	fanoutBudget  int
	config        *BalanceConfig
}

func newAllocationContext(
	nodes map[int64]*BalanceNode,
	resourceGroup string,
	baseRows map[int64]int64,
	shardRows int64,
	segmentCount int,
	cfg *BalanceConfig,
) *allocationContext {
	if cfg == nil {
		cfg = DefaultBalanceConfig()
	}
	ctx := &allocationContext{
		nodes:        nodes,
		eligibleSet:  make(map[int64]struct{}),
		baseRows:     make(map[int64]int64),
		assignedRows: make(map[int64]int64),
		openedNodes:  make(map[int64]struct{}),
		config:       cfg,
	}

	var totalBaseRows int64
	for _, nodeID := range candidateNodeIDs(nodes, resourceGroup) {
		node := nodes[nodeID]
		if !passHardConstraints(node, nil) {
			continue
		}
		rows := baseRows[nodeID]
		if rows < 0 {
			rows = 0
		}
		ctx.eligible = append(ctx.eligible, nodeID)
		ctx.eligibleSet[nodeID] = struct{}{}
		ctx.baseRows[nodeID] = rows
		totalBaseRows += rows
	}

	if shardRows < 0 {
		shardRows = 0
	}
	if len(ctx.eligible) > 0 {
		ctx.referenceRows = float64(totalBaseRows+shardRows) / float64(len(ctx.eligible))
	}
	ctx.fanoutBudget = calculateFanoutBudget(len(ctx.eligible), segmentCount, shardRows, cfg.TargetRowsPerShardNode)
	return ctx
}

func calculateFanoutBudget(eligibleNodes, segmentCount int, shardRows, targetRows int64) int {
	if eligibleNodes <= 0 || segmentCount <= 0 {
		return 0
	}
	if shardRows < 0 {
		shardRows = 0
	}
	desired := 1
	if targetRows > 0 && shardRows > 0 {
		desired = int(1 + (shardRows-1)/targetRows)
	}
	return min(eligibleNodes, segmentCount, desired)
}

func (ctx *allocationContext) projectedRows(nodeID int64, segmentRows int64) int64 {
	if segmentRows < 0 {
		segmentRows = 0
	}
	return ctx.baseRows[nodeID] + ctx.assignedRows[nodeID] + segmentRows
}

func (ctx *allocationContext) assign(nodeID int64, rows int64) {
	if rows < 0 {
		rows = 0
	}
	ctx.assignedRows[nodeID] += rows
	ctx.openedNodes[nodeID] = struct{}{}
}

func pickNode(
	ctx *allocationContext,
	seg *SegmentInfo,
	currentStates map[int64]coordview.SegmentState,
) (int64, bool) {
	var (
		best  nodeCandidate
		found bool
	)
	rows := segmentRows(seg)
	for _, nodeID := range ctx.eligible {
		projectedRows := ctx.projectedRows(nodeID, rows)
		stickiness := stickinessScore(nodeID, rows, currentStates, ctx.eligibleSet, ctx.config)
		candidate := nodeCandidate{
			nodeID:        nodeID,
			intent:        placementIntent(stickiness, nodeLoadScore(ctx.referenceRows, float64(projectedRows)), fanoutScore(nodeID, ctx.openedNodes, ctx.fanoutBudget), ctx.config),
			reusable:      reusableCopyOnNode(nodeID, currentStates),
			opened:        nodeIsOpened(nodeID, ctx.openedNodes),
			projectedRows: projectedRows,
		}
		if !found || candidate.betterThan(best) {
			best = candidate
			found = true
		}
	}
	return best.nodeID, found
}

type nodeCandidate struct {
	nodeID        int64
	intent        float64
	reusable      bool
	opened        bool
	projectedRows int64
}

func (candidate nodeCandidate) betterThan(other nodeCandidate) bool {
	if candidate.intent > other.intent+scoreEpsilon {
		return true
	}
	if math.Abs(candidate.intent-other.intent) > scoreEpsilon {
		return false
	}
	if candidate.reusable != other.reusable {
		return candidate.reusable
	}
	if candidate.opened != other.opened {
		return candidate.opened
	}
	if candidate.projectedRows != other.projectedRows {
		return candidate.projectedRows < other.projectedRows
	}
	return candidate.nodeID < other.nodeID
}

func reusableCopyOnNode(nodeID int64, states map[int64]coordview.SegmentState) bool {
	state, ok := states[nodeID]
	return ok && reusableState(state)
}

func nodeIsOpened(nodeID int64, opened map[int64]struct{}) bool {
	_, ok := opened[nodeID]
	return ok
}

func candidateNodeIDs(predicted map[int64]*BalanceNode, resourceGroup string) []int64 {
	ids := make([]int64, 0, len(predicted))
	for nodeID, node := range predicted {
		if node == nil || node.ResourceGroup != resourceGroup {
			continue
		}
		ids = append(ids, nodeID)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids
}

// passHardConstraints returns true iff the node can accept the segment. The
// checks match the row-count balancer's Phase 2 hard rules:
//
//   - Node alive and not in graceful shutdown
//
// Row count is a relative balance signal, not an admission-control capacity.
func passHardConstraints(node *BalanceNode, _ *SegmentInfo) bool {
	if !node.Alive || node.Stopping {
		return false
	}
	return true
}

func segmentRows(seg *SegmentInfo) int64 {
	if seg == nil {
		return 0
	}
	return seg.RowNum
}

func segmentInfoFor(snap *BalancerSnapshot, segmentID, partitionID int64) *SegmentInfo {
	if info, ok := snap.SegmentInfo(segmentID); ok && info != nil {
		return info
	}
	return &SegmentInfo{SegmentID: segmentID, PartitionID: partitionID}
}
