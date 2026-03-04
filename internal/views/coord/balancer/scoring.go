package balancer

import "github.com/milvus-io/milvus/internal/views/coord/coordview"

// This file implements Phase 2 node selection: given a segment and a set of
// candidate nodes, filter by hard constraints then pick the highest-scoring
// node via weighted soft constraints.
//
// The algorithm operates on a predictedLoad view of the nodes — a mutable
// copy of snap.Nodes maintained by the Policy and updated as each segment
// is placed. This gives cross-shard and intra-shard coordination without
// any global optimization pass.

// pickNode returns the best available node for the given segment, or
// (0, false) if no node passes the hard constraints.
//
//   - predicted:     the mutable per-node load tracker (this shard + earlier)
//   - seg:           segment metadata; MemSize drives stickiness and load
//   - currentStates: current per-node states for this segment. Controls the
//     reuse / avoidance score.
//   - replicaNodes:  nodeIDs eligible for this replica (from ReplicaAssignment).
//     Nodes outside this list are never considered.
//   - cfg:           scoring weights and baseline.
func pickNode(
	predicted map[int64]*BalanceNode,
	seg *SegmentInfo,
	currentStates map[int64]coordview.SegmentState,
	replicaNodes []int64,
	cfg *BalanceConfig,
) (int64, bool) {
	var (
		bestID    int64
		bestScore = -1.0
		found     bool
	)
	for _, nodeID := range replicaNodes {
		node, ok := predicted[nodeID]
		if !ok {
			continue
		}
		if !passHardConstraints(node, seg) {
			continue
		}
		s := score(node, seg, currentStates, cfg)
		if !found || s > bestScore {
			bestID = nodeID
			bestScore = s
			found = true
		}
	}
	return bestID, found
}

// passHardConstraints returns true iff the node can physically accept the
// segment right now. The checks match the design doc's Phase 2 hard rules:
//
//   - Node alive and not in graceful shutdown
//   - Predicted memory (UpMemLoad + PendingMemLoad + this segment) fits
//     inside the node's declared MemoryCapacity (when capacity > 0)
//
// Capacity == 0 is treated as "unknown / unrestricted" and never rejects.
func passHardConstraints(node *BalanceNode, seg *SegmentInfo) bool {
	if !node.Alive || node.Stopping {
		return false
	}
	if node.MemoryCapacity > 0 {
		predictedMem := node.UpMemLoad + node.PendingMemLoad + segmentLoad(seg)
		if predictedMem > node.MemoryCapacity {
			return false
		}
	}
	return true
}

// score computes the weighted soft-constraint score for placing seg on node.
// Higher is better. Weights live in BalanceConfig and should differ by orders
// of magnitude (Stickiness >> Memory >> SegmentCount) to give the chain a
// lexicographic feel: the higher-priority factor only loses to a
// lower-priority one when the higher-priority factor is equal across
// candidates.
//
// Contributors:
//   - Reuse / avoidance: seg.MemSize / BaselineSegmentSize ×
//     StickinessBaseWeight × state factor. Up gets full stickiness, Ready and
//     Preparing get progressively weaker stickiness, and Unrecoverable gets a
//     negative factor so the balancer prefers other nodes when possible.
//   - Memory balance: 1 − (UpMem + PendingMem) / Capacity. Capacity==0
//     collapses to 0 (no preference).
//   - Segment count balance: 1 / (1 + SegmentCount). Smooth decay so a
//     node with 0 segments beats one with 1, which beats one with 10, etc.
func score(node *BalanceNode, seg *SegmentInfo, currentStates map[int64]coordview.SegmentState, cfg *BalanceConfig) float64 {
	if cfg == nil {
		cfg = &BalanceConfig{}
	}
	var s float64
	load := segmentLoad(seg)

	if currentStates != nil {
		state, ok := currentStates[node.NodeID]
		if ok {
			bonus := cfg.StickinessBaseWeight * segmentStateAffinity(state)
			if cfg.BaselineSegmentSize > 0 {
				bonus *= float64(load) / float64(cfg.BaselineSegmentSize)
			}
			s += bonus
		}
	}

	if node.MemoryCapacity > 0 {
		used := float64(node.UpMemLoad + node.PendingMemLoad)
		ratio := used / float64(node.MemoryCapacity)
		s += cfg.MemoryWeight * (1 - ratio)
	}

	s += cfg.SegmentCountWeight * (1.0 / float64(1+node.SegmentCount))
	return s
}

func segmentStateAffinity(state coordview.SegmentState) float64 {
	switch state {
	case coordview.SegmentStateUp:
		return 1.0
	case coordview.SegmentStateReady:
		return 0.75
	case coordview.SegmentStatePreparing:
		return 0.25
	case coordview.SegmentStateUnrecoverable:
		return -1.0
	default:
		return 0
	}
}

func segmentLoad(seg *SegmentInfo) int64 {
	if seg == nil {
		return 0
	}
	if seg.MemSize > 0 {
		return seg.MemSize
	}
	return seg.RowNum
}

func segmentInfoFor(snap *BalancerSnapshot, segmentID, partitionID int64) *SegmentInfo {
	if info, ok := snap.SegmentInfo(segmentID); ok && info != nil {
		return info
	}
	return &SegmentInfo{SegmentID: segmentID, PartitionID: partitionID}
}
