package balancer

import (
	"sort"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// allocate produces a QueryViewAtCoordBuilder for shardID against the current
// snapshot and predictedLoad tracker. It returns nil if the shard cannot be
// allocated (missing DataView, missing replica, or any segment has no
// eligible node).
//
// Segments are processed in largest-first order (by MemSize). After each
// assignment, predictedLoad is updated so later segments in this shard — and
// later shards in this batch, when Policy reuses the tracker — see the
// accumulated effect.
//
// This is the Phase 2 "allocation" step; Phase 1 classification and Phase 3
// threshold gating live in classify.go / policy_impl.go respectively.
func allocate(
	snap *BalancerSnapshot,
	shardID qviews.ShardID,
	predicted map[int64]*BalanceNode,
) *qviews.QueryViewAtCoordBuilder {
	desired := snap.ConfigForShard(shardID)
	if desired == nil {
		return nil
	}
	replica := findReplica(desired, shardID.ReplicaID)
	if replica == nil {
		return nil
	}
	shardDV := snap.DataViewForShard(shardID)
	if shardDV == nil {
		return nil
	}

	// Collect segments with their owning partitionID, and sort largest-first.
	type segEntry struct {
		segmentID   int64
		partitionID int64
		load        int64
	}
	entries := make([]segEntry, 0)
	for _, p := range shardDV.GetPartitions() {
		for _, segID := range p.GetSegmentIds() {
			entries = append(entries, segEntry{
				segmentID:   segID,
				partitionID: p.GetPartitionId(),
				load:        segmentLoad(segmentInfoFor(snap, segID, p.GetPartitionId())),
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].load > entries[j].load
	})

	// Current per-node segment states for stickiness / avoidance lookup.
	current := currentSegmentStates(snap, shardID)

	// Fresh assignments map: nodeID → partitionID → []segmentID.
	assignments := make(map[int64]map[int64][]int64)

	for _, e := range entries {
		segInfo := segmentInfoFor(snap, e.segmentID, e.partitionID)
		nodeID, ok := pickNode(predicted, segInfo, current[e.segmentID], replica.Nodes, snap.Config)
		if !ok {
			return nil
		}
		if _, exists := assignments[nodeID]; !exists {
			assignments[nodeID] = make(map[int64][]int64)
		}
		assignments[nodeID][e.partitionID] = append(assignments[nodeID][e.partitionID], e.segmentID)

		// Track the allocation on predictedLoad so subsequent segments (and
		// subsequent shards) see the accumulated effect.
		if node, present := predicted[nodeID]; present {
			node.PendingMemLoad += segmentLoad(segInfo)
			node.SegmentCount++
		}
	}

	dataVersion, _ := snap.DataVersionForCollection(desired.CollectionID)
	builder := qviews.NewQueryViewAtCoordBuilder(
		replica.ReplicaID,
		syntheticDataView(desired, dataVersion, shardDV),
		shardID.VChannel,
	)
	builder.SetAssignments(assignments)
	builder.SetSettings(settingsFromConfig(desired))
	return builder
}

type segmentNodeStates map[int64]map[int64]coordview.SegmentState

// currentSegmentStates returns segmentID -> nodeID -> SegmentState from the
// shard's merged stats. Empty when no placement is tracked.
func currentSegmentStates(snap *BalancerSnapshot, shardID qviews.ShardID) segmentNodeStates {
	stats, ok := snap.ShardStatsMap()[shardID]
	if !ok {
		return nil
	}
	out := make(segmentNodeStates, len(stats.Segments))
	for segmentID, segment := range stats.Segments {
		states := make(map[int64]coordview.SegmentState, len(segment.Nodes))
		for nodeID, state := range segment.Nodes {
			states[nodeID] = state
		}
		out[segmentID] = states
	}
	return out
}

// currentSegmentNodes returns segmentID -> best node from the shard's merged
// states, used for optional-migration cost accounting. The best node follows
// the same state priority as ShardStats: Up > Ready > Preparing >
// Unrecoverable.
func currentSegmentNodes(snap *BalancerSnapshot, shardID qviews.ShardID) map[int64]int64 {
	return bestSegmentNodes(currentSegmentStates(snap, shardID))
}

func bestSegmentNodes(states segmentNodeStates) map[int64]int64 {
	out := make(map[int64]int64, len(states))
	for segmentID, nodeStates := range states {
		var (
			bestNode  int64
			bestState coordview.SegmentState
			found     bool
		)
		for nodeID, state := range nodeStates {
			if !found || state > bestState || (state == bestState && nodeID < bestNode) {
				bestNode = nodeID
				bestState = state
				found = true
			}
		}
		if found {
			out[segmentID] = bestNode
		}
	}
	return out
}

// findReplica returns the ReplicaAssignment whose ReplicaID matches.
func findReplica(cfg *loadmgr.LoadConfig, replicaID int64) *loadmgr.ReplicaAssignment {
	for _, r := range cfg.Replicas {
		if r.ReplicaID == replicaID {
			return r
		}
	}
	return nil
}

// syntheticDataView constructs the minimal DataViewOfCollection the builder
// needs. Only DataVersion, CollectionId, and the target shard are populated —
// the builder does not look at other shards.
func syntheticDataView(
	cfg *loadmgr.LoadConfig,
	dv qviews.DataVersion,
	shard *viewpb.DataViewOfShard,
) *viewpb.DataViewOfCollection {
	return &viewpb.DataViewOfCollection{
		CollectionId: cfg.CollectionID,
		DataVersion:  dv.IntoProto(),
		Shards:       []*viewpb.DataViewOfShard{shard},
	}
}

// settingsFromConfig derives QueryViewSettings from a LoadConfig. Partition IDs
// go into RequiredPartitions; field IDs into RequiredFields. Everything is a
// "required" list in the current design (no optional/lazy semantics yet).
func settingsFromConfig(cfg *loadmgr.LoadConfig) *viewpb.QueryViewSettings {
	fields := make([]int64, len(cfg.LoadFields))
	for i, f := range cfg.LoadFields {
		fields[i] = f.GetFieldId()
	}
	return &viewpb.QueryViewSettings{
		RequiredPartitions: append([]int64{}, cfg.PartitionIDs...),
		RequiredFields:     fields,
	}
}

// clonePredictedLoad returns a deep-enough copy of the Nodes map so the Policy
// can mutate it across shards without altering the source snapshot.
func clonePredictedLoad(nodes map[int64]*BalanceNode) map[int64]*BalanceNode {
	out := make(map[int64]*BalanceNode, len(nodes))
	for id, n := range nodes {
		clone := *n // shallow copy is fine: BalanceNode has no reference fields.
		out[id] = &clone
	}
	return out
}
