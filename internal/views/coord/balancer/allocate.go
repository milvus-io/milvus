package balancer

import (
	"maps"
	"math"
	"sort"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type allocationResult struct {
	builder     *qviews.QueryViewAtCoordBuilder
	assignments map[int64]int64
	rowsByNode  map[int64]int64
	more        bool
}

// allocate produces a complete candidate for shardID against the supplied
// steady-state base rows. It returns nil if the shard cannot be
// allocated (missing DataView, missing replica, or any segment has no
// eligible node).
//
// Segments are processed by RowNum descending and SegmentID ascending. A
// shard-local allocationContext tracks partial rows and opened nodes.
//
// This is the Phase 2 "allocation" step; Phase 1 classification and Phase 3
// exact assignment-change emission live in classify.go / policy_impl.go.
func allocate(p *planningContext, shardID qviews.ShardID, baseRows map[int64]int64, optional bool) *allocationResult {
	desired := p.ConfigForShard(shardID)
	shard := p.DataViewForShard(shardID)
	if desired == nil || shard == nil {
		return nil
	}
	entries := make([]*SegmentDataView, 0, shard.SegmentCount)
	for _, part := range shard.Partitions {
		entries = append(entries, part.Segments...)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].RowNum != entries[j].RowNum {
			return entries[i].RowNum > entries[j].RowNum
		}
		return entries[i].SegmentID < entries[j].SegmentID
	})
	ctx := newAllocationContext(p, shardID, baseRows, entries)
	if len(entries) > 0 && len(ctx.nodes) == 0 {
		return nil
	}
	original := currentSegmentNodes(p, shardID)
	eligible := make(map[int64]bool, len(ctx.nodes))
	for _, node := range ctx.nodes {
		eligible[node] = true
	}
	for _, segment := range entries {
		if node, exists := original[segment.SegmentID]; exists && eligible[node] {
			ctx.assign([]*SegmentDataView{segment}, node)
		}
	}
	for _, segment := range entries {
		if _, placed := ctx.assignments[segment.SegmentID]; placed {
			continue
		}
		if optional {
			return nil
		}
		best, bestGain := ctx.nodes[0], -math.MaxFloat64
		for _, node := range ctx.nodes {
			gain, _ := ctx.evaluate([]*SegmentDataView{segment}, node)
			if gain > bestGain+scoreEpsilon {
				best, bestGain = node, gain
			}
		}
		ctx.assign([]*SegmentDataView{segment}, best)
	}
	more := false
	if optional {
		before := ctx.score()
		baseline := maps.Clone(ctx.assignments)
		cursor := p.search[shardID]
		if cursor == nil {
			cursor = &searchCursor{}
			p.search[shardID] = cursor
		}
		more = ctx.optimize(entries, cursor)
		after := ctx.score()
		if after.global > before.global+scoreEpsilon || before.energy(ctx.config)-after.energy(ctx.config)-ctx.migrationCost(baseline) <= ctx.config.MinGainRows {
			ctx.assignments = baseline
			clear(ctx.rows)
			clear(ctx.counts)
			ctx.opened = 0
			for _, segment := range entries {
				node := baseline[segment.SegmentID]
				ctx.rows[node] += segment.RowNum
				if ctx.counts[node] == 0 {
					ctx.opened++
				}
				ctx.counts[node]++
			}
		}
	}
	assignments := make(map[int64]map[int64][]int64)
	for _, segment := range entries {
		node := ctx.assignments[segment.SegmentID]
		if assignments[node] == nil {
			assignments[node] = make(map[int64][]int64)
		}
		assignments[node][segment.PartitionID] = append(assignments[node][segment.PartitionID], segment.SegmentID)
	}
	dataVersion, _ := p.DataVersionForCollection(desired.CollectionID)
	builder := qviews.NewQueryViewAtCoordBuilder(shardID.ReplicaID, syntheticDataView(desired, dataVersion, shard), shardID.VChannel)
	builder.SetAssignments(assignments)
	builder.SetLoadInfoVersion(p.ConfigVersion(desired.CollectionID))
	return &allocationResult{builder: builder, assignments: ctx.assignments, rowsByNode: ctx.rows, more: more}
}

// currentSegmentNodes returns the selected intended placement, preferring a
// valid Preparing target over the serving Up view. The result is immutable.
func currentSegmentNodes(snap balanceInput, shardID qviews.ShardID) map[int64]int64 {
	if shard := snap.ShardEntry(shardID); shard != nil && shard.Target() != nil {
		return shard.Target().Assignments
	}
	return nil
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
// needs from the native shard snapshot. Only DataVersion, CollectionId, and
// the target shard are populated — the builder does not look at other shards.
// The native structure is decoupled from the viewpb wire format, so the shard
// is re-materialized as proto here at the builder boundary. TransformStartAfterTimetick
// stays 0: the DataView manager never sets it (matches the pre-split behavior).
func syntheticDataView(
	cfg *loadmgr.LoadConfig,
	dv qviews.DataVersion,
	shard *ShardDataView,
) *viewpb.DataViewOfCollection {
	return &viewpb.DataViewOfCollection{
		CollectionId: cfg.CollectionID,
		DataVersion:  dv.IntoProto(),
		Shards:       []*viewpb.DataViewOfShard{shardDataViewToProto(shard)},
	}
}

// shardDataViewToProto converts a native shard snapshot back to the viewpb
// wire form. Only the fields the QueryViewAtCoordBuilder consumes are
// populated (Vchannel plus partition membership).
func shardDataViewToProto(shard *ShardDataView) *viewpb.DataViewOfShard {
	if shard == nil {
		return nil
	}
	protoShard := &viewpb.DataViewOfShard{
		Vchannel:   shard.VChannel,
		Partitions: make([]*viewpb.DataViewOfPartition, 0, len(shard.Partitions)),
	}
	for _, partition := range shard.Partitions {
		protoPartition := &viewpb.DataViewOfPartition{
			PartitionId: partition.PartitionID,
			SegmentIds:  make([]int64, 0, len(partition.Segments)),
		}
		for _, segment := range partition.Segments {
			protoPartition.SegmentIds = append(protoPartition.SegmentIds, segment.SegmentID)
		}
		protoShard.Partitions = append(protoShard.Partitions, protoPartition)
	}
	return protoShard
}
