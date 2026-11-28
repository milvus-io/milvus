package balancer

import (
	"maps"
	"math"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type allocationResult struct {
	collectionID int64
	shard        *ShardDataView
	entries      []*SegmentDataView
	assignments  map[int64]int64
	rowsByNode   map[int64]int64
}

// allocate produces a complete candidate for shardID against the supplied
// steady-state base rows. It returns nil if the shard cannot be
// allocated (missing DataView, missing replica, or any segment has no
// eligible node).
//
// Segments are processed by LogicalSize descending and SegmentID ascending. A
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
	collection := p.collectionForShard(shardID)
	entries := shard.SegmentsBySize
	// Published-schema recovery uses every segment of the retained view.
	retained := p.recovery != nil && p.recovery.shardID == shardID
	if !retained && !collection.LoadsAllPartitions() {
		var loaded []*SegmentDataView
		for _, segment := range entries {
			if collection.LoadsPartition(segment.PartitionID) {
				loaded = append(loaded, segment)
			}
		}
		entries = loaded
	}
	replica := collection.Planning().Replica(shardID.ReplicaID)
	previous := replica.Fanout(shardID.VChannel)
	ctx := newAllocationContext(p, shardID, baseRows, entries, previous)
	if ctx.fanout != previous {
		replica.StoreFanout(shardID.VChannel, ctx.fanout)
	}
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
	if optional {
		before := ctx.score()
		baseline := maps.Clone(ctx.assignments)
		ctx.optimize(entries)
		after := ctx.score()
		if after.global > before.global+scoreEpsilon || before.energy(ctx.config)-after.energy(ctx.config)-ctx.migrationCost(baseline) <= ctx.config.MinScoreGain {
			ctx.assignments = baseline
			clear(ctx.rows)
			clear(ctx.counts)
			ctx.opened = 0
			for _, segment := range entries {
				node := baseline[segment.SegmentID]
				ctx.rows[node] += segment.LogicalSize
				if ctx.counts[node] == 0 {
					ctx.opened++
				}
				ctx.counts[node]++
			}
		}
	}
	return &allocationResult{collectionID: desired.CollectionID, shard: shard, entries: entries, assignments: ctx.assignments, rowsByNode: ctx.rows}
}

// materialize is called only for submitted candidates. The sorted slice
// shares immutable segment metadata; no native DataView protobuf is needed.
func (r *allocationResult) materialize(p *planningContext, shardID qviews.ShardID) *qviews.QueryViewAtCoordBuilder {
	assignments := make(map[int64]map[int64][]int64)
	manifests := make(map[int64]int64, len(r.entries))
	for _, segment := range r.entries {
		node := r.assignments[segment.SegmentID]
		if assignments[node] == nil {
			assignments[node] = make(map[int64][]int64)
		}
		assignments[node][segment.PartitionID] = append(assignments[node][segment.PartitionID], segment.SegmentID)
		manifests[segment.SegmentID] = segment.ManifestVersion
	}
	stats := make(map[int64]int64, len(r.shard.Partitions))
	for _, partition := range r.shard.Partitions {
		stats[partition.PartitionID] = partition.PartitionStatsVersion
	}
	dataVersion, _ := p.DataVersionForCollection(r.collectionID)
	return qviews.NewQueryViewAtCoordBuilderFromMetadata(shardID, qviews.QueryViewBuilderMetadata{
		CollectionID: r.collectionID, DataVersion: dataVersion,
		RequiredStreamingVersion:    r.shard.RequiredStreamingVersion,
		SchemaVersion:               p.SchemaVersion(r.collectionID),
		TransformStartAfterTimetick: r.shard.TransformStartAfterTimetick(),
		SegmentManifestVersions:     manifests, PartitionStatsVersions: stats,
	}).SetAssignments(assignments).SetLoadInfoVersion(p.ConfigVersion(r.collectionID)).
		SetPreparationPriority(p.preparationPriority(shardID))
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
// is re-materialized as proto here at the builder boundary, retaining the
// runtime transform frontier and the exact segment materialization metadata.
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
// wire form, including the coverage and materialization metadata consumed by
// QueryViewAtCoordBuilder.
func shardDataViewToProto(shard *ShardDataView) *viewpb.DataViewOfShard {
	if shard == nil {
		return nil
	}
	protoShard := &viewpb.DataViewOfShard{
		Vchannel:                             shard.VChannel,
		RequiredStreamingVersion:             shard.RequiredStreamingVersion,
		GrowingTransformStartAfterTimetick:   shard.GrowingTransformStartAfterTimetick,
		CompactedTransformStartAfterTimetick: shard.CompactedTransformStartAfterTimetick,
		Partitions:                           make([]*viewpb.DataViewOfPartition, 0, len(shard.Partitions)),
	}
	for _, partition := range shard.Partitions {
		protoPartition := &viewpb.DataViewOfPartition{
			PartitionStatsVersion: partition.PartitionStatsVersion,
			PartitionId:           partition.PartitionID,
			SegmentIds:            make([]int64, 0, len(partition.Segments)),
		}
		for _, segment := range partition.Segments {
			protoPartition.SegmentIds = append(protoPartition.SegmentIds, segment.SegmentID)
			protoPartition.SegmentManifestVersions = append(protoPartition.SegmentManifestVersions, segment.ManifestVersion)
			protoPartition.SegmentTransformStartAfterTimeticks = append(protoPartition.SegmentTransformStartAfterTimeticks, segment.TransformStartAfterTimetick)
		}
		protoShard.Partitions = append(protoShard.Partitions, protoPartition)
	}
	return protoShard
}
