package qviews

import (
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// QueryViewAtCoordBuilder builds a complete QueryViewOfShard from a DataViewOfCollection.
// The builder takes segment-to-node assignments and produces the proto that Coord
// persists and pushes to all nodes via SyncQueryView.
type QueryViewAtCoordBuilder struct {
	collectionID                  int64
	replicaID                     int64
	vchannel                      string
	dataVersion                   DataVersion
	queryVersion                  int64
	deleteApplyStartAfterTimetick uint64
	settings                      *viewpb.QueryViewSettings
	// nodeID -> partitionID -> []segmentID
	assignments map[int64]map[int64][]int64
}

// NewQueryViewAtCoordBuilder creates a new builder from a DataViewOfCollection and vchannel.
// It extracts collection ID, data version, and the target shard from the DataView.
// The caller then assigns segments to query nodes and builds the final QueryViewOfShard.
func NewQueryViewAtCoordBuilder(
	replicaID int64,
	dataView *viewpb.DataViewOfCollection,
	vchannel string,
) *QueryViewAtCoordBuilder {
	var shardView *viewpb.DataViewOfShard
	for _, s := range dataView.Shards {
		if s.Vchannel == vchannel {
			shardView = s
			break
		}
	}
	if shardView == nil {
		panic("vchannel " + vchannel + " not found in DataViewOfCollection")
	}
	return &QueryViewAtCoordBuilder{
		collectionID:                  dataView.CollectionId,
		replicaID:                     replicaID,
		vchannel:                      vchannel,
		dataVersion:                   FromProtoDataVersion(dataView.DataVersion),
		deleteApplyStartAfterTimetick: shardView.DeleteApplyStartAfterTimetick,
		queryVersion:                  1,
		assignments:                   make(map[int64]map[int64][]int64),
	}
}

// DataVersion returns the data version for this builder.
func (b *QueryViewAtCoordBuilder) DataVersion() DataVersion {
	return b.dataVersion
}

// SetQueryVersion sets the query version for load-level redistribution.
func (b *QueryViewAtCoordBuilder) SetQueryVersion(queryVersion int64) *QueryViewAtCoordBuilder {
	b.queryVersion = queryVersion
	return b
}

// SetSettings sets the load configuration for the query view.
func (b *QueryViewAtCoordBuilder) SetSettings(settings *viewpb.QueryViewSettings) *QueryViewAtCoordBuilder {
	b.settings = settings
	return b
}

// SetAssignments sets the segment-to-node assignments.
// The map is keyed by nodeID → partitionID → []segmentID.
func (b *QueryViewAtCoordBuilder) SetAssignments(assignments map[int64]map[int64][]int64) *QueryViewAtCoordBuilder {
	b.assignments = assignments
	return b
}

// Build constructs the final QueryViewOfShard proto in Preparing state.
func (b *QueryViewAtCoordBuilder) Build() *viewpb.QueryViewOfShard {
	meta := &viewpb.QueryViewMeta{
		CollectionId: b.collectionID,
		ReplicaId:    b.replicaID,
		Vchannel:     b.vchannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  b.dataVersion.IntoProto(),
			QueryVersion: b.queryVersion,
		},
		State:                         viewpb.QueryViewState_QueryViewStatePreparing,
		Settings:                      b.settings,
		DeleteApplyStartAfterTimetick: b.deleteApplyStartAfterTimetick,
	}

	// Build sorted query node list for deterministic output.
	nodeIDs := make([]int64, 0, len(b.assignments))
	for nodeID := range b.assignments {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })

	queryNodes := make([]*viewpb.QueryViewOfQueryNode, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		partMap := b.assignments[nodeID]

		// Build sorted partition list for deterministic output.
		partIDs := make([]int64, 0, len(partMap))
		for partID := range partMap {
			partIDs = append(partIDs, partID)
		}
		sort.Slice(partIDs, func(i, j int) bool { return partIDs[i] < partIDs[j] })

		partitions := make([]*viewpb.QueryViewOfPartition, 0, len(partIDs))
		for _, partID := range partIDs {
			partitions = append(partitions, &viewpb.QueryViewOfPartition{
				PartitionId: partID,
				SegmentIds:  partMap[partID],
			})
		}

		queryNodes = append(queryNodes, &viewpb.QueryViewOfQueryNode{
			NodeId:     nodeID,
			Partitions: partitions,
		})
	}

	return &viewpb.QueryViewOfShard{
		Meta:          meta,
		QueryNode:     queryNodes,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
}
