package qviews

import (
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// QueryViewAtCoordBuilder builds a complete QueryViewOfShard from a DataViewOfCollection.
type QueryViewAtCoordBuilder struct {
	collectionID                int64
	replicaID                   int64
	vchannel                    string
	dataVersion                 DataVersion
	queryVersion                int64
	transformStartAfterTimetick uint64
	loadInfoVersion             uint64
	assignments                 map[int64]map[int64][]int64
}

func NewQueryViewAtCoordBuilder(replicaID int64, dataView *viewpb.DataViewOfCollection, vchannel string) *QueryViewAtCoordBuilder {
	var shardView *viewpb.DataViewOfShard
	for _, shard := range dataView.Shards {
		if shard.Vchannel == vchannel {
			shardView = shard
			break
		}
	}
	if shardView == nil {
		panic("vchannel " + vchannel + " not found in DataViewOfCollection")
	}
	return &QueryViewAtCoordBuilder{
		collectionID:                dataView.CollectionId,
		replicaID:                   replicaID,
		vchannel:                    vchannel,
		dataVersion:                 FromProtoDataVersion(dataView.DataVersion),
		transformStartAfterTimetick: shardView.TransformStartAfterTimetick,
		queryVersion:                1,
		assignments:                 make(map[int64]map[int64][]int64),
	}
}

func (b *QueryViewAtCoordBuilder) DataVersion() DataVersion {
	return b.dataVersion
}

func (b *QueryViewAtCoordBuilder) SetQueryVersion(queryVersion int64) *QueryViewAtCoordBuilder {
	b.queryVersion = queryVersion
	return b
}

func (b *QueryViewAtCoordBuilder) SetLoadInfoVersion(version uint64) *QueryViewAtCoordBuilder {
	b.loadInfoVersion = version
	return b
}

func (b *QueryViewAtCoordBuilder) SetAssignments(assignments map[int64]map[int64][]int64) *QueryViewAtCoordBuilder {
	b.assignments = assignments
	return b
}

func (b *QueryViewAtCoordBuilder) Build() *viewpb.QueryViewOfShard {
	meta := &viewpb.QueryViewMeta{
		CollectionId: b.collectionID,
		ReplicaId:    b.replicaID,
		Vchannel:     b.vchannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  b.dataVersion.IntoProto(),
			QueryVersion: b.queryVersion,
		},
		State:                       viewpb.QueryViewState_QueryViewStatePreparing,
		TransformStartAfterTimetick: b.transformStartAfterTimetick,
		LoadInfoVersion:             b.loadInfoVersion,
	}

	nodeIDs := make([]int64, 0, len(b.assignments))
	for nodeID := range b.assignments {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })

	queryNodes := make([]*viewpb.QueryViewOfQueryNode, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		partitionsByID := b.assignments[nodeID]
		partitionIDs := make([]int64, 0, len(partitionsByID))
		for partitionID := range partitionsByID {
			partitionIDs = append(partitionIDs, partitionID)
		}
		sort.Slice(partitionIDs, func(i, j int) bool { return partitionIDs[i] < partitionIDs[j] })

		partitions := make([]*viewpb.QueryViewOfPartition, 0, len(partitionIDs))
		for _, partitionID := range partitionIDs {
			partitions = append(partitions, &viewpb.QueryViewOfPartition{
				PartitionId: partitionID,
				SegmentIds:  partitionsByID[partitionID],
			})
		}
		queryNodes = append(queryNodes, &viewpb.QueryViewOfQueryNode{NodeId: nodeID, Partitions: partitions})
	}

	return &viewpb.QueryViewOfShard{
		Meta:          meta,
		QueryNode:     queryNodes,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
}
