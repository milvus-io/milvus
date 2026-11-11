package qviews

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestPlacementBuilderPreservesSource(t *testing.T) {
	source := &viewpb.QueryViewOfShard{
		Meta:          &viewpb.QueryViewMeta{CollectionId: 1, ReplicaId: 2, Vchannel: "v1", Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 3, CompactVersion: 4}, QueryVersion: 5}, SchemaVersion: 6, LoadInfoVersion: 7, TransformStartAfterTimetick: 8, State: viewpb.QueryViewState_QueryViewStatePreparing},
		QueryNode:     []*viewpb.QueryViewOfQueryNode{{NodeId: 10, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 100, SegmentIds: []int64{101, 102}, SegmentManifestVersions: []int64{11, 12}}}}},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{PartitionStats: []*viewpb.ClusteredPartitionStats{{PartitionId: 100, Version: 13}, {PartitionId: 200, Version: 14}}},
	}
	original := proto.Clone(source).(*viewpb.QueryViewOfShard)
	builder := NewQueryViewAtCoordBuilderFromView(source)
	require.True(t, proto.Equal(source, builder.Build()))
	builder.Assignments()[20] = map[int64][]int64{100: {102}}
	builder.Assignments()[10][100] = []int64{101}
	moved := builder.Build()
	require.True(t, proto.Equal(original, source), "the live source must remain immutable")
	require.True(t, proto.Equal(original.Meta, moved.Meta))
	require.True(t, proto.Equal(original.StreamingNode, moved.StreamingNode))
	require.Equal(t, []int64{12}, moved.QueryNode[1].Partitions[0].SegmentManifestVersions)
	// A placement snapshot is detached from its source. Build itself retains
	// the existing ownership contract for assignments.
	source.QueryNode[0].Partitions[0].SegmentIds[0] = 999
	source.StreamingNode.PartitionStats[0].Version = 999
	require.Equal(t, int64(101), builder.Build().QueryNode[0].Partitions[0].SegmentIds[0])
	require.Equal(t, int64(13), builder.Build().StreamingNode.PartitionStats[0].Version)
}
