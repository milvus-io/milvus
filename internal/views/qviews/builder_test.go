//go:build test && dynamic

package qviews

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryViewAtCoordBuilder(t *testing.T) {
	shardView := &viewpb.DataViewOfShard{
		Vchannel: "v1",
		Partitions: []*viewpb.DataViewOfPartition{
			{PartitionId: 1, SegmentIds: []int64{100, 101, 102}},
			{PartitionId: 2, SegmentIds: []int64{200, 201}},
		},
		DeleteApplyStartAfterTimetick: 12345,
	}
	dataView := &viewpb.DataViewOfCollection{
		CollectionId: 10,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 1},
		Shards:       []*viewpb.DataViewOfShard{shardView},
	}

	settings := &viewpb.QueryViewSettings{
		RequiredPartitions: []int64{1, 2},
	}

	assignments := map[int64]map[int64][]int64{
		1001: {1: {100, 101}, 2: {201}},
		1002: {1: {102}, 2: {200}},
	}

	result := NewQueryViewAtCoordBuilder(1, dataView, "v1").
		SetQueryVersion(3).
		SetSettings(settings).
		SetAssignments(assignments).
		Build()

	assert.Equal(t, DataVersion{StreamingVersion: 2, CompactVersion: 1},
		NewQueryViewAtCoordBuilder(1, dataView, "v1").DataVersion())

	// Verify meta.
	assert.Equal(t, int64(10), result.Meta.CollectionId)
	assert.Equal(t, int64(1), result.Meta.ReplicaId)
	assert.Equal(t, "v1", result.Meta.Vchannel)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, result.Meta.State)
	assert.Equal(t, int64(2), result.Meta.Version.DataVersion.StreamingVersion)
	assert.Equal(t, int64(1), result.Meta.Version.DataVersion.CompactVersion)
	assert.Equal(t, int64(3), result.Meta.Version.QueryVersion)
	assert.Equal(t, settings, result.Meta.Settings)
	assert.Equal(t, uint64(12345), result.Meta.DeleteApplyStartAfterTimetick)

	// Verify query nodes are sorted by node ID.
	assert.Len(t, result.QueryNode, 2)
	assert.Equal(t, int64(1001), result.QueryNode[0].NodeId)
	assert.Equal(t, int64(1002), result.QueryNode[1].NodeId)

	// Verify node 1001: partition 1 → [100, 101], partition 2 → [201].
	node1001 := result.QueryNode[0]
	assert.Len(t, node1001.Partitions, 2)
	assert.Equal(t, int64(1), node1001.Partitions[0].PartitionId)
	assert.Equal(t, []int64{100, 101}, node1001.Partitions[0].SegmentIds)
	assert.Equal(t, int64(2), node1001.Partitions[1].PartitionId)
	assert.Equal(t, []int64{201}, node1001.Partitions[1].SegmentIds)

	// Verify node 1002: partition 1 → [102], partition 2 → [200].
	node1002 := result.QueryNode[1]
	assert.Len(t, node1002.Partitions, 2)
	assert.Equal(t, int64(1), node1002.Partitions[0].PartitionId)
	assert.Equal(t, []int64{102}, node1002.Partitions[0].SegmentIds)
	assert.Equal(t, int64(2), node1002.Partitions[1].PartitionId)
	assert.Equal(t, []int64{200}, node1002.Partitions[1].SegmentIds)

	// Verify streaming node is present.
	assert.NotNil(t, result.StreamingNode)

	// Verify SegmentIDs on the per-node view.
	qv1001 := NewQueryViewAtWorkNodeFromProto(&viewpb.QueryViewOfShard{
		Meta:      result.Meta,
		QueryNode: []*viewpb.QueryViewOfQueryNode{node1001},
	})
	assert.ElementsMatch(t, []int64{100, 101, 201}, qv1001.(*QueryViewAtQueryNode).SegmentIDs())
}

func TestQueryViewAtCoordBuilder_Empty(t *testing.T) {
	dataView := &viewpb.DataViewOfCollection{
		CollectionId: 10,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		Shards:       []*viewpb.DataViewOfShard{{Vchannel: "v1"}},
	}

	result := NewQueryViewAtCoordBuilder(1, dataView, "v1").Build()

	assert.Equal(t, int64(10), result.Meta.CollectionId)
	assert.Equal(t, "v1", result.Meta.Vchannel)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, result.Meta.State)
	assert.Len(t, result.QueryNode, 0)
	assert.NotNil(t, result.StreamingNode)
}

func TestQueryViewAtCoordBuilder_MissingVChannelPanics(t *testing.T) {
	dataView := &viewpb.DataViewOfCollection{
		CollectionId: 10,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		Shards:       []*viewpb.DataViewOfShard{{Vchannel: "v1"}},
	}

	require.Panics(t, func() {
		NewQueryViewAtCoordBuilder(1, dataView, "v2")
	})
}
