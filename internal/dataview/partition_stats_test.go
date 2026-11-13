package dataview

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestPartitionStatsSnapshotInheritance(t *testing.T) {
	view := &viewpb.DataViewOfCollection{}
	require.NoError(t, addSegments(view, []LoadableSegment{{SegmentID: 10, VChannel: "p_1v0", PartitionID: 2, PartitionStatsVersion: 7}}))
	previous := proto.Clone(view).(*viewpb.DataViewOfCollection)
	require.NoError(t, addSegments(view, []LoadableSegment{{SegmentID: 11, VChannel: "p_1v0", PartitionID: 2}}))
	require.Equal(t, int64(7), view.Shards[0].Partitions[0].PartitionStatsVersion)
	require.NoError(t, rebuildSegments(view, []LoadableSegment{{SegmentID: 20, VChannel: "p_1v0", PartitionID: 2, PartitionStatsVersion: 8}}))
	require.Equal(t, int64(8), view.Shards[0].Partitions[0].PartitionStatsVersion)
	require.Equal(t, int64(7), previous.Shards[0].Partitions[0].PartitionStatsVersion)
	// A stats-only update must not compare equal to the previous snapshot.
	latest := proto.Clone(view).(*viewpb.DataViewOfCollection)
	require.NoError(t, addSegments(view, []LoadableSegment{{SegmentID: 20, VChannel: "p_1v0", PartitionID: 2, PartitionStatsVersion: 9}}))
	require.False(t, proto.Equal(latest, view))
}
