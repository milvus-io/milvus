package qviews

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSegmentSelectionPreservesPhysicalBindings(t *testing.T) {
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 2, SegmentIds: []int64{10, 11, 12}, SegmentManifestVersions: []int64{4, 5, 6}, ReadySegmentIds: []int64{10, 11, 12}}}}
	selection := SelectSegments([]int64{10, 11, 12}, map[int64]struct{}{11: {}})
	require.True(t, selection.Exclude)
	filtered := FilterSelectedSegments(view, selection)
	require.Equal(t, []int64{10, 12}, filtered.Partitions[0].SegmentIds)
	require.Equal(t, []int64{4, 6}, filtered.Partitions[0].SegmentManifestVersions)
	require.Equal(t, []int64{10, 12}, filtered.Partitions[0].ReadySegmentIds)
	require.Len(t, view.Partitions[0].SegmentIds, 3)
	require.Empty(t, FilterSelectedSegments(view, &viewpb.SegmentSelection{}).Partitions)
	require.Same(t, view, FilterSelectedSegments(view, nil))
}
