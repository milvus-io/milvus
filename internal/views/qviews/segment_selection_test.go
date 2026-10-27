package qviews

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSegmentSelectionPreservesPhysicalBindings(t *testing.T) {
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 2, SegmentIds: []int64{10, 11, 12}, SegmentManifestVersions: []int64{4, 5, 6}, ReadySegmentIds: []int64{10, 11, 12}}}}
	selection := SelectSegments([]int64{10, 11, 12}, map[int64]struct{}{11: {}})
	require.Equal(t, viewpb.SegmentSelection_Exclude, selection.GetMode())
	filtered := FilterSelectedSegments(view, selection)
	require.Equal(t, []int64{10, 12}, filtered.Partitions[0].SegmentIds)
	require.Equal(t, []int64{4, 6}, filtered.Partitions[0].SegmentManifestVersions)
	require.Equal(t, []int64{10, 12}, filtered.Partitions[0].ReadySegmentIds)
	require.Len(t, view.Partitions[0].SegmentIds, 3)
	require.Empty(t, FilterSelectedSegments(view, &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_None}).Partitions)
	require.Same(t, view, FilterSelectedSegments(view, nil))
}

func TestSegmentSelectionModes(t *testing.T) {
	candidates := []int64{10, 11, 12, 13}
	for _, tc := range []struct {
		name     string
		excluded map[int64]struct{}
		mode     viewpb.SegmentSelection_Mode
		ids      []int64
		selected []int64
	}{
		{name: "all", mode: viewpb.SegmentSelection_All, selected: candidates},
		{name: "irrelevant_exclusion", excluded: map[int64]struct{}{99: {}}, mode: viewpb.SegmentSelection_All, selected: candidates},
		{name: "none", excluded: map[int64]struct{}{10: {}, 11: {}, 12: {}, 13: {}}, mode: viewpb.SegmentSelection_None},
		{name: "include", excluded: map[int64]struct{}{10: {}, 11: {}, 12: {}}, mode: viewpb.SegmentSelection_Include, ids: []int64{13}, selected: []int64{13}},
		{name: "exclude", excluded: map[int64]struct{}{11: {}}, mode: viewpb.SegmentSelection_Exclude, ids: []int64{11}, selected: []int64{10, 12, 13}},
		{name: "tie", excluded: map[int64]struct{}{10: {}, 12: {}}, mode: viewpb.SegmentSelection_Include, ids: []int64{11, 13}, selected: []int64{11, 13}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selection := SelectSegments(candidates, tc.excluded)
			require.Equal(t, tc.mode, selection.GetMode())
			require.Equal(t, tc.ids, selection.GetSegmentIds())
			encoded, err := proto.Marshal(selection)
			require.NoError(t, err)
			decoded := &viewpb.SegmentSelection{}
			require.NoError(t, proto.Unmarshal(encoded, decoded))
			accepts := SegmentSelector(decoded)
			var actual []int64
			for _, id := range candidates {
				if accepts(id) {
					actual = append(actual, id)
				}
			}
			require.Equal(t, tc.selected, actual)
			view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 2, SegmentIds: candidates}}}
			filtered := FilterSelectedSegments(view, decoded)
			var filteredIDs []int64
			for _, part := range filtered.Partitions {
				filteredIDs = append(filteredIDs, part.SegmentIds...)
			}
			require.Equal(t, tc.selected, filteredIDs)
			if tc.mode == viewpb.SegmentSelection_All {
				require.Same(t, view, filtered)
			}
		})
	}
	require.Equal(t, viewpb.SegmentSelection_None, SelectSegments(nil, nil).GetMode())
	require.False(t, SegmentSelector(SelectSegments(nil, nil))(10))
	require.True(t, SegmentSelector(nil)(10), "an omitted selection is All")
}

func TestSegmentSelectionWirePreservesHybridSlots(t *testing.T) {
	selections := []*viewpb.SegmentSelection{
		{Mode: viewpb.SegmentSelection_All},
		{Mode: viewpb.SegmentSelection_Include, SegmentIds: []int64{10}},
		{Mode: viewpb.SegmentSelection_Exclude, SegmentIds: []int64{11}},
		{Mode: viewpb.SegmentSelection_None},
	}
	worker := &viewpb.QueryPlanWorkNode{SubSelections: selections}
	data, err := proto.Marshal(worker)
	require.NoError(t, err)
	decoded := &viewpb.QueryPlanWorkNode{}
	require.NoError(t, proto.Unmarshal(data, decoded))
	require.Len(t, decoded.SubSelections, 4)
	for i, expected := range selections {
		require.True(t, proto.Equal(expected, decoded.SubSelections[i]), "slot %d", i)
	}
	// All is a zero-byte body but retains its two-byte repeated-message envelope.
	// Previously explicit hybrid All used exclude=true, adding two body bytes.
	require.Zero(t, proto.Size(selections[0]))
	require.Equal(t, 2, proto.Size(&viewpb.QueryPlanWorkNode{SubSelections: selections[:1]}))
	require.Equal(t, 2, proto.Size(selections[3]))
	require.Empty(t, selections[0].SegmentIds)
	require.Empty(t, selections[3].SegmentIds)
}
