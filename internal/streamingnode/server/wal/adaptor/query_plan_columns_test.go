package adaptor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestCollectPlanColumnsUsesWorkerAndSubquerySelection(t *testing.T) {
	view := &viewpb.QueryViewOfShard{
		StreamingNode: &viewpb.QueryViewOfStreamingNode{SealedFieldBytes: map[int64]*viewpb.FieldLogicalBytes{
			1: {PayloadBytes: map[int64]uint64{100: 9, 101: 12}},
			2: {PayloadBytes: map[int64]uint64{100: 14, 101: 20}},
			3: {PayloadBytes: map[int64]uint64{100: 81}},
		}},
		QueryNode: []*viewpb.QueryViewOfQueryNode{
			{NodeId: 10, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 9, SegmentIds: []int64{1, 2}}, {PartitionId: 8, SegmentIds: []int64{3}}}},
			{NodeId: 20, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 9, SegmentIds: []int64{4}}}},
		},
	}
	plan := &viewpb.QueryPlan{WorkNodes: []*viewpb.QueryPlanWorkNode{
		{
			Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 10}},
			SubSelections: []*viewpb.SegmentSelection{
				{Mode: viewpb.SegmentSelection_Exclude, SegmentIds: []int64{2}},
				{Mode: viewpb.SegmentSelection_Include, SegmentIds: []int64{2}},
			},
		},
	}}
	for sub, expected := range []map[int64]uint64{{100: 9, 101: 12}, {100: 14, 101: 20}} {
		read := newPlannedColumnRead([]int64{100, 101, 100}, 3)
		require.NoError(t, collectSealedPlanColumns(read, plan, view, []int64{9}, sub))
		require.Equal(t, expected, read.GetColumns().GetPayloadBytes())
		require.Equal(t, []int64{100, 101, 100}, read.GetFieldIds())
	}
	read := newPlannedColumnRead([]int64{100, 101}, 1)
	require.NoError(t, collectSealedPlanColumns(read, plan, view, nil, -1))
	require.Equal(t, map[int64]uint64{100: 104}, read.GetColumns().GetPayloadBytes())
}
