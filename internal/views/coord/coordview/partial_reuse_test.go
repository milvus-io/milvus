package coordview

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestPartialReadyResourcesSurvivePrepareFailure(t *testing.T) {
	patch := mockey.Mock((stubDataViewRef).Stats).To(func(_ stubDataViewRef, id int64) (qviews.SegmentStats, bool) {
		return qviews.SegmentStats{LogicalSize: id}, true
	}).Build()
	defer patch.UnPatch()
	view := buildTestViewWithVersion(2, 1, 1, 1)
	view.Meta.LoadInfoVersion = 7
	sm := NewCoordQueryViewStateMachineWithRef(view, stubDataViewRef{})
	sm.qnReadySegments[1] = []int64{1000}
	mgr := &ShardViewManager{views: map[qviews.QueryViewVersion]*CoordQueryViewStateMachine{sm.Version(): sm}}
	before := mgr.Stats()
	require.NotNil(t, before.PreparingPlacement)
	require.Equal(t, int64(1000), before.PreparingPlacement.PartitionSizes[10][1])
	require.Equal(t, int64(1001), before.PreparingPlacement.PartitionSizes[10][2])
	key := ResourceKey{SegmentID: 1000, PartitionID: 10, DataVersion: sm.Version().DataVersion, LoadInfoVersion: 7}
	require.Contains(t, before.Resources[1], key)
	require.Empty(t, before.Resources[2])
	sm.EnterUnrecoverable()
	failed := mgr.Stats()
	require.Nil(t, failed.PreparingPlacement)
	require.Contains(t, failed.Resources[1], key)
	require.Empty(t, failed.Resources[2])
	require.NotNil(t, before.PreparingPlacement, "previous publications remain immutable")
	sm.EnterDropping()
	require.Empty(t, mgr.Stats().Resources, "teardown without handover is not guaranteed reusable")
}

func TestViewPlacementPreservesPartitionFootprints(t *testing.T) {
	patch := mockey.Mock((stubDataViewRef).Stats).To(func(_ stubDataViewRef, id int64) (qviews.SegmentStats, bool) {
		return qviews.SegmentStats{LogicalSize: id}, id != 3000
	}).Build()
	defer patch.UnPatch()
	view := buildTestView(1)
	view.QueryNode[0].Partitions = append(view.QueryNode[0].Partitions,
		&viewpb.QueryViewOfPartition{PartitionId: 20, SegmentIds: []int64{2000}},
		&viewpb.QueryViewOfPartition{PartitionId: 30, SegmentIds: []int64{3000}})
	sm := NewCoordQueryViewStateMachineWithRef(view, stubDataViewRef{})
	p := viewPlacement(sm)
	require.Equal(t, map[int64]int64{1000: 1, 2000: 1, 3000: 1}, p.Assignments)
	require.Equal(t, int64(1000), p.PartitionSizes[10][1])
	require.Equal(t, int64(2000), p.PartitionSizes[20][1])
	require.Empty(t, p.PartitionSizes[30])
	require.Equal(t, []int64{3000}, p.LostStats)
}
