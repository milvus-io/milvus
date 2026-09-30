package coordview

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestPartialReadyResourcesSurvivePrepareFailure(t *testing.T) {
	patch := mockey.Mock((stubDataViewRef).Stats).To(func(_ stubDataViewRef, id int64) (qviews.SegmentStats, bool) {
		return qviews.SegmentStats{RowNum: id}, true
	}).Build()
	defer patch.UnPatch()
	view := buildTestViewWithVersion(2, 1, 1, 1)
	view.Meta.LoadInfoVersion = 7
	sm := NewCoordQueryViewStateMachineWithRef(view, stubDataViewRef{})
	sm.qnReadySegments[1] = []int64{1000}
	mgr := &ShardViewManager{views: map[qviews.QueryViewVersion]*CoordQueryViewStateMachine{sm.Version(): sm}}
	before := mgr.Stats()
	require.NotNil(t, before.PreparingPlacement)
	require.Equal(t, int64(1000), before.PreparingPlacement.Rows[1])
	require.Equal(t, int64(1001), before.PreparingPlacement.Rows[2])
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
