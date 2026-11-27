package snview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type lineageMockRuntime struct {
	mockQueryRuntime
	ticks      map[string]uint64
	lineageErr error
}

func (r *lineageMockRuntime) WaitLineageVisible(_ context.Context, ticks map[string]uint64) error {
	r.ticks = ticks
	return r.lineageErr
}

func TestSNWaitQueryMVCCVisibleIncludesLineage(t *testing.T) {
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 100, ReadTimetick: 130, LineageTransformingTimeticks: map[string]uint64{"target1": 110, "target2": 120}}
	r := &lineageMockRuntime{}
	require.NoError(t, WaitQueryMVCCVisible(context.Background(), r, mvcc))
	require.Equal(t, uint64(100), r.growingTimetick)
	require.Equal(t, uint64(100), r.transformingTimetick)
	require.Equal(t, mvcc.GetLineageTransformingTimeticks(), r.ticks)
	require.Error(t, WaitQueryMVCCVisible(context.Background(), &mockQueryRuntime{}, mvcc))
	require.NoError(t, WaitQueryMVCCVisible(context.Background(), &mockQueryRuntime{}, &viewpb.QueryPlanMVCC{GrowingTimetick: 100}))
}

func TestSNRejectsLineagePlanMismatchBeforeRuntimeWait(t *testing.T) {
	for _, lineage := range []*viewpb.QueryViewLineage{nil, {Vchannels: []string{"target"}, SwitchTimetick: 100}} {
		mgr := newMockResourceManager()
		mgr.runtime = &mockQueryRuntime{}
		h := recoverSNQueryViewHandler(context.Background(), testPChannel, newMockCatalog(), mgr, nil)
		meta := buildHandlerTestMeta(1)
		meta.Lineage = lineage
		view := qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
		h.ApplyViews([]handler.ApplyView{{View: view}})
		resource, ok := mgr.getAcquired(view.QueryViewKey())
		require.True(t, ok)
		resource.OnReady()
		meta.State = viewpb.QueryViewState_QueryViewStateUp
		h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})}})
		mvcc := &viewpb.QueryPlanMVCC{}
		if lineage == nil {
			mvcc = &viewpb.QueryPlanMVCC{ReadTimetick: 120, LineageTransformingTimeticks: map[string]uint64{"target": 110}}
		}
		_, err := h.AcquireSearchSegmentTasks(context.Background(), view.ShardID(), view.QueryViewKey().QueryViewVersion, mvcc, &internalpb.SearchRequest{}, nil)
		require.ErrorContains(t, err, "lineage")
		_, err = h.AcquireQuerySegmentTasks(context.Background(), view.ShardID(), view.QueryViewKey().QueryViewVersion, mvcc, &internalpb.RetrieveRequest{}, nil)
		require.ErrorContains(t, err, "lineage")
		_, err = h.Statistics(context.Background(), &viewpb.StatisticsOnViewRequest{ShardId: view.ShardID().IntoProto(), Version: view.QueryViewKey().QueryViewVersion.IntoProto(), Mvcc: mvcc})
		require.ErrorContains(t, err, "lineage")
	}
}
