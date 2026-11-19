//go:build test && dynamic

package qnview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// lineageWaitHandle records the per-stream ticks a query waited for.
type lineageWaitHandle struct {
	mockSealedSegmentHandle
	waited  map[string]uint64
	waitErr error
}

func (h *lineageWaitHandle) WaitLineageTransformVisible(_ context.Context, ticks map[string]uint64) error {
	h.waited = ticks
	return h.waitErr
}

type lineageHandlerFixture struct {
	mgr  *mockSegmentManager
	h    *QNQueryViewHandler
	view qviews.QueryViewAtWorkNode
	key  qviews.QueryViewKey
}

func setupLineageHandlerTest(t *testing.T, lineage *viewpb.QueryViewLineage, handles ...SealedSegmentHandle) *lineageHandlerFixture {
	mgr := newMockSegmentManager()
	mgr.queryHandles = handles
	h := NewQNQueryViewHandler(mgr)
	meta := buildHandlerTestMeta(1)
	meta.Lineage = lineage
	view := qviews.NewQueryViewAtQueryNode(meta, buildHandlerTestQNView(1))
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
	preparation, ok := mgr.getAcquired(view.QueryViewKey())
	require.True(t, ok)
	preparation.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})
	return &lineageHandlerFixture{mgr: mgr, h: h, view: view, key: view.QueryViewKey()}
}

func (f *lineageHandlerFixture) search(t *testing.T, mvcc *viewpb.QueryPlanMVCC) error {
	tasks, err := f.h.AcquireSearchSegmentTasks(t.Context(), f.view.ShardID(), f.key.QueryViewVersion, mvcc, &internalpb.SearchRequest{CollectionID: testCollectionID}, nil)
	if err == nil {
		for _, task := range tasks.Tasks() {
			require.Same(t, mvcc, task.(*QNSearchSegmentTask).MVCC)
		}
		tasks.Release()
	}
	return err
}

func (f *lineageHandlerFixture) query(t *testing.T, mvcc *viewpb.QueryPlanMVCC) error {
	tasks, err := f.h.AcquireQuerySegmentTasks(t.Context(), f.view.ShardID(), f.key.QueryViewVersion, mvcc, &internalpb.RetrieveRequest{CollectionID: testCollectionID}, nil)
	if err == nil {
		for _, task := range tasks.Tasks() {
			require.Same(t, mvcc, task.(*QNQuerySegmentTask).MVCC)
		}
		tasks.Release()
	}
	return err
}

func (f *lineageHandlerFixture) statistics(t *testing.T, mvcc *viewpb.QueryPlanMVCC) error {
	_, err := f.h.Statistics(t.Context(), &viewpb.StatisticsOnViewRequest{
		ShardId: f.view.ShardID().IntoProto(), Version: f.key.QueryViewVersion.IntoProto(), Mvcc: mvcc,
	})
	return err
}

var (
	testLineage     = &viewpb.QueryViewLineage{Vchannels: []string{"t1", "t2"}, SwitchTimetick: 100}
	testLineageMVCC = &viewpb.QueryPlanMVCC{
		GrowingTimetick: 100, TransformingTimetick: 100, ReadTimetick: 135,
		LineageTransformingTimeticks: map[string]uint64{"t1": 110, "t2": 130},
	}
)

func TestQNHandlerLineageViewWaitsPerStream(t *testing.T) {
	mockey.PatchConvey("every request type waits the own stream to its tick and each lineage stream to its own", t, func() {
		mockey.Mock((*lineageWaitHandle).Segment).Return(&viewSchemaSegment{read: &segments.LocalSegment{}}).Build()
		mockey.Mock((*segments.LocalSegment).RowNum).Return(int64(3)).Build()
		for name, run := range map[string]func(*lineageHandlerFixture, *testing.T, *viewpb.QueryPlanMVCC) error{
			"search": (*lineageHandlerFixture).search, "query": (*lineageHandlerFixture).query, "statistics": (*lineageHandlerFixture).statistics,
		} {
			handle := &lineageWaitHandle{mockSealedSegmentHandle: mockSealedSegmentHandle{id: 1000, partitionID: 10}}
			f := setupLineageHandlerTest(t, testLineage, handle)
			require.NoError(t, run(f, t, testLineageMVCC), name)
			// The own stream ends at the switch tick; the read point is not a wait target.
			require.EqualValues(t, 100, f.mgr.waitTimetick, name)
			require.Equal(t, map[string]uint64{"t1": 110, "t2": 130}, handle.waited, name)
			require.Equal(t, 1, handle.releaseCount, name)
		}
	})
}

func TestQNHandlerLineageMismatchIsRefusedBeforeWaiting(t *testing.T) {
	plain := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}
	for name, tc := range map[string]struct {
		lineage *viewpb.QueryViewLineage
		mvcc    *viewpb.QueryPlanMVCC
	}{
		"lineage view, plan without read point": {testLineage, plain},
		"plain view, plan with read point":      {nil, testLineageMVCC},
		"lineage view, plan for other targets": {testLineage, &viewpb.QueryPlanMVCC{
			TransformingTimetick: 100, ReadTimetick: 135, LineageTransformingTimeticks: map[string]uint64{"t1": 110, "t3": 130},
		}},
	} {
		handle := &lineageWaitHandle{mockSealedSegmentHandle: mockSealedSegmentHandle{id: 1000, partitionID: 10}}
		f := setupLineageHandlerTest(t, tc.lineage, handle)
		for _, run := range []func(*lineageHandlerFixture, *testing.T, *viewpb.QueryPlanMVCC) error{
			(*lineageHandlerFixture).search, (*lineageHandlerFixture).query, (*lineageHandlerFixture).statistics,
		} {
			err := run(f, t, tc.mvcc)
			// Retryable for the client: it planned against another serving generation.
			require.True(t, viewerror.AsViewError(err).IsViewInvalidated(), name)
			require.True(t, viewerror.AsViewError(err).IsRetryable(), name)
		}
		require.False(t, f.mgr.waitCalled, name)
		require.Nil(t, handle.waited, name)
		require.Zero(t, handle.releaseCount, name)
	}
}

func TestQNHandlerViewWithoutLineageKeepsSingleWait(t *testing.T) {
	// A handle that cannot wait for lineage streams is all a plain view needs.
	handle := &mockSealedSegmentHandle{id: 1000, partitionID: 10}
	f := setupLineageHandlerTest(t, nil, handle)
	plain := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}
	require.NoError(t, f.search(t, plain))
	require.NoError(t, f.query(t, plain))
	require.EqualValues(t, 90, f.mgr.waitTimetick)
	require.Equal(t, 2, handle.releaseCount)
}

func TestQNHandlerLineageWaitFailureReleasesHandles(t *testing.T) {
	waitErr := errors.New("lineage stream failed")
	first := &lineageWaitHandle{mockSealedSegmentHandle: mockSealedSegmentHandle{id: 1000, partitionID: 10}}
	second := &lineageWaitHandle{mockSealedSegmentHandle: mockSealedSegmentHandle{id: 1001, partitionID: 10}, waitErr: waitErr}
	f := setupLineageHandlerTest(t, testLineage, first, second)
	require.ErrorIs(t, f.search(t, testLineageMVCC), waitErr)
	require.Equal(t, 1, first.releaseCount)
	require.Equal(t, 1, second.releaseCount)

	// A handle that cannot wait for lineage streams must not be read.
	blind := &mockSealedSegmentHandle{id: 1000, partitionID: 10}
	f = setupLineageHandlerTest(t, testLineage, blind)
	require.Error(t, f.query(t, testLineageMVCC))
	require.Equal(t, 1, blind.releaseCount)

	// Failures of the own-stream wait and of the acquisition are returned as before.
	f = setupLineageHandlerTest(t, testLineage, first)
	f.mgr.waitErr = waitErr
	require.ErrorIs(t, f.query(t, testLineageMVCC), waitErr)
	f.mgr.waitErr, f.mgr.queryErr = nil, waitErr
	require.ErrorIs(t, f.query(t, testLineageMVCC), waitErr)
}
