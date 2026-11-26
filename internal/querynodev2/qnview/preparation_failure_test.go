package qnview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQNPreparationFailureReportRetainsExactCause(t *testing.T) {
	for _, lateReady := range []bool{false, true} {
		t.Run(map[bool]string{false: "already-ready", true: "delayed-ready"}[lateReady], func(t *testing.T) {
			sm := NewQNQueryViewStateMachine(buildTestMeta(), buildTestQNView())
			if !lateReady {
				sm.OnSegmentsReady(map[int64][]int64{10: {1001}})
			}
			failure := &viewpb.SegmentPerparingFailure{SegmentId: 1000, Reason: viewpb.SegmentPerparingFailure_DiskLimitExceeded}
			sm.OnUnrecoverable(failure)
			failure.SegmentId = 9999
			if lateReady {
				sm.OnSegmentsReady(map[int64][]int64{10: {1001}})
			}
			first := sm.ConsumeReport()
			require.Equal(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, first.Meta.State)
			require.Len(t, first.QueryNode[0].FailedSegments, 1)
			require.EqualValues(t, 1000, first.QueryNode[0].FailedSegments[0].SegmentId)
			require.Equal(t, viewpb.SegmentPerparingFailure_DiskLimitExceeded, first.QueryNode[0].FailedSegments[0].Reason)
			// Reconnect repeats the original failure, independent of callers or
			// consumers mutating their own protobuf copies.
			first.QueryNode[0].FailedSegments[0].SegmentId = 9998
			sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
			repeated := sm.ConsumeReport()
			require.EqualValues(t, 1000, repeated.QueryNode[0].FailedSegments[0].SegmentId)
			require.Equal(t, !lateReady, len(repeated.QueryNode[0].Partitions[0].ReadySegmentIds) > 0)
		})
	}
}

func TestCapacityFailureReportExcludesCatchupSibling(t *testing.T) {
	mockey.PatchConvey("released admission is not readiness and does not blame a catching-up sibling", t, func() {
		f := setupResourceTest(t, nil)
		f.manager.estimator = &resourceTestEstimator{}
		catchup := newFakeTransformRegistration()
		f.registerMock.UnPatch()
		mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).Return(catchup, nil).Build()
		var released atomic.Bool
		mockey.Mock((*resourceTestReservation).Release).To(func(*resourceTestReservation) { released.Store(true) }).Build()
		mockey.Mock((*resourceTestEstimator).Reserve).To(func(_ *resourceTestEstimator, _ context.Context, info *querypb.SegmentLoadInfo, _ CollectionRuntime) (ResourceReservation, bool, error) {
			if info.SegmentID == 1001 {
				return &resourceTestReservation{}, false, nil
			}
			return nil, !released.Load(), merr.ErrServiceMemoryLimitExceeded
		}).Build()
		h := NewQNQueryViewHandler(f.manager)
		req := f.request(1, 1, 1)
		req.View.Partitions[0].SegmentIds = []int64{1000, 1001}
		req.View.Partitions[0].SegmentManifestVersions = []int64{1, 1}
		view := qviews.NewQueryViewAtQueryNode(req.Meta, req.View)
		reports := make(chan *viewpb.QueryViewOfShard, 8)
		h.ApplyViews([]handler.ApplyView{{View: view, OnReport: func(report qviews.QueryViewAtWorkNode) {
			reports <- proto.Clone(report.IntoProto()).(*viewpb.QueryViewOfShard)
		}}})
		defer func() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_ = h.Close(ctx)
			require.NoError(t, h.Close(context.Background()))
		}()
		select {
		case report := <-reports:
			require.True(t, released.Load())
			require.Equal(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, report.Meta.State)
			require.Empty(t, report.QueryNode[0].Partitions[0].ReadySegmentIds)
			require.Len(t, report.QueryNode[0].FailedSegments, 1)
			require.EqualValues(t, 1000, report.QueryNode[0].FailedSegments[0].SegmentId)
			require.Equal(t, viewpb.SegmentPerparingFailure_MemoryLimitExceeded, report.QueryNode[0].FailedSegments[0].Reason)
			require.EqualValues(t, 1, f.loads.Load(), "only the unfailed sibling physically loaded")
		case <-time.After(15 * time.Second):
			t.Fatal("capacity failure did not reach the QN report")
		}
	})
}
