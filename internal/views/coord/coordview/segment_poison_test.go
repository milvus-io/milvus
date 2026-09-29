package coordview

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestPoisonReportTriggersRecovery(t *testing.T) {
	for _, state := range []qviews.QueryViewState{qviews.QueryViewStatePreparing, qviews.QueryViewStateReady, qviews.QueryViewStateUp} {
		t.Run(state.String(), func(t *testing.T) {
			view := buildTestView(1)
			sm := NewCoordQueryViewStateMachine(view)
			sm.state = state
			sm.ConsumeFlush()
			report := qnReport(view, 1, qviews.QueryViewStateReady, 1000).IntoProto()
			report.QueryNode[0].PoisonedSegments = []*viewpb.PoisonedSegment{{SegmentId: 1000, Generation: 7, FailedTimetick: 100}}
			sm.OnNodeStateReported(qviews.NewQueryViewAtWorkNodeFromProto(report))
			require.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
			flush := sm.ConsumeFlush()
			require.Equal(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, flush.Persist.Meta.State)
			require.Empty(t, flush.Sync, "must not promote an invalid view")
			require.Empty(t, sm.QNReadySegments()[1], "poison must not appear reusable in placement stats")
			sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady, 1000))
			require.Empty(t, sm.QNReadySegments()[1], "stale Ready must not clear sticky Poison")
		})
	}
}
