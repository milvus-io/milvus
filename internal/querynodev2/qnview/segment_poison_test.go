package qnview

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPoisonBoundaryPinsInstanceAndReports(t *testing.T) {
	key := qviews.NewQueryViewAtQueryNode(buildTestMeta(), buildTestQNView()).QueryViewKey()
	reports := make(chan *viewpb.PoisonedSegment, 2)
	state := &transformSegmentState{state: transformSegmentLoaded, generation: 7, segment: &fakeTransformSegment{id: 1000, partitionID: 10}, refs: map[qviews.QueryViewKey]struct{}{key: {}}}
	healthy := &transformSegmentState{state: transformSegmentLoaded, generation: 8, segment: &fakeTransformSegment{id: 2000, partitionID: 20}, refs: map[qviews.QueryViewKey]struct{}{key: {}}}
	m := &QueryViewSegmentReadinessManager{segments: map[int64]*transformSegmentState{1000: state, 2000: healthy}, views: map[qviews.QueryViewKey]*transformViewRef{key: {onPoisoned: func(p *viewpb.PoisonedSegment) { reports <- p }}}}
	s := &observedTransformSegment{TransformSegment: state.segment, manager: m, state: state}
	s.OnTransformFailed(100, merr.WrapErrServiceUnavailableMsg("delete failed"))
	select {
	case p := <-reports:
		require.EqualValues(t, 100, p.FailedTimetick)
		require.EqualValues(t, 7, p.Generation)
	case <-time.After(time.Second):
		t.Fatal("missing report")
	}
	for _, ts := range []uint64{99, 100, 101} {
		view := &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
		handles, err := m.AcquireSealedSegmentHandles(context.Background(), key, view)
		require.NoError(t, err)
		err = checkTransformReadable(handles, ts)
		if ts < 100 {
			require.NoError(t, err)
			handles[0].Release()
		} else {
			require.ErrorContains(t, err, "poisoned")
		}
		require.Zero(t, state.queryRefs)
	}
	// Partition pruning excludes the poisoned instance.
	handles, err := m.AcquireSealedSegmentHandles(context.Background(), key, &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 20, SegmentIds: []int64{2000}}}})
	require.NoError(t, err)
	require.NoError(t, checkTransformReadable(handles, 101))
	handles[0].Release()
	s.OnTransformFailed(101, merr.WrapErrServiceUnavailableMsg("second failure"))
	require.EqualValues(t, 100, state.poison.FailedTimetick)
	replacement := &transformSegmentState{generation: 9}
	m.segments[1000] = replacement
	s.OnTransformFailed(102, merr.WrapErrServiceUnavailableMsg("late failure"))
	require.Nil(t, replacement.poison)
	select {
	case <-reports:
		t.Fatal("duplicate or stale report")
	default:
	}
}

func TestPoisonReportRetainsReadyAndSurvivesReconnect(t *testing.T) {
	sm := newReadySM()
	poison := &viewpb.PoisonedSegment{SegmentId: 1000, Generation: 7, FailedTimetick: 100}
	sm.OnSegmentPoisoned(poison)
	require.Equal(t, qviews.QueryViewStateReady, sm.State())
	report := sm.ConsumeReport()
	require.Len(t, report.QueryNode[0].PoisonedSegments, 1)
	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	require.EqualValues(t, 100, sm.ConsumeReport().QueryNode[0].PoisonedSegments[0].FailedTimetick)
	sm.OnSegmentPoisoned(poison)
	require.Nil(t, sm.ConsumeReport())
	preparing := newTestSM()
	preparing.OnSegmentPoisoned(poison)
	require.Equal(t, qviews.QueryViewStateUnrecoverable, preparing.State())
}

func TestCatchupPoisonFailsPreparationWithoutReadiness(t *testing.T) {
	key := qviews.NewQueryViewAtQueryNode(buildTestMeta(), buildTestQNView()).QueryViewKey()
	failed := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiter := transformSegmentWaiter{key: key, onUnrecoverable: func() { failed <- struct{}{} }}
	state := &transformSegmentState{state: transformSegmentCatchingUp, generation: 1, catchupCancel: cancel, segment: &fakeTransformSegment{id: 1000}, refs: map[qviews.QueryViewKey]struct{}{key: {}}, waiters: map[qviews.QueryViewKey]transformSegmentWaiter{key: waiter}}
	m := &QueryViewSegmentReadinessManager{segments: map[int64]*transformSegmentState{1000: state}, views: map[qviews.QueryViewKey]*transformViewRef{key: {}}}
	m.poisonSegment(1000, state, 100, merr.WrapErrServiceUnavailableMsg("catchup failed"))
	require.Empty(t, m.markSegmentReady(segmentCatchupTask{ctx: ctx, segment: state.segment, state: state}))
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("preparation must fail even without a report callback")
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.NotNil(t, state.segment, "Poison retains the instance until normal teardown")
}

func TestShardPoisonReportIgnoresRetiredViewEntry(t *testing.T) {
	sm := newReadySM()
	view := qviews.NewQueryViewAtQueryNode(sm.Meta(), sm.QNView())
	key := view.QueryViewKey()
	reports := 0
	entry := &qnViewEntry{sm: sm, ApplyView: handler.ApplyView{View: view, OnReport: func(v qviews.QueryViewAtWorkNode) {
		reports++
		require.Equal(t, qviews.QueryViewStateReady, v.State())
		require.Len(t, v.IntoProto().QueryNode[0].PoisonedSegments, 1)
	}}}
	shard := &qnShardView{views: map[qviews.QueryViewVersion]*qnViewEntry{key.QueryViewVersion: entry}}
	poison := &viewpb.PoisonedSegment{SegmentId: 1000, Generation: 7, FailedTimetick: 100}
	shard.notifySegmentPoisoned(key.QueryViewVersion, &qnViewEntry{}, poison)
	require.Zero(t, reports)
	shard.notifySegmentPoisoned(key.QueryViewVersion, entry, poison)
	require.Equal(t, 1, reports)
	dropped := newDroppedSM()
	dropped.ConsumeReport()
	dropped.OnSegmentPoisoned(poison)
	require.Nil(t, dropped.ConsumeReport())
}
