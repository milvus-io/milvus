package qnview

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLocalPoisonBoundaryPinsInstance(t *testing.T) {
	key := qviews.NewQueryViewAtQueryNode(buildTestMeta(), buildTestQNView()).QueryViewKey()
	state := &transformSegmentState{state: transformSegmentLoaded, generation: 7, segment: &fakeTransformSegment{id: 1000, partitionID: 10}, refs: map[qviews.QueryViewKey]struct{}{key: {}}}
	healthy := &transformSegmentState{state: transformSegmentLoaded, generation: 8, segment: &fakeTransformSegment{id: 2000, partitionID: 20}, refs: map[qviews.QueryViewKey]struct{}{key: {}}}
	m := &QueryViewSegmentReadinessManager{segments: map[int64]*transformSegmentState{1000: state, 2000: healthy}, views: map[qviews.QueryViewKey]*transformViewRef{key: {physicalReady: map[int64]bool{1000: true, 2000: true}}}}
	s := &observedTransformSegment{TransformSegment: state.segment, manager: m, state: state}
	s.OnTransformFailed(100, merr.WrapErrServiceUnavailableMsg("delete failed"))
	require.EqualValues(t, 100, state.poison.failedTimeTick)
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
	require.EqualValues(t, 100, state.poison.failedTimeTick)
	replacement := &transformSegmentState{generation: 9}
	m.segments[1000] = replacement
	s.OnTransformFailed(102, merr.WrapErrServiceUnavailableMsg("late failure"))
	require.Nil(t, replacement.poison)
}

func TestCatchupPoisonFailsPreparationWithoutReadiness(t *testing.T) {
	key := qviews.NewQueryViewAtQueryNode(buildTestMeta(), buildTestQNView()).QueryViewKey()
	failed := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiter := transformSegmentWaiter{key: key, onUnrecoverable: func() { failed <- struct{}{} }}
	state := &transformSegmentState{state: transformSegmentCatchingUp, generation: 1, catchupCancel: cancel, segment: &fakeTransformSegment{id: 1000}, refs: map[qviews.QueryViewKey]struct{}{key: {}}, waiters: map[qviews.QueryViewKey]transformSegmentWaiter{key: waiter}}
	m := &QueryViewSegmentReadinessManager{segments: map[int64]*transformSegmentState{1000: state}, views: map[qviews.QueryViewKey]*transformViewRef{key: {onUnrecoverable: waiter.onUnrecoverable}}}
	m.poisonSegment(1000, state, 100)
	require.Empty(t, m.markSegmentReady(segmentCatchupTask{ctx: ctx, segment: state.segment, state: state}))
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("preparation must report its ordinary failure")
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.NotNil(t, state.segment, "Poison retains the instance until normal teardown")
}
