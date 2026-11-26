package coordview

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestAbsentCapacityFailuresAllocateNothing(t *testing.T) {
	for _, state := range []qviews.QueryViewState{qviews.QueryViewStatePreparing, qviews.QueryViewStateUnrecoverable} {
		view := buildTestView(1)
		sm := NewCoordQueryViewStateMachineWithRef(view, nil)
		sm.OnNodeStateReported(qnReport(view, 1, state, 1000))
		var snapshot map[int64]map[FailedResourceKey]ResourceFailure
		allocs := testing.AllocsPerRun(100, func() {
			snapshot = fillFailedResources(cloneFailedResources(nil), sm)
		})
		require.Nil(t, snapshot)
		require.Zero(t, allocs, "absent failure facts must not allocate per shard publication")
	}
}

func TestFailedResourcesKeepPartialSuccessAndServingCoverage(t *testing.T) {
	view := buildTestView(2)
	view.Meta.LoadInfoVersion = 7
	view.QueryNode[0].Partitions[0].SegmentIds = []int64{1001, 1003, 1004}
	view.QueryNode[1].Partitions[0].SegmentIds = []int64{2001}
	sm := NewCoordQueryViewStateMachineWithRef(view, nil)
	var failed map[int64]map[FailedResourceKey]ResourceFailure
	failed = fillFailedResources(failed, sm)
	require.Empty(t, failed)
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady, 2001))
	report := qnReport(view, 1, qviews.QueryViewStateUnrecoverable, 1001).(*qviews.QueryViewAtQueryNode)
	report.ViewOfQueryNode().FailedSegments = []*viewpb.SegmentPerparingFailure{{SegmentId: 1003, Reason: viewpb.SegmentPerparingFailure_MemoryLimitExceeded}}
	sm.OnNodeStateReported(report)
	failed = fillFailedResources(failed, sm)
	key := FailedResourceKey{PartitionID: 10, SegmentID: 1003, DataVersion: sm.Version().DataVersion, LoadInfoVersion: 7}
	require.Len(t, failed, 1)
	require.Len(t, failed[1], 1)
	require.Equal(t, sm.Version(), failed[1][key].Version)
	require.Equal(t, viewpb.SegmentPerparingFailure_MemoryLimitExceeded, failed[1][key].Reason)

	// An older Up on the same node must not erase new-load failure evidence.
	old := buildTestView(1)
	old.Meta.LoadInfoVersion = 6
	old.QueryNode[0].Partitions[0].SegmentIds = []int64{1001}
	oldSM := NewCoordQueryViewStateMachineWithRef(old, nil)
	oldSM.OnNodeStateReported(qnReport(old, 1, qviews.QueryViewStateReady))
	oldSM.OnNodeStateReported(snReport(old, qviews.QueryViewStateReady))
	oldSM.OnNodeStateReported(snReport(old, qviews.QueryViewStateUp))
	manager := &ShardViewManager{preparationFailures: failed, views: map[qviews.QueryViewVersion]*CoordQueryViewStateMachine{
		{QueryVersion: 1}: oldSM, {QueryVersion: 2}: sm,
	}}
	stats := manager.Stats()
	require.NotNil(t, stats.UpVersion)
	require.Equal(t, SegmentStateUp, stats.Segments[1001].Nodes[1])
	require.Equal(t, failed, stats.FailedResources)
	require.Contains(t, stats.Resources[1], ResourceKey{
		PartitionID: 10, SegmentID: 1001,
		DataVersion: sm.Version().DataVersion, LoadInfoVersion: 7,
	})
	// Teardown and administrative preemption do not report capacity failures.
	sm.EnterDropping()
	failed = nil
	failed = fillFailedResources(failed, sm)
	require.Empty(t, failed)
	sm = NewCoordQueryViewStateMachineWithRef(buildTestView(1), nil)
	sm.EnterUnrecoverable()
	failed = fillFailedResources(failed, sm)
	require.Empty(t, failed)
}

func simulateCapacityFailure(t *testing.T, s *mockSyncer, node qviews.QueryNode, version qviews.QueryViewVersion) {
	t.Helper()
	cb := s.findOnSyncResponse(node, version)
	require.NotNil(t, cb)
	cb(qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{
		CollectionId: testCollectionID, ReplicaId: testReplicaID, Vchannel: testVChannel,
		Version: version.IntoProto(), State: viewpb.QueryViewState_QueryViewStateUnrecoverable,
	}, &viewpb.QueryViewOfQueryNode{
		NodeId: node.ID,
		FailedSegments: []*viewpb.SegmentPerparingFailure{{
			SegmentId: 1000 + node.ID, Reason: viewpb.SegmentPerparingFailure_MemoryLimitExceeded,
		}},
	}))
}

func TestFailedResourcesRequireExplicitCapacityReason(t *testing.T) {
	for _, reason := range []viewpb.SegmentPerparingFailure_Reason{
		viewpb.SegmentPerparingFailure_Unknown,
		viewpb.SegmentPerparingFailure_Reason(99),
		viewpb.SegmentPerparingFailure_MemoryLimitExceeded,
		viewpb.SegmentPerparingFailure_DiskLimitExceeded,
		viewpb.SegmentPerparingFailure_ResourceReservationFailed,
	} {
		t.Run(reason.String(), func(t *testing.T) {
			view := buildTestView(1)
			view.Meta.LoadInfoVersion = 7
			sm := NewCoordQueryViewStateMachineWithRef(view, nil)
			report := qnReport(view, 1, qviews.QueryViewStateUnrecoverable).(*qviews.QueryViewAtQueryNode)
			failure := &viewpb.SegmentPerparingFailure{SegmentId: 1000, Reason: reason}
			report.ViewOfQueryNode().FailedSegments = []*viewpb.SegmentPerparingFailure{failure, {
				SegmentId: 9999, Reason: viewpb.SegmentPerparingFailure_MemoryLimitExceeded,
			}}
			sm.OnNodeStateReported(report)
			failure.SegmentId = 9999 // Accepted snapshots must not alias the report.
			var failed map[int64]map[FailedResourceKey]ResourceFailure
			failed = fillFailedResources(failed, sm)
			if reason == viewpb.SegmentPerparingFailure_Unknown || reason == 99 {
				require.Empty(t, failed)
			} else {
				require.Len(t, failed[1], 1)
				require.Contains(t, failed[1], FailedResourceKey{PartitionID: 10, SegmentID: 1000, DataVersion: sm.Version().DataVersion, LoadInfoVersion: 7})
			}
			// A generic/legacy Unrecoverable report does not imply that any
			// specific unready requirement failed on this node.
			sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateUnrecoverable))
			failed = nil
			failed = fillFailedResources(failed, sm)
			require.Empty(t, failed)
		})
	}
}

func TestFailureHistorySurvivesReplacement(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	mgr := newTestManager(t, catalog, syncer)
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1).SetLoadInfoVersion(7)))
	first := mgr.Stats().PreparingVersion
	require.NotNil(t, first)
	simulateCapacityFailure(t, syncer, testQN1, *first)
	snapshot := mgr.Stats()
	require.NotEmpty(t, snapshot.FailedResources[1])
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1).SetLoadInfoVersion(7)))
	second := mgr.Stats().PreparingVersion
	require.Equal(t, snapshot.FailedResources, mgr.Stats().FailedResources, "Dropping the previous view cannot forget rejection")
	simulateCapacityFailure(t, syncer, testQN1, *second)
	for _, version := range mgr.Stats().FailedResources[1] {
		require.Equal(t, *second, version.Version)
	}
	for _, version := range snapshot.FailedResources[1] {
		require.Equal(t, *first, version.Version, "published failure snapshots are immutable")
	}
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1).SetLoadInfoVersion(8)))
	require.Empty(t, mgr.Stats().FailedResources, "new loading identity starts fresh")
	third := mgr.Stats().PreparingVersion
	simulateCapacityFailure(t, syncer, testQN1, *third)
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 1).SetLoadInfoVersion(8)))
	fourth := mgr.Stats().PreparingVersion
	simulateNodeResponse(t, syncer, testQN1, *fourth, qviews.QueryViewStateReady)
	simulateNodeResponse(t, syncer, testSN, *fourth, qviews.QueryViewStateReady)
	simulateNodeResponse(t, syncer, testSN, *fourth, qviews.QueryViewStateUp)
	require.Empty(t, mgr.Stats().FailedResources, "successful replacement clears failure history")
}

func TestFailureHistoryIncludesLateNodeReport(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	mgr := newTestManager(t, catalog, syncer)
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 2).SetLoadInfoVersion(7)))
	version := *mgr.Stats().PreparingVersion
	simulateCapacityFailure(t, syncer, testQN1, version)
	simulateCapacityFailure(t, syncer, qviews.QueryNode{ID: 2}, version)
	require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(1, 1, 2).SetLoadInfoVersion(7)))
	require.NotEmpty(t, mgr.Stats().FailedResources[1])
	require.NotEmpty(t, mgr.Stats().FailedResources[2], "late failure survives retirement of the failed view")
}
