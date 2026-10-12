package qnview

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

const (
	testCollectionID int64 = 100
	testReplicaID    int64 = 1
	testVChannel           = "v0_c0"
)

func buildTestMeta() *viewpb.QueryViewMeta {
	return &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
			QueryVersion: 1,
		},
		State: viewpb.QueryViewState_QueryViewStatePreparing,
	}
}

// buildTestQNView creates a QN view with two partitions:
//
//	partition 10: segments [1000, 1001, 1002]
//	partition 20: segments [2000, 2001]
func buildTestQNView() *viewpb.QueryViewOfQueryNode {
	return &viewpb.QueryViewOfQueryNode{
		NodeId: 1,
		Partitions: []*viewpb.QueryViewOfPartition{
			{PartitionId: 10, SegmentIds: []int64{1000, 1001, 1002}},
			{PartitionId: 20, SegmentIds: []int64{2000, 2001}},
		},
	}
}

// allSegments returns a map covering all segments in buildTestQNView.
func allSegments() map[int64][]int64 {
	return map[int64][]int64{
		10: {1000, 1001, 1002},
		20: {2000, 2001},
	}
}

func newTestSM() *QNQueryViewStateMachine {
	return NewQNQueryViewStateMachine(buildTestMeta(), buildTestQNView())
}

// newReadySM returns a SM in Ready state with all pending drained.
func newReadySM() *QNQueryViewStateMachine {
	sm := newTestSM()
	sm.OnSegmentsReady(allSegments())
	sm.ConsumeReport()
	return sm
}

// newUnrecoverableSM returns a SM in Unrecoverable state with all pending drained.
func newUnrecoverableSM() *QNQueryViewStateMachine {
	sm := newTestSM()
	sm.OnUnrecoverable()
	sm.ConsumeReport()
	return sm
}

// newDroppingSM returns a SM in Dropping state (from Ready) with report drained.
func newDroppingSM() *QNQueryViewStateMachine {
	sm := newReadySM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	sm.ConsumeRelease()
	return sm
}

// newDroppedSM returns a SM in Dropped state (from Dropping) with all pending drained.
func newDroppedSM() *QNQueryViewStateMachine {
	sm := newDroppingSM()
	sm.OnDropped()
	sm.ConsumeReport()
	return sm
}

func assertReportState(t *testing.T, sm *QNQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	v := sm.ConsumeReport()
	require.NotNil(t, v, "expected pending report with state %s", expected)

	// Verify state.
	assert.Equal(t, viewpb.QueryViewState(expected), v.Meta.State)

	// Verify meta fields are correctly carried.
	assert.Equal(t, sm.Meta().CollectionId, v.Meta.CollectionId)
	assert.Equal(t, sm.Meta().ReplicaId, v.Meta.ReplicaId)
	assert.Equal(t, sm.Meta().Vchannel, v.Meta.Vchannel)
	assert.Equal(t, sm.Meta().Version.QueryVersion, v.Meta.Version.QueryVersion)
	assert.Equal(t, sm.Meta().Version.DataVersion.StreamingVersion, v.Meta.Version.DataVersion.StreamingVersion)
	assert.Equal(t, sm.Meta().Version.DataVersion.CompactVersion, v.Meta.Version.DataVersion.CompactVersion)

	// Verify report structure: QN report has QueryNode, no StreamingNode.
	require.Len(t, v.QueryNode, 1)
	assert.Nil(t, v.StreamingNode)

	// Verify report meta is a clone (mutation doesn't affect SM).
	v.Meta.CollectionId = -1
	assert.NotEqual(t, int64(-1), sm.Meta().CollectionId)
}

func assertNoReport(t *testing.T, sm *QNQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumeReport(), "expected no pending report")
}

// getReadySegments extracts ReadySegmentIds from the report for a given partition.
func getReadySegments(report *viewpb.QueryViewOfShard, partitionID int64) []int64 {
	for _, qn := range report.QueryNode {
		for _, p := range qn.Partitions {
			if p.PartitionId == partitionID {
				return p.ReadySegmentIds
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// 1. Construction
// ---------------------------------------------------------------------------

func TestNew_InitialState(t *testing.T) {
	sm := newTestSM()

	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	// No pending report on construction; local events drive progress.
	assertNoReport(t, sm)
}

func TestNew_MetaAndViewPreserved(t *testing.T) {
	meta := buildTestMeta()
	qnView := buildTestQNView()
	sm := NewQNQueryViewStateMachine(meta, qnView)
	assert.Equal(t, meta, sm.Meta())
	assert.Equal(t, qnView, sm.QNView())
}

func TestNew_ReportStructure(t *testing.T) {
	sm := newTestSM()
	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})

	report := sm.ConsumeReport()
	require.NotNil(t, report)
	assert.NotNil(t, report.Meta)
	assert.Len(t, report.QueryNode, 1)
	assert.Nil(t, report.StreamingNode)
}

func TestNew_ReportMetaIsClone(t *testing.T) {
	sm := newTestSM()
	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})

	report := sm.ConsumeReport()
	require.NotNil(t, report)
	report.Meta.CollectionId = 999
	assert.Equal(t, testCollectionID, sm.Meta().CollectionId)
}

// ---------------------------------------------------------------------------
// 2. Normal flow: Preparing → Ready → Dropped
// ---------------------------------------------------------------------------

func TestNormalFlow_AllSegmentsAtOnce(t *testing.T) {
	sm := newTestSM()

	sm.OnSegmentsReady(allSegments())
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)
	assertNoReport(t, sm)
}

func TestNormalFlow_ReadyToDropping(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assert.True(t, sm.ConsumeRelease())
}

func TestNormalFlow_DroppingToDropped(t *testing.T) {
	sm := newDroppingSM()

	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestNormalFlow_FullLifecycle(t *testing.T) {
	sm := newTestSM()

	// Incremental loading.
	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertReportState(t, sm, qviews.QueryViewStatePreparing)

	sm.OnSegmentsReady(map[int64][]int64{10: {1001, 1002}, 20: {2000, 2001}})
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)

	// Coord pushes Dropped → Dropping.
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assert.True(t, sm.ConsumeRelease())

	// Release completes → Dropped.
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)

	assertNoReport(t, sm)
}

// ---------------------------------------------------------------------------
// 3. Error path: Preparing → Unrecoverable → Dropped
// ---------------------------------------------------------------------------

func TestErrorPath_PreparingToUnrecoverable(t *testing.T) {
	sm := newTestSM()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoReport(t, sm)
}

func TestErrorPath_UnrecoverableToDropping(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assert.True(t, sm.ConsumeRelease())

	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestErrorPath_PartialProgressThenUnrecoverable(t *testing.T) {
	sm := newTestSM()

	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	sm.ConsumeReport()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

// ---------------------------------------------------------------------------
// 4. OnSegmentsReady — incremental loading & deduplication
// ---------------------------------------------------------------------------

func TestSegments_IncrementalProgress(t *testing.T) {
	sm := newTestSM()

	// Batch 1: partial.
	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	report := sm.ConsumeReport()
	require.NotNil(t, report)
	assert.Len(t, getReadySegments(report, 10), 1)
	assert.Empty(t, getReadySegments(report, 20))

	// Batch 2: more segments.
	sm.OnSegmentsReady(map[int64][]int64{10: {1001}, 20: {2000}})
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	report = sm.ConsumeReport()
	require.NotNil(t, report)
	assert.Len(t, getReadySegments(report, 10), 2)
	assert.Len(t, getReadySegments(report, 20), 1)

	// Batch 3: completes all.
	sm.OnSegmentsReady(map[int64][]int64{10: {1002}, 20: {2001}})
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
}

func TestSegments_PendingReportIsEventSnapshot(t *testing.T) {
	sm := newTestSM()
	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})

	// Mutate the tracked progress without producing another state-machine event.
	// ConsumeReport must return the snapshot built by OnSegmentsReady, not rebuild
	// it from the later in-memory state.
	sm.readySegments[10][1001] = struct{}{}
	sm.readyCount++

	report := sm.ConsumeReport()
	require.NotNil(t, report)
	assert.Equal(t, []int64{1000}, getReadySegments(report, 10))
}

func TestSegments_DuplicateIdempotent(t *testing.T) {
	sm := newTestSM()

	// Report same segment twice.
	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	sm.ConsumeReport()

	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	report := sm.ConsumeReport()
	require.NotNil(t, report)
	// Still only 1 ready segment — not double-counted.
	assert.Len(t, getReadySegments(report, 10), 1)
}

func TestSegments_DuplicateInSameBatch(t *testing.T) {
	sm := newTestSM()

	// Same segment ID twice in one call.
	sm.OnSegmentsReady(map[int64][]int64{10: {1000, 1000, 1000}})
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	report := sm.ConsumeReport()
	require.NotNil(t, report)
	assert.Len(t, getReadySegments(report, 10), 1)
}

func TestSegments_UnknownPartitionIgnored(t *testing.T) {
	sm := newTestSM()

	// Partition 99 is not in qnView — should be silently ignored.
	sm.OnSegmentsReady(map[int64][]int64{99: {9000}})
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	// Still generates a progress report.
	report := sm.ConsumeReport()
	require.NotNil(t, report)
}

func TestSegments_EmptyBatch(t *testing.T) {
	sm := newTestSM()

	sm.OnSegmentsReady(map[int64][]int64{})
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	// Generates a progress report even for empty batch.
	report := sm.ConsumeReport()
	require.NotNil(t, report)
}

func TestSegments_IgnoredInReady(t *testing.T) {
	sm := newReadySM()

	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoReport(t, sm)
}

func TestSegments_IgnoredInUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoReport(t, sm)
}

func TestSegments_IgnoredInDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

func TestSegments_ReadyReportCarriesAllSegments(t *testing.T) {
	sm := newTestSM()

	sm.OnSegmentsReady(allSegments())
	report := sm.ConsumeReport()
	require.NotNil(t, report)

	assert.Equal(t, viewpb.QueryViewState_QueryViewStateReady, report.Meta.State)
	assert.ElementsMatch(t, []int64{1000, 1001, 1002}, getReadySegments(report, 10))
	assert.ElementsMatch(t, []int64{2000, 2001}, getReadySegments(report, 20))
}

func TestSegments_ZeroSegmentViewReadyImmediately(t *testing.T) {
	meta := buildTestMeta()
	qnView := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{},
	}
	sm := NewQNQueryViewStateMachine(meta, qnView)

	// totalSegments == 0 → first OnSegmentsReady with empty batch triggers Ready.
	sm.OnSegmentsReady(map[int64][]int64{})
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)
}

// ---------------------------------------------------------------------------
// 5. OnUnrecoverable — idempotency
// ---------------------------------------------------------------------------

func TestUnrecoverable_IgnoredInReady(t *testing.T) {
	sm := newReadySM()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoReport(t, sm)
}

func TestUnrecoverable_IgnoredInDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

func TestUnrecoverable_IgnoredInUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoReport(t, sm)
}

// ---------------------------------------------------------------------------
// 6. Coord re-push Preparing — distributed state recoverability
//
// Coord pushes Preparing when it doesn't know the node's current state
// (e.g., after Coord crash recovery or message loss).
// If QN has advanced past Preparing, it must re-report its current state
// so Coord can fast-forward (doc 1.1).
// If QN is still Preparing, no re-report is needed (local events drive it).
// ---------------------------------------------------------------------------

func TestCoordPreparing_StillPreparing_NoReport(t *testing.T) {
	sm := newTestSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoReport(t, sm)
}

func TestCoordPreparing_StillPreparing_MultipleRePush_NoReport(t *testing.T) {
	sm := newTestSM()

	for range 3 {
		sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
		assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
		assertNoReport(t, sm)
	}
}

func TestCoordPreparing_Ready_ReReportsReady(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)
}

func TestCoordPreparing_Unrecoverable_ReReportsUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

func TestCoordPreparing_Dropping_ReReportsDropping(t *testing.T) {
	sm := newDroppingSM()

	// Coord re-pushes Preparing while SM is in Dropping.
	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	// SM has advanced past Preparing → re-report current state.
	assertReportState(t, sm, qviews.QueryViewStateDropping)
}

func TestCoordPreparing_Dropped_ReReportsDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

// ---------------------------------------------------------------------------
// 7. Coord Dropped — transition from any state & re-push in Dropped
//
// Coord in Dropping pushes Dropped to all nodes (doc 1.6).
// QN must accept Dropped from any state.
// If already Dropped and Coord re-pushes (report was lost), re-report.
// ---------------------------------------------------------------------------

func TestCoordDropped_FromPreparing(t *testing.T) {
	sm := newTestSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assert.True(t, sm.ConsumeRelease())
}

func TestCoordDropped_FromReady(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assert.True(t, sm.ConsumeRelease())
}

func TestCoordDropped_FromUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assert.True(t, sm.ConsumeRelease())
}

func TestCoordDropped_RePushInDropping_Ignored(t *testing.T) {
	sm := newDroppingSM()

	// Already in Dropping, re-push Dropped → no state change, no extra release.
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assert.False(t, sm.ConsumeRelease()) // no double Release
}

func TestCoordDropped_RePushInDropped_ReReportsDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestCoordDropped_RePushMultiple(t *testing.T) {
	sm := newDroppedSM()

	for range 3 {
		sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
		assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
		assertReportState(t, sm, qviews.QueryViewStateDropped)
	}
}

// ---------------------------------------------------------------------------
// 8. Unrecognized Coord pushes — no handler, no side effect
//
// QN only handles Preparing and Dropped. Up/Down/etc. are SN-only
// and QN has no handler for them.
// ---------------------------------------------------------------------------

func TestUnrecognizedPush_UpIgnoredInPreparing(t *testing.T) {
	sm := newTestSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoReport(t, sm)
}

func TestUnrecognizedPush_DownIgnoredInPreparing(t *testing.T) {
	sm := newTestSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoReport(t, sm)
}

func TestUnrecognizedPush_UpIgnoredInReady(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoReport(t, sm)
}

func TestUnrecognizedPush_DownIgnoredInDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

// ---------------------------------------------------------------------------
// 9. Dropped terminal — local events silently ignored
// ---------------------------------------------------------------------------

func TestDroppedTerminal_IgnoresOnSegmentsReady(t *testing.T) {
	sm := newDroppedSM()

	sm.OnSegmentsReady(allSegments())
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

func TestDroppedTerminal_IgnoresOnUnrecoverable(t *testing.T) {
	sm := newDroppedSM()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

// ---------------------------------------------------------------------------
// 10. Consume idempotency — double consume returns nil
// ---------------------------------------------------------------------------

func TestConsume_DoubleConsumeReturnsNil(t *testing.T) {
	sm := newTestSM()
	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})

	v := sm.ConsumeReport()
	require.NotNil(t, v)
	assertNoReport(t, sm)
}

func TestConsume_NoEventNoReport(t *testing.T) {
	sm := newTestSM()

	assertNoReport(t, sm)
}

// ---------------------------------------------------------------------------
// 11. Distributed recoverability — Coord crash + re-push scenarios
//
// Simulates Coord crash-recovery: Coord re-pushes Preparing to all nodes.
// QN must re-report its current state so Coord can reconstruct progress.
// ---------------------------------------------------------------------------

func TestRecoverability_ReadyAfterCoordCrash(t *testing.T) {
	sm := newTestSM()

	// QN loads all segments.
	sm.OnSegmentsReady(allSegments())
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	// Coord consumed the Ready report, then crashes before persisting.
	sm.ConsumeReport()

	// Coord recovers from ETCD (still Preparing), re-pushes Preparing.
	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	// QN re-reports Ready so Coord can fast-forward.
	assertReportState(t, sm, qviews.QueryViewStateReady)
}

func TestRecoverability_UnrecoverableAfterCoordCrash(t *testing.T) {
	sm := newTestSM()

	sm.OnUnrecoverable()
	sm.ConsumeReport()

	// Coord re-pushes Preparing after crash.
	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

func TestRecoverability_DroppedAfterCoordCrash(t *testing.T) {
	sm := newDroppedSM()

	// Coord re-pushes Dropped (Dropping not persisted, re-executes flow).
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestRecoverability_RepeatedRePushAlwaysProducesReport(t *testing.T) {
	sm := newReadySM()

	// Simulate multiple Coord re-pushes (e.g., retries due to network issues).
	for range 5 {
		sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
		assertReportState(t, sm, qviews.QueryViewStateReady)
	}
}

func TestRecoverability_ReadyReportCarriesSegmentProgress(t *testing.T) {
	sm := newTestSM()

	sm.OnSegmentsReady(allSegments())
	sm.ConsumeReport()

	// Coord re-pushes after crash.
	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	report := sm.ConsumeReport()
	require.NotNil(t, report)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateReady, report.Meta.State)
	// All segments should still be reflected in the re-report.
	assert.ElementsMatch(t, []int64{1000, 1001, 1002}, getReadySegments(report, 10))
	assert.ElementsMatch(t, []int64{2000, 2001}, getReadySegments(report, 20))
}

// ---------------------------------------------------------------------------
// 12. Event ordering edge cases
// ---------------------------------------------------------------------------

func TestOrdering_CoordDroppedDuringSegmentLoading(t *testing.T) {
	sm := newTestSM()

	// Partial loading in progress.
	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	sm.ConsumeReport()

	// Coord aborts view → Dropping.
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assert.True(t, sm.ConsumeRelease())

	// Further segment loading ignored in Dropping.
	sm.OnSegmentsReady(map[int64][]int64{10: {1001, 1002}, 20: {2000, 2001}})
	assertNoReport(t, sm)

	// Release completes → Dropped.
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestOrdering_UnrecoverableBeforeAnySegments(t *testing.T) {
	sm := newTestSM()

	// OOM before any segments loaded.
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())

	// Subsequent segment notifications ignored.
	sm.ConsumeReport()
	sm.OnSegmentsReady(allSegments())
	assertNoReport(t, sm)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
}

func TestOrdering_SegmentsReadyThenUnrecoverable(t *testing.T) {
	sm := newTestSM()

	// Some segments loaded, then fatal error.
	sm.OnSegmentsReady(map[int64][]int64{10: {1000, 1001}})
	sm.ConsumeReport()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

// ---------------------------------------------------------------------------
// 13. Pending report overwrite — latest event wins
// ---------------------------------------------------------------------------

func TestPendingOverwrite_SegmentsThenDropped(t *testing.T) {
	sm := newTestSM()

	// OnSegmentsReady sets a Preparing report, then Dropped transitions to Dropping.
	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)

	// Dropping clears the Preparing report (no report in Dropping).
	assertNoReport(t, sm)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assert.True(t, sm.ConsumeRelease())

	// Release completes → Dropped report.
	sm.OnDropped()
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoReport(t, sm)
}

func TestPendingOverwrite_SegmentsThenUnrecoverable(t *testing.T) {
	sm := newTestSM()

	sm.OnSegmentsReady(map[int64][]int64{10: {1000}})
	sm.OnUnrecoverable()

	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoReport(t, sm)
}
