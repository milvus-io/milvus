package snview

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

func buildTestSNView() *viewpb.QueryViewOfStreamingNode {
	return &viewpb.QueryViewOfStreamingNode{}
}

func newTestSM() *SNQueryViewStateMachine {
	return NewSNQueryViewStateMachine(buildTestMeta(), buildTestSNView())
}

// newReadySM returns a SM in Ready state with all pending drained.
func newReadySM() *SNQueryViewStateMachine {
	sm := newTestSM()
	sm.ConsumeReport() // drain Preparing report
	sm.OnReady()
	sm.ConsumeReport()
	return sm
}

// newUpSM returns a SM in Up state with all pending drained.
func newUpSM() *SNQueryViewStateMachine {
	sm := newReadySM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	sm.ConsumeReport()
	sm.ConsumePersist()
	return sm
}

// newDownSM returns a SM in Down state with all pending drained.
func newDownSM() *SNQueryViewStateMachine {
	sm := newUpSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	sm.ConsumeReport()
	sm.ConsumePersist()
	return sm
}

// newUnrecoverableSM returns a SM in Unrecoverable state (from Preparing) with all pending drained.
func newUnrecoverableSM() *SNQueryViewStateMachine {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnUnrecoverable()
	sm.ConsumeReport()
	return sm
}

// newDroppingFromPreparingSM returns a SM in Dropping state (from Preparing) with all pending drained.
func newDroppingFromPreparingSM() *SNQueryViewStateMachine {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	sm.ConsumeRelease()
	return sm
}

// newDroppedSM returns a SM in Dropped state with all pending drained.
func newDroppedSM() *SNQueryViewStateMachine {
	sm := newDroppingFromPreparingSM()
	sm.OnDropped()
	sm.ConsumeReport()
	return sm
}

// newRecoveringSM returns a SM in UpRecovering state with all pending drained.
func newRecoveringSM() *SNQueryViewStateMachine {
	return RecoverSNQueryViewStateMachine(buildTestMeta(), buildTestSNView())
}

func assertReportState(t *testing.T, sm *SNQueryViewStateMachine, expected qviews.QueryViewState) {
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

	// Verify report structure: SN report has StreamingNode, no QueryNode.
	assert.NotNil(t, v.StreamingNode)
	assert.Nil(t, v.QueryNode)

	// Verify report meta is a clone (mutation doesn't affect SM).
	v.Meta.CollectionId = -1
	assert.NotEqual(t, int64(-1), sm.Meta().CollectionId)
}

func assertNoReport(t *testing.T, sm *SNQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumeReport(), "expected no pending report")
}

func assertPersistState(t *testing.T, sm *SNQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	v := sm.ConsumePersist()
	require.NotNil(t, v, "expected pending persist with state %s", expected)

	// Verify state.
	assert.Equal(t, viewpb.QueryViewState(expected), v.Meta.State)

	// Verify meta fields are correctly carried.
	assert.Equal(t, sm.Meta().CollectionId, v.Meta.CollectionId)
	assert.Equal(t, sm.Meta().ReplicaId, v.Meta.ReplicaId)
	assert.Equal(t, sm.Meta().Vchannel, v.Meta.Vchannel)
	assert.Equal(t, sm.Meta().Version.QueryVersion, v.Meta.Version.QueryVersion)
	assert.Equal(t, sm.Meta().Version.DataVersion.StreamingVersion, v.Meta.Version.DataVersion.StreamingVersion)
	assert.Equal(t, sm.Meta().Version.DataVersion.CompactVersion, v.Meta.Version.DataVersion.CompactVersion)

	// Verify persist structure: SN persist has StreamingNode, no QueryNode.
	assert.NotNil(t, v.StreamingNode)
	assert.Nil(t, v.QueryNode)

	// Verify persist meta is a clone (mutation doesn't affect SM).
	v.Meta.CollectionId = -1
	assert.NotEqual(t, int64(-1), sm.Meta().CollectionId)
}

func assertNoPersist(t *testing.T, sm *SNQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumePersist(), "expected no pending persist")
}

func assertRelease(t *testing.T, sm *SNQueryViewStateMachine) {
	t.Helper()
	assert.True(t, sm.ConsumeRelease(), "expected pending release")
}

func assertNoRelease(t *testing.T, sm *SNQueryViewStateMachine) {
	t.Helper()
	assert.False(t, sm.ConsumeRelease(), "expected no pending release")
}

// ---------------------------------------------------------------------------
// 1. Construction — NewSNQueryViewStateMachine
// ---------------------------------------------------------------------------

func TestNew_InitialState(t *testing.T) {
	sm := newTestSM()
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assert.False(t, sm.IsRecovering())
}

func TestNew_PendingReport(t *testing.T) {
	sm := newTestSM()
	// Constructor generates a Preparing report to acknowledge receipt.
	assertReportState(t, sm, qviews.QueryViewStatePreparing)
	assertNoReport(t, sm)
}

func TestNew_NoPendingPersist(t *testing.T) {
	sm := newTestSM()
	assertNoPersist(t, sm)
}

func TestNew_NoPendingRelease(t *testing.T) {
	sm := newTestSM()
	assertNoRelease(t, sm)
}

func TestNew_MetaAndViewPreserved(t *testing.T) {
	meta := buildTestMeta()
	snView := buildTestSNView()
	sm := NewSNQueryViewStateMachine(meta, snView)
	assert.Equal(t, meta, sm.Meta())
	assert.Equal(t, snView, sm.SNView())
}

func TestNew_ReportIsClone(t *testing.T) {
	sm := newTestSM()
	report := sm.ConsumeReport()
	require.NotNil(t, report)
	report.Meta.CollectionId = 999
	assert.Equal(t, testCollectionID, sm.Meta().CollectionId)
}

func TestNew_ReportStructure(t *testing.T) {
	sm := newTestSM()
	report := sm.ConsumeReport()
	require.NotNil(t, report)
	assert.NotNil(t, report.Meta)
	assert.NotNil(t, report.StreamingNode)
	assert.Nil(t, report.QueryNode)
}

// ---------------------------------------------------------------------------
// 2. Construction — RecoverSNQueryViewStateMachine
// ---------------------------------------------------------------------------

func TestRecover_InitialState(t *testing.T) {
	sm := newRecoveringSM()
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	assert.True(t, sm.IsRecovering())
}

func TestRecover_NoPendingReport(t *testing.T) {
	sm := newRecoveringSM()
	assertNoReport(t, sm)
}

func TestRecover_NoPendingPersist(t *testing.T) {
	sm := newRecoveringSM()
	assertNoPersist(t, sm)
}

func TestRecover_NoPendingRelease(t *testing.T) {
	sm := newRecoveringSM()
	assertNoRelease(t, sm)
}

// ---------------------------------------------------------------------------
// 3. Normal flow: Preparing → Ready → Up → Down → Dropping → Dropped
// ---------------------------------------------------------------------------

func TestNormalFlow_PreparingToReady(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)
	assertNoPersist(t, sm)
	assertNoRelease(t, sm)
}

func TestNormalFlow_ReadyToUp(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	assertPersistState(t, sm, qviews.QueryViewStateUp)
	assertNoRelease(t, sm)
}

func TestNormalFlow_UpToDown(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)
	assertNoRelease(t, sm)
}

func TestNormalFlow_DownToDropping(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	// No report in Dropping: wait for OnDropped callback.
	assertNoReport(t, sm)
	assertNoPersist(t, sm) // Down already deleted recovery info.
	assertRelease(t, sm)
}

func TestNormalFlow_DroppingToDropped(t *testing.T) {
	sm := newDroppingFromPreparingSM()

	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
	assertNoRelease(t, sm)
}

func TestNormalFlow_FullLifecycle(t *testing.T) {
	sm := newTestSM()

	// Preparing
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertReportState(t, sm, qviews.QueryViewStatePreparing)
	assertNoPersist(t, sm)

	// Ready
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)
	assertNoPersist(t, sm)

	// Up
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	assertPersistState(t, sm, qviews.QueryViewStateUp)

	// Down
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)

	// Dropping
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
	assertRelease(t, sm)

	// Dropped (Release callback)
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
	assertNoRelease(t, sm)

	// Terminal
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 4. Error path: Preparing → Unrecoverable → Dropping → Dropped
// ---------------------------------------------------------------------------

func TestErrorPath_PreparingToUnrecoverable(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
	// No persist: no recovery info was persisted in Preparing.
	assertNoPersist(t, sm)
	assertNoRelease(t, sm)
}

func TestErrorPath_UnrecoverableToDropping(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	// Idempotent persist deletion: safe even for Preparing→Unrecoverable
	// (no persisted data), required for UpRecovering→Unrecoverable (stale Up on disk).
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
	assertRelease(t, sm)
}

func TestErrorPath_UnrecoverableToDroppingToDropped(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	sm.ConsumePersist()
	sm.ConsumeRelease()

	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 5. Abort paths: Preparing → Dropping → Dropped, Ready → Dropping → Dropped
// ---------------------------------------------------------------------------

func TestAbort_PreparingToDropping(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
	assertRelease(t, sm)
}

func TestAbort_PreparingToDroppingToDropped(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	sm.ConsumeRelease()

	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestAbort_ReadyToDropping(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
	assertRelease(t, sm)
}

func TestAbort_ReadyToDroppingToDropped(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	sm.ConsumeRelease()

	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 6. Dropping from Up/UpRecovering — must delete recovery info
// ---------------------------------------------------------------------------

func TestDropping_FromUp_DeletesPersist(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	// Must delete recovery info immediately.
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
	assertRelease(t, sm)
}

func TestDropping_FromUpRecovering_DeletesPersist(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	// Must delete recovery info immediately.
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
	assertRelease(t, sm)
}

// ---------------------------------------------------------------------------
// 7. Recovery flow: UpRecovering → Up / Down / Unrecoverable
// ---------------------------------------------------------------------------

func TestRecovery_RecoveringDone(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assert.False(t, sm.IsRecovering())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	// Already persisted as Up before crash — no new persist.
	assertNoPersist(t, sm)
}

func TestRecovery_UpRecoveringToDown(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assert.False(t, sm.IsRecovering())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	// Must delete recovery info.
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

func TestRecovery_UpRecoveringToUnrecoverable(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	// No report: Coord is not notified. The query path detects the
	// unavailable view and triggers replacement.
	assertNoReport(t, sm)
	// No persist: recovery info retained for retry on restart.
	assertNoPersist(t, sm)
}

func TestRecovery_UpRecoveringToDropping(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	// Must delete recovery info immediately.
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
	assertRelease(t, sm)
}

func TestRecovery_FullFlow_RecoveringToUpToDownToDropped(t *testing.T) {
	sm := newRecoveringSM()

	// UpRecovering → Up
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	assertNoPersist(t, sm)

	// Up → Down
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)

	// Down → Dropping
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assertRelease(t, sm)

	// Dropping → Dropped
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestRecovery_UnrecoverableToDropped(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnUnrecoverable()
	assertNoReport(t, sm)  // No report for UpRecovering→Unrecoverable
	assertNoPersist(t, sm) // Recovery info retained

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	// Dropping from Unrecoverable deletes stale recovery info.
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
	assertRelease(t, sm)

	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

// ---------------------------------------------------------------------------
// 8. Coord re-push Preparing — distributed state recoverability
// ---------------------------------------------------------------------------

func TestCoordPreparing_StillPreparing_NoReport(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoReport(t, sm)
}

func TestCoordPreparing_StillPreparing_MultipleRePush_NoReport(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

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

func TestCoordPreparing_Up_ReReportsUp(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
}

func TestCoordPreparing_Down_ReReportsDown(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
}

func TestCoordPreparing_Unrecoverable_ReReportsUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

func TestCoordPreparing_Dropped_ReReportsDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestCoordPreparing_Dropping_NoReport(t *testing.T) {
	sm := newDroppingFromPreparingSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	// Don't report: wait for OnDropped callback.
	assertNoReport(t, sm)
}

func TestCoordPreparing_UpRecovering_NoReport(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	// Don't report: wait for WAL catch-up, then report Up for fast-forward.
	assertNoReport(t, sm)
}

func TestCoordPreparing_UpRecovering_MultipleRePush_NoReport(t *testing.T) {
	sm := newRecoveringSM()

	for range 3 {
		sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
		assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
		assertNoReport(t, sm)
	}
}

// ---------------------------------------------------------------------------
// 9. Coord re-push Up — fast-forward guarantee
// ---------------------------------------------------------------------------

func TestCoordUp_Ready_TransitionsToUp(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	assertPersistState(t, sm, qviews.QueryViewStateUp)
}

func TestCoordUp_AlreadyUp_ReReportsUp(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	// No new persist: already persisted.
	assertNoPersist(t, sm)
}

func TestCoordUp_Down_ReReportsDown(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
}

func TestCoordUp_Dropped_ReReportsDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestCoordUp_Dropping_NoReport(t *testing.T) {
	sm := newDroppingFromPreparingSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	// Don't report: wait for OnDropped callback.
	assertNoReport(t, sm)
}

func TestCoordUp_Preparing_ReReportsPreparing(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertReportState(t, sm, qviews.QueryViewStatePreparing)
}

func TestCoordUp_Unrecoverable_ReReportsUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

func TestCoordUp_UpRecovering_ReReportsUp(t *testing.T) {
	sm := newRecoveringSM()

	// UpRecovering maps to Up for Coord-visible state.
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
}

// ---------------------------------------------------------------------------
// 10. Coord re-push Down — fast-forward guarantee
// ---------------------------------------------------------------------------

func TestCoordDown_Up_TransitionsToDown(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

func TestCoordDown_UpRecovering_TransitionsToDown(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

func TestCoordDown_AlreadyDown_ReReportsDown(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertNoPersist(t, sm)
}

func TestCoordDown_Dropped_ReReportsDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestCoordDown_Dropping_NoReport(t *testing.T) {
	sm := newDroppingFromPreparingSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	// Don't report: wait for OnDropped callback.
	assertNoReport(t, sm)
}

func TestCoordDown_Preparing_ReReportsPreparing(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertReportState(t, sm, qviews.QueryViewStatePreparing)
}

func TestCoordDown_Ready_ReReportsReady(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateReady)
}

func TestCoordDown_Unrecoverable_ReReportsUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

// ---------------------------------------------------------------------------
// 11. Coord Dropped — transitions to Dropping (not directly to Dropped)
// ---------------------------------------------------------------------------

func TestCoordDropped_FromPreparing(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
	assertRelease(t, sm)
}

func TestCoordDropped_FromReady(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
	assertRelease(t, sm)
}

func TestCoordDropped_FromUp(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	// Must delete recovery info immediately.
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
	assertRelease(t, sm)
}

func TestCoordDropped_FromUpRecovering(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	// Must delete recovery info immediately.
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
	assertRelease(t, sm)
}

func TestCoordDropped_FromDown(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
	assertRelease(t, sm)
}

func TestCoordDropped_FromUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
	// Idempotent persist deletion (safe for Preparing→Unrecoverable,
	// required for UpRecovering→Unrecoverable which retains stale Up on disk).
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
	assertRelease(t, sm)
}

func TestCoordDropped_FromDropping_Ignored(t *testing.T) {
	sm := newDroppingFromPreparingSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	// No new report, release, or persist — already releasing.
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
	assertNoRelease(t, sm)
}

func TestCoordDropped_RePushInDropped(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
	assertNoRelease(t, sm)
}

func TestCoordDropped_RePushMultiple(t *testing.T) {
	sm := newDroppedSM()

	for range 3 {
		sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
		assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
		assertReportState(t, sm, qviews.QueryViewStateDropped)
		assertNoPersist(t, sm)
		assertNoRelease(t, sm)
	}
}

// ---------------------------------------------------------------------------
// 12. Local event idempotency — events ignored in invalid states
// ---------------------------------------------------------------------------

func TestOnReady_IgnoredInReady(t *testing.T) {
	sm := newReadySM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInUp(t *testing.T) {
	sm := newUpSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInDown(t *testing.T) {
	sm := newDownSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInDropped(t *testing.T) {
	sm := newDroppedSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInDropping(t *testing.T) {
	sm := newDroppingFromPreparingSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
}

func TestOnReady_IgnoredInUpRecovering(t *testing.T) {
	sm := newRecoveringSM()
	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInReady(t *testing.T) {
	sm := newReadySM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInUp(t *testing.T) {
	sm := newUpSM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInDown(t *testing.T) {
	sm := newDownSM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInDropped(t *testing.T) {
	sm := newDroppedSM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInDropping(t *testing.T) {
	sm := newDroppingFromPreparingSM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
}

func TestOnUnrecoverable_IgnoredInUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()
	sm.OnUnrecoverable()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInPreparing(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInReady(t *testing.T) {
	sm := newReadySM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInUp(t *testing.T) {
	sm := newUpSM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInDown(t *testing.T) {
	sm := newDownSM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInDropped(t *testing.T) {
	sm := newDroppedSM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInDropping(t *testing.T) {
	sm := newDroppingFromPreparingSM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoReport(t, sm)
}

func TestOnRecoveringDone_IgnoredInUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()
	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoReport(t, sm)
}

func TestOnDropped_IgnoredInPreparing(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoReport(t, sm)
}

func TestOnDropped_IgnoredInReady(t *testing.T) {
	sm := newReadySM()
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoReport(t, sm)
}

func TestOnDropped_IgnoredInUp(t *testing.T) {
	sm := newUpSM()
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoReport(t, sm)
}

func TestOnDropped_IgnoredInDown(t *testing.T) {
	sm := newDownSM()
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoReport(t, sm)
}

func TestOnDropped_IgnoredInDropped(t *testing.T) {
	sm := newDroppedSM()
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoReport(t, sm)
}

func TestOnDropped_IgnoredInUnrecoverable(t *testing.T) {
	sm := newUnrecoverableSM()
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoReport(t, sm)
}

func TestOnDropped_IgnoredInUpRecovering(t *testing.T) {
	sm := newRecoveringSM()
	sm.OnDropped()
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	assertNoReport(t, sm)
}

// ---------------------------------------------------------------------------
// 13. Dropped terminal — all events ignored
// ---------------------------------------------------------------------------

func TestDroppedTerminal_AllLocalEventsIgnored(t *testing.T) {
	sm := newDroppedSM()

	sm.OnReady()
	assertNoReport(t, sm)
	sm.OnUnrecoverable()
	assertNoReport(t, sm)
	sm.OnRecoveringDone()
	assertNoReport(t, sm)
	sm.OnDropped()
	assertNoReport(t, sm)

	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
}

func TestDroppedTerminal_AllCoordPushesReReportDropped(t *testing.T) {
	sm := newDroppedSM()

	pushes := []qviews.QueryViewState{
		qviews.QueryViewStatePreparing,
		qviews.QueryViewStateUp,
		qviews.QueryViewStateDown,
		qviews.QueryViewStateDropped,
	}
	for _, push := range pushes {
		sm.OnCoordStateDelivered(push)
		assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
		assertReportState(t, sm, qviews.QueryViewStateDropped)
		assertNoPersist(t, sm)
		assertNoRelease(t, sm)
	}
}

// ---------------------------------------------------------------------------
// 14. Consume idempotency — double consume returns nil/false
// ---------------------------------------------------------------------------

func TestConsume_DoubleConsumeReportReturnsNil(t *testing.T) {
	sm := newTestSM()
	v := sm.ConsumeReport()
	require.NotNil(t, v)
	assertNoReport(t, sm)
}

func TestConsume_DoubleConsumePersistReturnsNil(t *testing.T) {
	sm := newReadySM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	v := sm.ConsumePersist()
	require.NotNil(t, v)
	assertNoPersist(t, sm)
}

func TestConsume_DoubleConsumeReleaseReturnsFalse(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.True(t, sm.ConsumeRelease())
	assertNoRelease(t, sm)
}

func TestConsume_NoEventNoReport(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport() // drain initial
	assertNoReport(t, sm)
	assertNoPersist(t, sm)
	assertNoRelease(t, sm)
}

// ---------------------------------------------------------------------------
// 15. Distributed recoverability — Coord crash + re-push scenarios
// ---------------------------------------------------------------------------

func TestRecoverability_ReadyAfterCoordCrash(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnReady()
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assertReportState(t, sm, qviews.QueryViewStateReady)
}

func TestRecoverability_UpAfterCoordCrash(t *testing.T) {
	sm := newUpSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assertReportState(t, sm, qviews.QueryViewStateUp)
}

func TestRecoverability_DownAfterCoordCrash(t *testing.T) {
	sm := newDownSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assertReportState(t, sm, qviews.QueryViewStateDown)
}

func TestRecoverability_UnrecoverableAfterCoordCrash(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnUnrecoverable()
	sm.ConsumeReport()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
}

func TestRecoverability_DroppedAfterCoordCrash(t *testing.T) {
	sm := newDroppedSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertReportState(t, sm, qviews.QueryViewStateDropped)
}

func TestRecoverability_RepeatedRePushAlwaysProducesReport(t *testing.T) {
	sm := newUpSM()

	for range 5 {
		sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
		assertReportState(t, sm, qviews.QueryViewStateUp)
	}
}

func TestRecoverability_UpRecoveringThenCoordCrash(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	assertNoReport(t, sm)

	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
}

func TestRecoverability_SNCrashRecovery_FullFlow(t *testing.T) {
	sm := RecoverSNQueryViewStateMachine(buildTestMeta(), buildTestSNView())
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
	assertNoReport(t, sm)
	assertNoPersist(t, sm)

	sm.OnRecoveringDone()
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateUp)
	assertNoPersist(t, sm)

	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

// ---------------------------------------------------------------------------
// 16. coordVisibleState — UpRecovering maps to Up in reports
// ---------------------------------------------------------------------------

func TestCoordVisibleState_UpRecoveringReportsAsUp(t *testing.T) {
	sm := newRecoveringSM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	report := sm.ConsumeReport()
	require.NotNil(t, report)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUp, report.Meta.State)
	assert.Equal(t, qviews.QueryViewStateUpRecovering, sm.State())
}

// ---------------------------------------------------------------------------
// 17. Persist semantics — Up persists, Down/Unrecoverable/Dropped deletes
// ---------------------------------------------------------------------------

func TestPersist_UpSaves(t *testing.T) {
	sm := newReadySM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUp, persist.Meta.State)
}

func TestPersist_DownDeletes(t *testing.T) {
	sm := newUpSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDown, persist.Meta.State)
}

func TestPersist_UnrecoverableFromUpRecoveringRetainsRecoveryInfo(t *testing.T) {
	sm := newRecoveringSM()
	sm.OnUnrecoverable()
	// Must NOT persist: recovery info retained until Coord pushes Dropped.
	assertNoPersist(t, sm)
}

func TestPersist_DroppingFromUnrecoverableDeletesStaleRecoveryInfo(t *testing.T) {
	// UpRecovering → Unrecoverable retains persist (tested above).
	// Coord then pushes Dropped → Dropping must delete the stale persist.
	sm := newRecoveringSM()
	sm.OnUnrecoverable()
	assertNoPersist(t, sm) // retained

	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, persist.Meta.State)
}

func TestPersist_DroppingFromUpDeletes(t *testing.T) {
	sm := newUpSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, persist.Meta.State)
}

func TestPersist_DroppingFromUpRecoveringDeletes(t *testing.T) {
	sm := newRecoveringSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, persist.Meta.State)
}

func TestPersist_PreparingNoPersist(t *testing.T) {
	sm := newTestSM()
	assertNoPersist(t, sm)
}

func TestPersist_ReadyNoPersist(t *testing.T) {
	sm := newReadySM()
	assertNoPersist(t, sm)
}

func TestPersist_UnrecoverableFromPreparingNoPersist(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnUnrecoverable()
	assertNoPersist(t, sm)
}

func TestPersist_DroppingFromDownNoPersist(t *testing.T) {
	sm := newDownSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestPersist_DroppingFromPreparingNoPersist(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestPersist_DroppingFromReadyNoPersist(t *testing.T) {
	sm := newReadySM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	assertNoPersist(t, sm)
}

func TestPersist_DroppingFromUnrecoverableDeletes(t *testing.T) {
	sm := newUnrecoverableSM()
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)
	// Idempotent delete: safe even when no persisted data exists.
	assertPersistState(t, sm, qviews.QueryViewStateDropped)
}

// ---------------------------------------------------------------------------
// 18. Pending report overwrite — latest event wins
// ---------------------------------------------------------------------------

func TestPendingOverwrite_ReadyThenDropping(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnReady()
	// Before consuming Ready report, Coord pushes Dropped → Dropping.
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropped)

	// Ready report is cleared by Dropping transition.
	assertNoReport(t, sm)
	assertRelease(t, sm)
}

func TestPendingOverwrite_ReadyThenUnrecoverable(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	sm.OnUnrecoverable()
	assertReportState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoReport(t, sm)
}

func TestPendingOverwrite_CoordUpThenDown(t *testing.T) {
	sm := newReadySM()

	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	sm.OnCoordStateDelivered(qviews.QueryViewStateDown)

	assertReportState(t, sm, qviews.QueryViewStateDown)
	assertPersistState(t, sm, qviews.QueryViewStateDown)
}

// ---------------------------------------------------------------------------
// 19. Unrecognized Coord pushes — no handler in OnCoordStateDelivered
// ---------------------------------------------------------------------------

func TestUnrecognizedPush_DroppingIgnored(t *testing.T) {
	sm := newTestSM()
	sm.ConsumeReport()

	// Dropping is a local-only state; Coord has no handler for it.
	sm.OnCoordStateDelivered(qviews.QueryViewStateDropping)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoReport(t, sm)
}
