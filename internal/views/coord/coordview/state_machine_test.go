package coordview

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

// buildTestView creates a QueryViewOfShard in Preparing state with the given
// number of query nodes. QN IDs are 1..numQN.
func buildTestView(numQN int) *viewpb.QueryViewOfShard {
	qns := make([]*viewpb.QueryViewOfQueryNode, numQN)
	for i := range qns {
		qns[i] = &viewpb.QueryViewOfQueryNode{
			NodeId: int64(i + 1),
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, SegmentIds: []int64{1000 + int64(i)}},
			},
		}
	}
	return &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: testCollectionID,
			ReplicaId:    testReplicaID,
			Vchannel:     testVChannel,
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
				QueryVersion: 1,
			},
			State: viewpb.QueryViewState_QueryViewStatePreparing,
		},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
		QueryNode:     qns,
	}
}

// snReport creates a StreamingNode report with the given state.
func snReport(view *viewpb.QueryViewOfShard, state qviews.QueryViewState) qviews.QueryViewAtWorkNode {
	meta := &viewpb.QueryViewMeta{
		CollectionId: view.Meta.CollectionId,
		ReplicaId:    view.Meta.ReplicaId,
		Vchannel:     view.Meta.Vchannel,
		Version:      view.Meta.Version,
		State:        viewpb.QueryViewState(state),
	}
	return qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
}

// qnReport creates a QueryNode report with the given state and optional ready segment IDs.
func qnReport(view *viewpb.QueryViewOfShard, nodeID int64, state qviews.QueryViewState, readySegs ...int64) qviews.QueryViewAtWorkNode {
	meta := &viewpb.QueryViewMeta{
		CollectionId: view.Meta.CollectionId,
		ReplicaId:    view.Meta.ReplicaId,
		Vchannel:     view.Meta.Vchannel,
		Version:      view.Meta.Version,
		State:        viewpb.QueryViewState(state),
	}
	partitions := []*viewpb.QueryViewOfPartition{
		{PartitionId: 10, SegmentIds: []int64{1000}, ReadySegmentIds: readySegs},
	}
	return qviews.NewQueryViewAtQueryNode(meta, &viewpb.QueryViewOfQueryNode{
		NodeId:     nodeID,
		Partitions: partitions,
	})
}

// assertPendingPersistState checks that ConsumePersist returns a view with the
// expected state, then clears it.
func assertPendingPersistState(t *testing.T, sm *CoordQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	v := sm.ConsumePersist()
	require.NotNil(t, v, "expected pending persist with state %s", expected)
	assert.Equal(t, viewpb.QueryViewState(expected), v.Meta.State)
}

// assertPendingSyncState checks that ConsumeSync returns views all with the
// expected state, then clears it.
func assertPendingSyncState(t *testing.T, sm *CoordQueryViewStateMachine, expected qviews.QueryViewState) {
	t.Helper()
	views := sm.ConsumeSync()
	require.NotEmpty(t, views, "expected pending sync with state %s", expected)
	for _, v := range views {
		assert.Equal(t, expected, v.State(), "sync view state mismatch")
	}
}

// assertNoPendingPersist checks that ConsumePersist returns nil.
func assertNoPendingPersist(t *testing.T, sm *CoordQueryViewStateMachine) {
	t.Helper()
	assert.Nil(t, sm.ConsumePersist(), "expected no pending persist")
}

// assertNoPendingSync checks that ConsumeSync returns nil/empty.
func assertNoPendingSync(t *testing.T, sm *CoordQueryViewStateMachine) {
	t.Helper()
	assert.Empty(t, sm.ConsumeSync(), "expected no pending sync")
}

// assertNoPending checks that both ConsumePersist and ConsumeSync return nil.
func assertNoPending(t *testing.T, sm *CoordQueryViewStateMachine) {
	t.Helper()
	assertNoPendingPersist(t, sm)
	assertNoPendingSync(t, sm)
}

// drainPending consumes and discards both pending persist and sync.
func drainPending(sm *CoordQueryViewStateMachine) {
	sm.ConsumePersist()
	sm.ConsumeSync()
}

// ===========================================================================
// 1. NORMAL STATE TRANSITIONS (Happy Path)
// ===========================================================================

// TestNormalFlow_SingleQN validates the complete normal lifecycle:
// Preparing → Ready → Up → Down → Dropping → Dropped
// Verifies both State() and Consume outputs at every step.
func TestNormalFlow_SingleQN(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	// --- Initial: Preparing ---
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStatePreparing)
	assertPendingSyncState(t, sm, qviews.QueryViewStatePreparing)

	// QN1 reports Ready — SN not ready yet, no transition
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)

	// SN reports Ready → all nodes ready → Preparing→Ready
	// Ready: pendingSync=Up (push Up to SN), no persist (Ready is ephemeral)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)

	// --- Ready → Up ---
	// SN reports Up → Ready→Up
	// Up: pendingPersist=Up (write-ahead), no sync
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUp)
	assertNoPendingSync(t, sm)

	// --- Up → Down ---
	// EnterDown: pendingPersist=Down, pendingSync=Down
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDown)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDown)

	// --- Down → Dropping ---
	// SN reports Down → Down→Dropping
	// Dropping: pendingSync=Dropped (push Dropped to all), no persist (ephemeral)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)

	// --- Dropping: SN Dropped but QN1 not yet ---
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPending(t, sm)

	// --- Dropping → Dropped ---
	// QN1 Dropped → all nodes Dropped → Dropping→Dropped
	// Dropped: pendingPersist=Dropped (delete from ETCD), no sync
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingSync(t, sm)
}

// TestNormalFlow_MultipleQN validates that transition from Preparing to Ready
// requires ALL QNs to report Ready, and verifies Consume at each step.
func TestNormalFlow_MultipleQN(t *testing.T) {
	view := buildTestView(3)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// QN1 Ready — no transition, no pending
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)

	// QN2 Ready — no transition, no pending
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)

	// SN Ready — QN3 still missing, no transition, no pending
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)

	// QN3 Ready → all ready → Ready
	// pendingSync=Up, no persist
	sm.OnNodeStateReported(qnReport(view, 3, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)
}

// TestNormalFlow_DroppingRequiresAllNodesDropped validates Dropping → Dropped
// needs ALL nodes (SN + all QNs) to report Dropped.
func TestNormalFlow_DroppingRequiresAllNodesDropped(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Fast-forward to Dropping state
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	drainPending(sm)
	sm.EnterDown()
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	drainPending(sm)

	// SN Dropped, QN1 Dropped — QN2 missing, no transition
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPending(t, sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPending(t, sm)

	// QN2 Dropped → all Dropped → Dropped
	// pendingPersist=Dropped, no sync
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingSync(t, sm)
}

func TestQueryNodeLost_DroppingCountsAsDropped(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Fast-forward to Dropping state.
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	drainPending(sm)
	sm.EnterDown()
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	drainPending(sm)

	// QN2 disappears during cleanup. The view should no longer wait for a
	// Dropped response from that node.
	sm.OnQueryNodeLost(qviews.NewQueryNode(2))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPending(t, sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPending(t, sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingSync(t, sm)
}

// ===========================================================================
// 2. RECOVERY FAST-FORWARD (SN already Up during Preparing)
// ===========================================================================

// TestPreparing_SNAlreadyUp_FastForward tests recovery scenario where SN
// reports Up while Coord is still in Preparing. Fast-forward to Up.
func TestPreparing_SNAlreadyUp_FastForward(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// QN1 Ready, SN Up → fast-forward to Up
	// pendingPersist=Up (persist Up), no sync (SN already Up)
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assertNoPending(t, sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUp)
	assertNoPendingSync(t, sm)
}

// TestPreparing_SNUpBeforeQNReady_WaitsForQN ensures fast-forward only
// happens after ALL QNs are also Ready.
func TestPreparing_SNUpBeforeQNReady_WaitsForQN(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// SN reports Up but QNs not ready — no transition, no pending
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)

	// QN1 Ready — still waiting for QN2
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)

	// QN2 Ready → fast-forward to Up
	// pendingPersist=Up, no sync
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUp)
	assertNoPendingSync(t, sm)
}

// ===========================================================================
// 3. UNRECOVERABLE TRANSITIONS (Error Path)
// ===========================================================================

// TestPreparing_SNUnrecoverable: Preparing → Unrecoverable via SN.
// pendingPersist=Unrecoverable, no sync.
func TestPreparing_SNUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)
}

// TestPreparing_QNUnrecoverable: Preparing → Unrecoverable via QN.
func TestPreparing_QNUnrecoverable(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assertNoPending(t, sm)

	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)
}

// TestReady_SNUnrecoverable: Ready → Unrecoverable via SN.
func TestReady_SNUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	drainPending(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)
}

// TestReady_QNUnrecoverable: Ready → Unrecoverable via QN.
func TestReady_QNUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	drainPending(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)
}

// TestUp_SNUnrecoverable: Up → Unrecoverable via SN.
func TestUp_SNUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Up
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	drainPending(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)
}

// TestUp_QNUnrecoverable: Up → Unrecoverable via QN.
func TestUp_QNUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Up
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	drainPending(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)
}

// TestUnrecoverable_EnterDropping_ToDropping validates Manager-triggered
// Unrecoverable → Dropping. pendingSync=Dropped, no persist.
func TestUnrecoverable_EnterDropping_ToDropping(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	drainPending(sm)

	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)
}

// TestUnrecoverable_FullCleanupCycle validates complete error recovery path:
// Unrecoverable → Dropping → Dropped with Consume checks at every step.
func TestUnrecoverable_FullCleanupCycle(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// → Unrecoverable
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)

	// → Dropping
	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)

	// SN Dropped — QN1 not yet
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPending(t, sm)

	// QN1 Dropped → all Dropped → Dropped
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingSync(t, sm)
}

// ===========================================================================
// 4. IDEMPOTENCY TESTS
// ===========================================================================

// TestIdempotency_EnterDown_NotInUp verifies EnterDown is no-op when not in Up:
// no state change, no pending persist, no pending sync.
func TestIdempotency_EnterDown_NotInUp(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*CoordQueryViewStateMachine, *viewpb.QueryViewOfShard)
		state qviews.QueryViewState
	}{
		{
			name:  "Preparing",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {},
			state: qviews.QueryViewStatePreparing,
		},
		{
			name: "Ready",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(qnReport(v, 1, qviews.QueryViewStateReady))
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateReady))
			},
			state: qviews.QueryViewStateReady,
		},
		{
			name: "Down",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(qnReport(v, 1, qviews.QueryViewStateReady))
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateReady))
				drainPending(sm)
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateUp))
				drainPending(sm)
				sm.EnterDown()
			},
			state: qviews.QueryViewStateDown,
		},
		{
			name: "Unrecoverable",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateUnrecoverable))
			},
			state: qviews.QueryViewStateUnrecoverable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			view := buildTestView(1)
			sm := NewCoordQueryViewStateMachine(view)
			drainPending(sm)
			tc.setup(sm, view)
			assert.Equal(t, tc.state, sm.State())
			drainPending(sm)

			sm.EnterDown()
			assert.Equal(t, tc.state, sm.State(), "EnterDown should be no-op in %s", tc.state)
			assertNoPending(t, sm)
		})
	}
}

// TestIdempotency_EnterDropping_NotInUnrecoverable verifies EnterDropping is
// no-op when not in Unrecoverable: no state change, no pending.
func TestIdempotency_EnterDropping_NotInUnrecoverable(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*CoordQueryViewStateMachine, *viewpb.QueryViewOfShard)
		state qviews.QueryViewState
	}{
		{
			name:  "Preparing",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {},
			state: qviews.QueryViewStatePreparing,
		},
		{
			name: "Up",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(qnReport(v, 1, qviews.QueryViewStateReady))
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateReady))
				drainPending(sm)
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateUp))
			},
			state: qviews.QueryViewStateUp,
		},
		{
			name: "Down",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(qnReport(v, 1, qviews.QueryViewStateReady))
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateReady))
				drainPending(sm)
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateUp))
				drainPending(sm)
				sm.EnterDown()
			},
			state: qviews.QueryViewStateDown,
		},
		{
			name: "Dropping",
			setup: func(sm *CoordQueryViewStateMachine, v *viewpb.QueryViewOfShard) {
				sm.OnNodeStateReported(snReport(v, qviews.QueryViewStateUnrecoverable))
				drainPending(sm)
				sm.EnterDropping()
			},
			state: qviews.QueryViewStateDropping,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			view := buildTestView(1)
			sm := NewCoordQueryViewStateMachine(view)
			drainPending(sm)
			tc.setup(sm, view)
			assert.Equal(t, tc.state, sm.State())
			drainPending(sm)

			sm.EnterDropping()
			assert.Equal(t, tc.state, sm.State(), "EnterDropping should be no-op in %s", tc.state)
			assertNoPending(t, sm)
		})
	}
}

// TestIdempotency_DuplicateNodeReports validates that processing the same
// node report multiple times produces no spurious pending operations.
func TestIdempotency_DuplicateNodeReports(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Report QN Ready three times — each time no transition, no pending
	for i := 0; i < 3; i++ {
		sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
		assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
		assertNoPending(t, sm)
	}

	// First SN Ready → transition to Ready
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)

	// Duplicate SN Ready in Ready state → re-push Up (SN not Up yet)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)
}

// TestIdempotency_EnterDown_CalledTwice verifies second EnterDown is no-op.
func TestIdempotency_EnterDown_CalledTwice(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Up
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	drainPending(sm)

	// First call
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDown)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDown)

	// Second call — no-op, no pending
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoPending(t, sm)
}

// TestIdempotency_EnterDropping_CalledTwice verifies second EnterDropping is no-op.
func TestIdempotency_EnterDropping_CalledTwice(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	drainPending(sm)

	// First call
	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)

	// Second call — no-op, no pending
	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPending(t, sm)
}

// ===========================================================================
// 5. COORDINATOR CRASH RECOVERY
// ===========================================================================

// TestRecovery_Preparing: re-push Preparing sync, no persist (already persisted).
func TestRecovery_Preparing(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStatePreparing

	sm := RecoverCoordQueryViewStateMachine(view)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStatePreparing)

	// Can proceed normally: all Ready → Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assertNoPending(t, sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)
}

// TestRecovery_Up: no pending, waits for events.
func TestRecovery_Up(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUp

	sm := RecoverCoordQueryViewStateMachine(view)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoPending(t, sm)

	// Can receive EnterDown
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDown)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDown)
}

// TestRecovery_Down: re-push Down sync to SN, no persist.
func TestRecovery_Down(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateDown

	sm := RecoverCoordQueryViewStateMachine(view)
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDown)

	// SN reports Down → Dropping
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)
}

// TestRecovery_Unrecoverable: stays, no pending, waits for Manager.
func TestRecovery_Unrecoverable(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable

	sm := RecoverCoordQueryViewStateMachine(view)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertNoPending(t, sm)

	// Manager calls EnterDropping
	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)
}

// TestRecovery_InvalidState panics on non-persistable states.
func TestRecovery_InvalidState(t *testing.T) {
	invalidStates := []viewpb.QueryViewState{
		viewpb.QueryViewState_QueryViewStateReady,
		viewpb.QueryViewState_QueryViewStateDropping,
		viewpb.QueryViewState_QueryViewStateDropped,
		viewpb.QueryViewState_QueryViewStateUnknown,
	}

	for _, state := range invalidStates {
		t.Run(state.String(), func(t *testing.T) {
			view := buildTestView(1)
			view.Meta.State = state
			assert.Panics(t, func() {
				RecoverCoordQueryViewStateMachine(view)
			}, "recovery from %s should panic", state)
		})
	}
}

// TestRecovery_Preparing_ThenUnrecoverable: after recovering in Preparing,
// Unrecoverable works and produces correct pending.
func TestRecovery_Preparing_ThenUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStatePreparing

	sm := RecoverCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)
}

// TestRecovery_Up_ThenUnrecoverable: after recovering in Up,
// QN Unrecoverable works and produces correct pending.
func TestRecovery_Up_ThenUnrecoverable(t *testing.T) {
	view := buildTestView(1)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUp

	sm := RecoverCoordQueryViewStateMachine(view)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)
}

// ===========================================================================
// 6. PENDING I/O CONSUMPTION SEMANTICS
// ===========================================================================

// TestConsumePersist_ConsumeOnce: second ConsumePersist returns nil.
func TestConsumePersist_ConsumeOnce(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	v := sm.ConsumePersist()
	require.NotNil(t, v)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, v.Meta.State)

	assert.Nil(t, sm.ConsumePersist())
	assert.Nil(t, sm.ConsumePersist()) // third call also nil
}

// TestConsumeSync_ConsumeOnce: second ConsumeSync returns nil.
func TestConsumeSync_ConsumeOnce(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	views := sm.ConsumeSync()
	require.NotEmpty(t, views)
	for _, v := range views {
		assert.Equal(t, qviews.QueryViewStatePreparing, v.State())
	}

	assert.Empty(t, sm.ConsumeSync())
	assert.Empty(t, sm.ConsumeSync()) // third call also nil
}

// TestPendingPersist_Dropped_MeansDelete: Dropped persist signals ETCD delete.
func TestPendingPersist_Dropped_MeansDelete(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Drive to Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	drainPending(sm)
	sm.EnterDropping()
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))

	v := sm.ConsumePersist()
	require.NotNil(t, v)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, v.Meta.State)
	assertNoPendingSync(t, sm)
}

// TestPendingOverwrite_LastWins: if multiple events fire before Manager
// consumes, the last pending value wins.
func TestPendingOverwrite_LastWins(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Ready — sets pendingSync=Up
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	// Don't drain — let it accumulate

	// SN reports Ready again — re-pushes Up, overwriting previous sync
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	// Should still have pendingSync=Up (overwritten with same value)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)
	assertNoPendingPersist(t, sm)
}

// ===========================================================================
// 7. RE-PUSH BEHAVIOR (Retries on stale node state)
// ===========================================================================

// TestReady_SNNotUpYet_RePushUp: SN reports non-Up in Ready → re-push Up.
func TestReady_SNNotUpYet_RePushUp(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	drainPending(sm)

	// SN reports Ready (hasn't picked up Up) → re-push Up
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)
}

// TestReady_SNReportsPreparing_RePushUp: SN reports Preparing in Ready → re-push Up.
func TestReady_SNReportsPreparing_RePushUp(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)

	// SN reports Preparing → re-push Up
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStatePreparing))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)
}

// TestDown_SNNotDownYet_RePushDown: SN reports non-Down in Down → re-push Down.
func TestDown_SNNotDownYet_RePushDown(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Down
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	drainPending(sm)
	sm.EnterDown()
	drainPending(sm)

	// SN reports Up → re-push Down
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDown)
}

// TestDown_SNDropped_FastForwardToDropping: SN reports Dropped in Down
// (e.g., Coord crash recovery regressed from Dropping to Down) → skip to Dropping.
func TestDown_SNDropped_FastForwardToDropping(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Down
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	drainPending(sm)
	sm.EnterDown()
	drainPending(sm)

	// SN reports Dropped → same as Down, advance to Dropping
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)
}

// TestDropping_NodeNotDropped_RePushDropped: node reports non-Dropped in
// Dropping → re-push Dropped.
func TestDropping_NodeNotDropped_RePushDropped(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Drive to Dropping
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	drainPending(sm)
	sm.EnterDropping()
	drainPending(sm)

	// SN reports Ready → re-push Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)
}

// TestDropping_QNNotDropped_RePushDropped: QN reports non-Dropped in
// Dropping → re-push Dropped.
func TestDropping_QNNotDropped_RePushDropped(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Drive to Dropping
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	drainPending(sm)
	sm.EnterDropping()
	drainPending(sm)

	// QN reports Ready → re-push Dropped
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)
}

// ===========================================================================
// 8. UNRECOVERABLE IS A STABLE STATE (ignores node reports)
// ===========================================================================

// TestUnrecoverable_IgnoresNodeReports: no node report moves out of Unrecoverable,
// and no spurious pending operations are generated.
func TestUnrecoverable_IgnoresNodeReports(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	drainPending(sm)

	reports := []qviews.QueryViewAtWorkNode{
		snReport(view, qviews.QueryViewStateReady),
		snReport(view, qviews.QueryViewStateUp),
		snReport(view, qviews.QueryViewStateDown),
		snReport(view, qviews.QueryViewStateDropped),
		snReport(view, qviews.QueryViewStatePreparing),
		qnReport(view, 1, qviews.QueryViewStateReady),
		qnReport(view, 2, qviews.QueryViewStateUnrecoverable),
		qnReport(view, 1, qviews.QueryViewStateDropped),
	}

	for _, report := range reports {
		sm.OnNodeStateReported(report)
		assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
		assertNoPending(t, sm)
	}
}

// ===========================================================================
// 9. DOWN STATE IGNORES QN REPORTS
// ===========================================================================

// TestDown_QNReportIgnored: QN reports in Down state produce no transition
// and no pending operations.
func TestDown_QNReportIgnored(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Down
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	drainPending(sm)
	sm.EnterDown()
	drainPending(sm)

	// QN reports should not affect state or produce pending
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoPending(t, sm)

	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertNoPending(t, sm)
}

// ===========================================================================
// 10. READY STATE IGNORES QN REPORTS
// ===========================================================================

// TestReady_QNReportIgnored: QN reports (non-Unrecoverable) in Ready state
// don't trigger transitions or pending operations.
func TestReady_QNReportIgnored(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Ready
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	drainPending(sm)

	// QN report in Ready state — ignored
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPending(t, sm)
}

// ===========================================================================
// 11. UP STATE: NON-UNRECOVERABLE REPORTS PRODUCE NO PENDING
// ===========================================================================

// TestUp_NormalReports_NoPending: in Up state, non-Unrecoverable node
// reports produce no state change and no pending operations.
func TestUp_NormalReports_NoPending(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Up
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	drainPending(sm)

	// Various non-Unrecoverable reports
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assertNoPending(t, sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assertNoPending(t, sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assertNoPending(t, sm)

	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
}

// ===========================================================================
// 12. QN READY SEGMENTS TRACKING
// ===========================================================================

// TestQNReadySegments_TrackedDuringPreparing validates that ready segments
// reported by QNs are tracked and no spurious pending is generated.
func TestQNReadySegments_TrackedDuringPreparing(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady, 100, 101))
	assertNoPending(t, sm)
	assert.Equal(t, []int64{100, 101}, sm.QNReadySegments()[1])

	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady, 200))
	assertNoPending(t, sm)
	assert.Equal(t, []int64{100, 101}, sm.QNReadySegments()[1])
	assert.Equal(t, []int64{200}, sm.QNReadySegments()[2])
}

// TestQNReadySegments_UpdatedOnReReport validates re-reports update segments.
func TestQNReadySegments_UpdatedOnReReport(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady, 100))
	assert.Equal(t, []int64{100}, sm.QNReadySegments()[1])

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady, 100, 101, 102))
	assert.Equal(t, []int64{100, 101, 102}, sm.QNReadySegments()[1])
}

// ===========================================================================
// 13. VIEW ACCESSOR
// ===========================================================================

// TestView_ReturnsSameReference ensures View() returns the original proto.
func TestView_ReturnsSameReference(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	assert.Equal(t, view, sm.View())
}

// ===========================================================================
// 14. NEW STATE MACHINE INITIAL STATE
// ===========================================================================

// TestNewStateMachine_InitialState validates all initial properties and pending.
func TestNewStateMachine_InitialState(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)

	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assert.Equal(t, view, sm.View())
	assert.NotNil(t, sm.QNReadySegments())
	assert.Len(t, sm.QNReadySegments(), 0)

	// Both pending set to Preparing
	assertPendingPersistState(t, sm, qviews.QueryViewStatePreparing)
	assertPendingSyncState(t, sm, qviews.QueryViewStatePreparing)

	// After consuming, both are nil
	assertNoPending(t, sm)
}

// ===========================================================================
// 15. EDGE CASES
// ===========================================================================

// TestNoQN_NormalFlow validates state machine with zero query nodes.
func TestNoQN_NormalFlow(t *testing.T) {
	view := buildTestView(0)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// SN Ready → all ready (no QNs to wait for) → Ready
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)

	// SN Up → Up
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUp)
	assertNoPendingSync(t, sm)
}

// TestNoQN_DroppingOnlySN validates zero QN Dropping→Dropped needs only SN.
func TestNoQN_DroppingOnlySN(t *testing.T) {
	view := buildTestView(0)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Fast path to Dropping
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	drainPending(sm)
	sm.EnterDropping()
	drainPending(sm)

	// SN Dropped → all dropped (no QNs) → Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingSync(t, sm)
}

// TestPreparing_QNReportsBeforeSN validates QN reports accumulate with no
// pending until SN is Ready.
func TestPreparing_QNReportsBeforeSN(t *testing.T) {
	view := buildTestView(3)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	for i := 1; i <= 3; i++ {
		sm.OnNodeStateReported(qnReport(view, int64(i), qviews.QueryViewStateReady))
		assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
		assertNoPending(t, sm)
	}

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)
}

// TestDown_UnrecoverableFromSN_NotHandled: Down handler only checks SN Down,
// so SN Unrecoverable triggers re-push Down (not Unrecoverable transition).
func TestDown_UnrecoverableFromSN(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Down
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	drainPending(sm)
	sm.EnterDown()
	drainPending(sm)

	// SN Unrecoverable → Unrecoverable (persist, wait for Manager).
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)
}

func TestDown_UnrecoverableFromQN(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Advance to Down
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	drainPending(sm)
	sm.EnterDown()
	drainPending(sm)

	// QN Unrecoverable → Unrecoverable (persist, wait for Manager).
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)
}

// TestDropping_PartialDropped_StaysDropping: verifies incremental Dropped
// reports produce no pending until last node completes.
func TestDropping_PartialDropped_StaysDropping(t *testing.T) {
	view := buildTestView(3)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	drainPending(sm)
	sm.EnterDropping()
	drainPending(sm)

	// Each Dropped node — no transition, no pending
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPending(t, sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPending(t, sm)

	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPending(t, sm)

	// Last node → Dropped
	sm.OnNodeStateReported(qnReport(view, 3, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingSync(t, sm)
}

// TestPendingSync_PreservesMeta validates pending sync views preserve metadata.
func TestPendingSync_PreservesMeta(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	views := sm.ConsumeSync()
	require.NotEmpty(t, views)
	expectedVersion := qviews.FromProtoQueryViewVersion(view.Meta.Version)
	for _, v := range views {
		assert.Equal(t, expectedVersion, v.Version())
	}
}

// TestPendingPersist_PreservesMeta validates pending persist views preserve metadata.
func TestPendingPersist_PreservesMeta(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	persist := sm.ConsumePersist()
	require.NotNil(t, persist)
	assert.Equal(t, view.Meta.CollectionId, persist.Meta.CollectionId)
	assert.Equal(t, view.Meta.ReplicaId, persist.Meta.ReplicaId)
	assert.Equal(t, view.Meta.Vchannel, persist.Meta.Vchannel)
}

// TestViewWithState_IsClone verifies returned views are clones, not references.
func TestViewWithState_IsClone(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)

	persist := sm.ConsumePersist()
	require.NotNil(t, persist)

	persist.Meta.CollectionId = 999
	assert.Equal(t, testCollectionID, sm.View().Meta.CollectionId)
}

// ===========================================================================
// 16. COMPLETE LIFECYCLE INTEGRATION TESTS
// ===========================================================================

// TestCompleteLifecycle_3QN_ErrorRecovery: full cycle with 3 QNs where QN2
// fails during Preparing, Consume verified at every step.
func TestCompleteLifecycle_3QN_ErrorRecovery(t *testing.T) {
	view := buildTestView(3)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// QN1 Ready — no transition
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)

	// QN2 Unrecoverable → Unrecoverable
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateUnrecoverable))
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateUnrecoverable)
	assertNoPendingSync(t, sm)

	// Manager → EnterDropping
	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)

	// SN, QN1, QN2 Dropped — QN3 pending
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assertNoPending(t, sm)
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assertNoPending(t, sm)
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateDropped))
	assertNoPending(t, sm)
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())

	// QN3 Dropped → all Dropped → Dropped
	sm.OnNodeStateReported(qnReport(view, 3, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingSync(t, sm)
}

// TestCompleteLifecycle_UpThenRecovery: Coord crash recovery from Up, then
// normal Down→Dropping→Dropped with Consume at every step.
func TestCompleteLifecycle_UpThenRecovery(t *testing.T) {
	view := buildTestView(2)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUp

	sm := RecoverCoordQueryViewStateMachine(view)
	assert.Equal(t, qviews.QueryViewStateUp, sm.State())
	assertNoPending(t, sm)

	// EnterDown
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDown, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDown)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDown)

	// SN Down → Dropping
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
	assert.Equal(t, qviews.QueryViewStateDropping, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateDropped)

	// All Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	assertNoPending(t, sm)
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assertNoPending(t, sm)

	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertPendingPersistState(t, sm, qviews.QueryViewStateDropped)
	assertNoPendingSync(t, sm)
}

// ===========================================================================
// 17. DROPPED STATE IS TERMINAL
// ===========================================================================

// TestDropped_IsTerminal: no operations produce state change or pending.
func TestDropped_IsTerminal(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	// Drive to Dropped
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUnrecoverable))
	drainPending(sm)
	sm.EnterDropping()
	drainPending(sm)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	drainPending(sm)

	// EnterDown — no-op
	sm.EnterDown()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoPending(t, sm)

	// EnterDropping — no-op
	sm.EnterDropping()
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoPending(t, sm)

	// Node report — no-op
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoPending(t, sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateDropped, sm.State())
	assertNoPending(t, sm)
}

// ===========================================================================
// 18. SN PREPARING DOES NOT ADVANCE STATE
// ===========================================================================

// TestPreparing_SNPreparing_NoTransition: SN Preparing does not advance.
func TestPreparing_SNPreparing_NoTransition(t *testing.T) {
	view := buildTestView(1)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assertNoPending(t, sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStatePreparing))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)
}

// ===========================================================================
// 19. ORDERING OF NODE REPORTS
// ===========================================================================

// TestPreparing_SNReadyBeforeAllQN: SN Ready arrives first.
func TestPreparing_SNReadyBeforeAllQN(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	assertNoPending(t, sm)

	// Last QN triggers transition
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)
}

// TestPreparing_AllQNReadyThenSNReady: all QNs Ready before SN.
func TestPreparing_AllQNReadyThenSNReady(t *testing.T) {
	view := buildTestView(2)
	sm := NewCoordQueryViewStateMachine(view)
	drainPending(sm)

	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	assertNoPending(t, sm)
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	assertNoPending(t, sm)
	assert.Equal(t, qviews.QueryViewStatePreparing, sm.State())

	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	assert.Equal(t, qviews.QueryViewStateReady, sm.State())
	assertNoPendingPersist(t, sm)
	assertPendingSyncState(t, sm, qviews.QueryViewStateUp)
}
