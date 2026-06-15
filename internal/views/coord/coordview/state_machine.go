package coordview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// CoordQueryViewStateMachine manages the lifecycle state machine of a single
// query view on the Coordinator.
//
// The state machine is purely in-memory and non-blocking.
// All I/O (ETCD persistence, node sync) is signaled through pending proto
// consumed by the Manager via ConsumePersist / ConsumeSync.
//
// State flow:
//
//	Normal:  Preparing → Ready → Up → Down → Dropping → Dropped
//	Error:   Preparing/Ready/Up → Unrecoverable → Dropping → Dropped
//
// Unrecoverable is a stable state. The Manager decides when to advance to
// Dropping (typically after generating a replacement view) via EnterDropping.
//
// Thread-safety: NOT thread-safe. The caller (Manager) must serialize access.
type CoordQueryViewStateMachine struct {
	state qviews.QueryViewState
	view  *viewpb.QueryViewOfShard

	// Per-node reported states.
	snState  qviews.QueryViewState
	qnStates map[int64]qviews.QueryViewState

	// Per-QN ready segment IDs reported during Preparing.
	// Used by Balancer/Manager for progress tracking and decision-making.
	qnReadySegments map[int64][]int64

	// Pending I/O signals, consumed once by Manager.
	pendingPersist *viewpb.QueryViewOfShard
	pendingSync    []qviews.QueryViewAtWorkNode
}

// NewCoordQueryViewStateMachine creates a state machine for a freshly
// generated query view.
//
// After construction:
//   - ConsumePersist returns the Preparing view (write-ahead).
//   - ConsumeSync returns the Preparing view (push to all nodes).
func NewCoordQueryViewStateMachine(view *viewpb.QueryViewOfShard) *CoordQueryViewStateMachine {
	sm := &CoordQueryViewStateMachine{
		state:           qviews.QueryViewStatePreparing,
		view:            view,
		snState:         qviews.QueryViewStateNil,
		qnStates:        make(map[int64]qviews.QueryViewState, len(view.QueryNode)),
		qnReadySegments: make(map[int64][]int64, len(view.QueryNode)),
	}
	for _, qn := range view.QueryNode {
		sm.qnStates[qn.NodeId] = qviews.QueryViewStateNil
	}
	sm.pendingPersist = sm.viewWithState(qviews.QueryViewStatePreparing)
	sm.pendingSync = sm.syncViewsForState(qviews.QueryViewStatePreparing)
	return sm
}

// RecoverCoordQueryViewStateMachine reconstructs a state machine from a view
// loaded from ETCD during Coordinator crash recovery.
//
// Recovery behavior by persisted state:
//   - Preparing:     re-push Preparing to all nodes.
//   - Up:            no pending (wait for events).
//   - Down:          re-push Down to SN.
//   - Unrecoverable: stays Unrecoverable, waits for Manager to call EnterDropping.
func RecoverCoordQueryViewStateMachine(view *viewpb.QueryViewOfShard) *CoordQueryViewStateMachine {
	recoveredState := qviews.QueryViewState(view.Meta.State)
	sm := &CoordQueryViewStateMachine{
		state:           recoveredState,
		view:            view,
		snState:         qviews.QueryViewStateNil,
		qnStates:        make(map[int64]qviews.QueryViewState, len(view.QueryNode)),
		qnReadySegments: make(map[int64][]int64, len(view.QueryNode)),
	}
	for _, qn := range view.QueryNode {
		sm.qnStates[qn.NodeId] = qviews.QueryViewStateNil
	}

	switch recoveredState {
	case qviews.QueryViewStatePreparing:
		// Already persisted; re-push to all nodes.
		sm.pendingSync = sm.syncViewsForState(qviews.QueryViewStatePreparing)
	case qviews.QueryViewStateUp:
		// Active view; no re-push needed. Up is persisted precisely to
		// avoid unnecessary Coord↔node communication on recovery.
	case qviews.QueryViewStateDown:
		// Re-push Down to SN.
		sm.pendingSync = sm.syncViewsForState(qviews.QueryViewStateDown)
	case qviews.QueryViewStateUnrecoverable:
		// Stable state; wait for Manager to call EnterDropping.
	default:
		panic("coordview: invalid recovered state: " + recoveredState.String())
	}
	return sm
}

// State returns the current in-memory state of the query view.
func (sm *CoordQueryViewStateMachine) State() qviews.QueryViewState {
	return sm.state
}

// View returns the original query view proto definition.
func (sm *CoordQueryViewStateMachine) View() *viewpb.QueryViewOfShard {
	return sm.view
}

// Version returns the parsed QueryViewVersion of this view.
func (sm *CoordQueryViewStateMachine) Version() qviews.QueryViewVersion {
	return qviews.FromProtoQueryViewVersion(sm.view.Meta.Version)
}

// QNReadySegments returns the ready segment IDs reported by each QN.
// The map is keyed by QN node ID. Used by Balancer/Manager for decisions.
func (sm *CoordQueryViewStateMachine) QNReadySegments() map[int64][]int64 {
	return sm.qnReadySegments
}

// OnNodeStateReported is called when a work node (SN or QN) reports its
// current state for this view via SyncQueryView response.
func (sm *CoordQueryViewStateMachine) OnNodeStateReported(report qviews.QueryViewAtWorkNode) {
	sm.updateNodeState(report)

	switch sm.state {
	case qviews.QueryViewStatePreparing:
		sm.handlePreparing(report)
	case qviews.QueryViewStateReady:
		sm.handleReady(report)
	case qviews.QueryViewStateUp:
		sm.handleUp(report)
	case qviews.QueryViewStateDown:
		sm.handleDown(report)
	case qviews.QueryViewStateDropping:
		sm.handleDropping(report)
	}
}

// EnterUnrecoverable is called by the Manager to force this view into
// Unrecoverable state. Used for preemption and RequestRelease.
// Valid from Preparing, Ready, or Up. No-op in other states.
func (sm *CoordQueryViewStateMachine) EnterUnrecoverable() {
	switch sm.state {
	case qviews.QueryViewStatePreparing, qviews.QueryViewStateReady, qviews.QueryViewStateUp:
		sm.transitionToUnrecoverable()
	}
}

// OnQueryNodeLost is called by the Manager when QueryNode service discovery
// removes a QueryNode targeted by this view.
//
// StreamingNode loss is intentionally not modeled here: SN availability is
// handled by the channel assignment layer, not by the per-view state machine.
func (sm *CoordQueryViewStateMachine) OnQueryNodeLost(node qviews.QueryNode) {
	if _, ok := sm.qnStates[node.ID]; !ok {
		return
	}

	switch sm.state {
	case qviews.QueryViewStatePreparing:
		sm.transitionToUnrecoverable()
	case qviews.QueryViewStateDropping:
		sm.qnStates[node.ID] = qviews.QueryViewStateDropped
		if sm.allNodesDropped() {
			sm.state = qviews.QueryViewStateDropped
			sm.pendingPersist = sm.viewWithState(qviews.QueryViewStateDropped)
		}
	}
}

// EnterDown is called by the Manager to transition this view from Up to Down.
// Triggers: higher-version view is Up (Manager decision), or ReleaseCollection.
// No-op if not in Up state.
func (sm *CoordQueryViewStateMachine) EnterDown() {
	if sm.state != qviews.QueryViewStateUp {
		return
	}
	sm.state = qviews.QueryViewStateDown
	sm.pendingPersist = sm.viewWithState(qviews.QueryViewStateDown)
	sm.pendingSync = sm.syncViewsForState(qviews.QueryViewStateDown)
}

// EnterDropping is called by the Manager to transition this view from
// Unrecoverable to Dropping. The Manager typically calls this after
// generating a replacement view, so both views can be pushed atomically.
// No-op if not in Unrecoverable state.
func (sm *CoordQueryViewStateMachine) EnterDropping() {
	if sm.state != qviews.QueryViewStateUnrecoverable {
		return
	}
	sm.state = qviews.QueryViewStateDropping
	sm.pendingSync = sm.syncViewsForState(qviews.QueryViewStateDropped)
}

// ConsumePersist returns the view to persist to ETCD and clears the flag.
// Returns nil if no persistence is needed.
//
//   - Meta.State == Dropped → Manager should delete from ETCD.
//   - Meta.State == other   → Manager should save/overwrite in ETCD.
func (sm *CoordQueryViewStateMachine) ConsumePersist() *viewpb.QueryViewOfShard {
	v := sm.pendingPersist
	sm.pendingPersist = nil
	return v
}

// ConsumeSync returns the per-node views to push and clears the flag.
// Returns nil if no sync is needed.
func (sm *CoordQueryViewStateMachine) ConsumeSync() []qviews.QueryViewAtWorkNode {
	v := sm.pendingSync
	sm.pendingSync = nil
	return v
}

// --- State handlers ---

// Preparing: wait for all nodes to report Ready.
//   - Any Unrecoverable → Unrecoverable (persist, wait for Manager).
//   - All ready, SN=Ready → Ready (push Up to SN).
//   - All ready, SN=Up (recovery fast-forward) → Up (persist Up).
func (sm *CoordQueryViewStateMachine) handlePreparing(report qviews.QueryViewAtWorkNode) {
	if report.State() == qviews.QueryViewStateUnrecoverable {
		sm.transitionToUnrecoverable()
		return
	}
	if !sm.allNodesReady() {
		return
	}
	if sm.snState == qviews.QueryViewStateUp {
		// Fast-forward: SN already Up from recovery.
		sm.state = qviews.QueryViewStateUp
		sm.pendingPersist = sm.viewWithState(qviews.QueryViewStateUp)
	} else {
		// Normal flow: all Ready → Ready, push Up to SN.
		sm.state = qviews.QueryViewStateReady
		sm.pendingSync = sm.syncViewsForState(qviews.QueryViewStateUp)
	}
}

// Ready: wait for SN to confirm Up.
//   - Any Unrecoverable → Unrecoverable.
//   - SN reports Up → Up (persist Up).
//   - SN not Up → re-push Up.
func (sm *CoordQueryViewStateMachine) handleReady(report qviews.QueryViewAtWorkNode) {
	if report.State() == qviews.QueryViewStateUnrecoverable {
		sm.transitionToUnrecoverable()
		return
	}
	if !sm.isSNReport(report) {
		return
	}
	if report.State() == qviews.QueryViewStateUp {
		sm.state = qviews.QueryViewStateUp
		sm.pendingPersist = sm.viewWithState(qviews.QueryViewStateUp)
		return
	}
	// SN not Up yet (e.g., still Ready) → re-push Up.
	sm.pendingSync = sm.syncViewsForState(qviews.QueryViewStateUp)
}

// Up: active view serving queries.
//   - Any Unrecoverable → Unrecoverable.
//   - Up → Down is handled by EnterDown, not node reports.
func (sm *CoordQueryViewStateMachine) handleUp(report qviews.QueryViewAtWorkNode) {
	if report.State() == qviews.QueryViewStateUnrecoverable {
		sm.transitionToUnrecoverable()
	}
}

// Down: wait for SN to confirm Down.
//   - Any Unrecoverable → Unrecoverable (persist, wait for Manager).
//   - SN reports Down or Dropped → Dropping (push Dropped to all).
//     Dropped means the SN already advanced past Down (e.g., Coord crash
//     recovery regressed from Dropping to Down). Treat it the same as Down.
//   - SN not Down → re-push Down.
func (sm *CoordQueryViewStateMachine) handleDown(report qviews.QueryViewAtWorkNode) {
	if report.State() == qviews.QueryViewStateUnrecoverable {
		sm.transitionToUnrecoverable()
		return
	}
	if !sm.isSNReport(report) {
		return
	}
	if report.State() == qviews.QueryViewStateDown || report.State() == qviews.QueryViewStateDropped {
		sm.state = qviews.QueryViewStateDropping
		sm.pendingSync = sm.syncViewsForState(qviews.QueryViewStateDropped)
		return
	}
	// SN not Down yet → re-push Down.
	sm.pendingSync = sm.syncViewsForState(qviews.QueryViewStateDown)
}

// Dropping: wait for all nodes to confirm Dropped.
//   - All Dropped → Dropped (delete from ETCD).
//   - Node not Dropped → re-push Dropped.
func (sm *CoordQueryViewStateMachine) handleDropping(report qviews.QueryViewAtWorkNode) {
	if report.State() == qviews.QueryViewStateDropped {
		if sm.allNodesDropped() {
			sm.state = qviews.QueryViewStateDropped
			sm.pendingPersist = sm.viewWithState(qviews.QueryViewStateDropped)
		}
		return
	}
	// Node not Dropped → re-push Dropped.
	sm.pendingSync = sm.syncViewsForState(qviews.QueryViewStateDropped)
}

// transitionToUnrecoverable persists Unrecoverable and stays.
// The Manager must call EnterDropping to advance to Dropping.
func (sm *CoordQueryViewStateMachine) transitionToUnrecoverable() {
	sm.state = qviews.QueryViewStateUnrecoverable
	sm.pendingPersist = sm.viewWithState(qviews.QueryViewStateUnrecoverable)
}

// --- Helpers ---

// syncViewsForState builds per-node QueryViewAtWorkNode slices for the given state.
// SN is always included. QNs are included for Preparing and Dropped (states that
// require all nodes to acknowledge), excluded for Up and Down (SN-only transitions).
func (sm *CoordQueryViewStateMachine) syncViewsForState(state qviews.QueryViewState) []qviews.QueryViewAtWorkNode {
	includeQN := state != qviews.QueryViewStateUp && state != qviews.QueryViewStateDown

	meta := proto.Clone(sm.view.Meta).(*viewpb.QueryViewMeta)
	meta.State = viewpb.QueryViewState(state)

	cap := 1
	if includeQN {
		cap += len(sm.view.QueryNode)
	}
	views := make([]qviews.QueryViewAtWorkNode, 0, cap)
	views = append(views, qviews.NewQueryViewAtStreamingNode(meta, sm.view.StreamingNode))
	if includeQN {
		for _, qn := range sm.view.QueryNode {
			views = append(views, qviews.NewQueryViewAtQueryNode(meta, qn))
		}
	}
	return views
}

func (sm *CoordQueryViewStateMachine) updateNodeState(report qviews.QueryViewAtWorkNode) {
	switch n := report.WorkNode().(type) {
	case qviews.QueryNode:
		sm.qnStates[n.ID] = report.State()
		sm.updateQNReadySegments(n.ID, report)
	case qviews.StreamingNode:
		sm.snState = report.State()
	}
}

func (sm *CoordQueryViewStateMachine) updateQNReadySegments(nodeID int64, report qviews.QueryViewAtWorkNode) {
	qnReport, ok := report.(*qviews.QueryViewAtQueryNode)
	if !ok {
		return
	}
	var readySegs []int64
	for _, p := range qnReport.ViewOfQueryNode().Partitions {
		readySegs = append(readySegs, p.ReadySegmentIds...)
	}
	sm.qnReadySegments[nodeID] = readySegs
}

func (sm *CoordQueryViewStateMachine) isSNReport(report qviews.QueryViewAtWorkNode) bool {
	_, ok := report.WorkNode().(qviews.StreamingNode)
	return ok
}

// allNodesReady returns true when SN is Ready or Up (recovery) and all QNs are Ready.
func (sm *CoordQueryViewStateMachine) allNodesReady() bool {
	if sm.snState != qviews.QueryViewStateReady && sm.snState != qviews.QueryViewStateUp {
		return false
	}
	for _, state := range sm.qnStates {
		if state != qviews.QueryViewStateReady {
			return false
		}
	}
	return true
}

func (sm *CoordQueryViewStateMachine) allNodesDropped() bool {
	if sm.snState != qviews.QueryViewStateDropped {
		return false
	}
	for _, state := range sm.qnStates {
		if state != qviews.QueryViewStateDropped {
			return false
		}
	}
	return true
}

// viewWithState clones the view and sets Meta.State to the given state.
func (sm *CoordQueryViewStateMachine) viewWithState(state qviews.QueryViewState) *viewpb.QueryViewOfShard {
	v := proto.Clone(sm.view).(*viewpb.QueryViewOfShard)
	v.Meta.State = viewpb.QueryViewState(state)
	return v
}
