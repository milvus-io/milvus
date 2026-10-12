package snview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// snQueryViewStateMachine manages the lifecycle state machine of a single
// query view on a StreamingNode.
//
// The SN is a follower: it responds to Coord pushes and local events.
// The state machine is purely in-memory and non-blocking.
// I/O (reporting to Coord, persistence, resource release) is signaled through
// pending protos consumed via ConsumeReport / ConsumePersist / ConsumeRelease.
//
// The SN stores the complete shard view it receives from Coord. Its local
// resources are described by QueryViewOfStreamingNode, while QueryNode topology
// must be retained for query planning after SN crash recovery.
//
// State flow:
//
//	Normal:        Preparing → Ready → Up → Down → Dropping → Dropped
//	Error:         Preparing → Unrecoverable → Dropping → Dropped
//	Abort:         Preparing → Dropping → Dropped, Ready → Dropping → Dropped
//	Recovery:      UpRecovering → Up (WAL caught up)
//	Recovery err:  UpRecovering → Unrecoverable → Dropping → Dropped
//
// UpRecovering is a StreamingNode-only state (defined in proto but only used by SN).
// Coord sees UpRecovering as Up for state machine synchronization purposes.
//
// Thread-safety: NOT thread-safe. The caller must serialize access.
type snQueryViewStateMachine struct {
	state      qviews.QueryViewState
	meta       *viewpb.QueryViewMeta
	snView     *viewpb.QueryViewOfStreamingNode
	queryNodes []*viewpb.QueryViewOfQueryNode

	pendingReport  *viewpb.QueryViewOfShard
	pendingPersist *viewpb.QueryViewOfShard
	pendingRelease bool
}

// newSNQueryViewStateMachine creates a state machine when the SN receives
// a Preparing push from Coord.
//
// After construction:
//   - ConsumeReport returns Preparing (acknowledge receipt to Coord).
func newSNQueryViewStateMachine(meta *viewpb.QueryViewMeta, snView *viewpb.QueryViewOfStreamingNode, queryNodes []*viewpb.QueryViewOfQueryNode) *snQueryViewStateMachine {
	sm := &snQueryViewStateMachine{
		state:      qviews.QueryViewStatePreparing,
		meta:       proto.Clone(meta).(*viewpb.QueryViewMeta),
		snView:     cloneStreamingNodeView(snView),
		queryNodes: cloneQueryNodeViews(queryNodes),
	}
	sm.pendingReport = sm.buildReport()
	return sm
}

// recoverSNQueryViewStateMachine reconstructs a state machine from a
// persisted Up view after SN crash recovery.
//
// After construction:
//   - State is UpRecovering (WAL must catch up before serving).
//   - No pendingReport (don't report until WAL catches up).
//   - No pendingPersist (already persisted as Up).
func recoverSNQueryViewStateMachine(meta *viewpb.QueryViewMeta, snView *viewpb.QueryViewOfStreamingNode, queryNodes []*viewpb.QueryViewOfQueryNode) *snQueryViewStateMachine {
	return &snQueryViewStateMachine{
		state:      qviews.QueryViewStateUpRecovering,
		meta:       proto.Clone(meta).(*viewpb.QueryViewMeta),
		snView:     cloneStreamingNodeView(snView),
		queryNodes: cloneQueryNodeViews(queryNodes),
	}
}

// State returns the current state of the query view.
func (sm *snQueryViewStateMachine) State() qviews.QueryViewState {
	return sm.state
}

// IsRecovering returns true if the SN is in UpRecovering state.
func (sm *snQueryViewStateMachine) IsRecovering() bool {
	return sm.state == qviews.QueryViewStateUpRecovering
}

// Meta returns the query view meta.
func (sm *snQueryViewStateMachine) Meta() *viewpb.QueryViewMeta {
	return proto.Clone(sm.meta).(*viewpb.QueryViewMeta)
}

// SNView returns the StreamingNode-local portion of the retained shard view.
func (sm *snQueryViewStateMachine) SNView() *viewpb.QueryViewOfStreamingNode {
	return cloneStreamingNodeView(sm.snView)
}

// QueryNodes returns the query-node topology retained for query planning.
func (sm *snQueryViewStateMachine) QueryNodes() []*viewpb.QueryViewOfQueryNode {
	return cloneQueryNodeViews(sm.queryNodes)
}

// UpdateView records the latest complete shard topology pushed by Coord. State
// transitions still use the state machine's current state; incoming Meta.State
// is only a command input and is overwritten by buildReport/buildDroppedPersist.
func (sm *snQueryViewStateMachine) UpdateView(view *viewpb.QueryViewOfShard) {
	if view == nil {
		return
	}
	if view.GetMeta() != nil {
		sm.meta = proto.Clone(view.GetMeta()).(*viewpb.QueryViewMeta)
	}
	if view.GetStreamingNode() != nil {
		sm.snView = cloneStreamingNodeView(view.GetStreamingNode())
	}
	sm.queryNodes = cloneQueryNodeViews(view.GetQueryNode())
}

// OnCoordStateDelivered handles a state push from the Coordinator.
//
// In a distributed state machine, any Coord push must produce a response
// so that Coord can learn the node's current state and fast-forward.
// See design doc Section 1.1 (fast-forward logic) and Section 1.6 (Dropping).
func (sm *snQueryViewStateMachine) OnCoordStateDelivered(pushedState qviews.QueryViewState) {
	switch pushedState {
	case qviews.QueryViewStatePreparing:
		sm.handleCoordPreparing()
	case qviews.QueryViewStateUp:
		sm.handleCoordUp()
	case qviews.QueryViewStateDown:
		sm.handleCoordDown()
	case qviews.QueryViewStateDropped:
		sm.handleCoordDropped()
	}
}

// OnReady reports that async resource preparation completed successfully.
// Only valid in Preparing state; ignored in other states.
func (sm *snQueryViewStateMachine) OnReady() {
	if sm.state != qviews.QueryViewStatePreparing {
		return
	}
	sm.state = qviews.QueryViewStateReady
	sm.pendingReport = sm.buildReport()
}

// OnUnrecoverable reports a fatal error (e.g., WAL recovery OOM).
// Valid in Preparing and UpRecovering states; ignored in other states.
//
// Preparing: transitions to Unrecoverable and reports to Coord immediately.
//
// UpRecovering: transitions to Unrecoverable but does NOT report to Coord.
// The view is only marked as locally unavailable. Coord still believes it
// is Up. Discovery happens through the query path: the query planner
// detects the unavailable view and reports to Coord to generate a
// replacement. This avoids dependence on the OnReport callback (which
// may not be set during SN recovery) and prevents the irreversible
// cleanup cascade for what may be a transient failure. Persisted Up
// recovery info is retained for possible retry on SN restart.
func (sm *snQueryViewStateMachine) OnUnrecoverable() {
	switch sm.state {
	case qviews.QueryViewStatePreparing:
		sm.state = qviews.QueryViewStateUnrecoverable
		sm.pendingReport = sm.buildReport()
	case qviews.QueryViewStateUpRecovering:
		sm.state = qviews.QueryViewStateUnrecoverable
		// No pendingReport: Coord is not notified. The query path will
		// detect the unavailable view and trigger replacement.
		// No pendingPersist: persisted Up retained for retry on restart.
	}
}

// OnRecoveringDone reports that the WAL has caught up after crash recovery.
// Only valid in UpRecovering state; ignored in other states.
func (sm *snQueryViewStateMachine) OnRecoveringDone() {
	if sm.state != qviews.QueryViewStateUpRecovering {
		return
	}
	sm.state = qviews.QueryViewStateUp
	sm.pendingReport = sm.buildReport()
	// No pendingPersist: already persisted as Up before crash.
}

// OnDropped is called by the ResourceManager Release callback when resource
// release completes. Transitions Dropping → Dropped.
// Only valid in Dropping state; ignored in other states.
func (sm *snQueryViewStateMachine) OnDropped() {
	if sm.state != qviews.QueryViewStateDropping {
		return
	}
	sm.state = qviews.QueryViewStateDropped
	sm.pendingReport = sm.buildReport()
}

// ConsumeReport returns the view to report to the Coordinator and clears the flag.
// Returns nil if no report is needed.
func (sm *snQueryViewStateMachine) ConsumeReport() *viewpb.QueryViewOfShard {
	v := sm.pendingReport
	sm.pendingReport = nil
	return v
}

// ConsumePersist returns the view to persist for crash recovery and clears the flag.
// Returns nil if no persistence is needed.
//
// Persist semantics:
//   - Meta.State == Up → save/overwrite recovery info.
//   - Meta.State == Down, Unrecoverable, or Dropped → delete persisted recovery info.
func (sm *snQueryViewStateMachine) ConsumePersist() *viewpb.QueryViewOfShard {
	v := sm.pendingPersist
	sm.pendingPersist = nil
	return v
}

// ConsumeRelease returns true if the SM has a pending Release operation
// (i.e., entered Dropping state) and clears the flag.
func (sm *snQueryViewStateMachine) ConsumeRelease() bool {
	v := sm.pendingRelease
	sm.pendingRelease = false
	return v
}

// --- Coord push handlers ---

func (sm *snQueryViewStateMachine) handleCoordPreparing() {
	switch sm.state {
	case qviews.QueryViewStatePreparing:
		// Still Preparing: local events will drive progress. No re-report needed.
	case qviews.QueryViewStateUpRecovering:
		// Recovery: don't report yet (wait for WAL catch-up, then report Up
		// to allow Coord fast-forward). See design doc Section 2.4.
	case qviews.QueryViewStateDropping:
		// Already releasing resources, wait for OnDropped callback.
	default:
		// Node has advanced past Preparing: re-report current state so Coord
		// can fast-forward (e.g., Ready, Up, Down, Unrecoverable, Dropped).
		sm.pendingReport = sm.buildReport()
	}
}

func (sm *snQueryViewStateMachine) handleCoordUp() {
	switch sm.state {
	case qviews.QueryViewStateReady:
		// Normal transition: Ready → Up.
		sm.state = qviews.QueryViewStateUp
		sm.pendingReport = sm.buildReport()
		sm.pendingPersist = sm.buildReport()
	case qviews.QueryViewStateDropping:
		// Already releasing resources, wait for OnDropped callback.
	default:
		// Re-push or node has advanced/diverged: re-report current state
		// so Coord can fast-forward.
		sm.pendingReport = sm.buildReport()
	}
}

func (sm *snQueryViewStateMachine) handleCoordDown() {
	switch sm.state {
	case qviews.QueryViewStateUp:
		sm.state = qviews.QueryViewStateDown
		sm.pendingReport = sm.buildReport()
		sm.pendingPersist = sm.buildReport()
	case qviews.QueryViewStateUpRecovering:
		sm.state = qviews.QueryViewStateDown
		sm.pendingReport = sm.buildReport()
		sm.pendingPersist = sm.buildReport()
	case qviews.QueryViewStateDropping:
		// Already releasing resources, wait for OnDropped callback.
	default:
		// Re-push or node has advanced/diverged: re-report current state
		// so Coord can fast-forward.
		sm.pendingReport = sm.buildReport()
	}
}

func (sm *snQueryViewStateMachine) handleCoordDropped() {
	switch sm.state {
	case qviews.QueryViewStateDropping:
		// Already releasing resources, wait for OnDropped callback.
		return
	case qviews.QueryViewStateDropped:
		// Terminal state: re-report for Coord fast-forward.
		sm.pendingReport = sm.buildReport()
		return
	case qviews.QueryViewStateUp, qviews.QueryViewStateUpRecovering, qviews.QueryViewStateUnrecoverable:
		// Up/UpRecovering have persisted recovery info that must be deleted.
		// Unrecoverable may have entered from UpRecovering (persisted Up still
		// on disk since OnUnrecoverable intentionally retains it). Catalog
		// delete is idempotent, so safe for Preparing→Unrecoverable too.
		sm.state = qviews.QueryViewStateDropping
		sm.pendingReport = nil
		sm.pendingRelease = true
		sm.pendingPersist = sm.buildDroppedPersist()
	default:
		// Preparing/Ready/Down: no persisted recovery info.
		// Transition to Dropping: signal that Release should be called.
		// Clear any stale pending report.
		sm.state = qviews.QueryViewStateDropping
		sm.pendingReport = nil
		sm.pendingRelease = true
	}
}

// --- Helpers ---

// coordVisibleState returns the state visible to Coord.
// UpRecovering maps to Up (Coord is unaware of UpRecovering).
// Dropping maps to Dropping (Coord understands Dropping).
func (sm *snQueryViewStateMachine) coordVisibleState() qviews.QueryViewState {
	if sm.state == qviews.QueryViewStateUpRecovering {
		return qviews.QueryViewStateUp
	}
	return sm.state
}

// buildReport constructs a QueryViewOfShard report from the SN's current state.
// The report uses the Coord-visible state (UpRecovering → Up).
func (sm *snQueryViewStateMachine) buildReport() *viewpb.QueryViewOfShard {
	meta := proto.Clone(sm.meta).(*viewpb.QueryViewMeta)
	meta.State = viewpb.QueryViewState(sm.coordVisibleState())

	return &viewpb.QueryViewOfShard{
		Meta:          meta,
		QueryNode:     cloneQueryNodeViews(sm.queryNodes),
		StreamingNode: cloneStreamingNodeView(sm.snView),
	}
}

// buildDroppedPersist constructs a persist proto with Dropped state for deletion.
// Used when transitioning from Up/UpRecovering to Dropping to delete recovery info.
func (sm *snQueryViewStateMachine) buildDroppedPersist() *viewpb.QueryViewOfShard {
	meta := proto.Clone(sm.meta).(*viewpb.QueryViewMeta)
	meta.State = viewpb.QueryViewState(qviews.QueryViewStateDropped)

	return &viewpb.QueryViewOfShard{
		Meta:          meta,
		QueryNode:     cloneQueryNodeViews(sm.queryNodes),
		StreamingNode: cloneStreamingNodeView(sm.snView),
	}
}

func cloneStreamingNodeView(view *viewpb.QueryViewOfStreamingNode) *viewpb.QueryViewOfStreamingNode {
	if view == nil {
		return nil
	}
	return proto.Clone(view).(*viewpb.QueryViewOfStreamingNode)
}

func cloneQueryNodeViews(views []*viewpb.QueryViewOfQueryNode) []*viewpb.QueryViewOfQueryNode {
	if len(views) == 0 {
		return nil
	}
	out := make([]*viewpb.QueryViewOfQueryNode, 0, len(views))
	for _, view := range views {
		if view == nil {
			continue
		}
		out = append(out, proto.Clone(view).(*viewpb.QueryViewOfQueryNode))
	}
	return out
}
