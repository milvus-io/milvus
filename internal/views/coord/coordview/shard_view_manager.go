package coordview

import (
	"context"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ShardViewManager manages multiple QueryViews for a single shard (vchannel)
// within a single replica on the Coord side.
//
// It orchestrates CoordQueryViewStateMachine instances and their cross-view
// interactions. After each operation it emits one immutable shard-scoped dirty
// event; DirtyViewFlushScheduler owns all cross-shard batching and I/O.
//
// Invariants (maintained by all methods):
//   - At most one view in Preparing or Ready state (tracked by preparingView).
//   - At most one view in Up state (tracked by upView).
//
// Thread-safety: All methods are thread-safe.
type ShardViewManager struct {
	ctx              context.Context
	mu               sync.Mutex
	shardID          qviews.ShardID
	eventSubmitter   dirtyViewEventSubmitter
	observe          func(qviews.ShardID, *ShardViewManager, *ShardStats)
	onReleasedEmpty  func(qviews.ShardID, *ShardViewManager)
	releaseRequested bool

	// All active views keyed by version for O(1) lookup.
	views map[qviews.QueryViewVersion]*CoordQueryViewStateMachine

	// Fast pointers to the unique Preparing/Ready and Up views.
	// Invariant: at most one of each at any time.
	preparingView *CoordQueryViewStateMachine // Preparing or Ready state; nil if none
	upView        *CoordQueryViewStateMachine // Up state; nil if none

	// Accumulates persist and sync operations within a single lock-hold scope.
	// The accumulated effects are moved into one immutable dirtyViewEvent before
	// the manager releases the lock.
	// Must only be accessed under m.mu.
	pendingPersists []*viewpb.QueryViewOfShard
	pendingSyncs    []syncEntry
	pendingRemovals []*CoordQueryViewStateMachine
}

// syncEntry pairs a state machine with its per-node views for deferred event submission.
type syncEntry struct {
	sm    *CoordQueryViewStateMachine
	views []qviews.QueryViewAtWorkNode
}

// newShardViewManager creates a new ShardViewManager for the given shard.
//
// ctx is the lifecycle context used by callbacks and event observation.
// recoveredViews are views loaded from ETCD during crash recovery.
// Unrecoverable views remain Unrecoverable after construction, waiting for
// AddPreparing or RequestRelease to advance them to Dropping.
// Active views in other states are emitted through eventSubmitter for the
// DirtyViewFlushScheduler to persist and push to their target nodes.
func newShardViewManager(
	ctx context.Context,
	shardID qviews.ShardID,
	eventSubmitter dirtyViewEventSubmitter,
	recoveredViews []*viewpb.QueryViewOfShard,
) *ShardViewManager {
	m := &ShardViewManager{
		ctx:            ctx,
		shardID:        shardID,
		eventSubmitter: eventSubmitter,
		views:          make(map[qviews.QueryViewVersion]*CoordQueryViewStateMachine, len(recoveredViews)),
	}

	// Recover state machines from persisted views.
	recovered := make([]*CoordQueryViewStateMachine, 0, len(recoveredViews))
	for _, view := range recoveredViews {
		sm := RecoverCoordQueryViewStateMachine(view)
		recovered = append(recovered, sm)
		m.views[sm.Version()] = sm
	}

	// Sort by version ascending (older versions first) so that
	// processStateMachine sees older views before newer ones,
	// correctly setting preparingView/upView pointers.
	sort.Slice(recovered, func(i, j int) bool {
		return recovered[j].Version().GT(recovered[i].Version())
	})

	// Process each recovered view: handle Unrecoverable and push initial syncs.
	// processStateMachine sets preparingView/upView as views are processed.
	for _, sm := range recovered {
		m.processStateMachine(sm)
	}
	m.submitDirtyEvent(m.consumeDirtyEventLocked())
	return m
}

// SetStatsObserver installs the per-shard stats observer.
//
// Precondition: the observer MUST be a lightweight, non-blocking operation.
// It is invoked synchronously while the manager lock (m.mu) is held, so it
// must not call back into this manager or the registry (deadlock), perform
// metadata I/O, or block on other goroutines.
func (m *ShardViewManager) SetStatsObserver(observer func(qviews.ShardID, *ShardViewManager, *ShardStats)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observe = observer
}

// setOnReleasedEmpty installs the callback invoked once RequestRelease has
// completed and the manager contains no QueryViews.
func (m *ShardViewManager) setOnReleasedEmpty(callback func(qviews.ShardID, *ShardViewManager)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onReleasedEmpty = callback
}

// Stats returns an atomic snapshot of this shard's current placement state.
//
// The returned snapshot includes placements from the Up view, any in-flight
// Preparing/Ready view, and Unrecoverable views that still need to be accounted
// as live placement until cleanup reaches Dropping.
//
// The returned maps/slices are freshly allocated; callers may retain and
// inspect them without holding the manager's lock.
func (m *ShardViewManager) Stats() *ShardStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.statsLocked()
}

func (m *ShardViewManager) statsLocked() *ShardStats {
	stats := &ShardStats{
		Segments: make(map[int64]*SegmentStats),
	}

	for _, sm := range m.views {
		baseState, ok := segmentStateFromViewState(sm.State())
		if !ok {
			continue
		}

		version := sm.Version()
		switch sm.State() {
		case qviews.QueryViewStateUp:
			if stats.UpVersion == nil || version.GT(*stats.UpVersion) {
				stats.UpVersion = &version
				stats.UpLoadInfoVersion = sm.View().GetMeta().GetLoadInfoVersion()
			}
		case qviews.QueryViewStatePreparing, qviews.QueryViewStateReady:
			if stats.PreparingVersion == nil || version.GT(*stats.PreparingVersion) {
				stats.PreparingVersion = &version
			}
		}

		fillSegments(stats.Segments, sm.View().GetQueryNode(), baseState, sm.QNReadySegments())
	}

	return stats
}

func segmentStateFromViewState(state qviews.QueryViewState) (SegmentState, bool) {
	switch state {
	case qviews.QueryViewStatePreparing:
		return SegmentStatePreparing, true
	case qviews.QueryViewStateReady:
		return SegmentStatePreparing, true
	case qviews.QueryViewStateDown:
		return SegmentStateReady, true
	case qviews.QueryViewStateUp:
		return SegmentStateUp, true
	case qviews.QueryViewStateUnrecoverable:
		return SegmentStateUnrecoverable, true
	default:
		return 0, false
	}
}

// fillSegments merges placements from one view's QueryNode list into the
// segmentID-keyed map. When multiple views mention the same segment on the
// same node, the most reusable state wins: Up > Ready > Preparing >
// Unrecoverable.
func fillSegments(segments map[int64]*SegmentStats, queryNodes []*viewpb.QueryViewOfQueryNode, baseState SegmentState, readySegments map[int64][]int64) {
	for _, qn := range queryNodes {
		nodeID := qn.GetNodeId()
		readySet := segmentSet(readySegments[nodeID])
		for _, p := range qn.GetPartitions() {
			partID := p.GetPartitionId()
			for _, segID := range p.GetSegmentIds() {
				state := baseState
				if state != SegmentStateUp && readySet[segID] {
					state = SegmentStateReady
				}
				segment := segments[segID]
				if segment == nil {
					segment = &SegmentStats{
						SegmentID:   segID,
						PartitionID: partID,
						Nodes:       make(map[int64]SegmentState),
					}
					segments[segID] = segment
				}
				mergeSegmentState(segment, nodeID, state)
			}
		}
	}
}

func mergeSegmentState(segment *SegmentStats, nodeID int64, state SegmentState) {
	current, ok := segment.Nodes[nodeID]
	if !ok || state > current {
		segment.Nodes[nodeID] = state
	}
}

func segmentSet(segments []int64) map[int64]bool {
	if len(segments) == 0 {
		return nil
	}
	out := make(map[int64]bool, len(segments))
	for _, segment := range segments {
		out[segment] = true
	}
	return out
}

// AddPreparing adds a new view in Preparing state from a builder.
//
// The manager assigns the QueryVersion automatically:
//   - If the DataVersion matches existing views, QV = max(existing QV for same DV) + 1.
//   - Otherwise, QV = 1.
//
// Preemption: If an existing view is in Preparing or Ready state, it is preempted
// (injected with synthetic Unrecoverable → Dropping).
//
// Validation: The new DataVersion must not be lower than any existing view's DataVersion.
func (m *ShardViewManager) AddPreparing(_ context.Context, builder *qviews.QueryViewAtCoordBuilder) error {
	m.mu.Lock()

	// A shard whose release has started must not be re-prepared: RequestRelease
	// has already torn down its views, and resurrecting a Preparing view would
	// fight the teardown. The balancer retries next round after the release
	// completes and the registry has evicted this manager.
	if m.releaseRequested {
		m.mu.Unlock()
		return merr.WrapErrServiceInternalMsg("shard %s is being released, cannot add preparing view", m.shardID.String())
	}

	newDV := builder.DataVersion()

	// Validate no DataVersion rollback.
	if err := m.validateDataVersionLocked(newDV); err != nil {
		m.mu.Unlock()
		return err
	}

	// Preempt existing Preparing/Ready view.
	if m.preparingView != nil {
		m.preparingView.EnterUnrecoverable()
		m.processStateMachine(m.preparingView)
		// preparingView is cleared by processStateMachine (Unrecoverable case).
	}

	// Advance all Unrecoverable views (preempted or naturally failed) to
	// Dropping so their Dropped sync is batched with the new Preparing sync.
	m.advanceUnrecoverableToDropping()

	// Compute and assign QueryVersion.
	qv := m.nextQueryVersion(newDV)
	builder.SetQueryVersion(qv)

	// Build the view proto and create the state machine.
	view := builder.Build()
	sm := NewCoordQueryViewStateMachine(view)
	m.views[sm.Version()] = sm
	m.preparingView = sm

	// Process: collect persist and sync effects.
	m.processStateMachine(sm)

	// Move all accumulated effects into one shard-scoped event.
	event := m.consumeDirtyEventLocked()
	m.publishStatsLocked()
	m.submitDirtyEvent(event)
	m.mu.Unlock()
	return nil
}

// RequestRelease initiates teardown of all views in this shard.
//
// - Up views: transition to Down (normal teardown via SN confirmation).
// - Preparing/Ready views: force Unrecoverable → Dropping (abort immediately).
// - Down/Dropping views: already tearing down, no-op.
// - Empty manager: notify the registry for immediate removal.
//
// This is the only operation that makes the manager eligible for registry
// removal. Cleanup of resident views completes asynchronously through callbacks.
func (m *ShardViewManager) RequestRelease(_ context.Context) error {
	m.mu.Lock()
	m.releaseRequested = true

	if m.preparingView != nil {
		m.preparingView.EnterUnrecoverable()
		m.processStateMachine(m.preparingView)
		// preparingView is cleared by processStateMachine (Unrecoverable case).
	}

	if m.upView != nil {
		m.upView.EnterDown()
		m.processStateMachine(m.upView)
		// processStateMachine's Down case clears m.upView.
	}

	// Advance all Unrecoverable views (preempted or naturally failed) to Dropping.
	m.advanceUnrecoverableToDropping()

	event := m.consumeDirtyEventLocked()
	m.publishStatsLocked()
	m.submitDirtyEvent(event)
	empty := len(m.views) == 0
	onReleasedEmpty := m.onReleasedEmpty
	m.mu.Unlock()

	if empty && onReleasedEmpty != nil {
		onReleasedEmpty(m.shardID, m)
	}
	return nil
}

// processStateMachine consumes pending I/O from a state machine and handles
// cascading effects (Up-then-Down, Unrecoverable→Dropping, Dropped removal).
// I/O is collected into pendingPersists/pendingSyncs for deferred event
// submission.
//
// Also maintains preparingView/upView pointers on state transitions.
//
// Must be called under m.mu.
func (m *ShardViewManager) processStateMachine(sm *CoordQueryViewStateMachine) {
	// 1. ConsumeFlush persist effect → collect into pending batch.
	flush := sm.ConsumeFlush()
	if flush.Persist != nil {
		m.pendingPersists = append(m.pendingPersists, flush.Persist)
	}

	// 2. ConsumeFlush sync effects → collect into pending batch.
	if len(flush.Sync) > 0 {
		m.pendingSyncs = append(m.pendingSyncs, syncEntry{sm: sm, views: flush.Sync})
	}

	// 3. Handle cascading effects based on current state.
	switch sm.State() {
	case qviews.QueryViewStatePreparing, qviews.QueryViewStateReady:
		m.preparingView = sm

	case qviews.QueryViewStateUp:
		if m.preparingView == sm {
			m.preparingView = nil
		}
		m.downOlderUpView(sm)
		m.upView = sm

	case qviews.QueryViewStateDown:
		if m.upView == sm {
			m.upView = nil
		}

	case qviews.QueryViewStateUnrecoverable:
		if m.preparingView == sm {
			m.preparingView = nil
		}
		if m.upView == sm {
			m.upView = nil
		}
		// Stay Unrecoverable; wait for AddPreparing or RequestRelease
		// to advance to Dropping so that Dropped sync and new Preparing
		// sync can be batched together.

	case qviews.QueryViewStateDropping:

	case qviews.QueryViewStateDropped:
		if !m.hasPendingRemoval(sm) {
			m.pendingRemovals = append(m.pendingRemovals, sm)
		}

	default:
	}
}

// advanceUnrecoverableToDropping advances all Unrecoverable views to Dropping.
// This batches the Dropped sync with whatever operation triggered it
// (AddPreparing or RequestRelease), reducing the number of sync round-trips.
//
// Must be called under m.mu.
func (m *ShardViewManager) advanceUnrecoverableToDropping() {
	for _, sm := range m.views {
		if sm.State() == qviews.QueryViewStateUnrecoverable {
			sm.EnterDropping()
			m.processStateMachine(sm)
		}
	}
}

// downOlderUpView transitions the current Up view to Down if it differs from newUp.
//
// Must be called under m.mu.
func (m *ShardViewManager) downOlderUpView(newUp *CoordQueryViewStateMachine) {
	if m.upView != nil && m.upView != newUp {
		m.upView.EnterDown()
		m.processStateMachine(m.upView)
		// processStateMachine's Down case clears m.upView.
	}
}

// consumeDirtyEventLocked moves the current operation's accumulated effects
// into an immutable shard event. Cross-shard merging and batch execution belong
// to DirtyViewFlushScheduler.
func (m *ShardViewManager) consumeDirtyEventLocked() dirtyViewEvent {
	event := dirtyViewEvent{
		shardID:  m.shardID,
		persists: m.pendingPersists,
	}
	for _, entry := range m.pendingSyncs {
		version := entry.sm.Version()
		for _, view := range entry.views {
			var onQueryNodeLost func(qviews.QueryNode)
			if _, ok := view.WorkNode().(qviews.QueryNode); ok {
				onQueryNodeLost = m.makeOnQueryNodeLost(version)
			}
			event.syncs = append(event.syncs, syncer.SyncView{
				View:            view,
				OnSyncResponse:  m.makeOnSyncResponse(version, view),
				OnQueryNodeLost: onQueryNodeLost,
			})
		}
	}
	for _, sm := range m.pendingRemovals {
		target := sm
		event.afterPersist = append(event.afterPersist, func() {
			m.finalizeRemoval(target)
		})
	}
	m.pendingPersists = nil
	m.pendingSyncs = nil
	m.pendingRemovals = nil
	return event
}

// makeOnSyncResponse creates a callback that processes node responses for a view sync.
//
// The callback acquires m.mu, calls sm.OnNodeStateReported, calls processStateMachine.
// Returns true when this node has completed the sync represented by target.
func (m *ShardViewManager) makeOnSyncResponse(version qviews.QueryViewVersion, target qviews.QueryViewAtWorkNode) func(resp qviews.QueryViewAtWorkNode) bool {
	return func(resp qviews.QueryViewAtWorkNode) bool {
		m.mu.Lock()

		sm, ok := m.views[version]
		if !ok {
			m.mu.Unlock()
			return true // view already removed, stop tracking
		}

		sm.OnNodeStateReported(resp)
		m.processStateMachine(sm)
		event := m.consumeDirtyEventLocked()
		m.publishStatsLocked()

		_, exists := m.views[version]
		completed := !exists || syncResponseCompletesTarget(target.State(), resp.State())
		m.submitDirtyEvent(event)
		m.mu.Unlock()
		return completed
	}
}

func syncResponseCompletesTarget(target, reported qviews.QueryViewState) bool {
	if reported == qviews.QueryViewStateUnrecoverable {
		return true
	}

	switch target {
	case qviews.QueryViewStatePreparing:
		return reported == qviews.QueryViewStateReady || reported == qviews.QueryViewStateUp
	case qviews.QueryViewStateUp:
		return reported == qviews.QueryViewStateUp
	case qviews.QueryViewStateDown:
		return reported == qviews.QueryViewStateDown || reported == qviews.QueryViewStateDropped
	case qviews.QueryViewStateDropped:
		return reported == qviews.QueryViewStateDropped
	default:
		return false
	}
}

func (m *ShardViewManager) makeOnQueryNodeLost(version qviews.QueryViewVersion) func(qviews.QueryNode) {
	return func(node qviews.QueryNode) {
		m.mu.Lock()

		sm, ok := m.views[version]
		if !ok {
			m.mu.Unlock()
			return // view already removed
		}

		sm.OnQueryNodeLost(node)
		m.processStateMachine(sm)
		event := m.consumeDirtyEventLocked()
		m.publishStatsLocked()
		m.submitDirtyEvent(event)
		m.mu.Unlock()
	}
}

func (m *ShardViewManager) submitDirtyEvent(event dirtyViewEvent) {
	if !event.empty() {
		m.eventSubmitter.Submit(event)
	}
}

func (m *ShardViewManager) publishStatsLocked() {
	if m.observe != nil {
		m.observe(m.shardID, m, m.statsLocked())
	}
}

// finalizeRemoval removes a Dropped state machine only after its terminal
// state has been durably persisted.
func (m *ShardViewManager) finalizeRemoval(target *CoordQueryViewStateMachine) {
	m.mu.Lock()
	if m.views[target.Version()] != target {
		m.mu.Unlock()
		return
	}
	m.removeView(target)
	m.publishStatsLocked()
	released := m.releaseRequested && len(m.views) == 0
	onReleasedEmpty := m.onReleasedEmpty
	m.mu.Unlock()

	if released && onReleasedEmpty != nil {
		onReleasedEmpty(m.shardID, m)
	}
}

// hasPendingRemoval reports whether target already has a post-persist removal
// callback waiting to be emitted.
//
// Must be called under m.mu.
func (m *ShardViewManager) hasPendingRemoval(target *CoordQueryViewStateMachine) bool {
	for _, pending := range m.pendingRemovals {
		if pending == target {
			return true
		}
	}
	return false
}

// removeView removes the state machine from the views map and clears any
// fast pointers that reference it.
//
// Must be called under m.mu.
func (m *ShardViewManager) removeView(target *CoordQueryViewStateMachine) {
	if m.preparingView == target {
		m.preparingView = nil
	}
	if m.upView == target {
		m.upView = nil
	}
	delete(m.views, target.Version())
}

// validateDataVersionLocked checks that the new DataVersion is not lower than
// any existing view's DataVersion.
//
// Must be called under m.mu.
func (m *ShardViewManager) validateDataVersionLocked(newDV qviews.DataVersion) error {
	for _, sm := range m.views {
		if sm.Version().DataVersion.GT(newDV) {
			return merr.WrapErrServiceInternal("new data version must not be lower than any existing view's data version")
		}
	}
	return nil
}

// nextQueryVersion computes the next QueryVersion for a given DataVersion.
// Returns max(QV for views with same DV) + 1, or 1 if no matching DV exists.
//
// Must be called under m.mu.
func (m *ShardViewManager) nextQueryVersion(newDV qviews.DataVersion) int64 {
	var maxQV int64
	for _, sm := range m.views {
		v := sm.Version()
		if v.DataVersion.EQ(newDV) && v.QueryVersion > maxQV {
			maxQV = v.QueryVersion
		}
	}
	return maxQV + 1
}
