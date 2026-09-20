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

// DataViewRefProvider acquires DataViewRefs for QueryViews. The DataView
// Manager (internal/dataview.Manager) satisfies it directly, so the wiring
// layer injects the Manager as-is.
type DataViewRefProvider = qviews.DataViewRefProvider

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
	ctx            context.Context // lifecycle context used by callbacks and event observation
	mu             sync.Mutex
	shardID        qviews.ShardID
	eventSubmitter dirtyViewEventSubmitter
	observe        func(qviews.ShardID, *ShardStats)
	onEmpty        func(qviews.ShardID, *ShardViewManager)

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

	// dataViewRefs acquires DataViewRefs for resident QueryViews so the
	// published per-segment RowNum footprint stays visible while a view is
	// alive (lifetime(QueryView) < lifetime(DataView)).
	//
	// PRECONDITION (non-nil): the wiring layer always injects a working
	// provider; methods rely on it without nil checks. A provider whose Get
	// returns a nil ref expresses "version does not exist": new plans must
	// retry, while recovered views enter terminal cleanup.
	dataViewRefs DataViewRefProvider
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
	dataViewRefs DataViewRefProvider,
) *ShardViewManager {
	m := &ShardViewManager{
		ctx:            ctx,
		shardID:        shardID,
		eventSubmitter: eventSubmitter,
		views:          make(map[qviews.QueryViewVersion]*CoordQueryViewStateMachine, len(recoveredViews)),
		dataViewRefs:   dataViewRefs,
	}

	// Recover state machines from persisted views. Unlike
	// RecoverShardViewManager, this simple path does not re-acquire DataView
	// refs (used by Ensure with no recovered views, and by tests).
	recovered := make([]*CoordQueryViewStateMachine, 0, len(recoveredViews))
	for _, view := range recoveredViews {
		sm := RecoverCoordQueryViewStateMachine(view, nil)
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

// RecoverShardViewManager restores a shard manager and rebuilds every durable
// QueryView's DataView reference before the view can become active again.
func RecoverShardViewManager(
	ctx context.Context,
	shardID qviews.ShardID,
	eventSubmitter dirtyViewEventSubmitter,
	dataViewRefs DataViewRefProvider,
	recoveredViews []*viewpb.QueryViewOfShard,
) (*ShardViewManager, error) {
	m := &ShardViewManager{
		ctx:            ctx,
		shardID:        shardID,
		eventSubmitter: eventSubmitter,
		views:          make(map[qviews.QueryViewVersion]*CoordQueryViewStateMachine, len(recoveredViews)),
		dataViewRefs:   dataViewRefs,
	}

	recovered := make([]*CoordQueryViewStateMachine, 0, len(recoveredViews))
	for _, view := range recoveredViews {
		version := qviews.FromProtoQueryViewVersion(view.GetMeta().GetVersion())

		// Re-acquire the ref the crashed process held (the DataView version
		// outlives the QueryView) and bind it at construction. nil ref = the
		// version no longer exists (already GC'd) -> prepare terminal
		// recovery; error = provider failure -> abort recovery.
		ref, err := m.dataViewRefs.Get(ctx, view.GetMeta().GetCollectionId(), version.DataVersion.IntoProto())
		if err != nil {
			m.releaseAllRefs()
			return nil, err
		}

		sm := RecoverCoordQueryViewStateMachine(view, ref)
		if ref == nil {
			m.prepareTerminalRecovery(sm)
		}
		recovered = append(recovered, sm)
		m.views[version] = sm
	}

	sort.Slice(recovered, func(i, j int) bool {
		return recovered[j].Version().GT(recovered[i].Version())
	})
	for _, sm := range recovered {
		m.processStateMachine(sm)
	}
	m.advanceUnrecoverableToDropping()
	m.submitDirtyEvent(m.consumeDirtyEventLocked())
	return m, nil
}

func (m *ShardViewManager) SetStatsObserver(observer func(qviews.ShardID, *ShardStats)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observe = observer
}

// setOnEmpty installs the callback invoked after the manager's last
// QueryView has completed durable removal.
func (m *ShardViewManager) setOnEmpty(callback func(qviews.ShardID, *ShardViewManager)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onEmpty = callback
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

	m.fillShardRows(stats)
	return stats
}

// fillShardRows attaches the published RowNum of each placed segment, read
// lock-free from the retained DataViewRefs. Each version has its own segment
// membership and stats. Prefer the newest contributing view with known stats,
// falling back to older views for segments no longer in the latest version.
// Recovered versions may have no stats; preserve that distinction from zero.
func (m *ShardViewManager) fillShardRows(stats *ShardStats) {
	views := make([]*CoordQueryViewStateMachine, 0, len(m.views))
	for _, sm := range m.views {
		if _, contributes := segmentStateFromViewState(sm.State()); contributes && sm.Ref() != nil {
			views = append(views, sm)
		}
	}
	sort.Slice(views, func(i, j int) bool {
		return views[i].Version().GT(views[j].Version())
	})
	for segmentID, segment := range stats.Segments {
		for _, sm := range views {
			if segmentStats, ok := sm.Ref().Stats(segmentID); ok {
				segment.RowNum = segmentStats.RowNum
				segment.HasRowNum = true
				break
			}
		}
	}
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
func (m *ShardViewManager) AddPreparing(ctx context.Context, builder *qviews.QueryViewAtCoordBuilder) error {
	m.mu.Lock()

	newDV := builder.DataVersion()

	// Validate no DataVersion rollback.
	if err := m.validateDataVersionLocked(newDV); err != nil {
		m.mu.Unlock()
		return err
	}

	// Assign and build the new view before mutating any existing state. The
	// DataView ref acquisition is the linearization point against
	// collection-scoped GC; the acquired ref is bound to the state machine
	// at construction. A snapshot used for planning does not itself pin the
	// version, so GC may have collected it before this acquisition.
	qv := m.nextQueryVersion(newDV)
	builder.SetQueryVersion(qv)
	view := builder.Build()
	ref, err := m.dataViewRefs.Get(ctx, view.GetMeta().GetCollectionId(), newDV.IntoProto())
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if ref == nil {
		m.mu.Unlock()
		return merr.WrapErrServiceUnavailableMsg("DataView %s of collection %d is no longer available; replan the QueryView", newDV.String(), view.GetMeta().GetCollectionId())
	}
	sm := NewCoordQueryViewStateMachine(view, ref)

	// Preempt existing Preparing/Ready view.
	if m.preparingView != nil {
		m.preparingView.EnterUnrecoverable()
		m.processStateMachine(m.preparingView)
		// preparingView is cleared by processStateMachine (Unrecoverable case).
	}

	// Advance all Unrecoverable views (preempted or naturally failed) to
	// Dropping so their Dropped sync is batched with the new Preparing sync.
	m.advanceUnrecoverableToDropping()

	m.views[sm.Version()] = sm
	m.preparingView = sm

	// Process: collect persist, sync, and post-persist effects.
	m.processStateMachine(sm)

	// Move all accumulated effects into one shard-scoped event.
	event := m.consumeDirtyEventLocked()
	m.publishStatsLocked()
	m.mu.Unlock()
	m.submitDirtyEvent(event)
	return nil
}

// RequestRelease initiates teardown of all views in this shard.
//
// - Up views: transition to Down (normal teardown via SN confirmation).
// - Preparing/Ready views: force Unrecoverable → Dropping (abort immediately).
// - Down/Dropping views: already tearing down, no-op.
//
// The actual cleanup completes asynchronously through callbacks.
func (m *ShardViewManager) RequestRelease(ctx context.Context) error {
	m.mu.Lock()

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
	m.mu.Unlock()
	m.submitDirtyEvent(event)
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
	for {
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
			return

		case qviews.QueryViewStateUp:
			if m.preparingView == sm {
				m.preparingView = nil
			}
			m.downOlderUpView(sm)
			m.upView = sm
			return

		case qviews.QueryViewStateDown:
			if m.upView == sm {
				m.upView = nil
			}
			return

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
			return

		case qviews.QueryViewStateDropping:
			return

		case qviews.QueryViewStateDropped:
			if !m.hasPendingRemoval(sm) {
				m.pendingRemovals = append(m.pendingRemovals, sm)
			}
			return

		default:
			return
		}
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
		m.mu.Unlock()
		m.submitDirtyEvent(event)
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
		m.mu.Unlock()
		m.submitDirtyEvent(event)
	}
}

func (m *ShardViewManager) submitDirtyEvent(event dirtyViewEvent) {
	if !event.empty() {
		m.eventSubmitter.Submit(event)
	}
}

func (m *ShardViewManager) keyForStateMachine(sm *CoordQueryViewStateMachine) qviews.QueryViewKey {
	return qviews.QueryViewKey{
		ShardID:          m.shardID,
		QueryViewVersion: sm.Version(),
	}
}

func resourceReadyPercent(report qviews.QueryViewAtWorkNode) int64 {
	if _, ok := report.WorkNode().(qviews.StreamingNode); !ok {
		return 0
	}
	switch report.State() {
	case qviews.QueryViewStateReady, qviews.QueryViewStateUp, qviews.QueryViewStateDown, qviews.QueryViewStateDropped:
		return 100
	default:
		return 0
	}
}

func (m *ShardViewManager) publishStatsLocked() {
	if m.observe != nil {
		m.observe(m.shardID, m.statsLocked())
	}
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

// finalizeRemoval releases a DataView reference only after the corresponding
// Dropped QueryView state has been persisted by DirtyViewFlushScheduler.
func (m *ShardViewManager) finalizeRemoval(target *CoordQueryViewStateMachine) {
	m.mu.Lock()
	if m.views[target.Version()] != target {
		m.mu.Unlock()
		return
	}
	m.removeView(target)
	target.ReleaseRef()
	m.publishStatsLocked()
	empty := len(m.views) == 0
	onEmpty := m.onEmpty
	m.mu.Unlock()

	if empty && onEmpty != nil {
		onEmpty(m.shardID, m)
	}
}

func (m *ShardViewManager) hasPendingRemoval(target *CoordQueryViewStateMachine) bool {
	for _, sm := range m.pendingRemovals {
		if sm == target {
			return true
		}
	}
	return false
}

// releaseAllRefs releases every resident QueryView's DataView ref (recovery
// abort path). Must be called with m.mu held.
func (m *ShardViewManager) releaseAllRefs() {
	for _, sm := range m.views {
		sm.ReleaseRef()
	}
}

func (m *ShardViewManager) prepareTerminalRecovery(sm *CoordQueryViewStateMachine) {
	switch sm.State() {
	case qviews.QueryViewStatePreparing, qviews.QueryViewStateReady:
		sm.EnterUnrecoverable()
	case qviews.QueryViewStateUp:
		sm.EnterDown()
	}
}

// validateDataVersionLocked checks that the new DataVersion is not lower than
// any existing view's DataVersion.
//
// Must be called under m.mu.
func (m *ShardViewManager) validateDataVersionLocked(newDV qviews.DataVersion) error {
	for _, sm := range m.views {
		if sm.Version().DataVersion.GT(newDV) {
			return errDataVersionRollback
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
