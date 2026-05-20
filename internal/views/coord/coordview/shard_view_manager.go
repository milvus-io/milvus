package coordview

import (
	"context"
	"sort"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ShardViewManager manages multiple QueryViews for a single shard (vchannel)
// within a single replica on the Coord side.
//
// It orchestrates CoordQueryViewStateMachine instances and their cross-view
// interactions, calling Catalog for ETCD persistence and ReliableSyncer for
// node sync. Node responses are delivered via callbacks registered with the syncer.
//
// Invariants (maintained by all methods):
//   - At most one view in Preparing or Ready state (tracked by preparingView).
//   - At most one view in Up state (tracked by upView).
//
// Thread-safety: All methods are thread-safe.
type ShardViewManager struct {
	ctx     context.Context // lifecycle context for Catalog calls within callbacks
	mu      sync.Mutex
	shardID qviews.ShardID
	catalog queryview.QueryViewCatalog
	syncer  syncer.ReliableSyncer
	observe func(qviews.ShardID, *ShardStats)

	// All active views keyed by version for O(1) lookup.
	views map[qviews.QueryViewVersion]*CoordQueryViewStateMachine

	// Fast pointers to the unique Preparing/Ready and Up views.
	// Invariant: at most one of each at any time.
	preparingView *CoordQueryViewStateMachine // Preparing or Ready state; nil if none
	upView        *CoordQueryViewStateMachine // Up state; nil if none

	// Accumulates persist and sync operations within a single lock-hold scope.
	// All persists are flushed in a single SaveQueryViews call, then all syncs
	// are flushed in a single SyncViews call. This ensures atomicity at the
	// persistence layer and reduces the number of I/O calls.
	// Must only be accessed under m.mu.
	pendingPersists []*viewpb.QueryViewOfShard
	pendingSyncs    []syncEntry
}

// syncEntry pairs a state machine with its per-node views for deferred sync dispatch.
type syncEntry struct {
	sm    *CoordQueryViewStateMachine
	views []qviews.QueryViewAtWorkNode
}

// NewShardViewManager creates a new ShardViewManager for the given shard.
//
// ctx is the lifecycle context used for Catalog calls within callbacks.
// recoveredViews are views loaded from ETCD during crash recovery.
// Unrecoverable views remain Unrecoverable after construction, waiting for
// AddPreparing or RequestRelease to advance them to Dropping.
// Active views in other states are pushed to their target nodes via syncer.
func NewShardViewManager(
	ctx context.Context,
	shardID qviews.ShardID,
	catalog queryview.QueryViewCatalog,
	s syncer.ReliableSyncer,
	recoveredViews []*viewpb.QueryViewOfShard,
) *ShardViewManager {
	m := &ShardViewManager{
		ctx:     ctx,
		shardID: shardID,
		catalog: catalog,
		syncer:  s,
		views:   make(map[qviews.QueryViewVersion]*CoordQueryViewStateMachine, len(recoveredViews)),
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
	m.flush()
	return m
}

func (m *ShardViewManager) SetStatsObserver(observer func(qviews.ShardID, *ShardStats)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observe = observer
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
				stats.UpSettings = sm.View().GetMeta().GetSettings()
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
func (m *ShardViewManager) AddPreparing(ctx context.Context, builder *qviews.QueryViewAtCoordBuilder) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	newDV := builder.DataVersion()

	// Validate no DataVersion rollback.
	if err := m.validateDataVersionLocked(newDV); err != nil {
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

	// Process: persist write-ahead + collect sync.
	m.processStateMachine(sm)

	// Flush all accumulated I/O.
	m.flush()
	m.publishStatsLocked()
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
	defer m.mu.Unlock()

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

	m.flush()
	m.publishStatsLocked()
	return nil
}

// processStateMachine consumes pending I/O from a state machine and handles
// cascading effects (Up-then-Down, Unrecoverable→Dropping, Dropped removal).
// I/O is collected into pendingPersists/pendingSyncs for deferred execution.
//
// Also maintains preparingView/upView pointers on state transitions.
//
// Must be called under m.mu.
func (m *ShardViewManager) processStateMachine(sm *CoordQueryViewStateMachine) {
	for {
		// 1. ConsumePersist → collect into pending batch.
		if persist := sm.ConsumePersist(); persist != nil {
			m.pendingPersists = append(m.pendingPersists, persist)
		}

		// 2. ConsumeSync → collect into pending batch.
		if views := sm.ConsumeSync(); len(views) > 0 {
			m.pendingSyncs = append(m.pendingSyncs, syncEntry{sm: sm, views: views})
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
			m.removeView(sm)
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

// flush executes all accumulated I/O: first persist all, then sync all.
//
// Both Catalog (ETCD) and ReliableSyncer provide reliable delivery:
// they only return errors when the Coordinator is shutting down (context
// canceled). In that case the Coordinator will perform a full recovery
// on next startup, reconstructing all state machines from ETCD, so
// transient in-memory / persisted inconsistencies are self-healing.
//
// Must be called under m.mu.
func (m *ShardViewManager) flush() {
	// 1. Persist all in a single call.
	if len(m.pendingPersists) > 0 {
		if err := m.catalog.SaveQueryViews(m.ctx, m.pendingPersists); err != nil {
			// SaveQueryViews only fails when the Coordinator is shutting down
			// (ctx canceled). On next startup a full recovery from ETCD will
			// reconstruct all state machines, so this is safe to log-and-skip.
			log.Ctx(m.ctx).Warn("failed to persist query views",
				zap.String("shardID", m.shardID.String()),
				zap.Int("count", len(m.pendingPersists)),
				zap.Error(err),
			)
		}
		m.pendingPersists = m.pendingPersists[:0]
	}

	// 2. Sync all in a single call.
	if len(m.pendingSyncs) > 0 {
		viewsByNode := make(map[qviews.WorkNodeKey][]syncer.SyncView)
		for _, entry := range m.pendingSyncs {
			version := entry.sm.Version()
			for _, view := range entry.views {
				node := view.WorkNode()
				key := node.Key()
				var onQueryNodeLost func(qviews.QueryNode)
				if _, ok := node.(qviews.QueryNode); ok {
					onQueryNodeLost = m.makeOnQueryNodeLost(version)
				}
				viewsByNode[key] = append(viewsByNode[key], syncer.SyncView{
					View:            view,
					OnSyncResponse:  m.makeOnSyncResponse(version, view),
					OnQueryNodeLost: onQueryNodeLost,
				})
			}
		}
		if len(viewsByNode) > 0 {
			if err := m.syncer.SyncViews(m.ctx, syncer.SyncGroup{ViewsByNode: viewsByNode}); err != nil {
				// SyncViews only fails when the Coordinator is shutting down
				// (ctx canceled or syncer closed). On next startup a full
				// recovery will re-push all outstanding syncs.
				log.Ctx(m.ctx).Warn("failed to sync views to nodes",
					zap.String("shardID", m.shardID.String()),
					zap.Error(err),
				)
			}
		}
		m.pendingSyncs = m.pendingSyncs[:0]
	}
}

// makeOnSyncResponse creates a callback that processes node responses for a view sync.
//
// The callback acquires m.mu, calls sm.OnNodeStateReported, calls processStateMachine.
// Returns true when this node has completed the sync represented by target.
func (m *ShardViewManager) makeOnSyncResponse(version qviews.QueryViewVersion, target qviews.QueryViewAtWorkNode) func(resp qviews.QueryViewAtWorkNode) bool {
	return func(resp qviews.QueryViewAtWorkNode) bool {
		m.mu.Lock()
		defer m.mu.Unlock()

		sm, ok := m.views[version]
		if !ok {
			return true // view already removed, stop tracking
		}

		sm.OnNodeStateReported(resp)
		m.processStateMachine(sm)
		m.flush()
		m.publishStatsLocked()

		_, exists := m.views[version]
		return !exists || syncResponseCompletesTarget(target.State(), resp.State())
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
		defer m.mu.Unlock()

		sm, ok := m.views[version]
		if !ok {
			return // view already removed
		}

		sm.OnQueryNodeLost(node)
		m.processStateMachine(sm)
		m.flush()
		m.publishStatsLocked()
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
