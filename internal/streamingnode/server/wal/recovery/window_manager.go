package recovery

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type windowEvictionConfig struct {
	windowTTL  time.Duration
	maxBytes   int
	minEntries int
	maxEntries int
}

type windowManager struct {
	mlog.Binder
	pchannel string
	cfg      *config
	metrics  *recoveryMetrics

	// mu guards all window state below. The lock order is always
	// recoveryStorageImpl.mu -> windowManager.mu: recoveryStorageImpl methods
	// acquire rs.mu then call into windowManager (which takes mu), while
	// windowManager holds no reference back to recoveryStorageImpl and never
	// acquires rs.mu, so the ordering is one-directional and deadlock-free.
	mu                           sync.Mutex
	windowBackgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	windowSnapshotCheckpoint     trackedCheckpoint
	persistedConsumeCheckpoint   *WALCheckpoint
	windows                      map[string]*vchannelWindow
	activeViewsInitialized       bool
	// droppedWindowVChannels queues vchannels whose windows were reclaimed; the
	// background task removes their persisted window metas batched (guarded by mu).
	droppedWindowVChannels            []string
	recoveryMode                      bool
	evictionConfig                    windowEvictionConfig
	pendingIdempotencyPersistSnapshot *RecoverySnapshot
}

func newWindowManager(pchannel string, cfg *config, metrics *recoveryMetrics, persistedConsumeCheckpoint *WALCheckpoint, evictionCfg windowEvictionConfig) *windowManager {
	return &windowManager{
		pchannel:                     pchannel,
		cfg:                          cfg,
		metrics:                      metrics,
		windowBackgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		persistedConsumeCheckpoint:   cloneWALCheckpoint(persistedConsumeCheckpoint),
		windows:                      make(map[string]*vchannelWindow),
		recoveryMode:                 true,
		evictionConfig:               evictionCfg,
	}
}

// idempotencyWindows returns the live window map WITHOUT locking. Contract:
// the caller must either hold m.mu (the observe path locks it in
// recoveryStorageImpl.ObserveMessage, the background task in its own loop; lock
// order is always rs.mu -> m.mu) or run in the single-threaded recovery
// bootstrap before windowBackgroundTask starts. The unlocked writers below
// (setIdempotencyWindows / setIdempotencyWindow / getOrCreateIdempotencyWindow)
// carry the same contract; removeIdempotencyWindow locks m.mu itself because
// its caller does not.
func (m *windowManager) idempotencyWindows() map[string]*vchannelWindow {
	if m == nil {
		return nil
	}
	return m.windows
}

func (m *windowManager) resetIdempotencyWindows() {
	m.setIdempotencyWindows(nil)
}

func (m *windowManager) setIdempotencyWindows(windows map[string]*vchannelWindow) {
	if windows == nil {
		windows = make(map[string]*vchannelWindow)
	}
	m.windows = windows
}

func (m *windowManager) initializeIdempotencyWindowsFromMeta(
	vchannels map[string]*vchannelRecoveryInfo,
	checkpoint *WALCheckpoint,
	metas []*streamingpb.VChannelWindowMeta,
) {
	m.resetIdempotencyWindows()
	m.setPChannelWindowSnapshotCheckpoint(checkpoint)
	m.ensureActiveIdempotencyWindows(vchannels, checkpoint)
	m.applyRecoveredIdempotencyWindowMetas(metas)
}

func (m *windowManager) setIdempotencyWindow(vchannel string, state *vchannelWindow) {
	if vchannel == "" || state == nil {
		return
	}
	m.windows[vchannel] = state
}

func (m *windowManager) getOrCreateIdempotencyWindow(vchannel string, checkpoint *WALCheckpoint) *vchannelWindow {
	if vchannel == "" {
		return nil
	}
	if state, ok := m.windows[vchannel]; ok {
		return state
	}
	state := newEmptyVChannelWindow(m.pchannel, vchannel, checkpoint)
	// The view's retention policy drives the durable-retention ledger, which
	// decides how far back chunks stay recoverable across restarts.
	state.evictionCfg = m.evictionConfig
	m.windows[vchannel] = state
	return state
}

// removeIdempotencyWindow drops the in-memory window for a reclaimed (dropped)
// vchannel. Without this, m.windows grows without bound under collection
// create/drop churn and every per-message / per-timetick scan keeps walking dead
// windows. The vchannel's persisted window meta is NOT removed here (that would
// mean catalog IO on the drop hot path); the vchannel is queued and the window
// background task drains the removals batched (drainDroppedWindowMetas), so
// dropped vchannels do not leave permanent etcd keys behind either.
func (m *windowManager) removeIdempotencyWindow(vchannel string) {
	if m == nil || vchannel == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.windows, vchannel)
	m.droppedWindowVChannels = append(m.droppedWindowVChannels, vchannel)
}

// ensureActiveIdempotencyWindows advances every existing window and creates a
// window for each active vchannel. It is used by the recovery bootstrap paths,
// which materialize the full active set at once; the per-message observe loop
// uses advanceIdempotencyWindowCheckpoints + ensureIdempotencyWindow instead, so
// window lifecycle management stays out of the hot path.
func (m *windowManager) ensureActiveIdempotencyWindows(vchannels map[string]*vchannelRecoveryInfo, checkpoint *WALCheckpoint) {
	if m == nil || !m.cfg.idempotencyEnabled {
		return
	}
	m.advanceIdempotencyWindowCheckpointsUnsafe(checkpoint)
	for vchannel, info := range vchannels {
		if !info.IsActive() {
			continue
		}
		m.getOrCreateIdempotencyWindow(vchannel, checkpoint)
	}
}

// advanceIdempotencyWindowCheckpoints advances every existing window's snapshot
// checkpoint. This is the per-message slice of the old
// EnsureActiveIdempotencyWindows: window creation has moved to vchannel lifecycle
// events (ensureIdempotencyWindow), so the observe loop no longer scans the full
// active vchannel set on every message.
func (m *windowManager) advanceIdempotencyWindowCheckpoints(checkpoint *WALCheckpoint) {
	if m == nil || !m.cfg.idempotencyEnabled {
		return
	}
	m.advanceIdempotencyWindowCheckpointsUnsafe(checkpoint)
}

func (m *windowManager) advanceIdempotencyWindowCheckpointsUnsafe(checkpoint *WALCheckpoint) {
	for _, state := range m.idempotencyWindows() {
		state.advanceCheckpointTo(checkpoint)
	}
}

// ensureIdempotencyWindow creates the idempotency window for a vchannel that just
// became active (a create-collection message during WAL replay), if absent.
func (m *windowManager) ensureIdempotencyWindow(vchannel string, checkpoint *WALCheckpoint) {
	if m == nil || !m.cfg.idempotencyEnabled {
		return
	}
	m.getOrCreateIdempotencyWindow(vchannel, checkpoint)
}

func (m *windowManager) observeMessage(msg message.ImmutableMessage) {
	windows := m.idempotencyWindows()
	if len(windows) == 0 || msg == nil {
		return
	}
	if msg.MessageType() == message.MessageTypeTimeTick {
		for _, window := range windows {
			window.advanceCheckpoint(msg)
			// Time passing expires ledger generations by TTL; recompute the
			// chunk-retention boundary so GC keeps advancing on idle vchannels.
			window.refreshMinRequiredGeneration()
		}
		if m.recoveryMode {
			evictBeforeTT := evictBeforeTimetick(msg.TimeTick(), m.evictionConfig.windowTTL)
			for _, window := range windows {
				window.evictForRecovery(evictBeforeTT, m.evictionConfig.minEntries, m.evictionConfig.maxEntries, m.evictionConfig.maxBytes)
			}
		}
		return
	}
	if msg.VChannel() == "" || msg.IsPChannelLevel() {
		return
	}
	if window, ok := windows[msg.VChannel()]; ok {
		window.observeMessage(msg)
	}
}

func (m *windowManager) hasDirtyWindowUnsafe() bool {
	for _, window := range m.idempotencyWindows() {
		if window.dirty {
			return true
		}
	}
	return false
}

func (m *windowManager) getWindowSnapshotCheckpointUnsafe() *WALCheckpoint {
	var minimum *WALCheckpoint
	for _, window := range m.idempotencyWindows() {
		cp := window.checkpoint()
		if cp == nil {
			continue
		}
		minimum = minCheckpointByTimeTick(minimum, cp)
	}
	return minimum
}

func (m *windowManager) consumePendingCommittedWriteRecords() (map[string][]committedWriteRecord, map[string]*idempotencyWindowMetaUpdate, *WALCheckpoint) {
	if len(m.idempotencyWindows()) == 0 || !m.hasDirtyWindowUnsafe() {
		return nil, nil, nil
	}
	recordsByVChannel := make(map[string][]committedWriteRecord)
	metaUpdates := make(map[string]*idempotencyWindowMetaUpdate)
	for _, window := range m.idempotencyWindows() {
		records, metaUpdate := window.consumePendingCommittedWriteRecords()
		if len(records) > 0 {
			recordsByVChannel[window.vchannel] = records
		}
		if metaUpdate != nil {
			metaUpdates[window.vchannel] = metaUpdate
		}
	}
	return recordsByVChannel, metaUpdates, m.getPChannelWindowSnapshotCheckpointUnsafe()
}

func (m *windowManager) applyRecoveredIdempotencyWindowMetas(metas []*streamingpb.VChannelWindowMeta) {
	for _, meta := range metas {
		if meta == nil || meta.GetVchannel() == "" {
			continue
		}
		state, ok := m.idempotencyWindows()[meta.GetVchannel()]
		if !ok {
			continue
		}
		state.latestAppliedGeneration = maxUint64(state.latestAppliedGeneration, meta.GetLatestAppliedGeneration())
		state.minRequiredGeneration = meta.GetMinRequiredGeneration()
	}
}

func (m *windowManager) markIdempotencyWindowsPersisted(recordsByVChannel map[string][]committedWriteRecord, metas map[string]*streamingpb.VChannelWindowMeta, generation uint64) {
	if generation == 0 && len(recordsByVChannel) == 0 && len(metas) == 0 {
		return
	}
	for vchannel, records := range recordsByVChannel {
		if window, ok := m.idempotencyWindows()[vchannel]; ok {
			window.markCommittedWriteRecordsPersisted(records, generation)
		}
	}
	for vchannel, meta := range metas {
		if window, ok := m.idempotencyWindows()[vchannel]; ok && meta != nil {
			window.latestAppliedGeneration = maxUint64(window.latestAppliedGeneration, meta.GetLatestAppliedGeneration())
			window.minRequiredGeneration = meta.GetMinRequiredGeneration()
		}
	}
	for _, window := range m.idempotencyWindows() {
		window.latestAppliedGeneration = maxUint64(window.latestAppliedGeneration, generation)
		window.refreshMinRequiredGeneration()
	}
}

// minRequiredGeneration returns the lowest min-required generation across the
// idempotency windows (and any supplied per-vchannel meta overrides), plus
// whether any window contributed a boundary. persistedGeneration is the
// generation about to be persisted, used to project a window's boundary forward.
func (m *windowManager) minRequiredGeneration(idempotencyWindowMetas map[string]*streamingpb.VChannelWindowMeta, persistedGeneration uint64) (uint64, bool) {
	aggregator := minRequiredGenerationAggregator{}
	overriddenIdempotencyWindows := make(map[string]struct{}, len(idempotencyWindowMetas))
	for vchannel, meta := range idempotencyWindowMetas {
		if meta == nil {
			continue
		}
		overriddenIdempotencyWindows[vchannel] = struct{}{}
		aggregator.Observe(meta)
	}

	for vchannel, window := range m.idempotencyWindows() {
		if _, ok := overriddenIdempotencyWindows[vchannel]; ok {
			continue
		}
		aggregator.Observe(window.windowMetaAtGeneration(persistedGeneration))
	}
	return aggregator.Value(), aggregator.Initialized()
}

func (m *windowManager) markActiveViewsInitialized() {
	m.activeViewsInitialized = true
}

func (m *windowManager) setNormalMode() {
	m.recoveryMode = false
}

// evictPersistedEntries drops every already-persisted entry from each
// idempotency window's in-memory staging buffer. It is keyed off persistence
// progress (an entry's generation being assigned), not the chunk-GC / in-use
// boundary: whether an entry is persisted and whether a chunk is still in use
// are independent concerns.
func (m *windowManager) evictPersistedEntries() {
	if m.recoveryMode {
		return
	}
	for _, state := range m.idempotencyWindows() {
		state.evictPersisted()
	}
}

type minRequiredGenerationAggregator struct {
	minimum     uint64
	initialized bool
}

func (a *minRequiredGenerationAggregator) Observe(meta *streamingpb.VChannelWindowMeta) {
	if meta == nil {
		return
	}
	generation := meta.GetMinRequiredGeneration()
	if !a.initialized || generation < a.minimum {
		a.minimum = generation
		a.initialized = true
	}
}

func (a *minRequiredGenerationAggregator) Value() uint64 {
	if !a.initialized {
		return 0
	}
	return a.minimum
}

func (a *minRequiredGenerationAggregator) Initialized() bool {
	return a.initialized
}
