package recovery

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type summaryEvictionConfig struct {
	entryTTL   time.Duration
	maxBytes   int
	minEntries int
}

type summaryManager struct {
	mlog.Binder
	pchannel string
	// term is this owner's WAL assignment term, persisted into every chunk
	// footer and pchannel summary meta it writes. Used for best-effort
	// split-brain fencing: a durable term newer than ours means another owner
	// took over and our writes must stop.
	term    int64
	cfg     *config
	metrics *recoveryMetrics

	// mu guards all summary state below. The lock order is always
	// recoveryStorageImpl.mu -> summaryManager.mu: recoveryStorageImpl methods
	// acquire rs.mu then call into summaryManager (which takes mu), while
	// summaryManager holds no reference back to recoveryStorageImpl and never
	// acquires rs.mu, so the ordering is one-directional and deadlock-free.
	mu                            sync.Mutex
	summaryBackgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	summarySnapshotCheckpoint     trackedCheckpoint
	persistedConsumeCheckpoint    *WALCheckpoint
	vchannelSummaries             map[string]*vchannelSummary
	activeViewsInitialized        bool
	// droppedSummaryVChannels queues vchannels whose summaries were reclaimed; the
	// background task removes their persisted summary metas batched (guarded by mu).
	droppedSummaryVChannels           []string
	recoveryMode                      bool
	evictionConfig                    summaryEvictionConfig
	pendingIdempotencyPersistSnapshot *RecoverySnapshot
}

func newSummaryManager(pchannel string, term int64, cfg *config, metrics *recoveryMetrics, persistedConsumeCheckpoint *WALCheckpoint, evictionCfg summaryEvictionConfig) *summaryManager {
	return &summaryManager{
		pchannel:                      pchannel,
		term:                          term,
		cfg:                           cfg,
		metrics:                       metrics,
		summaryBackgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		persistedConsumeCheckpoint:    cloneWALCheckpoint(persistedConsumeCheckpoint),
		vchannelSummaries:             make(map[string]*vchannelSummary),
		recoveryMode:                  true,
		evictionConfig:                evictionCfg,
	}
}

// summaries returns the live summary map WITHOUT locking. Contract:
// the caller must either hold m.mu (the observe path locks it in
// recoveryStorageImpl.ObserveMessage, the background task in its own loop; lock
// order is always rs.mu -> m.mu) or run in the single-threaded recovery
// bootstrap before summaryBackgroundTask starts. The unlocked writers below
// (setSummaries / setSummary / getOrCreateSummary)
// carry the same contract; removeSummary locks m.mu itself because
// its caller does not.
func (m *summaryManager) summaries() map[string]*vchannelSummary {
	if m == nil {
		return nil
	}
	return m.vchannelSummaries
}

func (m *summaryManager) resetSummaries() {
	m.setSummaries(nil)
}

func (m *summaryManager) setSummaries(summaries map[string]*vchannelSummary) {
	if summaries == nil {
		summaries = make(map[string]*vchannelSummary)
	}
	m.vchannelSummaries = summaries
}

func (m *summaryManager) initializeSummariesFromMeta(
	vchannels map[string]*vchannelRecoveryInfo,
	checkpoint *WALCheckpoint,
	metas []*streamingpb.VChannelSummaryMeta,
) {
	m.resetSummaries()
	m.setPChannelSummarySnapshotCheckpoint(checkpoint)
	m.ensureActiveSummaries(vchannels, checkpoint)
	m.applyRecoveredSummaryMetas(metas)
}

func (m *summaryManager) setSummary(vchannel string, state *vchannelSummary) {
	if vchannel == "" || state == nil {
		return
	}
	m.vchannelSummaries[vchannel] = state
}

func (m *summaryManager) getOrCreateSummary(vchannel string, checkpoint *WALCheckpoint) *vchannelSummary {
	if vchannel == "" {
		return nil
	}
	if state, ok := m.vchannelSummaries[vchannel]; ok {
		return state
	}
	state := newEmptyVChannelSummary(m.pchannel, vchannel, checkpoint)
	// The view's retention policy drives the durable-retention ledger, which
	// decides how far back chunks stay recoverable across restarts.
	state.evictionCfg = m.evictionConfig
	m.vchannelSummaries[vchannel] = state
	return state
}

// removeSummary drops the in-memory summary for a reclaimed (dropped)
// vchannel. Without this, m.vchannelSummaries grows without bound under collection
// create/drop churn and every per-message / per-timetick scan keeps walking dead
// summaries. The vchannel's persisted summary meta is NOT removed here (that would
// mean catalog IO on the drop hot path); the vchannel is queued and the summary
// background task drains the removals batched (drainDroppedSummaryMetas), so
// dropped vchannels do not leave permanent etcd keys behind either.
func (m *summaryManager) removeSummary(vchannel string) {
	if m == nil || vchannel == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.vchannelSummaries, vchannel)
	m.droppedSummaryVChannels = append(m.droppedSummaryVChannels, vchannel)
}

// ensureActiveSummaries advances every existing summary and creates a
// summary for each active vchannel. It is used only by recovery bootstrap paths,
// which materialize the full active set at once. The per-message observe loop
// updates only the target vchannel for ordinary messages and all summaries for the
// already-global TimeTick path.
func (m *summaryManager) ensureActiveSummaries(vchannels map[string]*vchannelRecoveryInfo, checkpoint *WALCheckpoint) {
	if m == nil || !m.cfg.idempotencyEnabled {
		return
	}
	m.advanceAllSummaryCheckpointsUnsafe(checkpoint)
	for vchannel, info := range vchannels {
		if !info.IsActive() {
			continue
		}
		m.getOrCreateSummary(vchannel, checkpoint)
	}
}

func (m *summaryManager) advanceAllSummaryCheckpointsUnsafe(checkpoint *WALCheckpoint) {
	for _, state := range m.summaries() {
		state.advanceCheckpointTo(checkpoint)
	}
}

// ensureSummary creates the idempotency summary for a vchannel that just
// became active (a create-collection message during WAL replay), if absent.
func (m *summaryManager) ensureSummary(vchannel string, checkpoint *WALCheckpoint) {
	if m == nil || !m.cfg.idempotencyEnabled {
		return
	}
	m.getOrCreateSummary(vchannel, checkpoint)
}

func (m *summaryManager) observeMessage(msg message.ImmutableMessage) {
	summaries := m.summaries()
	if len(summaries) == 0 || msg == nil {
		return
	}
	if msg.MessageType() == message.MessageTypeTimeTick {
		for _, summary := range summaries {
			summary.advanceCheckpoint(msg)
			// Time passing expires ledger generations by TTL; recompute the
			// chunk-retention boundary so GC keeps advancing on idle vchannels.
			summary.refreshMinRequiredGeneration()
		}
		if m.recoveryMode {
			evictBeforeTT := evictBeforeTimetick(msg.TimeTick(), m.evictionConfig.entryTTL)
			for _, summary := range summaries {
				summary.evictForRecovery(evictBeforeTT, m.evictionConfig.minEntries, m.evictionConfig.maxBytes)
			}
		}
		return
	}
	if msg.VChannel() == "" || msg.IsPChannelLevel() {
		return
	}
	if summary, ok := summaries[msg.VChannel()]; ok {
		summary.observeMessage(msg)
	}
}

func (m *summaryManager) hasDirtySummaryUnsafe() bool {
	for _, summary := range m.summaries() {
		if summary.dirty {
			return true
		}
	}
	return false
}

func (m *summaryManager) consumePendingSummaryEntries() (map[string][]*streamingpb.SummaryEntry, map[string]*summaryMetaUpdate, *WALCheckpoint) {
	if len(m.summaries()) == 0 || !m.hasDirtySummaryUnsafe() {
		return nil, nil, nil
	}
	recordsByVChannel := make(map[string][]*streamingpb.SummaryEntry)
	metaUpdates := make(map[string]*summaryMetaUpdate)
	for _, summary := range m.summaries() {
		records, metaUpdate := summary.consumePendingSummaryEntries()
		if len(records) > 0 {
			recordsByVChannel[summary.vchannel] = records
		}
		if metaUpdate != nil {
			metaUpdates[summary.vchannel] = metaUpdate
		}
	}
	return recordsByVChannel, metaUpdates, m.getPChannelSummarySnapshotCheckpointUnsafe()
}

func (m *summaryManager) applyRecoveredSummaryMetas(metas []*streamingpb.VChannelSummaryMeta) {
	for _, meta := range metas {
		if meta == nil || meta.GetVchannel() == "" {
			continue
		}
		state, ok := m.summaries()[meta.GetVchannel()]
		if !ok {
			continue
		}
		state.latestAppliedGeneration = maxUint64(state.latestAppliedGeneration, meta.GetLatestAppliedGeneration())
		state.minRequiredGeneration = meta.GetMinRequiredGeneration()
	}
}

func (m *summaryManager) markSummariesPersisted(recordsByVChannel map[string][]*streamingpb.SummaryEntry, metas map[string]*streamingpb.VChannelSummaryMeta, generation uint64) {
	if generation == 0 && len(recordsByVChannel) == 0 && len(metas) == 0 {
		return
	}
	for vchannel, records := range recordsByVChannel {
		if summary, ok := m.summaries()[vchannel]; ok {
			summary.markSummaryEntriesPersisted(records, generation)
		}
	}
	for vchannel, meta := range metas {
		if summary, ok := m.summaries()[vchannel]; ok && meta != nil {
			summary.latestAppliedGeneration = maxUint64(summary.latestAppliedGeneration, meta.GetLatestAppliedGeneration())
			summary.minRequiredGeneration = meta.GetMinRequiredGeneration()
		}
	}
	for _, summary := range m.summaries() {
		summary.latestAppliedGeneration = maxUint64(summary.latestAppliedGeneration, generation)
		summary.refreshMinRequiredGeneration()
	}
}

// minRequiredGeneration returns the lowest min-required generation across the
// idempotency summaries (and any supplied per-vchannel meta overrides), plus
// whether any summary contributed a boundary. persistedGeneration is the
// generation about to be persisted, used to project a summary's boundary forward.
func (m *summaryManager) minRequiredGeneration(summaryMetas map[string]*streamingpb.VChannelSummaryMeta, persistedGeneration uint64) (uint64, bool) {
	aggregator := minRequiredGenerationAggregator{}
	overriddenSummaries := make(map[string]struct{}, len(summaryMetas))
	for vchannel, meta := range summaryMetas {
		if meta == nil {
			continue
		}
		overriddenSummaries[vchannel] = struct{}{}
		aggregator.Observe(meta)
	}

	for vchannel, summary := range m.summaries() {
		if _, ok := overriddenSummaries[vchannel]; ok {
			continue
		}
		aggregator.Observe(summary.summaryMetaAtGeneration(persistedGeneration))
	}
	return aggregator.Value(), aggregator.Initialized()
}

func (m *summaryManager) markActiveViewsInitialized() {
	m.activeViewsInitialized = true
}

func (m *summaryManager) setNormalMode() {
	m.recoveryMode = false
}

// evictPersistedEntries drops every already-persisted entry from each
// idempotency summary's in-memory staging buffer. It is keyed off persistence
// progress (an entry's generation being assigned), not the chunk-GC / in-use
// boundary: whether an entry is persisted and whether a chunk is still in use
// are independent concerns.
func (m *summaryManager) evictPersistedEntries() {
	if m.recoveryMode {
		return
	}
	for _, state := range m.summaries() {
		state.evictPersisted()
	}
}

type minRequiredGenerationAggregator struct {
	minimum     uint64
	initialized bool
}

func (a *minRequiredGenerationAggregator) Observe(meta *streamingpb.VChannelSummaryMeta) {
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
