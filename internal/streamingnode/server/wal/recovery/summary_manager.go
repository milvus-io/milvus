package recovery

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// summaryEvictionConfig bounds what a replay may materialize before the
// consumer takes ownership. It is not a retention policy: retention on the
// consumer side is the window's own byte cap, and on the durable side a byte
// budget over the manifest.
type summaryEvictionConfig struct {
	retainedBytes int
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

	// mu guards EVERYTHING below it: the per-vchannel summaries, and the durable
	// store's own state -- manifest, completedGC, nextGeneration, latestCoveredTT.
	// The store state needs it because two goroutines reach it: the recovery
	// background task publishes manifests from the persist path, and the summary
	// background task deletes objects from the gc path.
	//
	// Object storage is never touched while holding it. Both paths read state
	// under the lock, do their IO without it, and commit the result back under it,
	// so a slow or hanging object store cannot block message observation.
	//
	// The lock order is always recoveryStorageImpl.mu -> summaryManager.mu:
	// recoveryStorageImpl methods acquire rs.mu then call into summaryManager,
	// while summaryManager holds no reference back and never acquires rs.mu, so
	// the ordering is one-directional and deadlock-free.
	mu                            sync.Mutex
	summaryBackgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	vchannelSummaries             map[string]*vchannelSummary
	activeViewsInitialized        bool
	recoveryMode                  bool
	evictionConfig                summaryEvictionConfig

	// manifest is the live description of the chunk set: what recovery reads,
	// what a lazy reader indexes by, and what GC deletes. It is rewritten by
	// inheriting itself and amending, never rebuilt from scratch.
	manifest *streamingpb.PChannelSummaryManifest
	// nextGeneration is the generation the next chunk will claim. It advances in
	// memory: the meta is not consulted per chunk, because the meta is allowed to
	// lag and reading it on the write path would put etcd on the persist path.
	nextGeneration uint64
	// latestCoveredTT is the newest source timetick a chunk has covered. The
	// retention TTL horizon is derived from it rather than the wall clock, so
	// retention measures time the way the write path does and simply stops
	// advancing while the channel is idle.
	latestCoveredTT uint64
	// completedGC records the chunks the GC worker finished deleting. They are
	// dropped from the manifest at the next write, so a crash before that only
	// replays deletes that are already no-ops.
	completedGC map[pchannelSummaryGCRef]struct{}
}

func newSummaryManager(pchannel string, term int64, cfg *config, metrics *recoveryMetrics, evictionCfg summaryEvictionConfig) *summaryManager {
	return &summaryManager{
		pchannel:                      pchannel,
		term:                          term,
		cfg:                           cfg,
		metrics:                       metrics,
		summaryBackgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		vchannelSummaries:             make(map[string]*vchannelSummary),
		recoveryMode:                  true,
		evictionConfig:                evictionCfg,
		manifest:                      &streamingpb.PChannelSummaryManifest{},
		completedGC:                   make(map[pchannelSummaryGCRef]struct{}),
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
	m.vchannelSummaries[vchannel] = state
	return state
}

// removeSummary drops the in-memory summary for a reclaimed (dropped) vchannel.
// Without it, m.vchannelSummaries grows without bound under collection
// create/drop churn and every per-message scan keeps walking dead summaries.
// Nothing is persisted per vchannel, so there is nothing else to clean up.
func (m *summaryManager) removeSummary(vchannel string) {
	if m == nil || vchannel == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.vchannelSummaries, vchannel)
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
		state.advanceAppliedTo(checkpoint)
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
			summary.advanceApplied(msg.TimeTick())
		}
		if m.recoveryMode {
			// Bound what a replay may materialize before the consumer takes over
			// and applies its own cap. Not a retention decision.
			for _, summary := range summaries {
				summary.capRecoveryBytes(m.evictionConfig.retainedBytes)
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
		if summary.hasPendingSummaryRecords() {
			return true
		}
	}
	return false
}

func (m *summaryManager) markActiveViewsInitialized() {
	m.activeViewsInitialized = true
}

func (m *summaryManager) setNormalMode() {
	m.recoveryMode = false
}

// evictPersistedRecords releases each vchannel's staging buffer once its
// contents are in a written chunk. Whether a chunk is still in use is a separate
// concern and is decided by retention, not here.
func (m *summaryManager) evictPersistedRecordsUnsafe() {
	if m.recoveryMode {
		return
	}
	for _, state := range m.summaries() {
		state.evictPersisted()
	}
}

// hasDirtySummary reports whether any vchannel has committed facts not yet
// carried by a chunk.
func (m *summaryManager) hasDirtySummary() bool {
	if m == nil || !m.cfg.idempotencyEnabled {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasDirtySummaryUnsafe()
}
