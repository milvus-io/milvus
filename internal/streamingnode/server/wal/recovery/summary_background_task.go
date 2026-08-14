package recovery

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func (m *summaryManager) summaryBackgroundTask() {
	var ticker *time.Ticker
	var tick <-chan time.Time
	if m.cfg.idempotencySnapshotInterval > 0 {
		ticker = time.NewTicker(m.cfg.idempotencySnapshotInterval)
		tick = ticker.C
	}
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
		m.Logger().Info(context.TODO(), "idempotency summary background task, perform a graceful exit...")
		if err := m.persistIdempotencySnapshotWhenClosing(); err != nil {
			m.Logger().Warn(context.TODO(), "failed to persist idempotency summary snapshot when closing", mlog.Err(err))
		}
		m.summaryBackgroundTaskNotifier.Finish(struct{}{})
		m.Logger().Info(context.TODO(), "idempotency summary background task exit")
	}()

	for {
		select {
		case <-m.summaryBackgroundTaskNotifier.Context().Done():
			return
		case <-tick:
			if err := m.persistIdempotencySnapshot(m.summaryBackgroundTaskNotifier.Context(), mlog.DebugLevel); err != nil {
				return
			}
			m.advanceIdleSourceCheckpoint(m.summaryBackgroundTaskNotifier.Context())
			m.drainDroppedSummaryMetas(m.summaryBackgroundTaskNotifier.Context())
			if err := m.cleanPChannelSummary(m.summaryBackgroundTaskNotifier.Context(), m.Logger()); err != nil {
				m.Logger().Warn(context.TODO(), "failed to clean pchannel summary", mlog.Err(err))
			}
		}
	}
}

// advanceIdleSourceCheckpoint persists a meta-only SourceCheckpoint advance
// when the summary store is clean but the observed position moved on (an idle
// pchannel sees only timeticks; nothing marks a summary dirty, so no chunk is
// written and the durable source checkpoint would freeze). Without it the
// truncation clamp (truncateClampCheckpoint) pins WAL truncation at the last
// busy period forever. Safe: a clean store means every keyed committed write
// observed so far is already persisted, and any write arriving after the check
// carries a timetick beyond the advanced position. Best-effort — a failure only
// logs and the next tick retries.
func (m *summaryManager) advanceIdleSourceCheckpoint(ctx context.Context) {
	if !m.cfg.idempotencyEnabled {
		return
	}
	m.mu.Lock()
	if !m.activeViewsInitialized || m.hasDirtySummaryUnsafe() || m.pendingIdempotencyPersistSnapshot != nil {
		m.mu.Unlock()
		return
	}
	current := m.getPChannelSummarySnapshotCheckpointUnsafe()
	persisted := m.getPersistedPChannelSummarySnapshotCheckpointUnsafe()
	m.mu.Unlock()
	if current == nil || persisted == nil || current.TimeTick <= persisted.TimeTick {
		return
	}
	catalog := resource.Resource().StreamingNodeCatalog()
	metaPB, err := catalog.GetPChannelSummaryMeta(ctx, m.pchannel)
	if err != nil || metaPB == nil {
		if err != nil {
			m.Logger().Warn(ctx, "failed to load pchannel summary meta for idle source checkpoint advance", mlog.Err(err))
		}
		return
	}
	if metaPB.GetTerm() > m.term {
		// A newer owner took over the store: a stale owner advancing the
		// source checkpoint would prematurely unclamp WAL truncation for the
		// current owner. Stop silently; this WAL is about to close anyway.
		return
	}
	updated := proto.Clone(metaPB).(*streamingpb.PChannelSummaryMeta)
	updated.SourceCheckpointTimetick = current.TimeTick
	if current.MessageID != nil {
		updated.SourceCheckpointMessageId = current.MessageID.IntoProto()
	}
	updated.Term = m.term
	swapped, err := compareAndSwapPChannelSummaryMeta(ctx, m.Logger(), m.pchannel, metaPB, updated)
	if err != nil {
		m.Logger().Warn(ctx, "failed to advance idle pchannel summary source checkpoint", mlog.Err(err))
		return
	}
	if !swapped {
		m.Logger().Warn(ctx, "pchannel summary source checkpoint advance lost CAS race")
		return
	}
	m.mu.Lock()
	m.markPChannelSummarySnapshotCheckpointPersisted(current)
	m.mu.Unlock()
}

// drainDroppedSummaryMetas removes the persisted vchannel summary metas of
// vchannels reclaimed since the last tick — off the drop hot path, batched, and
// retried on the next tick on failure. Without it every dropped vchannel leaves
// a permanent etcd key behind.
func (m *summaryManager) drainDroppedSummaryMetas(ctx context.Context) {
	m.mu.Lock()
	dropped := m.droppedSummaryVChannels
	m.droppedSummaryVChannels = nil
	m.mu.Unlock()
	if len(dropped) == 0 {
		return
	}
	if err := resource.Resource().StreamingNodeCatalog().RemoveVChannelSummaryMetas(ctx, m.pchannel, common.VChannelSummaryViewTypeIdempotency, dropped); err != nil {
		m.Logger().Warn(ctx, "failed to remove dropped vchannel summary metas; will retry next tick", mlog.Err(err))
		m.mu.Lock()
		m.droppedSummaryVChannels = append(m.droppedSummaryVChannels, dropped...)
		m.mu.Unlock()
	}
}

func (m *summaryManager) persistIdempotencySnapshotWhenClosing() error {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.gracefulTimeout)
	defer cancel()

	for m.isSummaryDirty() {
		if err := m.persistIdempotencySnapshot(ctx, mlog.InfoLevel); err != nil {
			return err
		}
	}
	return m.cleanPChannelSummary(ctx, m.Logger())
}

func (r *recoveryStorageImpl) ForcePersistSummaryToTimeTick(ctx context.Context, targetTimeTick uint64) (*WALCheckpoint, error) {
	return r.summaryManager.forcePersistSummaryToTimeTick(ctx, targetTimeTick)
}

func (m *summaryManager) forcePersistSummaryToTimeTick(ctx context.Context, targetTimeTick uint64) (*WALCheckpoint, error) {
	if m == nil || !m.cfg.idempotencyEnabled {
		return &WALCheckpoint{TimeTick: targetTimeTick}, nil
	}
	for {
		m.mu.Lock()
		persisted := m.getPersistedPChannelSummarySnapshotCheckpointUnsafe()
		if persisted != nil && persisted.TimeTick >= targetTimeTick {
			m.mu.Unlock()
			return persisted, nil
		}
		current := m.getPChannelSummarySnapshotCheckpointUnsafe()
		if current == nil || current.TimeTick < targetTimeTick {
			m.mu.Unlock()
			return persisted, nil
		}
		snapshot := m.pendingIdempotencyPersistSnapshot
		if snapshot == nil {
			if m.hasDirtySummaryUnsafe() {
				snapshot = m.consumeIdempotencySnapshotLocked()
			} else {
				snapshot = &RecoverySnapshot{
					Checkpoint:                      current,
					pchannelSummarySourceCheckpoint: current,
				}
			}
			m.pendingIdempotencyPersistSnapshot = snapshot
		}
		m.mu.Unlock()

		if err := m.persistIdempotencySnapshotData(ctx, mlog.InfoLevel, snapshot); err != nil {
			return nil, err
		}
	}
}

func (m *summaryManager) isSummaryDirty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isSummaryDirtyUnsafe()
}

func (m *summaryManager) isSummaryDirtyUnsafe() bool {
	return m.pendingIdempotencyPersistSnapshot != nil || m.hasDirtySummaryUnsafe()
}

func (m *summaryManager) persistIdempotencySnapshot(ctx context.Context, lvl mlog.Level) error {
	snapshot := m.ensurePendingIdempotencyPersistSnapshot()
	return m.persistIdempotencySnapshotData(ctx, lvl, snapshot)
}

func (m *summaryManager) consumeIdempotencySnapshot() *RecoverySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.consumeIdempotencySnapshotLocked()
}

func (m *summaryManager) consumeIdempotencySnapshotLocked() *RecoverySnapshot {
	if !m.hasDirtySummaryUnsafe() {
		return nil
	}
	pchannelSummaryRecords, vchannelSummaryMetaUpdates, pchannelSummarySourceCheckpoint := m.consumePendingSummaryEntries()
	return &RecoverySnapshot{
		pchannelSummaryRecords:          pchannelSummaryRecords,
		vchannelSummaryMetaUpdates:      vchannelSummaryMetaUpdates,
		pchannelSummarySourceCheckpoint: pchannelSummarySourceCheckpoint,
		// The current summary snapshot checkpoint is advanced per message from the
		// consume checkpoint; use it instead of reaching into rs so the background
		// task stays self-contained under the summary's own lock.
		Checkpoint: m.getPChannelSummarySnapshotCheckpointUnsafe(),
	}
}

func (m *summaryManager) persistIdempotencySnapshotData(ctx context.Context, lvl mlog.Level, snapshot *RecoverySnapshot) (err error) {
	if snapshot == nil {
		return nil
	}

	logger := m.Logger().With(
		mlog.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		mlog.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
		mlog.Int("pchannelSummaryVChannelCount", len(snapshot.pchannelSummaryRecords)),
	)
	defer func() {
		if err != nil {
			logger.Warn(ctx, "failed to persist idempotency summary snapshot", mlog.Err(err))
			return
		}
		m.clearPendingIdempotencyPersistSnapshot()
		logger.Log(ctx, lvl, "persist idempotency summary snapshot")
	}()

	if snapshot.pchannelSummarySourceCheckpoint == nil {
		return nil
	}

	summaryMetas, generation, err := m.persistPChannelSummary(ctx, logger, snapshot.pchannelSummaryRecords, snapshot.vchannelSummaryMetaUpdates, snapshot.pchannelSummarySourceCheckpoint)
	if err != nil {
		m.metrics.ObserveIdempotencySnapshot(false)
		return err
	}
	m.metrics.ObserveIdempotencySnapshot(true)
	mainCheckpointSeconds := tsoutil.PhysicalTimeSeconds(snapshot.Checkpoint.TimeTick)
	pchannelSummaryCheckpointSeconds := tsoutil.PhysicalTimeSeconds(snapshot.pchannelSummarySourceCheckpoint.TimeTick)
	m.metrics.ObserveIdempotencySnapshotCheckpointLag(mainCheckpointSeconds - pchannelSummaryCheckpointSeconds)
	m.markVChannelSummariesPersisted(snapshot.pchannelSummaryRecords, summaryMetas, generation, snapshot.pchannelSummarySourceCheckpoint)
	return nil
}

func (m *summaryManager) ensurePendingIdempotencyPersistSnapshot() *RecoverySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingIdempotencyPersistSnapshot == nil {
		m.pendingIdempotencyPersistSnapshot = m.consumeIdempotencySnapshotLocked()
	}
	return m.pendingIdempotencyPersistSnapshot
}

func (m *summaryManager) clearPendingIdempotencyPersistSnapshot() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingIdempotencyPersistSnapshot = nil
}
