package recovery

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func (m *windowManager) windowBackgroundTask() {
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
		m.Logger().Info(context.TODO(), "idempotency window background task, perform a graceful exit...")
		if err := m.persistIdempotencySnapshotWhenClosing(); err != nil {
			m.Logger().Warn(context.TODO(), "failed to persist idempotency window snapshot when closing", mlog.Err(err))
		}
		m.windowBackgroundTaskNotifier.Finish(struct{}{})
		m.Logger().Info(context.TODO(), "idempotency window background task exit")
	}()

	for {
		select {
		case <-m.windowBackgroundTaskNotifier.Context().Done():
			return
		case <-tick:
			if err := m.persistIdempotencySnapshot(m.windowBackgroundTaskNotifier.Context(), mlog.DebugLevel); err != nil {
				return
			}
			m.advanceIdleSourceCheckpoint(m.windowBackgroundTaskNotifier.Context())
			m.drainDroppedWindowMetas(m.windowBackgroundTaskNotifier.Context())
			if err := m.cleanPChannelWindow(m.windowBackgroundTaskNotifier.Context(), m.Logger()); err != nil {
				m.Logger().Warn(context.TODO(), "failed to clean pchannel window", mlog.Err(err))
			}
		}
	}
}

// advanceIdleSourceCheckpoint persists a meta-only SourceCheckpoint advance
// when the window store is clean but the observed position moved on (an idle
// pchannel sees only timeticks; nothing marks a window dirty, so no chunk is
// written and the durable source checkpoint would freeze). Without it the
// truncation clamp (truncateClampCheckpoint) pins WAL truncation at the last
// busy period forever. Safe: a clean store means every keyed committed write
// observed so far is already persisted, and any write arriving after the check
// carries a timetick beyond the advanced position. Best-effort — a failure only
// logs and the next tick retries.
func (m *windowManager) advanceIdleSourceCheckpoint(ctx context.Context) {
	if !m.cfg.idempotencyEnabled {
		return
	}
	m.mu.Lock()
	if !m.activeViewsInitialized || m.hasDirtyWindowUnsafe() || m.pendingIdempotencyPersistSnapshot != nil {
		m.mu.Unlock()
		return
	}
	current := m.getPChannelWindowSnapshotCheckpointUnsafe()
	persisted := m.getPersistedPChannelWindowSnapshotCheckpointUnsafe()
	m.mu.Unlock()
	if current == nil || persisted == nil || current.TimeTick <= persisted.TimeTick {
		return
	}
	catalog := resource.Resource().StreamingNodeCatalog()
	metaPB, err := catalog.GetPChannelWindowMeta(ctx, m.pchannel)
	if err != nil || metaPB == nil {
		if err != nil {
			m.Logger().Warn(ctx, "failed to load pchannel window meta for idle source checkpoint advance", mlog.Err(err))
		}
		return
	}
	if metaPB.GetTerm() > m.term {
		// A newer owner took over the store: a stale owner advancing the
		// source checkpoint would prematurely unclamp WAL truncation for the
		// current owner. Stop silently; this WAL is about to close anyway.
		return
	}
	metaPB.SourceCheckpointTimetick = current.TimeTick
	if current.MessageID != nil {
		metaPB.SourceCheckpointMessageId = current.MessageID.IntoProto()
	}
	metaPB.Term = m.term
	if err := catalog.SavePChannelWindowMeta(ctx, m.pchannel, metaPB); err != nil {
		m.Logger().Warn(ctx, "failed to advance idle pchannel window source checkpoint", mlog.Err(err))
		return
	}
	m.markPChannelWindowSnapshotCheckpointPersisted(current)
}

// drainDroppedWindowMetas removes the persisted vchannel window metas of
// vchannels reclaimed since the last tick — off the drop hot path, batched, and
// retried on the next tick on failure. Without it every dropped vchannel leaves
// a permanent etcd key behind.
func (m *windowManager) drainDroppedWindowMetas(ctx context.Context) {
	m.mu.Lock()
	dropped := m.droppedWindowVChannels
	m.droppedWindowVChannels = nil
	m.mu.Unlock()
	if len(dropped) == 0 {
		return
	}
	if err := resource.Resource().StreamingNodeCatalog().RemoveVChannelWindowMetas(ctx, m.pchannel, common.VChannelWindowViewTypeIdempotency, dropped); err != nil {
		m.Logger().Warn(ctx, "failed to remove dropped vchannel window metas; will retry next tick", mlog.Err(err))
		m.mu.Lock()
		m.droppedWindowVChannels = append(m.droppedWindowVChannels, dropped...)
		m.mu.Unlock()
	}
}

func (m *windowManager) persistIdempotencySnapshotWhenClosing() error {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.gracefulTimeout)
	defer cancel()

	for m.isIdempotencyWindowDirty() {
		if err := m.persistIdempotencySnapshot(ctx, mlog.InfoLevel); err != nil {
			return err
		}
	}
	return m.cleanPChannelWindow(ctx, m.Logger())
}

func (m *windowManager) isIdempotencyWindowDirty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isIdempotencyWindowDirtyUnsafe()
}

func (m *windowManager) isIdempotencyWindowDirtyUnsafe() bool {
	return m.pendingIdempotencyPersistSnapshot != nil || m.hasDirtyWindowUnsafe()
}

func (m *windowManager) persistIdempotencySnapshot(ctx context.Context, lvl mlog.Level) error {
	snapshot := m.ensurePendingIdempotencyPersistSnapshot()
	return m.persistIdempotencySnapshotData(ctx, lvl, snapshot)
}

func (m *windowManager) consumeIdempotencySnapshot() *RecoverySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.consumeIdempotencySnapshotLocked()
}

func (m *windowManager) consumeIdempotencySnapshotLocked() *RecoverySnapshot {
	if !m.hasDirtyWindowUnsafe() {
		return nil
	}
	pchannelWindowRecords, vchannelWindowMetaUpdates, pchannelWindowSourceCheckpoint := m.consumePendingCommittedWriteRecords()
	return &RecoverySnapshot{
		pchannelWindowRecords:          pchannelWindowRecords,
		vchannelWindowMetaUpdates:      vchannelWindowMetaUpdates,
		pchannelWindowSourceCheckpoint: pchannelWindowSourceCheckpoint,
		// The current window snapshot checkpoint is advanced per message from the
		// consume checkpoint; use it instead of reaching into rs so the background
		// task stays self-contained under the window's own lock.
		Checkpoint: m.getPChannelWindowSnapshotCheckpointUnsafe(),
	}
}

func (m *windowManager) persistIdempotencySnapshotData(ctx context.Context, lvl mlog.Level, snapshot *RecoverySnapshot) (err error) {
	if snapshot == nil {
		return nil
	}

	logger := m.Logger().With(
		mlog.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		mlog.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
		mlog.Int("pchannelWindowVChannelCount", len(snapshot.pchannelWindowRecords)),
	)
	defer func() {
		if err != nil {
			logger.Warn(ctx, "failed to persist idempotency window snapshot", mlog.Err(err))
			return
		}
		m.clearPendingIdempotencyPersistSnapshot()
		logger.Log(ctx, lvl, "persist idempotency window snapshot")
	}()

	if snapshot.pchannelWindowSourceCheckpoint == nil {
		return nil
	}

	windowMetas, generation, err := m.persistPChannelWindow(ctx, logger, snapshot.pchannelWindowRecords, snapshot.vchannelWindowMetaUpdates, snapshot.pchannelWindowSourceCheckpoint)
	if err != nil {
		m.metrics.ObserveIdempotencySnapshot(false)
		return err
	}
	m.metrics.ObserveIdempotencySnapshot(true)
	mainCheckpointSeconds := tsoutil.PhysicalTimeSeconds(snapshot.Checkpoint.TimeTick)
	pchannelWindowCheckpointSeconds := tsoutil.PhysicalTimeSeconds(snapshot.pchannelWindowSourceCheckpoint.TimeTick)
	m.metrics.ObserveIdempotencySnapshotCheckpointLag(mainCheckpointSeconds - pchannelWindowCheckpointSeconds)
	m.markVChannelWindowsPersisted(snapshot.pchannelWindowRecords, windowMetas, generation, snapshot.pchannelWindowSourceCheckpoint)
	return nil
}

func (m *windowManager) ensurePendingIdempotencyPersistSnapshot() *RecoverySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingIdempotencyPersistSnapshot == nil {
		m.pendingIdempotencyPersistSnapshot = m.consumeIdempotencySnapshotLocked()
	}
	return m.pendingIdempotencyPersistSnapshot
}

func (m *windowManager) clearPendingIdempotencyPersistSnapshot() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingIdempotencyPersistSnapshot = nil
}
