package recovery

import (
	"context"
	"time"

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
			if err := m.cleanPChannelWindow(m.windowBackgroundTaskNotifier.Context(), m.Logger()); err != nil {
				m.Logger().Warn(context.TODO(), "failed to clean pchannel window", mlog.Err(err))
			}
		}
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
