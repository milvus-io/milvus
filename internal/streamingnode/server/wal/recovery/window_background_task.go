package recovery

import (
	"context"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

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
		m.Logger().Info("idempotency window background task, perform a graceful exit...")
		if err := m.persistIdempotencySnapshotWhenClosing(); err != nil {
			m.Logger().Warn("failed to persist idempotency window snapshot when closing", zap.Error(err))
		}
		m.windowBackgroundTaskNotifier.Finish(struct{}{})
		m.Logger().Info("idempotency window background task exit")
	}()

	for {
		select {
		case <-m.windowBackgroundTaskNotifier.Context().Done():
			return
		case <-tick:
			if err := m.persistIdempotencySnapshot(m.windowBackgroundTaskNotifier.Context(), zap.DebugLevel); err != nil {
				return
			}
			if err := m.cleanPChannelWindow(m.windowBackgroundTaskNotifier.Context(), m.Logger()); err != nil {
				m.Logger().Warn("failed to clean pchannel window", zap.Error(err))
			}
		}
	}
}

func (m *windowManager) persistIdempotencySnapshotWhenClosing() error {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.gracefulTimeout)
	defer cancel()

	for m.isIdempotencyWindowDirty() {
		if err := m.persistIdempotencySnapshot(ctx, zap.InfoLevel); err != nil {
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

func (m *windowManager) persistIdempotencySnapshot(ctx context.Context, lvl zapcore.Level) error {
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
	pchannelWindowRecords, vchannelWindowMetaUpdates, pchannelWindowSourceCheckpoint :=
		m.consumePendingCommittedWriteRecords()
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

func (m *windowManager) persistIdempotencySnapshotData(ctx context.Context, lvl zapcore.Level, snapshot *RecoverySnapshot) (err error) {
	if snapshot == nil {
		return nil
	}

	logger := m.Logger().With(
		zap.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		zap.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
		zap.Int("pchannelWindowVChannelCount", len(snapshot.pchannelWindowRecords)),
	)
	defer func() {
		if err != nil {
			logger.Warn("failed to persist idempotency window snapshot", zap.Error(err))
			return
		}
		m.clearPendingIdempotencyPersistSnapshot()
		logger.Log(lvl, "persist idempotency window snapshot")
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
