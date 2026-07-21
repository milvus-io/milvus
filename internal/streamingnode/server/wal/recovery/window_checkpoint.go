package recovery

// effectivePersistCheckpoint clamps the consume checkpoint by the pchannel
// window snapshot checkpoint and the flusher checkpoint, both supplied by the
// caller (windowManager no longer reaches into recoveryStorageImpl for them).
func (m *windowManager) effectivePersistCheckpoint(snapshot *RecoverySnapshot, flusherCheckpoint *WALCheckpoint) *WALCheckpoint {
	return clampPersistCheckpoint(snapshot.Checkpoint, m.pchannelWindowCheckpointForPersist(snapshot), m.flusherClampCheckpoint(flusherCheckpoint))
}

// flusherClampCheckpoint gates the flusher term of the persist clamp on the
// idempotency feature: only window replay (which must re-observe messages the
// flusher has not sealed yet) needs the consume checkpoint held back to the
// flusher position. Without idempotency this clamp would just pin the persisted
// consume checkpoint to the slowest vchannel's flusher and blow up the WAL span
// replayed on restart — WAL truncation takes its own min against the flusher
// separately (simpleTruncateCheckpoint), so it never needed this clamp.
func (m *windowManager) flusherClampCheckpoint(flusherCheckpoint *WALCheckpoint) *WALCheckpoint {
	if !m.cfg.idempotencyEnabled {
		return nil
	}
	return flusherCheckpoint
}

// clampPersistCheckpoint lowers base to the earliest (by timetick) of itself, the
// pchannel window snapshot checkpoint, and the flusher checkpoint, so the consume
// checkpoint never advances past un-persisted window data or unflushed data.
func clampPersistCheckpoint(base, pchannelWindowCheckpoint, flusherCheckpoint *WALCheckpoint) *WALCheckpoint {
	if base == nil {
		return nil
	}
	checkpoint := base.Clone()
	if pchannelWindowCheckpoint != nil {
		checkpoint = clampCheckpointPositionByTimeTick(checkpoint, pchannelWindowCheckpoint)
	}
	if flusherCheckpoint != nil {
		checkpoint = clampCheckpointPositionByTimeTick(checkpoint, flusherCheckpoint)
	}
	return checkpoint
}

// canPersistConsumeCheckpoint reports whether the consume checkpoint, clamped by
// the window snapshot checkpoint, has advanced past what is already persisted. It
// reads window state, so it takes m.mu; callers hold rs.mu, preserving the
// rs.mu -> m.mu order.
func (m *windowManager) canPersistConsumeCheckpoint(consumeCheckpoint, flusherCheckpoint *WALCheckpoint) bool {
	if consumeCheckpoint == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.canPersistConsumeCheckpointUnsafe(
		m.effectivePersistCheckpointUnsafe(consumeCheckpoint, flusherCheckpoint),
	)
}

func (m *windowManager) markConsumeCheckpointPersisted(checkpoint *WALCheckpoint) {
	if checkpoint == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.persistedConsumeCheckpoint == nil || !checkpointTimeTickAhead(m.persistedConsumeCheckpoint, checkpoint) {
		m.persistedConsumeCheckpoint = checkpoint.Clone()
	}
}

func (m *windowManager) effectivePersistCheckpointUnsafe(checkpoint, flusherCheckpoint *WALCheckpoint) *WALCheckpoint {
	return clampPersistCheckpoint(checkpoint, m.pchannelWindowCheckpointForPersistUnsafe(), m.flusherClampCheckpoint(flusherCheckpoint))
}

func (m *windowManager) pchannelWindowCheckpointForPersist(snapshot *RecoverySnapshot) *WALCheckpoint {
	if snapshot.pchannelWindowSourceCheckpoint != nil {
		return snapshot.pchannelWindowSourceCheckpoint
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pchannelWindowCheckpointForPersistUnsafe()
}

func (m *windowManager) pchannelWindowCheckpointForPersistUnsafe() *WALCheckpoint {
	if !m.hasDirtyWindowUnsafe() && m.pendingIdempotencyPersistSnapshot == nil {
		return nil
	}
	return m.getPersistedPChannelWindowSnapshotCheckpointUnsafe()
}

func (m *windowManager) setPChannelWindowSnapshotCheckpoint(checkpoint *WALCheckpoint) {
	m.windowSnapshotCheckpoint.set(checkpoint)
}

func (m *windowManager) advancePChannelWindowSnapshotCheckpoint(checkpoint *WALCheckpoint) {
	m.windowSnapshotCheckpoint.advance(checkpoint)
}

func (m *windowManager) getPChannelWindowSnapshotCheckpointUnsafe() *WALCheckpoint {
	return m.windowSnapshotCheckpoint.currentClone()
}

func (m *windowManager) markPChannelWindowSnapshotCheckpointPersisted(checkpoint *WALCheckpoint) {
	m.windowSnapshotCheckpoint.markPersisted(checkpoint)
}

func (m *windowManager) getPersistedPChannelWindowSnapshotCheckpointUnsafe() *WALCheckpoint {
	return m.windowSnapshotCheckpoint.persistedClone()
}

// trackedCheckpoint holds a checkpoint position together with the position that
// has already been durably persisted. Both advance monotonically by time tick;
// persisted trails current.
type trackedCheckpoint struct {
	current   *WALCheckpoint
	persisted *WALCheckpoint
}

func (t *trackedCheckpoint) set(checkpoint *WALCheckpoint) {
	if checkpoint == nil {
		t.current, t.persisted = nil, nil
		return
	}
	t.current = checkpoint.Clone()
	t.persisted = checkpoint.Clone()
}

func (t *trackedCheckpoint) advance(checkpoint *WALCheckpoint) {
	if checkpoint == nil {
		return
	}
	if t.current == nil || t.current.TimeTick < checkpoint.TimeTick {
		t.current = checkpoint.Clone()
	}
}

func (t *trackedCheckpoint) markPersisted(checkpoint *WALCheckpoint) {
	if checkpoint == nil {
		return
	}
	if t.persisted == nil || t.persisted.TimeTick < checkpoint.TimeTick {
		t.persisted = checkpoint.Clone()
	}
}

func (t *trackedCheckpoint) currentClone() *WALCheckpoint {
	if t.current == nil {
		return nil
	}
	return t.current.Clone()
}

func (t *trackedCheckpoint) persistedClone() *WALCheckpoint {
	if t.persisted == nil {
		return nil
	}
	return t.persisted.Clone()
}

func (m *windowManager) canPersistConsumeCheckpointUnsafe(checkpoint *WALCheckpoint) bool {
	if m.persistedConsumeCheckpoint == nil || checkpoint == nil {
		return false
	}
	return checkpointTimeTickAhead(checkpoint, m.persistedConsumeCheckpoint)
}

func checkpointTimeTickAhead(left, right *WALCheckpoint) bool {
	return left != nil && right != nil && left.TimeTick > right.TimeTick
}
