package recovery

// effectivePersistCheckpoint clamps the consume checkpoint by the pchannel
// summary snapshot checkpoint and the flusher checkpoint. Both are supplied by
// the caller: summaryManager holds no reference back to recoveryStorageImpl, so
// the lock order stays one-directional.
func (m *summaryManager) effectivePersistCheckpoint(snapshot *RecoverySnapshot, flusherCheckpoint *WALCheckpoint) *WALCheckpoint {
	return clampPersistCheckpoint(snapshot.Checkpoint, m.pchannelSummaryCheckpointForPersist(snapshot), m.flusherClampCheckpoint(flusherCheckpoint))
}

// flusherClampCheckpoint gates the flusher term of the persist clamp on the
// idempotency feature: only summary replay (which must re-observe messages the
// flusher has not sealed yet) needs the consume checkpoint held back to the
// flusher position. Without idempotency this clamp would just pin the persisted
// consume checkpoint to the slowest vchannel's flusher and blow up the WAL span
// replayed on restart — WAL truncation takes its own min against the flusher
// separately (simpleTruncateCheckpoint), so it never needed this clamp.
func (m *summaryManager) flusherClampCheckpoint(flusherCheckpoint *WALCheckpoint) *WALCheckpoint {
	if !m.cfg.idempotencyEnabled {
		return nil
	}
	return flusherCheckpoint
}

// truncateClampCheckpoint returns the durable pchannel summary source checkpoint
// that WAL truncation must never pass, or nil when nothing constrains it.
//
// On restart rewindCheckpointForPChannelSummaryReplay resumes consuming from the
// source checkpoint recorded in the persisted summary meta, so that position must
// still be readable from the WAL. Unlike the persist clamp above, this one is NOT
// gated on the summary being dirty: an idle pchannel never marks a summary dirty
// (only summary entrys do), so no snapshot is taken and the durable
// source checkpoint freezes while timeticks keep pushing the consume and flusher
// checkpoints forward — truncating by those alone would drop the WAL entries the
// next restart rewinds to. It reads the persisted (not the current) position
// because only that one is what the catalog will hand back after a restart.
//
// Truncation therefore stalls at the frozen position while a pchannel stays idle,
// and resumes as soon as any write makes the summary dirty and advances the
// durable source checkpoint again.
func (m *summaryManager) truncateClampCheckpoint() *WALCheckpoint {
	if m == nil || !m.cfg.idempotencyEnabled {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getPersistedPChannelSummarySnapshotCheckpointUnsafe()
}

// clampPersistCheckpoint lowers base to the earliest (by timetick) of itself, the
// pchannel summary snapshot checkpoint, and the flusher checkpoint, so the consume
// checkpoint never advances past un-persisted summary data or unflushed data.
func clampPersistCheckpoint(base, pchannelSummaryCheckpoint, flusherCheckpoint *WALCheckpoint) *WALCheckpoint {
	if base == nil {
		return nil
	}
	checkpoint := base.Clone()
	if pchannelSummaryCheckpoint != nil {
		checkpoint = clampCheckpointPositionByTimeTick(checkpoint, pchannelSummaryCheckpoint)
	}
	if flusherCheckpoint != nil {
		checkpoint = clampCheckpointPositionByTimeTick(checkpoint, flusherCheckpoint)
	}
	return checkpoint
}

// canPersistConsumeCheckpoint reports whether the consume checkpoint, clamped by
// the summary snapshot checkpoint, has advanced past what is already persisted. It
// reads summary state, so it takes m.mu; callers hold rs.mu, preserving the
// rs.mu -> m.mu order.
func (m *summaryManager) canPersistConsumeCheckpoint(consumeCheckpoint, flusherCheckpoint *WALCheckpoint) bool {
	if consumeCheckpoint == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.canPersistConsumeCheckpointUnsafe(
		m.effectivePersistCheckpointUnsafe(consumeCheckpoint, flusherCheckpoint),
	)
}

func (m *summaryManager) markConsumeCheckpointPersisted(checkpoint *WALCheckpoint) {
	if checkpoint == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.persistedConsumeCheckpoint == nil || !checkpointTimeTickAhead(m.persistedConsumeCheckpoint, checkpoint) {
		m.persistedConsumeCheckpoint = checkpoint.Clone()
	}
}

func (m *summaryManager) effectivePersistCheckpointUnsafe(checkpoint, flusherCheckpoint *WALCheckpoint) *WALCheckpoint {
	return clampPersistCheckpoint(checkpoint, m.pchannelSummaryCheckpointForPersistUnsafe(), m.flusherClampCheckpoint(flusherCheckpoint))
}

func (m *summaryManager) pchannelSummaryCheckpointForPersist(snapshot *RecoverySnapshot) *WALCheckpoint {
	if snapshot.pchannelSummarySourceCheckpoint != nil {
		return snapshot.pchannelSummarySourceCheckpoint
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pchannelSummaryCheckpointForPersistUnsafe()
}

func (m *summaryManager) pchannelSummaryCheckpointForPersistUnsafe() *WALCheckpoint {
	if !m.hasDirtySummaryUnsafe() && m.pendingIdempotencyPersistSnapshot == nil {
		return nil
	}
	return m.getPersistedPChannelSummarySnapshotCheckpointUnsafe()
}

func (m *summaryManager) setPChannelSummarySnapshotCheckpoint(checkpoint *WALCheckpoint) {
	m.summarySnapshotCheckpoint.set(checkpoint)
}

func (m *summaryManager) advancePChannelSummarySnapshotCheckpoint(checkpoint *WALCheckpoint) {
	m.summarySnapshotCheckpoint.advance(checkpoint)
}

func (m *summaryManager) getPChannelSummarySnapshotCheckpointUnsafe() *WALCheckpoint {
	return m.summarySnapshotCheckpoint.currentClone()
}

func (m *summaryManager) markPChannelSummarySnapshotCheckpointPersisted(checkpoint *WALCheckpoint) {
	m.summarySnapshotCheckpoint.markPersisted(checkpoint)
}

func (m *summaryManager) getPersistedPChannelSummarySnapshotCheckpointUnsafe() *WALCheckpoint {
	return m.summarySnapshotCheckpoint.persistedClone()
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

func (m *summaryManager) canPersistConsumeCheckpointUnsafe(checkpoint *WALCheckpoint) bool {
	if m.persistedConsumeCheckpoint == nil || checkpoint == nil {
		return false
	}
	return checkpointTimeTickAhead(checkpoint, m.persistedConsumeCheckpoint)
}

func checkpointTimeTickAhead(left, right *WALCheckpoint) bool {
	return left != nil && right != nil && left.TimeTick > right.TimeTick
}
