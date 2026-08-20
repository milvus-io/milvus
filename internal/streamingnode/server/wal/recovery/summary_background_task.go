package recovery

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// summaryBackgroundTask runs GC and nothing else.
//
// There is deliberately no background persist. A summary chunk is written
// synchronously from the recovery snapshot persist, immediately before the
// consume checkpoint it covers is saved (persistSummaryForCheckpoint). That
// ordering is what lets the WAL checkpoint stand as the single boundary between
// "already in a chunk" and "still in the WAL": a second, independently scheduled
// writer would reintroduce the drift this design exists to remove.
//
// A staged summary still gets written promptly on an otherwise idle pchannel,
// because a dirty summary is itself dirty recovery state and triggers the
// recovery persist.
func (m *summaryManager) summaryBackgroundTask() {
	ticker := time.NewTicker(m.cfg.idempotencyGCInterval)
	defer func() {
		ticker.Stop()
		m.Logger().Info(context.TODO(), "summary store background task, perform a graceful exit...")
		m.drainPendingGCWhenClosing()
		m.summaryBackgroundTaskNotifier.Finish(struct{}{})
		m.Logger().Info(context.TODO(), "summary store background task exit")
	}()

	for {
		select {
		case <-m.summaryBackgroundTaskNotifier.Context().Done():
			return
		case <-ticker.C:
			// The work list comes from the manifest: a chunk lands in pending_gc only
			// when the retention boundary slides during a persist. Nothing here
			// decides what to delete.
			if err := m.runPendingGC(m.summaryBackgroundTaskNotifier.Context(), m.Logger()); err != nil {
				m.Logger().Warn(context.TODO(), "failed to run pending summary gc", mlog.Err(err))
			}
		}
	}
}

func (m *summaryManager) drainPendingGCWhenClosing() {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.gracefulTimeout)
	defer cancel()
	if err := m.runPendingGC(ctx, m.Logger()); err != nil {
		m.Logger().Warn(ctx, "failed to drain pending summary gc while closing", mlog.Err(err))
	}
}

// persistSummaryForCheckpoint writes the chunk covering everything staged so
// far. It is called from the recovery snapshot persist BEFORE the consume
// checkpoint is saved, which is the whole of the ordering guarantee -- the
// checkpoint is not passed in because nothing here records it.
//
// Returning an error must fail that persist. A checkpoint that advanced past a
// chunk which was never written would leave those keys in neither the store nor
// the replayable WAL, and nothing downstream could detect it. Blocking on an
// unavailable object store is the correct outcome, not a degradation to work
// around.
func (m *summaryManager) persistSummaryForCheckpoint(ctx context.Context, logger *mlog.Logger) error {
	if m == nil || !m.cfg.idempotencyEnabled {
		return nil
	}
	m.mu.Lock()
	records := m.consumePendingSummaryRecordsUnsafe()
	m.mu.Unlock()
	if len(records) == 0 {
		return nil
	}
	if _, err := m.persistPChannelSummary(ctx, logger, records); err != nil {
		m.metrics.ObserveIdempotencyPersist(false)
		return err
	}
	m.metrics.ObserveIdempotencyPersist(true)
	m.evictPersistedRecords()
	return nil
}

func (m *summaryManager) consumePendingSummaryRecordsUnsafe() map[string][]*SummaryRecord {
	if len(m.summaries()) == 0 || !m.hasDirtySummaryUnsafe() {
		return nil
	}
	recordsByVChannel := make(map[string][]*SummaryRecord)
	for _, summary := range m.summaries() {
		if records := summary.consumePendingSummaryRecords(); len(records) > 0 {
			recordsByVChannel[summary.vchannel] = records
		}
	}
	return recordsByVChannel
}
