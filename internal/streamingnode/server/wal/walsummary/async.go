package walsummary

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// Run governs persistence and Delete-consumption backlog independently of
// source-message acknowledgements. Uploaded Deletes remain consumption backlog
// until materialized. Chunk and manifest I/O and retries use the scheduler.
func (m *Manager) Run(ctx context.Context, maxAge time.Duration, underPressure func() bool) {
	interval := time.Second
	if maxAge > 0 && maxAge < interval {
		interval = maxAge
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			force := underPressure != nil && underPressure()
			m.flushBacklog(now, maxAge, force)
			m.requestMaterializationBacklog(now, maxAge)
		}
	}
}

func (m *Manager) flushBacklog(now time.Time, maxAge time.Duration, force bool) {
	m.mu.Lock()
	if len(m.pending) == 0 || m.terminalErr != nil ||
		(!force && (maxAge <= 0 || now.Sub(m.pendingSince) < maxAge)) {
		m.mu.Unlock()
		return
	}
	target := m.lastObserved.TimeTick
	m.mu.Unlock()
	m.RequestFlushThrough(target)
}

// InitLastAcked seeds the caller's already published recovery position.
func (m *Manager) InitLastAcked(checkpoint *utility.WALCheckpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.advanceLastAckedLocked(checkpoint)
}

// LastAcked is the continuous, recoverable confirmation frontier. Message
// release does not authorize checkpoint advancement past this position.
func (m *Manager) LastAcked() *utility.WALCheckpoint {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastAcked == nil {
		return nil
	}
	return m.lastAcked.Clone()
}

func newSummaryCheckpoint(id message.MessageID, timetick uint64) *utility.WALCheckpoint {
	if id == nil {
		return nil
	}
	return &utility.WALCheckpoint{MessageID: id, TimeTick: timetick, Magic: utility.RecoveryMagicRecoveryStorageV2}
}

func summaryPosition(cp *utility.WALCheckpoint) *streamingpb.PChannelSummaryPosition {
	if cp == nil {
		return nil
	}
	return &streamingpb.PChannelSummaryPosition{TimeTick: cp.TimeTick, MessageId: messageIDProto(cp.MessageID)}
}

func summaryCheckpoint(p *streamingpb.PChannelSummaryPosition) *utility.WALCheckpoint {
	if p.GetMessageId() == nil {
		return nil
	}
	return newSummaryCheckpoint(message.MustUnmarshalMessageID(p.GetMessageId()), p.GetTimeTick())
}

func (m *Manager) seedLastAckedLocked(msg message.ImmutableMessage) {
	if m.lastAcked != nil {
		return
	}
	tt := msg.TimeTick()
	if tt > 0 {
		tt--
	}
	m.lastAcked = newSummaryCheckpoint(msg.LastConfirmedMessageID(), tt)
}

func (m *Manager) advanceLastAckedLocked(cp *utility.WALCheckpoint) {
	if m.terminalErr != nil || cp == nil || cp.MessageID == nil {
		return
	}
	if m.lastAcked == nil || (cp.TimeTick > m.lastAcked.TimeTick && !cp.MessageID.LT(m.lastAcked.MessageID)) {
		m.lastAcked = cp.Clone()
	}
}

func (m *Manager) refreshLastAckedLocked() {
	// Current-term data is discoverable only after its first manifest PUT.
	if m.manifestPublished {
		m.advanceLastAckedLocked(summaryCheckpoint(m.manifest.GetCoveredPosition()))
	}
	// Non-record messages need no new object. But a first-term unpublished
	// chunk must still pin confirmation even after leaving pendingSealed.
	if len(m.pending) == 0 && len(m.pendingSealed) == 0 &&
		(m.manifestPublished || m.manifest.GetLastChunk() == nil || m.manifest.GetLastChunk().GetTerm() != m.cfg.Term) {
		m.advanceLastAckedLocked(m.lastObserved)
	}
}

// RequestFlushThrough schedules progress through the observed position.
func (m *Manager) RequestFlushThrough(timetick uint64) {
	m.mu.Lock()
	covered := m.lastAcked != nil && timetick <= m.lastAcked.TimeTick
	seal := timetick > m.pendingFlushTimeTick
	m.mu.Unlock()
	if !covered && seal {
		m.seal()
	}
	m.scheduleWrite()
	m.scheduleManifest()
}

func (m *Manager) requestSeal() {
	m.seal()
	m.scheduleWrite()
}

func (m *Manager) scheduleWrite() {
	m.mu.Lock()
	if m.terminalErr != nil || m.cfg.Runtime.Scheduler == nil {
		m.mu.Unlock()
		return
	}
	var tasks []*chunkWriteTask
	for _, sc := range m.pendingSealed {
		if sc.task == nil {
			sc.task = &chunkWriteTask{manager: m, chunk: sc}
			tasks = append(tasks, sc.task)
		}
	}
	m.mu.Unlock()
	for _, task := range tasks {
		m.cfg.Runtime.Scheduler.Submit(task)
	}
}

func (m *Manager) scheduleManifest() {
	m.mu.Lock()
	if m.terminalErr != nil || m.cfg.Runtime.Scheduler == nil || m.manifestVersion == m.publishedVersion ||
		(m.manifestTask != nil && !m.manifestTask.Done()) {
		m.mu.Unlock()
		return
	}
	task := &manifestWriteTask{manager: m}
	m.manifestTask = task
	m.mu.Unlock()
	m.cfg.Runtime.Scheduler.Submit(task)
}

// HasPendingWork includes publication-only work, even without another chunk.
func (m *Manager) HasPendingWork() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pending) > 0 || len(m.pendingSealed) > 0 || m.manifestVersion != m.publishedVersion ||
		(m.manifestTask != nil && !m.manifestTask.Done()) || (m.gcTask != nil && !m.gcTask.Done())
}

func (m *Manager) taskError(ctx context.Context, err error) error {
	if !errors.Is(err, ErrStoreCorrupted) {
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	m.mu.Lock()
	m.terminalErr = err
	m.mu.Unlock()
	if m.cfg.Logger != nil {
		m.cfg.Logger.Error(ctx, "summary persistence failed", mlog.Err(err))
	}
	return err
}

type chunkWriteTask struct {
	manager *Manager
	chunk   *SealedChunk
	done    atomic.Bool
}

func (t *chunkWriteTask) Done() bool { return t.done.Load() }
func (t *chunkWriteTask) Execute(ctx context.Context) error {
	if t.Done() {
		return nil
	}
	if err := t.manager.writeChunk(ctx, t.chunk); err != nil {
		if errors.Is(err, ErrStoreCorrupted) {
			t.done.Store(true)
		}
		return t.manager.taskError(ctx, err)
	}
	t.done.Store(true)
	return nil
}

type manifestWriteTask struct {
	manager *Manager
	done    atomic.Bool
}

func (t *manifestWriteTask) Done() bool { return t.done.Load() }
func (t *manifestWriteTask) Execute(ctx context.Context) error {
	m := t.manager
	m.publishMu.Lock()
	defer m.publishMu.Unlock()
	if t.Done() {
		return nil
	}
	m.mu.Lock()
	if m.terminalErr != nil {
		err := m.terminalErr
		t.done.Store(true)
		m.mu.Unlock()
		return err
	}
	version := m.manifestVersion
	dirty := version != m.publishedVersion
	snapshot := proto.Clone(m.manifest).(*streamingpb.PChannelSummaryManifest)
	m.mu.Unlock()
	if dirty {
		if err := m.cfg.Store.WriteManifest(ctx, snapshot); err != nil {
			if errors.Is(err, ErrStoreCorrupted) {
				t.done.Store(true)
			}
			return m.taskError(ctx, err)
		}
		m.mu.Lock()
		m.publishedVersion = version
		m.manifestPublished = true
		m.refreshLastAckedLocked()
		m.mu.Unlock()
	}
	// A complete current-term manifest releases older manifests independently
	// of the lifetime of the old chunks it still references.
	if err := m.cfg.Store.DeleteManifestsBelowTerm(ctx, m.cfg.Term); err != nil {
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.manifestVersion != m.publishedVersion {
		// Yield between snapshots so a steady upload stream cannot monopolize
		// this worker and the publication mutex indefinitely.
		return nodescheduler.ErrDelay
	}
	t.done.Store(true)
	return nil
}

var (
	_ nodescheduler.Task = (*chunkWriteTask)(nil)
	_ nodescheduler.Task = (*manifestWriteTask)(nil)
)
