// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package walsummary

import (
	"context"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// DroppedVChannelTimeTick is the special GC position a caller reports through
// AdvanceGCTimeTick to tell the summary that a vchannel has been dropped: its
// cleanup snapshot is durable, so every record of it — staged, sealed or
// already chunked — may be released by retention GC regardless of
// materialization.
const DroppedVChannelTimeTick = math.MaxUint64

// Manager is the pchannel-scoped WALSummary runtime. A summary is one
// contiguous dense span of the pchannel log kept in two forms:
//
//   - in memory: the transform records of the span not yet sealed into a
//     chunk. ObserveMessage builds the record of every delete-carrying
//     message immediately and copies it into the pending buffer; the WAL
//     message itself is never retained.
//   - in object storage: sealed chunks, an append-only time-ordered log of
//     per-vchannel transform records, indexed by the manifest.
//
// The summary consumes the WAL continuously: a record is durable exactly when
// every earlier record is durable, so no message reference is needed to bound
// the WAL checkpoint. Instead the manager exposes its own confirmation
// frontier (lastAcked), and the recovery storage merges it with the ack
// tracker's completed point so the persisted checkpoint never outruns an
// unflushed record (see recoveryStorageImpl.consumeDirtySnapshot).
//
// A single manager-level lock guards every piece of state: the pending
// records, the sealed-but-unwritten chunks, the manifest and its version
// (used as a compare-and-swap token so the object-storage write can happen
// outside the lock), and the per-vchannel GC positions.
type Manager struct {
	mu  sync.Mutex
	cfg ManagerConfig

	// pending holds the transform records of the current (unsealed) chunk
	// span, in WAL order. Each record carries the built entry and the message
	// ID; the WAL message itself is NOT retained — the summary never holds a
	// reference, so message acknowledgement is fully decoupled from it. The
	// lifecycle of this buffer is exactly one FlushChunk: it accumulates from
	// the first observed message until the chunk is sealed, then starts over.
	pending      []stagedRecord
	pendingBytes uint64

	// pendingSealed holds the chunks sealed but not yet durable end to end,
	// in generation order. The single write task drains this queue; a chunk
	// is popped only after its object AND its manifest record are durable.
	pendingSealed []*SealedChunk
	// pendingFlushTimeTick is the newest timetick covered by a sealed chunk.
	// RequestFlushThrough is a no-op while it already covers the request.
	pendingFlushTimeTick uint64
	// flushTasks holds the in-flight / queued write tasks. At most one task
	// exists at a time; it drains the whole pendingSealed queue.
	flushTasks []*summaryWriteTask

	// manifest is the in-memory chunk index (the persistent form's index).
	// It is only ever replaced via publishManifest, which writes the edited
	// clone outside the lock and installs it back under the lock unless the
	// manifestVersion moved in between (CAS).
	manifest        *streamingpb.PChannelSummaryManifest
	manifestVersion uint64
	nextGeneration  uint64
	// latestCoveredTimeTick is the newest timetick covered by a durable chunk.
	latestCoveredTimeTick uint64
	// durableFrontiers holds the newest durable record timetick per vchannel,
	// restored from the manifest and advanced by every completed write. It is
	// the replay filter of ObserveMessage: recovery re-observes records the
	// manifest already covers, and they must not be staged again.
	durableFrontiers map[string]uint64

	// lastAcked is the newest WAL message the summary has confirmed: a message
	// that either produces no record (DDL/flush/barrier, confirmed at
	// observation) or whose record is durable. The recovery checkpoint never
	// advances past it, so a staged-but-not-yet-durable delete record is always
	// covered by the WAL truncation. nil means no message is confirmed yet (a
	// fresh pchannel); the first staged record initializes it.
	lastAcked *utility.WALCheckpoint

	// abandonedFrontier is the message ID of the oldest record whose chunk was
	// abandoned (never durable under this term). The confirmation frontier
	// must stay strictly before it — the abandoned record will only be
	// replayed after a restart, so the WAL checkpoint must not truncate it in
	// the meantime. nil when no record was abandoned.
	abandonedFrontier message.MessageID

	// gcFrontiers holds the GC position per vchannel: records with timetick
	// at or below the position (or everything, when DroppedVChannelTimeTick)
	// may be released. It is advanced by AdvanceGCTimeTick and restored from
	// the VChannelMeta by Restore.
	gcFrontiers map[string]uint64
}

// ManagerConfig carries the wiring of one pchannel's summary manager.
type ManagerConfig struct {
	PChannel string
	Term     int64
	// Store is the object storage layer of the summary store.
	Store *Store
	// Runtime provides the scheduler and the module notifier.
	Runtime moduleapi.Runtime
	// FlushMaxBytes is the pchannel-wide pending size that seals a chunk:
	// the manager accumulates the estimated size of the retained messages
	// and seals once the total reaches the threshold, so chunk size tracks
	// the configured value.
	FlushMaxBytes uint64
	// RetentionMaxBytes is the soft budget of the retained chunk objects. GC
	// releases chunks above the budget, bounded below by the per-vchannel GC
	// positions.
	RetentionMaxBytes uint64
	Logger            *mlog.Logger
}

// NewManager creates the summary manager of one pchannel.
func NewManager(config ManagerConfig) *Manager {
	return &Manager{
		cfg:              config,
		manifest:         &streamingpb.PChannelSummaryManifest{},
		durableFrontiers: make(map[string]uint64),
		gcFrontiers:      make(map[string]uint64),
	}
}

// ObserveMessage observes one WAL message at the pchannel level. It is called
// on the WAL observation path (recovery replay and the live scanner),
// independent of the vchannel modules, and must not block.
//
// Only delete-carrying messages produce a transform record (see
// BuildTransformLogEntry); DDL, flush and barrier messages never do. The
// record of a delete message is built here and copied into the pending
// buffer — the message handle is not retained, so its acknowledgement never
// depends on the summary. When the pending total reaches FlushMaxBytes, the
// pending span is sealed into a chunk and an asynchronous write task is
// submitted. Messages without a per-vchannel record (all-channel time ticks,
// pchannel-level broadcasts, control-channel messages) and records the
// manifest already covers are not staged; all-channel time ticks still
// advance the confirmation frontier so an idle pchannel refreshes its
// persisted checkpoint TimeTick (see lastAcked).
func (m *Manager) ObserveMessage(ctx context.Context, retained message.RetainedImmutableMessage) {
	if retained == nil {
		return
	}
	msg := retained.Message()
	vchannel := msg.VChannel()
	if funcutil.IsControlChannel(vchannel) {
		// The control channel is the control plane: it never produces
		// transform records and its progress is not summarized.
		return
	}
	if vchannel == "" {
		// All-channel messages carry no per-vchannel record. Only time ticks
		// advance the confirmation frontier: they carry the freshest WAL
		// position on an idle pchannel, and the recovery storage merges this
		// frontier into the persisted checkpoint — without it, an idle
		// pchannel would never refresh its checkpoint TimeTick. Other
		// vchannel-less messages (e.g. pchannel-level broadcasts) must not
		// move the frontier.
		if msg.MessageType() == message.MessageTypeTimeTick {
			m.mu.Lock()
			if len(m.pending) == 0 && len(m.pendingSealed) == 0 {
				m.advanceLastAcked(msg.MessageID(), msg.TimeTick())
			}
			m.mu.Unlock()
		}
		return
	}
	if messageutil.ClassifyTransformLogMessage(msg) != messageutil.TransformLogKindDelete {
		// No record is ever produced: the message is confirmed at observation.
		// The summary's own frontier may advance past it, unless an unflushed
		// delete record pins the frontier behind it.
		m.mu.Lock()
		if len(m.pending) == 0 && len(m.pendingSealed) == 0 {
			m.advanceLastAcked(msg.MessageID(), msg.TimeTick())
		}
		m.mu.Unlock()
		return
	}
	m.mu.Lock()
	if msg.TimeTick() <= m.durableFrontiers[vchannel] {
		m.mu.Unlock()
		return
	}
	m.stageDeleteLocked(msg)
	overThreshold := m.pendingBytes >= m.cfg.FlushMaxBytes
	m.mu.Unlock()
	if overThreshold {
		m.requestSeal()
	}
}

// stageDeleteLocked appends one delete record to the pending span. Caller
// holds m.mu. The entry is built here — the message payload is not retained,
// so it must be copied before the message is released.
func (m *Manager) stageDeleteLocked(msg message.ImmutableMessage) {
	if m.lastAcked == nil {
		// No restored checkpoint and no earlier message confirmed yet (a fresh
		// pchannel whose first message is a delete): pin the frontier to the
		// last confirmed message before this record. The message carries its
		// own last-confirmed pointer; when even that is missing, pin at the
		// record's own position minus one tick — truncation is strictly before
		// the checkpoint position, so the record itself survives.
		tt := msg.TimeTick()
		if tt > 0 {
			tt--
		}
		m.lastAcked = newSummaryCheckpoint(msg.LastConfirmedMessageID(), tt)
	}
	m.pending = append(m.pending, stagedRecord{
		vchannel:  msg.VChannel(),
		timeTick:  msg.TimeTick(),
		entry:     messageutil.BuildTransformLogEntry(msg, messageutil.TransformEntryOption{}),
		messageID: msg.MessageID(),
	})
	m.pendingBytes += uint64(msg.EstimateSize())
}

// advanceLastAcked moves the summary's confirmation frontier to the message
// (id, tt) when it is newer and does not outrun an abandoned record. Caller
// holds m.mu.
func (m *Manager) advanceLastAcked(id message.MessageID, tt uint64) {
	if cp := newSummaryCheckpoint(id, tt); cp != nil {
		if m.abandonedFrontier != nil && !cp.MessageID.LT(m.abandonedFrontier) {
			// A record before this message was abandoned and is not durable:
			// the frontier stays behind it so the WAL truncation never deletes
			// it before recovery replays it.
			return
		}
		if m.lastAcked == nil || m.lastAcked.MessageID == nil || m.lastAcked.MessageID.LT(cp.MessageID) {
			m.lastAcked = cp
		}
	}
}

// RequestFlushThrough triggers one FlushChunk: the pending span is sealed and
// an asynchronous write task is submitted, so the WAL checkpoint can advance
// past the records once the chunk is durable. It is a no-op when the pending
// flush already covers the requested timetick (or there is nothing to seal).
func (m *Manager) RequestFlushThrough(timetick uint64) {
	m.mu.Lock()
	noop := timetick <= m.pendingFlushTimeTick || len(m.pending) == 0
	m.mu.Unlock()
	if noop {
		return
	}
	m.requestSeal()
}

// AdvanceGCTimeTick reports the GC position of one vchannel: records with
// timetick at or below the position may be released by retention GC. It is
// called by the vchannel lifecycle as transform materialization advances, and
// with DroppedVChannelTimeTick when the vchannel's cleanup snapshot is
// durable (its records may be released regardless of materialization).
func (m *Manager) AdvanceGCTimeTick(vchannel string, timetick uint64) {
	m.mu.Lock()
	if timetick > m.gcFrontiers[vchannel] {
		m.gcFrontiers[vchannel] = timetick
	}
	m.mu.Unlock()
}

// requestSeal seals the pending span into one chunk and submits the write
// task. It is a no-op when there is nothing pending.
func (m *Manager) requestSeal() {
	m.seal()
	m.mu.Lock()
	task := m.newWriteTaskLocked()
	m.mu.Unlock()
	if task != nil {
		m.submitWriteTask(task)
	}
}

// seal takes the pending span out under the lock, organizes the records by
// vchannel, and enqueues the sealed chunk. The records are already built (see
// ObserveMessage); the vchannel grouping is the only organization left.
func (m *Manager) seal() *SealedChunk {
	m.mu.Lock()
	if len(m.pending) == 0 {
		m.mu.Unlock()
		return nil
	}
	pending := m.pending
	m.pending = nil
	m.pendingBytes = 0
	generation := m.nextGeneration
	m.nextGeneration++
	m.mu.Unlock()

	sc := buildSealedChunk(generation, pending)
	if len(sc.RecordsByVChannel) == 0 {
		// No record was staged (defensive: only delete-carrying messages are
		// appended, so this cannot happen). The generation was claimed but is
		// simply skipped.
		return nil
	}

	m.mu.Lock()
	m.pendingSealed = append(m.pendingSealed, sc)
	if sc.MaxTimeTick > m.pendingFlushTimeTick {
		m.pendingFlushTimeTick = sc.MaxTimeTick
	}
	m.mu.Unlock()
	return sc
}

// buildSealedChunk organizes one chunk span: it groups the already-built
// records by vchannel. Called without the lock.
func buildSealedChunk(generation uint64, pending []stagedRecord) *SealedChunk {
	recordsByVChannel := make(map[string][]*stagedRecord)
	var maxTimeTick uint64
	for i := range pending {
		record := &pending[i]
		recordsByVChannel[record.vchannel] = append(recordsByVChannel[record.vchannel], record)
		if record.timeTick > maxTimeTick {
			maxTimeTick = record.timeTick
		}
	}
	return &SealedChunk{
		Generation:        generation,
		RecordsByVChannel: recordsByVChannel,
		MaxTimeTick:       maxTimeTick,
	}
}

// writeOnce makes one sealed chunk durable end to end: write the chunk
// object, publish the manifest record, then advance the durable state. It
// reports whether the queue is drained. The generation is written
// idempotently, so a retry after a failure rewrites the exact same object.
func (m *Manager) writeOnce(ctx context.Context) (bool, error) {
	m.mu.Lock()
	if len(m.pendingSealed) == 0 {
		m.mu.Unlock()
		return true, nil
	}
	sc := m.pendingSealed[0]
	m.mu.Unlock()

	records := make(map[string][]*streamingpb.VChannelSummaryTransformRecord, len(sc.RecordsByVChannel))
	for vchannel, staged := range sc.RecordsByVChannel {
		rs := make([]*streamingpb.VChannelSummaryTransformRecord, 0, len(staged))
		for _, record := range staged {
			rs = append(rs, &streamingpb.VChannelSummaryTransformRecord{
				TimeTick: record.timeTick,
				Delete:   record.entry.GetDelete(),
			})
		}
		records[vchannel] = rs
	}
	footer, objectSize, err := m.cfg.Store.WriteChunk(ctx, sc.Generation, records)
	if err != nil {
		return false, err
	}
	if err := m.publishManifest(ctx, func(next *streamingpb.PChannelSummaryManifest) {
		recordChunk(next, chunkIndexEntryFromFooter(footer, objectSize))
	}); err != nil {
		return false, err
	}

	m.mu.Lock()
	for vchannel, staged := range sc.RecordsByVChannel {
		var end uint64
		for _, record := range staged {
			if record.timeTick > end {
				end = record.timeTick
			}
		}
		if end > m.durableFrontiers[vchannel] {
			m.durableFrontiers[vchannel] = end
		}
	}
	if sc.MaxTimeTick > m.latestCoveredTimeTick {
		m.latestCoveredTimeTick = sc.MaxTimeTick
	}
	// The chunk is now durable: every record it holds is confirmed. Advance
	// the frontier to the chunk's newest record, unless an abandoned record
	// still precedes it — then the frontier stays behind the abandoned record,
	// so the WAL checkpoint never truncates it before recovery replays it.
	if lastID := chunkNewestRecordID(sc); lastID != nil {
		if m.abandonedFrontier == nil || lastID.LT(m.abandonedFrontier) {
			if m.lastAcked == nil || m.lastAcked.MessageID == nil || m.lastAcked.MessageID.LT(lastID) {
				m.lastAcked = newSummaryCheckpoint(lastID, sc.MaxTimeTick)
			}
		}
	}
	m.pendingSealed = m.pendingSealed[1:]
	finished := len(m.pendingSealed) == 0
	m.mu.Unlock()
	return finished, nil
}

// chunkNewestRecordID returns the message ID of the record with the newest
// timetick in the chunk, or nil when the chunk holds no record.
func chunkNewestRecordID(sc *SealedChunk) message.MessageID {
	var newestID message.MessageID
	var newestTT uint64
	for _, staged := range sc.RecordsByVChannel {
		for _, record := range staged {
			if record.timeTick >= newestTT {
				newestTT = record.timeTick
				newestID = record.messageID
			}
		}
	}
	return newestID
}

// chunkEarliestRecordID returns the message ID of the record with the oldest
// timetick in the chunk, or nil when the chunk holds no record.
func chunkEarliestRecordID(sc *SealedChunk) message.MessageID {
	var earliestID message.MessageID
	var earliestTT uint64
	first := true
	for _, staged := range sc.RecordsByVChannel {
		for _, record := range staged {
			if first || record.timeTick < earliestTT {
				earliestTT = record.timeTick
				earliestID = record.messageID
				first = false
			}
		}
	}
	return earliestID
}

// publishManifest edits the manifest, writes the edited clone to object
// storage outside the lock, and installs it back only if the manifest did not
// move in between (the CAS token is manifestVersion). This serializes
// concurrent publishers — the write task and retention GC — without a second
// lock.
func (m *Manager) publishManifest(ctx context.Context, edit func(*streamingpb.PChannelSummaryManifest)) error {
	for {
		m.mu.Lock()
		next := proto.Clone(m.manifest).(*streamingpb.PChannelSummaryManifest)
		edit(next)
		version := m.manifestVersion
		m.mu.Unlock()
		if err := m.cfg.Store.WriteManifest(ctx, next); err != nil {
			return err
		}
		m.mu.Lock()
		if m.manifestVersion == version {
			m.manifest = next
			m.manifestVersion++
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()
	}
}

// newWriteTaskLocked returns a write task, or nil when one is already
// scheduled or in flight. Caller holds m.mu.
func (m *Manager) newWriteTaskLocked() *summaryWriteTask {
	m.flushTasks = compactWriteTasks(m.flushTasks)
	if len(m.flushTasks) > 0 {
		return nil
	}
	task := &summaryWriteTask{log: m}
	m.flushTasks = append(m.flushTasks, task)
	return task
}

func (m *Manager) submitWriteTask(task *summaryWriteTask) {
	if scheduler := m.cfg.Runtime.Scheduler; scheduler != nil {
		scheduler.Submit(task)
	}
}

// HasPendingWork reports whether a write is scheduled or in flight, or a
// sealed chunk is still waiting to be written. Recovery uses it to decide
// whether a drop cleanup may tear the summary down.
func (m *Manager) HasPendingWork() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushTasks = compactWriteTasks(m.flushTasks)
	return len(m.flushTasks) > 0 || len(m.pendingSealed) > 0
}

// ReadTransformEntries returns the durable transform entries of one vchannel
// with timetick in (from, to], collected from the retained chunks. The chunks
// are scanned in generation order; a chunk whose span ends at or before from
// is skipped, a chunk whose span starts after to stops the scan.
//
// The transform consumer never reads object storage at runtime: it observes
// messages directly. This method is the one-time recovery path: after a
// restart the consumer's in-memory window is empty, and recovery loads the
// durable backlog between the restored materialization frontier and the
// durable frontier through this method.
func (m *Manager) ReadTransformEntries(
	ctx context.Context,
	vchannel string,
	from, to uint64,
) ([]*streamingpb.TransformLogEntry, error) {
	m.mu.Lock()
	chunks := append([]*streamingpb.PChannelSummaryChunkIndexEntry(nil), m.manifest.GetChunks()...)
	m.mu.Unlock()
	out := make([]*streamingpb.TransformLogEntry, 0)
	for _, chunk := range chunks {
		if chunk.GetEndTimetick() <= from {
			continue
		}
		if chunk.GetStartTimetick() > to {
			break
		}
		index := vchannelChunkIndex(chunk, vchannel)
		if index == nil {
			continue
		}
		records, err := m.cfg.Store.ReadTransformSection(ctx, chunk.GetGeneration(), chunk.GetTerm(), vchannel, index)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			tt := record.GetTimeTick()
			if tt <= from || tt > to {
				continue
			}
			out = append(out, &streamingpb.TransformLogEntry{
				TimeTick: tt,
				Entry: &streamingpb.TransformLogEntry_Delete{
					Delete: record.GetDelete(),
				},
			})
		}
	}
	return out, nil
}

// DurableTimeTick returns the newest durable record timetick of one vchannel,
// derived from the manifest: the largest per-vchannel chunk index end across
// all recorded chunks.
func (m *Manager) DurableTimeTick(vchannel string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var frontier uint64
	for _, chunk := range m.manifest.GetChunks() {
		if index := vchannelChunkIndex(chunk, vchannel); index != nil && index.GetEndTimetick() > frontier {
			frontier = index.GetEndTimetick()
		}
	}
	return frontier
}

// LatestCoveredTimeTick returns the newest timetick covered by a durable
// chunk.
func (m *Manager) LatestCoveredTimeTick() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latestCoveredTimeTick
}

// vchannelChunkIndex returns a chunk's index entry of one vchannel, or nil.
func vchannelChunkIndex(chunk *streamingpb.PChannelSummaryChunkIndexEntry, vchannel string) *streamingpb.VChannelSummaryChunkIndex {
	for _, index := range chunk.GetVchannels() {
		if index.GetVchannel() == vchannel {
			return index
		}
	}
	return nil
}

// stagedRecord is one staged delete record: the built transform entry plus
// the WAL position it came from. The message itself is not retained; the
// message ID is kept so the confirmation frontier can advance to the record
// once its chunk is durable.
type stagedRecord struct {
	vchannel  string
	timeTick  uint64
	entry     *streamingpb.TransformLogEntry
	messageID message.MessageID
}

// newSummaryCheckpoint builds the summary's confirmation frontier at (id,
// timetick). A nil message ID yields nil: without a WAL position the frontier
// cannot bound the truncation and stays unset.
func newSummaryCheckpoint(id message.MessageID, timetick uint64) *utility.WALCheckpoint {
	if id == nil {
		return nil
	}
	return &utility.WALCheckpoint{
		MessageID: id,
		TimeTick:  timetick,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	}
}

// InitLastAcked seeds the summary's confirmation frontier from the restored
// recovery checkpoint, so the frontier never starts behind the checkpoint
// that was already persisted. It is called once after Restore, before the WAL
// replay re-observes messages.
func (m *Manager) InitLastAcked(checkpoint *utility.WALCheckpoint) {
	if checkpoint == nil || checkpoint.MessageID == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastAcked == nil || m.lastAcked.MessageID == nil || m.lastAcked.MessageID.LT(checkpoint.MessageID) {
		m.lastAcked = checkpoint.Clone()
	}
}

// LastAcked returns the newest WAL message the summary has confirmed, or nil
// when no message is confirmed yet. The recovery checkpoint must never
// advance past it: the recovery storage merges it with the ack tracker's
// completed point before persisting a snapshot.
func (m *Manager) LastAcked() *utility.WALCheckpoint {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastAcked == nil {
		return nil
	}
	return m.lastAcked.Clone()
}

// SealedChunk is one chunk span taken out of the pending buffer: the records
// are immutable once sealed, so the write task may build and rewrite the
// object without touching the manager state.
type SealedChunk struct {
	Generation        uint64
	RecordsByVChannel map[string][]*stagedRecord
	MaxTimeTick       uint64
}

// summaryWriteTask is a nodescheduler task draining the sealed chunk queue.
// At most one task exists at a time (see newWriteTaskLocked), so there is no
// predecessor graph.
type summaryWriteTask struct {
	log  *Manager
	done atomic.Bool
}

// Done reports whether the task finished.
func (t *summaryWriteTask) Done() bool {
	return t.done.Load()
}

// Execute drains the sealed chunk queue. Transient failures are marked
// ErrDelay so the scheduler retries with backoff; the failed chunk stays at
// the queue head and is rewritten idempotently. A terminal failure (corrupted
// or fenced store) abandons the failing chunk instead: it is popped from the
// queue — a successor task would only hit the same corruption — while the
// confirmation frontier stays behind it, so the WAL checkpoint stalls before
// it and recovery replays after a restart. The rest of the queue is written
// by the next task.
func (t *summaryWriteTask) Execute(ctx context.Context) error {
	for {
		finished, err := t.log.writeOnce(ctx)
		if err != nil {
			if isTerminalSummaryFlushError(err) {
				t.log.abandonHead(ctx)
				t.done.Store(true)
				if logger := t.log.cfg.Logger; logger != nil {
					logger.Error(ctx, "summary flush failed terminally, abandoning the chunk", mlog.Err(err))
				}
				return err
			}
			return errors.Mark(err, nodescheduler.ErrDelay)
		}
		if finished {
			break
		}
	}
	t.done.Store(true)
	return nil
}

// abandonHead pops the failing chunk off the sealed queue. The chunk is not
// durable and will never become durable under this term; recovery replays it
// after a restart. Its oldest record becomes the abandoned frontier, which
// pins the confirmation frontier (and thus the WAL checkpoint) strictly
// before it until the restart.
func (m *Manager) abandonHead(ctx context.Context) {
	m.mu.Lock()
	if len(m.pendingSealed) > 0 {
		sc := m.pendingSealed[0]
		m.pendingSealed = m.pendingSealed[1:]
		if id := chunkEarliestRecordID(sc); id != nil {
			if m.abandonedFrontier == nil || id.LT(m.abandonedFrontier) {
				m.abandonedFrontier = id
			}
		}
	}
	m.mu.Unlock()
}

func compactWriteTasks(tasks []*summaryWriteTask) []*summaryWriteTask {
	pending := tasks[:0]
	for _, task := range tasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
	}
	clear(pending[len(pending):])
	return pending
}

var _ nodescheduler.Task = (*summaryWriteTask)(nil)
