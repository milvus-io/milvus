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
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// Manager is the pchannel-scoped WALSummary runtime. A summary is one
// contiguous dense span of the pchannel log kept in two forms:
//
//   - in memory: the records of the span not yet sealed into a chunk.
//     ObserveMessage builds the record of every message that carries an
//     idempotency key immediately and copies it into the pending buffer; the
//     WAL message itself is never retained.
//   - in object storage: sealed chunks, an append-only time-ordered log of
//     per-vchannel records, indexed by the manifest.
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

	// pending holds the records of the current (unsealed) chunk
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

	// manifest is the in-memory chunk index (the persistent form's index).
	// It is only ever replaced via publishManifest, which writes the edited
	// clone outside the lock and installs it back under the lock unless the
	// manifestVersion moved in between (CAS).
	manifest        *streamingpb.PChannelSummaryManifest
	manifestVersion uint64

	// publishMu serializes manifest publication. It is NOT m.mu: the object
	// write must not hold the manager lock, but it also must not race another
	// publisher, because Store.WriteManifest is an unconditional PUT to a fixed
	// key with no conditional-write guard. Two publishers that clone under m.mu
	// and then write outside it can land in either order, so the older content
	// can overwrite the newer one on storage while the newer one won in memory.
	publishMu      sync.Mutex
	nextGeneration uint64
	// latestCoveredTimeTick is the newest timetick covered by a durable chunk.
	latestCoveredTimeTick uint64
	// durableFrontiers holds the newest durable record timetick per vchannel,
	// restored from the manifest and advanced by every completed write. It is
	// the replay filter of ObserveMessage: recovery re-observes records the
	// manifest already covers, and they must not be staged again.
	durableFrontiers map[string]uint64

	// pendingInvalidations holds the DDL invalidation timeticks observed but
	// not yet folded into a published manifest, per vchannel. They ride the
	// same persist as the records: Persist folds them into the manifest it
	// publishes, before the checkpoint that covers the DDL is saved.
	pendingInvalidations map[string]uint64
}

// ManagerConfig carries the wiring of one pchannel's summary manager.
type ManagerConfig struct {
	PChannel string
	Term     int64
	// Store is the object storage layer of the summary store.
	Store *Store
	// RetentionMaxBytes is the soft budget of the retained chunk objects. GC
	// releases the oldest chunks above the budget. Zero disables that bound.
	RetentionMaxBytes uint64
	// MaxRetainedChunks caps how many chunk objects stay retained, whatever
	// their size. It bounds what the byte budget cannot: the manifest entry
	// count and the number of object reads recovery pays. Zero disables it.
	MaxRetainedChunks int
	Logger            *mlog.Logger
}

// NewManager creates the summary manager of one pchannel.
func NewManager(config ManagerConfig) *Manager {
	return &Manager{
		cfg:                  config,
		manifest:             &streamingpb.PChannelSummaryManifest{},
		durableFrontiers:     make(map[string]uint64),
		pendingInvalidations: make(map[string]uint64),
	}
}

// ObserveMessage observes one WAL message at the pchannel level. It is called
// on the WAL observation path (recovery replay and the live scanner),
// independent of the vchannel modules, and must not block.
//
// Only a message carrying a client idempotency key produces a record; DDL,
// flush and barrier messages never do. The
// record of a delete message is built here and copied into the pending
// buffer — the message handle is not retained, so its acknowledgement never
// depends on the summary. When the pending total reaches FlushMaxBytes, the
// pending span is sealed into a chunk and an asynchronous write task is
// submitted. Messages without a per-vchannel record (all-channel time ticks,
// pchannel-level broadcasts, control-channel messages) and records the
// manifest already covers are not staged; all-channel time ticks still
// advance the confirmation frontier so an idle pchannel refreshes its
// persisted checkpoint TimeTick (see lastAcked).
func (m *Manager) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) {
	if msg == nil {
		return
	}
	vchannel := msg.VChannel()
	if funcutil.IsControlChannel(vchannel) {
		// The control channel is the control plane: it never produces
		// records and its progress is not summarized.
		return
	}
	if vchannel == "" {
		// All-channel messages carry no per-vchannel record, and nothing here
		// tracks WAL progress: the recovery storage's own checkpoint does that,
		// and the summary is written before it is saved.
		return
	}
	if idempotencyview.InvalidatesIdempotencyWindow(msg.MessageType()) {
		// The DDL produces no record, only a tombstone. It is folded into the
		// manifest by the next Persist -- which runs before the checkpoint that
		// covers this DDL is saved, so a restart can never come back to a
		// checkpoint past the DDL with the tombstone missing.
		m.invalidateVChannel(vchannel, msg.TimeTick())
		return
	}
	idempotency, insert := idempotencyHalvesOf(msg)

	m.mu.Lock()
	if idempotency == nil {
		// Nothing to record.
		m.mu.Unlock()
		return
	}
	// Recovery re-observes what the manifest already covers. The frontier is
	// per vchannel and not per section, which is exactly right here: both kinds
	// are staged into the same buffer and sealed into the same chunk, so
	// everything at or below it is durable whichever section it landed in.
	if msg.TimeTick() <= m.durableFrontiers[vchannel] {
		m.mu.Unlock()
		return
	}
	m.stageRecordLocked(msg, idempotency, insert)
	m.mu.Unlock()
}

// stageDeleteLocked appends one delete record to the pending span. Caller
// holds m.mu. The entry is built here — the message payload is not retained,
// so it must be copied before the message is released.
func (m *Manager) stageRecordLocked(
	msg message.ImmutableMessage,
	idempotency *streamingpb.VChannelSummaryIdempotencyRecord,
	insert *streamingpb.VChannelSummaryInsertRecord,
) {
	record := stagedRecord{
		vchannel:    msg.VChannel(),
		timeTick:    msg.TimeTick(),
		idempotency: idempotency,
		insert:      insert,
	}
	record.size = stagedRecordSize(msg, &record)
	m.pending = append(m.pending, record)
	m.pendingBytes += record.size
}

// stagedRecordSize estimates what the record will cost in a chunk. It is what
// the retention byte budget is spent on, and what a staged span is measured by.
//
// For a delete that is the message: its payload is the primary keys, and the
// record is those keys. For an idempotent insert it is emphatically not — the
// message carries the whole row including its vectors, while the record keeps
// only the client key, the row offsets and the primary keys. Charging the
// message would seal chunks orders of magnitude too early and turn every large
// insert into its own chunk.
func stagedRecordSize(msg message.ImmutableMessage, record *stagedRecord) uint64 {
	if record.insert == nil {
		return uint64(msg.EstimateSize())
	}
	size := uint64(proto.Size(record.insert)) + uint64(proto.Size(record.idempotency))
	return size
}

// idempotencyHalvesOf builds what the idempotency sections remember about a
// message, or nil when the message is not one the append path deduplicates.
//
// Only a write carrying a client key is staged. A keyless committed write
// materializes nothing for any consumer today -- the insert section is written
// for it only when it accompanies a keyed write in the same chunk -- and
// staging every insert would put the whole write path's primary keys into
// object storage for nobody to read.
func idempotencyHalvesOf(msg message.ImmutableMessage) (
	*streamingpb.VChannelSummaryIdempotencyRecord,
	*streamingpb.VChannelSummaryInsertRecord,
) {
	key := idempotencyKeyOf(msg)
	if key == "" {
		return nil, nil
	}
	insert := &streamingpb.VChannelSummaryInsertRecord{
		SourceMessageId:        messageIDProto(msg.MessageID()),
		SourceTimetick:         msg.TimeTick(),
		LastConfirmedMessageId: messageIDProto(msg.LastConfirmedMessageID()),
	}
	keys := &streamingpb.VChannelSummaryIdempotencyRecord{Key: key}
	result, hasResult := idempotentInsertResultOf(msg)
	if !hasResult && msg.MessageType() == message.MessageTypeTxn {
		// A keyed txn whose per-body results could not be rebuilt (corrupt or
		// absent headers) must produce NO record. Staging the key with nil Ids
		// would make a post-restart duplicate answer success with no primary
		// keys at all; producing nothing only costs the dedup opportunity,
		// which degrades to the behavior without this feature.
		return nil, nil
	}
	if hasResult {
		insert.Ids = result.GetIds()
		keys.RowOffsets = result.GetRowOffsets()
	}
	return keys, insert
}

// idempotencyKeyOf returns the client key a message was appended with, or ""
// when the summary must not remember one for it.
func idempotencyKeyOf(msg message.ImmutableMessage) string {
	// A replicated message preserves the SOURCE cluster's properties, including
	// its idempotency key. That key must never materialize a record here: the
	// local key history is independent of the source's, and a poisoned record
	// would drive replicated appends down the duplicate path after a restart.
	// Replicated writes are treated as keyless, matching the interceptor bypass.
	if msg.ReplicateHeader() != nil {
		return ""
	}
	// Gated to the message types the append path deduplicates: the key property
	// alone must not materialize a record for a type that is never deduped.
	//
	// A bare CommitTxn is deliberately NOT here. Every path into an observer
	// goes through a scanner that assembles transactions (the live flusher and
	// the recovery stream share one txn buffer), so a commit always arrives
	// wrapped in MessageTypeTxn and a CommitTxn case could never fire. Worse
	// than unreachable: idempotentInsertResultOf has no CommitTxn case either,
	// so if one ever did arrive it would stage a record with nil Ids and answer
	// a later duplicate with no primary keys.
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		return message.IdempotencyKeyOf(msg)
	case message.MessageTypeTxn:
		// What reaches an observer is the ASSEMBLED txn, not the CommitTxn the
		// interceptor deduplicated: the scanner's txn buffer packs begin + bodies
		// + commit into one message, and the assembly copies only the trace
		// context off the commit -- the idempotency key property stays on the
		// commit sub-message. Reading it from there is what keeps a multi-message
		// insert (a partition-key collection, or one split by maxMessageSize)
		// in the durable window at all.
		txnMsg := message.AsImmutableTxnMessage(msg)
		if txnMsg == nil {
			return ""
		}
		commit := txnMsg.Commit()
		if commit == nil || commit.MessageType() != message.MessageTypeCommitTxn {
			return ""
		}
		return message.IdempotencyKeyOf(commit)
	default:
		return ""
	}
}

// idempotentInsertResultOf returns what a duplicate append replays back to the
// client, when the message carries one.
//
// For a transaction it is REBUILT from the insert bodies rather than read off
// the commit: the interceptor merges the per-body results in memory and hands
// them to the window, and nothing stamps the merged value onto the CommitTxn
// message, so the wire has only the per-body headers. Merging them in
// RangeOver order reproduces exactly what the interceptor built (both go
// through MergeIdempotentInsertResults over the bodies in append order), which
// is what makes a post-restart duplicate answer with the same primary keys the
// first attempt returned.
func idempotentInsertResultOf(msg message.ImmutableMessage) (*messagespb.IdempotentInsertResult, bool) {
	if msg.ReplicateHeader() != nil {
		return nil, false
	}
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		insertMsg, err := message.AsImmutableInsertMessageV1(msg)
		if err != nil {
			return nil, false
		}
		return message.IdempotentInsertResultFromInsertHeader(insertMsg.Header())
	case message.MessageTypeTxn:
		txnMsg := message.AsImmutableTxnMessage(msg)
		if txnMsg == nil {
			return nil, false
		}
		var results []*messagespb.IdempotentInsertResult
		_ = txnMsg.RangeOver(func(sub message.ImmutableMessage) error {
			if sub.MessageType() != message.MessageTypeInsert {
				return nil
			}
			insertMsg, err := message.AsImmutableInsertMessageV1(sub)
			if err != nil {
				return nil
			}
			if result, ok := message.IdempotentInsertResultFromInsertHeader(insertMsg.Header()); ok {
				results = append(results, result)
			}
			return nil
		})
		merged, hadAny, err := message.MergeIdempotentInsertResults(results...)
		if err != nil || !hadAny {
			// Corruption and "no payload" are both answered as "no result" here
			// rather than by staging a half-record: a record whose Ids are nil
			// would answer a later duplicate with no primary keys at all, which
			// is worse than not recognizing the duplicate.
			return nil, false
		}
		return merged, true
	default:
		return nil, false
	}
}

func messageIDProto(id message.MessageID) *commonpb.MessageID {
	if id == nil {
		return nil
	}
	return id.IntoProto()
}

// invalidateVChannel records that everything of a vchannel at or below the
// timetick has been made meaningless by a DDL, and forgets the records still
// staged behind it.
//
// The floor is what the read path applies, so dropping the staged records is
// an optimization rather than the mechanism: they would be filtered out on
// every read anyway, and keeping them alive would hold memory and pin the
// confirmation frontier for facts nothing may serve.
func (m *Manager) invalidateVChannel(vchannel string, timetick uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if timetick > m.pendingInvalidations[vchannel] {
		m.pendingInvalidations[vchannel] = timetick
	}
	// A new slice, never a compaction in place: ReadIdempotencyEntries hands
	// out pointers into this backing array and reads them after releasing the
	// lock, so shifting the elements under it would make it read another
	// record. seal() upholds the same rule by handing the array off whole.
	kept := make([]stagedRecord, 0, len(m.pending))
	for i := range m.pending {
		if m.pending[i].vchannel == vchannel && m.pending[i].timeTick <= timetick {
			m.pendingBytes -= m.pending[i].size
			continue
		}
		kept = append(kept, m.pending[i])
	}
	m.pending = kept
}

// invalidationFloorLocked returns the timetick at or below which a vchannel's
// records must not be served: the published tombstone or a newer one still
// pending. Caller holds m.mu.
func (m *Manager) invalidationFloorLocked(vchannel string) uint64 {
	floor := m.manifest.GetInvalidatedVchannels()[vchannel]
	if pending := m.pendingInvalidations[vchannel]; pending > floor {
		floor = pending
	}
	return floor
}

// Persist writes everything staged into object storage and records it in the
// manifest. It is the ONLY thing that writes a chunk.
//
// The recovery storage calls it from its dirty-snapshot persist, BEFORE the
// consume checkpoint that covers those records is saved, and a failure here
// fails that persist. That ordering is the whole design:
//
//   - the chunk covering a range is durable before the checkpoint covering the
//     same range, so the checkpoint is itself the boundary between what the
//     store holds and what the WAL still holds;
//   - nothing needs a second position, nothing needs to clamp the checkpoint,
//     and recovery needs no rewind;
//   - batching is inherited from the checkpoint's own batching, so there is no
//     second timer cutting chunks on its own schedule.
//
// The cost is deliberate: object-storage latency sits on the checkpoint persist
// path, and an unavailable store stalls the checkpoint. That is the correct
// outcome -- a checkpoint that advanced past a chunk which was never written
// would leave those keys in neither the store nor the replayable WAL, and
// nothing downstream could detect it.
func (m *Manager) Persist(ctx context.Context) error {
	m.seal()
	for {
		finished, err := m.writeOnce(ctx)
		if err != nil {
			return err
		}
		if finished {
			break
		}
	}
	// A DDL tombstone usually has nothing behind it to write -- the DDL is what
	// destroyed the records -- so it would otherwise wait for an unrelated
	// future chunk. Publishing it here keeps it ordered before the checkpoint
	// that covers the DDL, which is the whole point of the tombstone.
	m.mu.Lock()
	pendingTombstones := len(m.pendingInvalidations) > 0
	m.mu.Unlock()
	if !pendingTombstones {
		return nil
	}
	// publishManifest folds them in itself, so the edit is empty.
	return m.publishManifest(ctx, func(*streamingpb.PChannelSummaryManifest) {})
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
		// No record was staged (defensive: only messages that produce a record
		// are appended, so this cannot happen). The generation was claimed but
		// is simply skipped.
		return nil
	}

	m.mu.Lock()
	m.pendingSealed = append(m.pendingSealed, sc)
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
		// A tombstone with no chunk behind it still has to reach the manifest;
		// publishManifest folds it in, so the edit itself is empty.
		pendingInvalidations := len(m.pendingInvalidations) > 0
		m.mu.Unlock()
		if pendingInvalidations {
			if err := m.publishManifest(ctx, func(*streamingpb.PChannelSummaryManifest) {}); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	sc := m.pendingSealed[0]
	m.mu.Unlock()

	sections := make(map[string]*ChunkSections, len(sc.RecordsByVChannel))
	for vchannel, staged := range sc.RecordsByVChannel {
		cs := &ChunkSections{}
		for _, record := range staged {
			if record.insert != nil {
				// The two halves are appended together and never apart: the
				// sections are paired by position, so a record contributing one
				// without the other would misalign every later pair.
				cs.Inserts = append(cs.Inserts, record.insert)
				cs.Idempotency = append(cs.Idempotency, record.idempotency)
			}
		}
		sections[vchannel] = cs
	}
	footer, objectSize, err := m.cfg.Store.WriteChunk(ctx, sc.Generation, sections)
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
	m.pendingSealed = m.pendingSealed[1:]
	finished := len(m.pendingSealed) == 0
	m.mu.Unlock()
	return finished, nil
}

// publishManifest edits the manifest, writes the edited clone to object storage
// without holding the manager lock, and installs it back.
//
// Publication is serialized by publishMu so the durable manifest can only move
// forward: the store has no conditional write, so ordering has to come from
// here. The manager lock is still released across the object write, which is
// the point of the separate mutex -- observation and reads keep running while a
// publish is in flight.
func (m *Manager) publishManifest(ctx context.Context, edit func(*streamingpb.PChannelSummaryManifest)) error {
	// One publisher at a time, held ACROSS the object write. Without this the
	// losing writer's older object can land after the winner's, dropping a
	// tombstone from durable storage while the winner has already cleared it
	// from pendingInvalidations and unpinned the frontier -- a crash before the
	// loser's retry then recovers a manifest missing an invalidation whose DDL
	// the checkpoint has already passed.
	m.publishMu.Lock()
	defer m.publishMu.Unlock()
	m.mu.Lock()
	next := proto.Clone(m.manifest).(*streamingpb.PChannelSummaryManifest)
	edit(next)
	// Every publisher carries the pending tombstones, whatever else it came
	// to write: a tombstone must not wait for a chunk of its own.
	//
	// A sealed chunk that has not been written yet may still hold records
	// below a tombstone, and the manifest cannot see them, so expiry has to
	// wait until the queue is drained -- otherwise a tombstone published
	// while a chunk is in flight would expire in the same breath and the
	// chunk would land unfiltered.
	folded := foldInvalidations(next, m.pendingInvalidations, len(m.pendingSealed) == 0)
	m.mu.Unlock()
	if err := m.cfg.Store.WriteManifest(ctx, next); err != nil {
		return err
	}
	m.mu.Lock()
	m.manifest = next
	m.manifestVersion++
	// Only now are they durable. A vchannel invalidated again while this
	// write was in flight keeps the newer timetick.
	for vchannel, timetick := range folded {
		if m.pendingInvalidations[vchannel] <= timetick {
			delete(m.pendingInvalidations, vchannel)
		}
	}
	m.mu.Unlock()
	return nil
}

// foldInvalidations merges the pending tombstones into the manifest and, when
// mayExpire, drops the ones no retained chunk reaches below. It returns what it
// merged.
//
// A tombstone only has to outlive the records it buries. Once every chunk that
// could hold them is released, the entry describes nothing and would otherwise
// accumulate for the life of the pchannel -- one entry per vchannel ever
// dropped.
func foldInvalidations(manifest *streamingpb.PChannelSummaryManifest, pending map[string]uint64, mayExpire bool) map[string]uint64 {
	folded := make(map[string]uint64, len(pending))
	if len(pending) > 0 && manifest.GetInvalidatedVchannels() == nil {
		manifest.InvalidatedVchannels = make(map[string]uint64, len(pending))
	}
	for vchannel, timetick := range pending {
		if timetick > manifest.InvalidatedVchannels[vchannel] {
			manifest.InvalidatedVchannels[vchannel] = timetick
		}
		folded[vchannel] = timetick
	}
	if mayExpire {
		for vchannel, timetick := range manifest.GetInvalidatedVchannels() {
			if !chunksReachBelow(manifest, vchannel, timetick) {
				delete(manifest.InvalidatedVchannels, vchannel)
			}
		}
	}
	return folded
}

// chunksReachBelow reports whether any retained chunk still holds records of
// the vchannel at or below the timetick.
func chunksReachBelow(manifest *streamingpb.PChannelSummaryManifest, vchannel string, timetick uint64) bool {
	for _, chunk := range manifest.GetChunks() {
		index := vchannelChunkIndex(chunk, vchannel)
		if index == nil {
			continue
		}
		if index.GetStartTimetick() <= timetick {
			return true
		}
	}
	return false
}

// IdempotencyVChannels returns every vchannel the summary holds idempotency
// records for, durable or not.
//
// It is what recovery iterates, rather than the recovered write path: the write
// path's vchannels are collections and segments, a different question from
// which channels have a dedup history. A pchannel can hold records for a
// vchannel the write path knows nothing about yet, and the window would then be
// rebuilt empty.
func (m *Manager) IdempotencyVChannels() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	seen := make(map[string]struct{})
	for _, chunk := range m.manifest.GetChunks() {
		for _, index := range chunk.GetVchannels() {
			if index.GetInserts() != nil {
				seen[index.GetVchannel()] = struct{}{}
			}
		}
	}
	for _, sc := range m.pendingSealed {
		for vchannel, records := range sc.RecordsByVChannel {
			for _, record := range records {
				if record.insert != nil {
					seen[vchannel] = struct{}{}
					break
				}
			}
		}
	}
	for i := range m.pending {
		if m.pending[i].insert != nil {
			seen[m.pending[i].vchannel] = struct{}{}
		}
	}
	vchannels := make([]string, 0, len(seen))
	for vchannel := range seen {
		vchannels = append(vchannels, vchannel)
	}
	sort.Strings(vchannels)
	return vchannels
}

// ReadIdempotencyEntries returns everything the summary knows about one
// vchannel with timetick in (from, to]: the retained chunks in generation
// order, then what is sealed but not yet durable, then what is still staged.
// It is the one-time recovery path -- the interceptor's window is empty after a
// restart and is rebuilt from this.
//
// The in-memory tail is not an optimization, it is required for correctness. A
// chunk is sealed on a byte threshold, so a low-volume pchannel can run for a
// long time with every idempotency record staged and nothing durable. Those
// records are not lost -- the confirmation frontier pins the WAL checkpoint
// behind them, so recovery replays their messages and ObserveMessage stages
// them again -- but they are reachable only here. Reading the manifest alone
// would hand back an empty window on exactly the pchannel that has written too
// little to flush, which is to say: idempotency would silently stop working
// across a restart until the first chunk was sealed.
//
// The two halves are returned as they are stored, paired by position, and
// joining them is the consumer's business (see idempotencyview).
//
// The pairing needs one repair the single-chunk read does not: a chunk whose
// writes all lacked a client key stores no idempotency section at all, so
// concatenating chunks would leave fewer keys than inserts and the join would
// pair a key with another write's rows. Whenever any chunk in the range
// contributes keys, the ones that contributed none are backfilled with the
// empty-key records they would have stored -- which is what a write without a
// key means anyway.
func (m *Manager) ReadIdempotencyEntries(
	ctx context.Context,
	vchannel string,
	from, to uint64,
) (*ChunkSections, error) {
	all, err := m.ReadIdempotencyEntriesOfVChannels(ctx, []string{vchannel}, from, to)
	if err != nil {
		return nil, err
	}
	if sections, ok := all[vchannel]; ok {
		return sections, nil
	}
	return &ChunkSections{}, nil
}

// ReadIdempotencyEntriesOfVChannels answers the same question for several
// vchannels at once, and is what recovery uses.
//
// The chunk loop is the OUTER one on purpose: a chunk is a pchannel-wide object
// with no range read, so reading vchannel by vchannel would download the same
// object once per vchannel. Here each chunk is fetched once, decoded for every
// vchannel that has a section in it, and released before the next one -- the
// transfer drops from O(chunks x vchannels) to O(chunks) with no more memory
// than a single chunk at a time.
func (m *Manager) ReadIdempotencyEntriesOfVChannels(
	ctx context.Context,
	vchannels []string,
	from, to uint64,
) (map[string]*ChunkSections, error) {
	if len(vchannels) == 0 {
		return nil, nil
	}
	// The manifest and the in-memory tail are captured under one lock. Taken
	// separately, a flush completing in between would pop a sealed chunk from
	// the queue after the manifest read missed it, and its records would be in
	// neither half.
	m.mu.Lock()
	// Everything at or below a DDL tombstone is unserveable, wherever it is
	// stored. Raising the lower bound applies that to the chunks, the sealed
	// queue and the staged span in one place. The floor is per vchannel, so the
	// bound is too.
	floors := make(map[string]uint64, len(vchannels))
	inMemory := make(map[string][][]*stagedRecord, len(vchannels))
	for _, vchannel := range vchannels {
		lower := from
		if floor := m.invalidationFloorLocked(vchannel); floor > lower {
			lower = floor
		}
		floors[vchannel] = lower

		tails := make([][]*stagedRecord, 0, len(m.pendingSealed)+1)
		for _, sc := range m.pendingSealed {
			tails = append(tails, sc.RecordsByVChannel[vchannel])
		}
		staged := make([]*stagedRecord, 0, len(m.pending))
		for i := range m.pending {
			if m.pending[i].vchannel == vchannel {
				staged = append(staged, &m.pending[i])
			}
		}
		inMemory[vchannel] = append(tails, staged)
	}
	chunks := append([]*streamingpb.PChannelSummaryChunkIndexEntry(nil), m.manifest.GetChunks()...)
	m.mu.Unlock()

	out := make(map[string]*ChunkSections, len(vchannels))
	anyKeys := make(map[string]bool, len(vchannels))
	for _, vchannel := range vchannels {
		out[vchannel] = &ChunkSections{}
	}

	for _, chunk := range chunks {
		// Which vchannels this chunk can answer for. A chunk below every
		// vchannel's lower bound, or above `to`, is never fetched.
		indexes := make(map[string]*streamingpb.VChannelSummaryChunkIndex)
		for _, vchannel := range vchannels {
			if chunk.GetEndTimetick() <= floors[vchannel] || chunk.GetStartTimetick() > to {
				continue
			}
			index := vchannelChunkIndex(chunk, vchannel)
			if index == nil || index.GetInserts() == nil {
				continue
			}
			indexes[vchannel] = index
		}
		if len(indexes) == 0 {
			continue
		}
		decoded, err := m.cfg.Store.ReadIdempotencySectionsOfChunk(ctx, chunk.GetGeneration(), chunk.GetTerm(), indexes)
		if err != nil {
			return nil, err
		}
		for vchannel, sections := range decoded {
			lower := floors[vchannel]
			hasKeys := len(sections.Idempotency) != 0
			anyKeys[vchannel] = anyKeys[vchannel] || hasKeys
			target := out[vchannel]
			for i, insert := range sections.Inserts {
				tt := insert.GetSourceTimetick()
				if tt <= lower || tt > to {
					continue
				}
				target.Inserts = append(target.Inserts, insert)
				if hasKeys {
					target.Idempotency = append(target.Idempotency, sections.Idempotency[i])
				} else {
					// Placeholder, dropped below if nothing in the range had a key.
					target.Idempotency = append(target.Idempotency, &streamingpb.VChannelSummaryIdempotencyRecord{})
				}
			}
		}
	}

	for _, vchannel := range vchannels {
		target := out[vchannel]
		lower := floors[vchannel]
		for _, records := range inMemory[vchannel] {
			anyKeys[vchannel] = appendStagedIdempotency(target, records, lower, to) || anyKeys[vchannel]
		}
		if !anyKeys[vchannel] {
			target.Idempotency = nil
		}
		if err := target.validateIdempotencyAlignment(vchannel); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// appendStagedIdempotency appends the idempotency halves of the staged records
// in the (from, to] window, reporting whether any of them carried a key.
func appendStagedIdempotency(out *ChunkSections, staged []*stagedRecord, from, to uint64) bool {
	anyKeys := false
	for _, record := range staged {
		if record.insert == nil || record.timeTick <= from || record.timeTick > to {
			continue
		}
		out.Inserts = append(out.Inserts, record.insert)
		out.Idempotency = append(out.Idempotency, record.idempotency)
		if record.idempotency.GetKey() != "" {
			anyKeys = true
		}
	}
	return anyKeys
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

// stagedRecord is one staged record: the built section halves plus the WAL
// position they came from. The message itself is not retained; the
// message ID is kept so the confirmation frontier can advance to the record
// once its chunk is durable.
type stagedRecord struct {
	vchannel string
	timeTick uint64

	// idempotency and insert are the two halves of the idempotency consumer's
	// record, set together or not at all. A message contributes to the sections
	// it has content for, so a committed txn carrying both a delete and an
	// idempotent insert contributes to both from one staged record.
	idempotency *streamingpb.VChannelSummaryIdempotencyRecord
	insert      *streamingpb.VChannelSummaryInsertRecord

	// size is what this record was charged to pendingBytes. It is kept so the
	// charge can be reversed when an invalidation drops the record before it
	// is ever sealed.
	size uint64
}

// SealedChunk is one chunk span taken out of the pending buffer: the records
// are immutable once sealed, so the write task may build and rewrite the
// object without touching the manager state.
type SealedChunk struct {
	Generation        uint64
	RecordsByVChannel map[string][]*stagedRecord
	MaxTimeTick       uint64
}
