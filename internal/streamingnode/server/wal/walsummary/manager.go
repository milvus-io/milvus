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
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// DroppedVChannelTimeTick releases all transform records after durable cleanup.
const DroppedVChannelTimeTick = math.MaxUint64

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
// Records are copied without retaining WAL handles. LastAcked exposes the
// continuous durable prefix that RecoveryStorage combines with its own
// completed point before publishing a checkpoint.
// mu guards in-memory state; publishMu serializes manifest writes.
type Manager struct {
	mu sync.Mutex
	// Readers pin a local snapshot against physical GC. Cross-owner fencing is
	// deliberately left to the GC design TODO.
	readMu                sync.RWMutex
	publishMu             sync.Mutex
	cfg                   ManagerConfig
	pending               []stagedRecord
	pendingBytes          uint64
	pendingSince          time.Time
	pendingSealed         []*SealedChunk
	pendingFlushTimeTick  uint64
	nextGeneration        uint64
	generationExhausted   bool
	reopenedTerm          bool
	manifest              *streamingpb.PChannelSummaryManifest
	manifestVersion       uint64
	publishedVersion      uint64
	manifestPublished     bool
	manifestTask          *manifestWriteTask
	gcTask                *summaryGCTask
	lastAcked             *utility.WALCheckpoint
	lastObserved          *utility.WALCheckpoint
	latestCoveredTimeTick uint64
	restoredTimeTick      uint64
	terminalErr           error
	gcFrontiers           map[string]uint64
	durableFrontiers      map[string]uint64
}

// ManagerConfig carries the wiring of one pchannel's summary manager.
type ManagerConfig struct {
	Runtime moduleapi.Runtime
	// FlushMaxBytes seals a chunk at this staging size. Zero disables size-based sealing.
	FlushMaxBytes uint64
	// EnableTransform stages delete records for a wired TransformLog consumer.
	// Consumers opt in when their recovery and retention paths are ready.
	EnableTransform bool
	PChannel        string
	Term            int64
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
		gcFrontiers:      make(map[string]uint64),
		cfg:              config,
		manifest:         &streamingpb.PChannelSummaryManifest{},
		durableFrontiers: make(map[string]uint64),
	}
}

// ObserveMessage copies one ordered WAL message into the summary without
// retaining its source handle. Size thresholds schedule asynchronous writes.
// DDL messages preserve the history of previously committed requests.
func (m *Manager) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) {
	if msg == nil || funcutil.IsControlChannel(msg.VChannel()) {
		return
	}
	if msg.VChannel() == "" && msg.MessageType() != message.MessageTypeTimeTick {
		return
	}
	idempotency, insert := idempotencyHalvesOf(msg)
	var entry *streamingpb.TransformLogEntry
	if m.cfg.EnableTransform && messageutil.ClassifyTransformLogMessage(msg) == messageutil.TransformLogKindDelete {
		entry = messageutil.BuildTransformLogEntry(msg, messageutil.TransformEntryOption{})
	}
	m.mu.Lock()
	m.lastObserved = newSummaryCheckpoint(msg.LastConfirmedMessageID(), msg.TimeTick())
	if (idempotency != nil || entry != nil) && msg.TimeTick() > m.restoredTimeTick && msg.TimeTick() > m.durableFrontiers[msg.VChannel()] {
		m.seedLastAckedLocked(msg)
		m.stageRecordLocked(msg, idempotency, insert, entry)
	}
	m.refreshLastAckedLocked()
	overThreshold := m.cfg.FlushMaxBytes > 0 && m.pendingBytes >= m.cfg.FlushMaxBytes
	m.mu.Unlock()
	if overThreshold {
		m.requestSeal()
	}
}

// stageRecordLocked appends one record to the pending span. Caller
// holds m.mu. The entry is built here — the message payload is not retained,
// so it must be copied before the message is released.
func (m *Manager) stageRecordLocked(
	msg message.ImmutableMessage,
	idempotency *streamingpb.VChannelSummaryIdempotencyRecord,
	insert *streamingpb.VChannelSummaryInsertRecord,
	entry *streamingpb.TransformLogEntry,
) {
	record := stagedRecord{
		entry:       entry,
		vchannel:    msg.VChannel(),
		timeTick:    msg.TimeTick(),
		idempotency: idempotency,
		insert:      insert,
	}
	if len(m.pending) == 0 {
		m.pendingSince = time.Now()
	}
	m.pending = append(m.pending, record)
	m.pendingBytes += stagedRecordSize(msg, &record)
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
	size := uint64(proto.Size(record.insert)) + uint64(proto.Size(record.idempotency)) + uint64(proto.Size(record.entry))
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
		return string(message.IdempotencyKeyOf(msg))
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
		return string(message.IdempotencyKeyOf(commit))
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

// seal takes the pending span out under the lock, organizes the records by
// vchannel, and enqueues the sealed chunk. The records are already built (see
// ObserveMessage); the vchannel grouping is the only organization left.
func (m *Manager) seal() *SealedChunk {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pending) == 0 || m.terminalErr != nil {
		return nil
	}
	if m.reopenedTerm {
		// Read-only same-term recovery is allowed, but a writer must use a new
		// assignment term: old objects may exist beyond the first recovered gap.
		m.terminalErr = storeCorruptedf("summary writes after recovery require a fresh assignment term")
		return nil
	}
	if m.generationExhausted {
		m.terminalErr = storeCorruptedf("summary generation exhausted")
		return nil
	}
	// Keep removal and queue insertion atomic: observers must never see a gap
	// where an unwritten chunk appears to have no pending records.
	sc := buildSealedChunk(m.nextGeneration, m.pending)
	if m.lastObserved != nil {
		sc.confirmedThrough = m.lastObserved.Clone()
	}
	if m.nextGeneration == math.MaxUint64 {
		m.generationExhausted = true
	} else {
		m.nextGeneration++
	}
	m.pending = nil
	m.pendingBytes = 0
	m.pendingSince = time.Time{}
	m.pendingSealed = append(m.pendingSealed, sc)
	m.pendingFlushTimeTick = sc.MaxTimeTick
	return sc
}

// buildSealedChunk organizes one chunk span by grouping records by vchannel.
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

// writeChunk uploads only this immutable chunk. Each sealed chunk has its own
// scheduler task, so another upload may finish first.
func (m *Manager) writeChunk(ctx context.Context, sc *SealedChunk) error {
	sections := make(map[string]*ChunkSections, len(sc.RecordsByVChannel))
	for vchannel, staged := range sc.RecordsByVChannel {
		cs := &ChunkSections{}
		for _, record := range staged {
			if record.entry != nil {
				cs.Transform = append(cs.Transform, &streamingpb.VChannelSummaryTransformRecord{
					TimeTick: record.timeTick, Delete: record.entry.GetDelete(),
				})
			}
			if record.insert != nil {
				cs.Inserts = append(cs.Inserts, record.insert)
				cs.Idempotency = append(cs.Idempotency, record.idempotency)
			}
		}
		sections[vchannel] = cs
	}
	footer, size, err := m.cfg.Store.WriteChunk(ctx, sc.Generation, sections, summaryPosition(sc.confirmedThrough))
	if err != nil {
		return err
	}
	m.mu.Lock()
	sc.index = chunkIndexEntryFromFooter(footer, size)
	for len(m.pendingSealed) > 0 && m.pendingSealed[0].index != nil {
		head := m.pendingSealed[0]
		recordChunk(m.manifest, head.index)
		for vchannel, records := range head.RecordsByVChannel {
			for _, record := range records {
				m.durableFrontiers[vchannel] = max(m.durableFrontiers[vchannel], record.timeTick)
			}
		}
		m.latestCoveredTimeTick = m.manifest.GetCoveredPosition().GetTimeTick()
		m.manifestVersion++
		m.pendingSealed[0] = nil
		m.pendingSealed = m.pendingSealed[1:]
	}
	m.refreshLastAckedLocked()
	m.mu.Unlock()
	m.scheduleManifest()
	return nil
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
// records are not lost -- the consume checkpoint is saved only after the
// persist that writes them, so recovery replays their messages and
// ObserveMessage stages them again -- but they are reachable only here. Reading the manifest alone
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
	m.readMu.RLock()
	defer m.readMu.RUnlock()
	m.mu.Lock()
	inMemory := make(map[string][][]*stagedRecord, len(vchannels))
	for _, vchannel := range vchannels {
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
		if chunk.GetEndTimetick() <= from || chunk.GetStartTimetick() > to {
			continue
		}
		indexes := make(map[string]*streamingpb.VChannelSummaryChunkIndex)
		for _, vchannel := range vchannels {
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
			hasKeys := len(sections.Idempotency) != 0
			anyKeys[vchannel] = anyKeys[vchannel] || hasKeys
			target := out[vchannel]
			for i, insert := range sections.Inserts {
				tt := insert.GetSourceTimetick()
				if tt <= from || tt > to {
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
		for _, records := range inMemory[vchannel] {
			anyKeys[vchannel] = appendStagedIdempotency(target, records, from, to) || anyKeys[vchannel]
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
// position they came from. The message itself is not retained.
type stagedRecord struct {
	entry    *streamingpb.TransformLogEntry
	vchannel string
	timeTick uint64

	// idempotency and insert are the two halves of the idempotency consumer's
	// record, set together or not at all. A message contributes to the sections
	// it has content for, so a committed txn carrying both a delete and an
	// idempotent insert contributes to both from one staged record.
	idempotency *streamingpb.VChannelSummaryIdempotencyRecord
	insert      *streamingpb.VChannelSummaryInsertRecord
}

// SealedChunk is one chunk span taken out of the pending buffer: the records
// are immutable once sealed, so the write task may build and rewrite the
// object without touching the manager state.
type SealedChunk struct {
	task              *chunkWriteTask
	index             *streamingpb.PChannelSummaryChunkIndexEntry
	confirmedThrough  *utility.WALCheckpoint
	Generation        uint64
	RecordsByVChannel map[string][]*stagedRecord
	MaxTimeTick       uint64
}

// ReadTransformEntries loads the durable transform backlog in (from, to].
// Consumers call this once during recovery and observe live deletes directly.
func (m *Manager) ReadTransformEntries(
	ctx context.Context,
	vchannel string,
	from, to uint64,
) ([]*streamingpb.TransformLogEntry, error) {
	m.readMu.RLock()
	defer m.readMu.RUnlock()
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
		if index == nil || index.GetTransform() == nil {
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

// AdvanceGCTimeTick reports a durable transform materialization or cleanup frontier.
func (m *Manager) AdvanceGCTimeTick(vchannel string, timetick uint64) {
	m.mu.Lock()
	if timetick > m.gcFrontiers[vchannel] {
		m.gcFrontiers[vchannel] = timetick
	}
	m.mu.Unlock()
}
