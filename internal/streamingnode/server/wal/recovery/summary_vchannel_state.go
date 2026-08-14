package recovery

import (
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type vchannelSummary struct {
	pchannel                   string
	vchannel                   string
	snapshotCheckpointTimetick uint64
	evictedWatermarkTimetick   uint64
	entries                    map[string]*summaryEntry
	commitOrder                []string
	pendingEntries             map[string]*streamingpb.SummaryEntry
	pendingRecords             []*streamingpb.SummaryEntry
	latestAppliedGeneration    uint64
	minRequiredGeneration      uint64
	dirty                      bool
	// entryBytes tracks the total serialized size of materialized entries for the
	// byte-cap eviction.
	entryBytes int

	// generationStats is the view's durable-retention ledger: one row per
	// persisted chunk generation, tracking how many summary entries it holds, how
	// many serialized bytes they occupy, and the newest commit timetick among
	// them. minRequiredGeneration derives from this ledger under evictionCfg
	// (TTL / min entries / max bytes) — NOT from the entries materialized in this
	// staging summary, which are cleared on persist (evictPersisted). Rebuilt on
	// restart from the chunk replay.
	generationStats map[uint64]*summaryGenerationStat
	// evictionCfg is the view's retention policy, assigned by the summaryManager on
	// construction. A zero policy (no TTL, no byte cap) makes no durable
	// retention promise: the ledger is ignored and only materialized entries pin
	// chunks.
	evictionCfg summaryEvictionConfig
}

// summaryGenerationStat aggregates the entries persisted at one chunk
// generation.
type summaryGenerationStat struct {
	entryCount  int
	byteSize    int
	maxCommitTT uint64
}

// summaryEntry is a committed idempotency entry together with the chunk
// generation it was persisted at. generationSet is false until the entry has
// been written to a chunk; an entry without a generation still lives in the WAL
// and must not advance the chunk-retention boundary.
type summaryEntry struct {
	entry         *streamingpb.SummaryEntry
	generation    uint64
	generationSet bool
}

type summaryMetaUpdate struct {
	meta             *streamingpb.VChannelSummaryMeta
	pendingEntryKeys []string
	// retentionPinned captures hasRetentionPin() at construction time: whether
	// any materialized entry or durable-retention-ledger generation still pins
	// chunk retention. WithPersistedGeneration must consult it instead of
	// inferring "nothing pinned" from EntryCount == 0 — the staging summary is
	// cleared on every persist, so an empty entry set says nothing about the
	// ledger, and projecting the boundary forward past a pinned generation
	// would poison the persisted meta (irreversibly, once chunk GC runs after
	// a restart).
	retentionPinned bool
}

func newEmptyVChannelSummary(pchannel, vchannel string, checkpoint *WALCheckpoint) *vchannelSummary {
	state := &vchannelSummary{
		pchannel:        pchannel,
		vchannel:        vchannel,
		entries:         make(map[string]*summaryEntry),
		pendingEntries:  make(map[string]*streamingpb.SummaryEntry),
		pendingRecords:  make([]*streamingpb.SummaryEntry, 0),
		generationStats: make(map[uint64]*summaryGenerationStat),
	}
	if checkpoint != nil {
		state.snapshotCheckpointTimetick = checkpoint.TimeTick
	}
	state.refreshEvictedWatermark()
	state.refreshMinRequiredGeneration()
	return state
}

func (s *vchannelSummary) observeMessage(msg message.ImmutableMessage) {
	if msg == nil {
		return
	}
	if msg.MessageType() == message.MessageTypeTimeTick {
		return
	}
	if msg.TimeTick() <= s.snapshotCheckpointTimetick {
		return
	}
	record, ok := newSummaryEntryFromMessage(s.pchannel, msg)
	if !ok {
		s.advanceCheckpoint(msg)
		return
	}
	_ = s.applySummaryEntry(record, true)
}

func (s *vchannelSummary) advanceCheckpoint(msg message.ImmutableMessage) {
	if msg == nil || msg.TimeTick() <= s.snapshotCheckpointTimetick {
		return
	}
	s.snapshotCheckpointTimetick = msg.TimeTick()
	s.refreshEvictedWatermark()
}

func (s *vchannelSummary) advanceCheckpointTo(checkpoint *WALCheckpoint) {
	if checkpoint == nil || checkpoint.TimeTick <= s.snapshotCheckpointTimetick {
		return
	}
	s.snapshotCheckpointTimetick = checkpoint.TimeTick
	s.refreshEvictedWatermark()
}

func (s *vchannelSummary) consumePendingSummaryEntries() ([]*streamingpb.SummaryEntry, *summaryMetaUpdate) {
	if !s.dirty {
		return nil, nil
	}
	records := sortedSummaryEntries(s.pendingRecords)
	pendingEntryKeys := make([]string, 0, len(s.pendingEntries))
	for key := range s.pendingEntries {
		pendingEntryKeys = append(pendingEntryKeys, key)
	}
	sort.Strings(pendingEntryKeys)
	update := &summaryMetaUpdate{
		meta:             s.summaryMeta(),
		pendingEntryKeys: pendingEntryKeys,
		retentionPinned:  s.hasRetentionPin(),
	}
	s.dirty = false
	s.pendingEntries = make(map[string]*streamingpb.SummaryEntry)
	s.pendingRecords = s.pendingRecords[:0]
	return records, update
}

// VChannelSummarySnapshot is what recovery hands an application view once it
// has replayed the chunks: one vchannel's retained summary entries, plus the
// retention state that decides which of them may still be served.
//
// It is NOT a snapshot of a VChannelSummaryChunk, and must not be named as if
// it were. A chunk is one generation's delta, keyless bookkeeping entries
// included, written durably. This is the accumulation across every replayed
// generation, deduplicated by key and narrowed by TTL / byte eviction, keyed
// entries only.
//
// It never leaves the process: built once at WAL open, consumed once by the
// view, never stored or sent. That is why it is a plain Go struct rather than a
// message in streaming.proto — a proto here would advertise a wire contract
// that does not exist. The entries themselves stay proto: they come out of the
// chunks as-is and go into the view as-is, with nothing converted in between.
type VChannelSummarySnapshot struct {
	PChannel                   string
	VChannel                   string
	EvictedWatermarkTimetick   uint64
	SnapshotCheckpointTimetick uint64
	Entries                    []*streamingpb.SummaryEntry
}

func (s *vchannelSummary) snapshot() *VChannelSummarySnapshot {
	entries := make([]*streamingpb.SummaryEntry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e.entry)
	}
	sortSummaryEntries(entries)
	return &VChannelSummarySnapshot{
		PChannel:                   s.pchannel,
		VChannel:                   s.vchannel,
		EvictedWatermarkTimetick:   s.evictedWatermarkTimetick,
		SnapshotCheckpointTimetick: s.snapshotCheckpointTimetick,
		Entries:                    entries,
	}
}

func (s *vchannelSummary) applySummaryEntriesAtGeneration(records []*streamingpb.SummaryEntry, generation uint64) error {
	records = sortedSummaryEntries(records)
	for _, record := range records {
		if err := s.applySummaryEntry(record, false); err != nil {
			return err
		}
		s.markSummaryEntryGeneration(record, generation)
	}
	s.latestAppliedGeneration = maxUint64(s.latestAppliedGeneration, generation)
	s.refreshMinRequiredGeneration()
	s.dirty = false
	s.pendingEntries = make(map[string]*streamingpb.SummaryEntry)
	s.pendingRecords = s.pendingRecords[:0]
	return nil
}

func (s *vchannelSummary) applySummaryEntry(record *streamingpb.SummaryEntry, markDirty bool) error {
	key := record.GetIdempotency().GetKey()
	if key == "" {
		// A keyless write is checkpoint bookkeeping only: it advances the summary
		// but materializes no entry.
		if markDirty {
			s.pendingRecords = append(s.pendingRecords, record)
		}
		s.advanceCheckpointToSummaryEntry(record, markDirty)
		return nil
	}
	if existing, ok := s.entries[key]; ok {
		if s.entryTTLExpiredAt(existing.entry, record.GetSourceTimetick()) {
			s.dropEntry(key)
		} else {
			if markDirty {
				s.pendingRecords = append(s.pendingRecords, record)
			}
			s.advanceCheckpointToSummaryEntry(record, markDirty)
			return nil
		}
	}
	// The chunk stores and the view materializes the same entry, so there is
	// nothing to project here: the keyed entry is retained as it stands.
	entry := record
	s.entries[key] = &summaryEntry{entry: entry}
	s.entryBytes += proto.Size(entry)
	s.commitOrder = append(s.commitOrder, key)
	if markDirty {
		s.pendingEntries[key] = entry
		s.pendingRecords = append(s.pendingRecords, record)
	}
	s.advanceCheckpointToSummaryEntry(record, markDirty)
	s.refreshMinRequiredGeneration()
	return nil
}

func (s *vchannelSummary) entryTTLExpiredAt(entry *streamingpb.SummaryEntry, nowTT uint64) bool {
	if entry == nil || s.evictionCfg.entryTTL <= 0 {
		return false
	}
	evictBeforeTT := evictBeforeTimetick(nowTT, s.evictionCfg.entryTTL)
	return evictBeforeTT > 0 && entry.GetSourceTimetick() < evictBeforeTT
}

func (s *vchannelSummary) dropEntry(key string) {
	e, ok := s.entries[key]
	if !ok {
		return
	}
	delete(s.entries, key)
	s.entryBytes -= proto.Size(e.entry)
	for idx, ordered := range s.commitOrder {
		if ordered == key {
			s.commitOrder = append(s.commitOrder[:idx], s.commitOrder[idx+1:]...)
			break
		}
	}
	if e.generationSet {
		s.rebuildGenerationStat(e.generation)
	}
}

func (s *vchannelSummary) rebuildGenerationStat(generation uint64) {
	var stat summaryGenerationStat
	for _, e := range s.entries {
		if e == nil || !e.generationSet || e.generation != generation {
			continue
		}
		stat.entryCount++
		stat.byteSize += proto.Size(e.entry)
		if e.entry.GetSourceTimetick() > stat.maxCommitTT {
			stat.maxCommitTT = e.entry.GetSourceTimetick()
		}
	}
	if stat.entryCount == 0 {
		delete(s.generationStats, generation)
		return
	}
	if s.generationStats == nil {
		s.generationStats = make(map[uint64]*summaryGenerationStat)
	}
	s.generationStats[generation] = &stat
}

func (s *vchannelSummary) advanceCheckpointToSummaryEntry(record *streamingpb.SummaryEntry, markDirty bool) {
	if record.SourceTimetick <= s.snapshotCheckpointTimetick {
		return
	}
	s.snapshotCheckpointTimetick = record.SourceTimetick
	s.refreshEvictedWatermark()
	if markDirty {
		s.dirty = true
	}
}

func (s *vchannelSummary) refreshEvictedWatermark() {
	for len(s.commitOrder) > 0 {
		did := s.commitOrder[0]
		e, ok := s.entries[did]
		if !ok {
			s.commitOrder = s.commitOrder[1:]
			continue
		}
		// The watermark is inclusive: it points to the oldest retained entry, not a strict evicted lower bound.
		s.evictedWatermarkTimetick = e.entry.GetSourceTimetick()
		return
	}
	s.evictedWatermarkTimetick = s.snapshotCheckpointTimetick
}

func (s *vchannelSummary) markSummaryEntriesPersisted(records []*streamingpb.SummaryEntry, generation uint64) {
	for _, record := range records {
		s.markSummaryEntryGeneration(record, generation)
	}
	s.latestAppliedGeneration = maxUint64(s.latestAppliedGeneration, generation)
	s.refreshMinRequiredGeneration()
}

func (s *vchannelSummary) markSummaryEntryGeneration(record *streamingpb.SummaryEntry, generation uint64) {
	key := record.GetIdempotency().GetKey()
	if key == "" {
		return
	}
	e, ok := s.entries[key]
	if !ok {
		return
	}
	if !e.generationSet {
		e.generation = generation
		e.generationSet = true
		s.registerGenerationEntry(generation, e.entry)
	}
}

// registerGenerationEntry records one persisted entry in the durable-retention
// ledger. The generationSet guard on the caller ensures each entry is counted
// exactly once.
func (s *vchannelSummary) registerGenerationEntry(generation uint64, entry *streamingpb.SummaryEntry) {
	if entry == nil {
		return
	}
	if s.generationStats == nil {
		s.generationStats = make(map[uint64]*summaryGenerationStat)
	}
	stat, ok := s.generationStats[generation]
	if !ok {
		stat = &summaryGenerationStat{}
		s.generationStats[generation] = stat
	}
	stat.entryCount++
	stat.byteSize += proto.Size(entry)
	if commitTT := entry.GetSourceTimetick(); commitTT > stat.maxCommitTT {
		stat.maxCommitTT = commitTT
	}
}

func (s *vchannelSummary) summaryMeta() *streamingpb.VChannelSummaryMeta {
	s.refreshMinRequiredGeneration()
	return &streamingpb.VChannelSummaryMeta{
		Pchannel:                   s.pchannel,
		Vchannel:                   s.vchannel,
		EvictedWatermarkTimetick:   s.evictedWatermarkTimetick,
		SnapshotCheckpointTimetick: s.snapshotCheckpointTimetick,
		LatestAppliedGeneration:    s.latestAppliedGeneration,
		MinRequiredGeneration:      s.minRequiredGeneration,
		ViewType:                   common.VChannelSummaryViewTypeIdempotency,
		EntryCount:                 uint64(len(s.entries)),
	}
}

func (s *vchannelSummary) summaryMetaAtGeneration(generation uint64) *streamingpb.VChannelSummaryMeta {
	meta := s.summaryMeta()
	if generation > meta.GetLatestAppliedGeneration() {
		meta.LatestAppliedGeneration = generation
		// Project the boundary forward only when nothing pins retention — the
		// staging summary being empty (EntryCount==0) is NOT sufficient: the
		// durable-retention ledger may still pin older generations even though
		// every persisted entry was evicted from staging memory.
		if !s.hasRetentionPin() {
			meta.MinRequiredGeneration = generation
		}
	}
	return meta
}

func (s *vchannelSummary) hasRetentionPin() bool {
	if _, ok := s.materializedMinGeneration(); ok {
		return true
	}
	_, ok := s.statsMinRequiredGeneration()
	return ok
}

// refreshMinRequiredGeneration recomputes the oldest chunk generation this view
// still needs. Two pins contribute, and the lower wins:
//   - materialized entries carrying a generation (recovery mode keeps replayed
//     entries in memory until TTL eviction);
//   - the durable-retention ledger (generationStats) under the view's eviction
//     policy — the staging summary is cleared on persist, so chunk retention
//     must NOT depend on entries still being materialized here; the ledger is
//     what keeps a TTL's worth of chunks recoverable across restarts.
//
// An entry that has never been persisted lives in the WAL, not in any chunk,
// and is recovered by replaying the WAL from the summary source checkpoint —
// it pins nothing. When nothing pins, nothing below the latest persisted
// generation is still needed. NOTE: during recovery, the value loaded from the
// persisted VChannelSummaryMeta must not be recomputed before the chunk replay
// has rebuilt the ledger (the single-threaded recovery sequence guarantees
// this today).
func (s *vchannelSummary) refreshMinRequiredGeneration() {
	minimum := s.latestAppliedGeneration
	if m, ok := s.materializedMinGeneration(); ok && m < minimum {
		minimum = m
	}
	if m, ok := s.statsMinRequiredGeneration(); ok && m < minimum {
		minimum = m
	}
	s.minRequiredGeneration = minimum
}

func (s *vchannelSummary) materializedMinGeneration() (uint64, bool) {
	var minimum uint64
	initialized := false
	for _, e := range s.entries {
		if !e.generationSet {
			continue
		}
		if !initialized || e.generation < minimum {
			minimum = e.generation
			initialized = true
		}
	}
	return minimum, initialized
}

// statsMinRequiredGeneration walks the durable-retention ledger newest-first
// under the eviction policy and returns the oldest generation still holding a
// retained entry: generations are kept while the byte cap is not exhausted and
// either some entry is within TTL or the minEntries floor still needs them.
// Rows older than the returned boundary are dropped from the ledger, keeping it
// bounded to the retained summary. A zero policy makes no durable retention
// promise and contributes no pin.
func (s *vchannelSummary) statsMinRequiredGeneration() (uint64, bool) {
	cfg := s.evictionCfg
	if len(s.generationStats) == 0 || (cfg.entryTTL <= 0 && cfg.maxBytes <= 0) {
		return 0, false
	}
	generations := make([]uint64, 0, len(s.generationStats))
	for generation := range s.generationStats {
		generations = append(generations, generation)
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i] > generations[j] })

	evictBefore := evictBeforeTimetick(s.snapshotCheckpointTimetick, cfg.entryTTL)
	cumulativeEntries := 0
	cumulativeBytes := 0
	var minRequired uint64
	retainedAny := false
	for _, generation := range generations {
		stat := s.generationStats[generation]
		if cfg.maxBytes > 0 && cumulativeBytes >= cfg.maxBytes {
			break
		}
		withinTTL := cfg.entryTTL <= 0 || stat.maxCommitTT >= evictBefore
		if !withinTTL && cumulativeEntries >= cfg.minEntries {
			break
		}
		minRequired = generation
		retainedAny = true
		cumulativeEntries += stat.entryCount
		cumulativeBytes += stat.byteSize
	}
	if !retainedAny {
		return 0, false
	}
	for generation := range s.generationStats {
		if generation < minRequired {
			delete(s.generationStats, generation)
		}
	}
	return minRequired, true
}

func (s *vchannelSummary) evictForRecovery(evictBeforeTT uint64, minEntries, maxBytes int) {
	for len(s.entries) > minEntries && len(s.commitOrder) > 0 {
		key := s.commitOrder[0]
		e, ok := s.entries[key]
		if !ok {
			s.commitOrder = s.commitOrder[1:]
			continue
		}
		if e.entry.GetSourceTimetick() >= evictBeforeTT {
			break
		}
		s.commitOrder = s.commitOrder[1:]
		s.dropEntry(key)
	}
	// Hard byte cap, overriding the minEntries floor: an entry-count floor cannot
	// bound memory when each entry carries a per-row PK list.
	for maxBytes > 0 && s.entryBytes > maxBytes && len(s.commitOrder) > 0 {
		key := s.commitOrder[0]
		s.commitOrder = s.commitOrder[1:]
		s.dropEntry(key)
	}
	s.refreshEvictedWatermark()
	s.refreshMinRequiredGeneration()
}

// dropEntryBytes subtracts a to-be-removed entry's serialized size from the
// summary's byte accounting.
func (s *vchannelSummary) dropEntryBytes(key string) {
	if e, ok := s.entries[key]; ok && e.entry != nil {
		s.entryBytes -= proto.Size(e.entry)
	}
}

// evictPersisted drops every entry that has already been durably persisted (its
// generation is assigned), stopping at the first entry that is not persisted
// yet. In normal mode the recovery-side summary is only a persist-staging buffer
// -- the interceptor serves live dedup from its own summary -- so a persisted
// entry is no longer needed here and is dropped regardless of TTL. Un-persisted
// entries are never dropped, so no observed write can be lost before it lands in
// a chunk.
func (s *vchannelSummary) evictPersisted() {
	for len(s.commitOrder) > 0 {
		key := s.commitOrder[0]
		e, ok := s.entries[key]
		if !ok {
			s.commitOrder = s.commitOrder[1:]
			continue
		}
		if !e.generationSet {
			break
		}
		s.commitOrder = s.commitOrder[1:]
		s.dropEntryBytes(key)
		delete(s.entries, key)
	}
	s.refreshEvictedWatermark()
	s.refreshMinRequiredGeneration()
}

// evictBeforeTimetick derives the TTL eviction bound from an observed message
// timetick. Kept in sync with the live summary's evictBeforeCommitTT (idempotency
// package), including the underflow guard for timeticks younger than the TTL.
func evictBeforeTimetick(nowTT uint64, ttl time.Duration) uint64 {
	if ttl <= 0 {
		return 0
	}
	physical, logical := tsoutil.ParseHybridTs(nowTT)
	msecs := ttl.Milliseconds()
	if physical <= msecs {
		return 0
	}
	return tsoutil.ComposeTS(physical-msecs, logical)
}

func (update *summaryMetaUpdate) WithPersistedGeneration(generation uint64) *streamingpb.VChannelSummaryMeta {
	if update == nil || update.meta == nil {
		return nil
	}
	meta := proto.Clone(update.meta).(*streamingpb.VChannelSummaryMeta)
	meta.LatestAppliedGeneration = maxUint64(meta.GetLatestAppliedGeneration(), generation)
	if meta.GetEntryCount() == 0 {
		// Advance the boundary only when nothing pinned retention at capture
		// time — mirroring summaryMetaAtGeneration. An empty staging summary is
		// NOT sufficient: the durable-retention ledger may still pin older
		// generations after evictPersisted cleared the entries, and the captured
		// meta already carries the ledger-derived MinRequiredGeneration.
		if !update.retentionPinned {
			meta.MinRequiredGeneration = meta.GetLatestAppliedGeneration()
		}
		return meta
	}
	if len(update.pendingEntryKeys) > 0 {
		if meta.GetMinRequiredGeneration() == 0 || generation < meta.GetMinRequiredGeneration() {
			meta.MinRequiredGeneration = generation
		}
	}
	return meta
}

func maxUint64(left, right uint64) uint64 {
	if right > left {
		return right
	}
	return left
}

func sortSummaryEntries(entries []*streamingpb.SummaryEntry) {
	sort.Slice(entries, func(i, j int) bool {
		left, right := entries[i], entries[j]
		if left.GetSourceTimetick() != right.GetSourceTimetick() {
			return left.GetSourceTimetick() < right.GetSourceTimetick()
		}
		return left.GetIdempotency().GetKey() < right.GetIdempotency().GetKey()
	})
}
