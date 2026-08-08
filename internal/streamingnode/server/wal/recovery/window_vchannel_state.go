package recovery

import (
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type vchannelWindow struct {
	pchannel                    string
	vchannel                    string
	snapshotCheckpointTimetick  uint64
	snapshotCheckpointMessageID *commonpb.MessageID
	evictedWatermarkTimetick    uint64
	entries                     map[string]*windowEntry
	commitOrder                 []string
	pendingEntries              map[string]*streamingpb.WindowEntry
	pendingRecords              []committedWriteRecord
	latestAppliedGeneration     uint64
	minRequiredGeneration       uint64
	dirty                       bool
	// entryBytes tracks the total serialized size of materialized entries for the
	// byte-cap eviction.
	entryBytes int

	// generationStats is the view's durable-retention ledger: one row per
	// persisted chunk generation, tracking how many window entries it holds, how
	// many serialized bytes they occupy, and the newest commit timetick among
	// them. minRequiredGeneration derives from this ledger under evictionCfg
	// (TTL / min entries / max bytes) — NOT from the entries materialized in this
	// staging window, which are cleared on persist (evictPersisted). Rebuilt on
	// restart from the chunk replay.
	generationStats map[uint64]*windowGenerationStat
	// evictionCfg is the view's retention policy, assigned by the windowManager on
	// construction. A zero policy (no TTL, no byte cap) makes no durable
	// retention promise: the ledger is ignored and only materialized entries pin
	// chunks.
	evictionCfg windowEvictionConfig
}

// windowGenerationStat aggregates the entries persisted at one chunk
// generation.
type windowGenerationStat struct {
	entryCount  int
	byteSize    int
	maxCommitTT uint64
}

// windowEntry is a committed idempotency entry together with the chunk
// generation it was persisted at. generationSet is false until the entry has
// been written to a chunk; an entry without a generation still lives in the WAL
// and must not advance the chunk-retention boundary.
type windowEntry struct {
	entry         *streamingpb.WindowEntry
	generation    uint64
	generationSet bool
}

type idempotencyWindowMetaUpdate struct {
	meta             *streamingpb.VChannelWindowMeta
	pendingEntryKeys []string
	// retentionPinned captures hasRetentionPin() at construction time: whether
	// any materialized entry or durable-retention-ledger generation still pins
	// chunk retention. WithPersistedGeneration must consult it instead of
	// inferring "nothing pinned" from EntryCount == 0 — the staging window is
	// cleared on every persist, so an empty entry set says nothing about the
	// ledger, and projecting the boundary forward past a pinned generation
	// would poison the persisted meta (irreversibly, once chunk GC runs after
	// a restart).
	retentionPinned bool
}

func newEmptyVChannelWindow(pchannel, vchannel string, checkpoint *WALCheckpoint) *vchannelWindow {
	state := &vchannelWindow{
		pchannel:        pchannel,
		vchannel:        vchannel,
		entries:         make(map[string]*windowEntry),
		pendingEntries:  make(map[string]*streamingpb.WindowEntry),
		pendingRecords:  make([]committedWriteRecord, 0),
		generationStats: make(map[uint64]*windowGenerationStat),
	}
	if checkpoint != nil {
		state.snapshotCheckpointTimetick = checkpoint.TimeTick
		if checkpoint.MessageID != nil {
			state.snapshotCheckpointMessageID = checkpoint.MessageID.IntoProto()
		}
	}
	state.refreshEvictedWatermark()
	state.refreshMinRequiredGeneration()
	return state
}

func newVChannelWindowFromSnapshot(snapshot *streamingpb.WindowSnapshot) (*vchannelWindow, error) {
	if snapshot == nil {
		return nil, merr.WrapErrServiceInternalMsg("nil idempotency window snapshot")
	}
	state := &vchannelWindow{
		pchannel:                    snapshot.GetPchannel(),
		vchannel:                    snapshot.GetVchannel(),
		snapshotCheckpointTimetick:  snapshot.GetSnapshotCheckpointTimetick(),
		snapshotCheckpointMessageID: cloneMessageIDProto(snapshot.GetSnapshotCheckpointMessageId()),
		evictedWatermarkTimetick:    snapshot.GetEvictedWatermarkTimetick(),
		entries:                     make(map[string]*windowEntry, len(snapshot.GetEntries())),
		pendingEntries:              make(map[string]*streamingpb.WindowEntry),
		pendingRecords:              make([]committedWriteRecord, 0),
		commitOrder:                 make([]string, 0, len(snapshot.GetEntries())),
		generationStats:             make(map[uint64]*windowGenerationStat),
	}
	sortedEntries := append([]*streamingpb.WindowEntry(nil), snapshot.GetEntries()...)
	sortWindowEntries(sortedEntries)
	for _, entry := range sortedEntries {
		state.entries[entry.GetKey()] = &windowEntry{entry: entry}
		state.entryBytes += proto.Size(entry)
		state.commitOrder = append(state.commitOrder, entry.GetKey())
	}
	state.refreshEvictedWatermark()
	state.refreshMinRequiredGeneration()
	return state, nil
}

func (s *vchannelWindow) observeMessage(msg message.ImmutableMessage) {
	if msg == nil {
		return
	}
	if msg.MessageType() == message.MessageTypeTimeTick {
		return
	}
	if msg.TimeTick() <= s.snapshotCheckpointTimetick {
		return
	}
	record, ok := newCommittedWriteRecordFromMessage(s.pchannel, msg)
	if !ok {
		s.advanceCheckpoint(msg)
		return
	}
	_ = s.applyCommittedWriteRecord(*record, true)
}

func (s *vchannelWindow) advanceCheckpoint(msg message.ImmutableMessage) {
	if msg == nil || msg.TimeTick() <= s.snapshotCheckpointTimetick {
		return
	}
	s.snapshotCheckpointTimetick = msg.TimeTick()
	s.snapshotCheckpointMessageID = safeMessageIDProto(msg.LastConfirmedMessageID())
	if s.snapshotCheckpointMessageID == nil && msg.MessageID() != nil {
		s.snapshotCheckpointMessageID = msg.MessageID().IntoProto()
	}
	s.refreshEvictedWatermark()
}

func (s *vchannelWindow) advanceCheckpointTo(checkpoint *WALCheckpoint) {
	if checkpoint == nil || checkpoint.TimeTick <= s.snapshotCheckpointTimetick {
		return
	}
	s.snapshotCheckpointTimetick = checkpoint.TimeTick
	if checkpoint.MessageID != nil {
		s.snapshotCheckpointMessageID = checkpoint.MessageID.IntoProto()
	}
	s.refreshEvictedWatermark()
}

func (s *vchannelWindow) consumePendingCommittedWriteRecords() ([]committedWriteRecord, *idempotencyWindowMetaUpdate) {
	if !s.dirty {
		return nil, nil
	}
	records := cloneAndSortCommittedWriteRecords(s.pchannel, s.vchannel, s.pendingRecords)
	pendingEntryKeys := make([]string, 0, len(s.pendingEntries))
	for key := range s.pendingEntries {
		pendingEntryKeys = append(pendingEntryKeys, key)
	}
	sort.Strings(pendingEntryKeys)
	update := &idempotencyWindowMetaUpdate{
		meta:             s.windowMeta(),
		pendingEntryKeys: pendingEntryKeys,
		retentionPinned:  s.hasRetentionPin(),
	}
	s.dirty = false
	s.pendingEntries = make(map[string]*streamingpb.WindowEntry)
	s.pendingRecords = s.pendingRecords[:0]
	return records, update
}

func (s *vchannelWindow) snapshot() *streamingpb.WindowSnapshot {
	entries := make([]*streamingpb.WindowEntry, 0, len(s.entries))
	for _, e := range s.entries {
		entries = append(entries, e.entry)
	}
	sortWindowEntries(entries)
	return s.snapshotWithEntries(entries)
}

func (s *vchannelWindow) snapshotWithEntries(entries []*streamingpb.WindowEntry) *streamingpb.WindowSnapshot {
	snapshot := &streamingpb.WindowSnapshot{
		Pchannel:                    s.pchannel,
		Vchannel:                    s.vchannel,
		EvictedWatermarkTimetick:    s.evictedWatermarkTimetick,
		SnapshotCheckpointTimetick:  s.snapshotCheckpointTimetick,
		SnapshotCheckpointMessageId: cloneMessageIDProto(s.snapshotCheckpointMessageID),
		Entries:                     entries,
	}
	return snapshot
}

func (s *vchannelWindow) applyCommittedWriteRecordsAtGeneration(records []committedWriteRecord, generation uint64) error {
	records = cloneAndSortCommittedWriteRecords(s.pchannel, s.vchannel, records)
	for _, record := range records {
		if err := s.applyCommittedWriteRecord(record, false); err != nil {
			return err
		}
		s.markCommittedWriteRecordGeneration(record, generation)
	}
	s.latestAppliedGeneration = maxUint64(s.latestAppliedGeneration, generation)
	s.refreshMinRequiredGeneration()
	s.dirty = false
	s.pendingEntries = make(map[string]*streamingpb.WindowEntry)
	s.pendingRecords = s.pendingRecords[:0]
	return nil
}

func (s *vchannelWindow) applyCommittedWriteRecord(record committedWriteRecord, markDirty bool) error {
	if record.SourcePChannel != "" && s.pchannel != "" && record.SourcePChannel != s.pchannel {
		return merr.WrapErrServiceInternalMsg("committed write record pchannel mismatch, state %s, record %s", s.pchannel, record.SourcePChannel)
	}
	if record.VChannel != "" && s.vchannel != "" && record.VChannel != s.vchannel {
		return merr.WrapErrServiceInternalMsg("committed write record vchannel mismatch, state %s, record %s", s.vchannel, record.VChannel)
	}
	if record.Idempotency == nil {
		if markDirty {
			s.pendingRecords = append(s.pendingRecords, cloneCommittedWriteRecord(record))
		}
		s.advanceCheckpointToCommittedWriteRecord(record, markDirty)
		return nil
	}
	key := record.Idempotency.Key
	if key == "" {
		return merr.WrapErrServiceInternalMsg("committed write record has malformed idempotency info")
	}
	if existing, ok := s.entries[key]; ok {
		if s.entryTTLExpiredAt(existing.entry, record.SourceTimeTick) {
			s.dropEntry(key)
		} else {
			if markDirty {
				s.pendingRecords = append(s.pendingRecords, cloneCommittedWriteRecord(record))
			}
			s.advanceCheckpointToCommittedWriteRecord(record, markDirty)
			return nil
		}
	}
	entry := record.WindowEntry()
	if entry == nil {
		return merr.WrapErrServiceInternalMsg("committed write record cannot materialize idempotency window entry")
	}
	s.entries[key] = &windowEntry{entry: entry}
	s.entryBytes += proto.Size(entry)
	s.commitOrder = append(s.commitOrder, key)
	if markDirty {
		s.pendingEntries[key] = entry
		s.pendingRecords = append(s.pendingRecords, cloneCommittedWriteRecord(record))
	}
	s.advanceCheckpointToCommittedWriteRecord(record, markDirty)
	s.refreshMinRequiredGeneration()
	return nil
}

func (s *vchannelWindow) entryTTLExpiredAt(entry *streamingpb.WindowEntry, nowTT uint64) bool {
	if entry == nil || s.evictionCfg.windowTTL <= 0 {
		return false
	}
	evictBeforeTT := evictBeforeTimetick(nowTT, s.evictionCfg.windowTTL)
	return evictBeforeTT > 0 && entry.GetCommitTimetick() < evictBeforeTT
}

func (s *vchannelWindow) dropEntry(key string) {
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

func (s *vchannelWindow) rebuildGenerationStat(generation uint64) {
	var stat windowGenerationStat
	for _, e := range s.entries {
		if e == nil || !e.generationSet || e.generation != generation {
			continue
		}
		stat.entryCount++
		stat.byteSize += proto.Size(e.entry)
		if e.entry.GetCommitTimetick() > stat.maxCommitTT {
			stat.maxCommitTT = e.entry.GetCommitTimetick()
		}
	}
	if stat.entryCount == 0 {
		delete(s.generationStats, generation)
		return
	}
	if s.generationStats == nil {
		s.generationStats = make(map[uint64]*windowGenerationStat)
	}
	s.generationStats[generation] = &stat
}

func (s *vchannelWindow) advanceCheckpointToCommittedWriteRecord(record committedWriteRecord, markDirty bool) {
	if record.SourceTimeTick <= s.snapshotCheckpointTimetick {
		return
	}
	s.snapshotCheckpointTimetick = record.SourceTimeTick
	s.snapshotCheckpointMessageID = committedWriteRecordCheckpointMessageID(record)
	s.refreshEvictedWatermark()
	if markDirty {
		s.dirty = true
	}
}

func (s *vchannelWindow) checkpoint() *WALCheckpoint {
	if s.snapshotCheckpointMessageID == nil {
		return nil
	}
	return &WALCheckpoint{
		MessageID: message.MustUnmarshalMessageID(s.snapshotCheckpointMessageID),
		TimeTick:  s.snapshotCheckpointTimetick,
	}
}

func (s *vchannelWindow) refreshEvictedWatermark() {
	for len(s.commitOrder) > 0 {
		did := s.commitOrder[0]
		e, ok := s.entries[did]
		if !ok {
			s.commitOrder = s.commitOrder[1:]
			continue
		}
		// The watermark is inclusive: it points to the oldest retained entry, not a strict evicted lower bound.
		s.evictedWatermarkTimetick = e.entry.GetCommitTimetick()
		return
	}
	s.evictedWatermarkTimetick = s.snapshotCheckpointTimetick
}

func (s *vchannelWindow) markCommittedWriteRecordsPersisted(records []committedWriteRecord, generation uint64) {
	for _, record := range records {
		s.markCommittedWriteRecordGeneration(record, generation)
	}
	s.latestAppliedGeneration = maxUint64(s.latestAppliedGeneration, generation)
	s.refreshMinRequiredGeneration()
}

func (s *vchannelWindow) markCommittedWriteRecordGeneration(record committedWriteRecord, generation uint64) {
	if record.Idempotency == nil {
		return
	}
	key := record.Idempotency.Key
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
func (s *vchannelWindow) registerGenerationEntry(generation uint64, entry *streamingpb.WindowEntry) {
	if entry == nil {
		return
	}
	if s.generationStats == nil {
		s.generationStats = make(map[uint64]*windowGenerationStat)
	}
	stat, ok := s.generationStats[generation]
	if !ok {
		stat = &windowGenerationStat{}
		s.generationStats[generation] = stat
	}
	stat.entryCount++
	stat.byteSize += proto.Size(entry)
	if commitTT := entry.GetCommitTimetick(); commitTT > stat.maxCommitTT {
		stat.maxCommitTT = commitTT
	}
}

func (s *vchannelWindow) windowMeta() *streamingpb.VChannelWindowMeta {
	s.refreshMinRequiredGeneration()
	return &streamingpb.VChannelWindowMeta{
		Pchannel:                    s.pchannel,
		Vchannel:                    s.vchannel,
		EvictedWatermarkTimetick:    s.evictedWatermarkTimetick,
		SnapshotCheckpointTimetick:  s.snapshotCheckpointTimetick,
		SnapshotCheckpointMessageId: cloneMessageIDProto(s.snapshotCheckpointMessageID),
		LatestAppliedGeneration:     s.latestAppliedGeneration,
		MinRequiredGeneration:       s.minRequiredGeneration,
		ViewType:                    common.VChannelWindowViewTypeIdempotency,
		EntryCount:                  uint64(len(s.entries)),
	}
}

func (s *vchannelWindow) windowMetaAtGeneration(generation uint64) *streamingpb.VChannelWindowMeta {
	meta := s.windowMeta()
	if generation > meta.GetLatestAppliedGeneration() {
		meta.LatestAppliedGeneration = generation
		// Project the boundary forward only when nothing pins retention — the
		// staging window being empty (EntryCount==0) is NOT sufficient: the
		// durable-retention ledger may still pin older generations even though
		// every persisted entry was evicted from staging memory.
		if !s.hasRetentionPin() {
			meta.MinRequiredGeneration = generation
		}
	}
	return meta
}

func (s *vchannelWindow) hasRetentionPin() bool {
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
//     policy — the staging window is cleared on persist, so chunk retention
//     must NOT depend on entries still being materialized here; the ledger is
//     what keeps a TTL's worth of chunks recoverable across restarts.
//
// An entry that has never been persisted lives in the WAL, not in any chunk,
// and is recovered by replaying the WAL from the window source checkpoint —
// it pins nothing. When nothing pins, nothing below the latest persisted
// generation is still needed. NOTE: during recovery, the value loaded from the
// persisted VChannelWindowMeta must not be recomputed before the chunk replay
// has rebuilt the ledger (the single-threaded recovery sequence guarantees
// this today).
func (s *vchannelWindow) refreshMinRequiredGeneration() {
	minimum := s.latestAppliedGeneration
	if m, ok := s.materializedMinGeneration(); ok && m < minimum {
		minimum = m
	}
	if m, ok := s.statsMinRequiredGeneration(); ok && m < minimum {
		minimum = m
	}
	s.minRequiredGeneration = minimum
}

func (s *vchannelWindow) materializedMinGeneration() (uint64, bool) {
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
// bounded to the retained window. A zero policy makes no durable retention
// promise and contributes no pin.
func (s *vchannelWindow) statsMinRequiredGeneration() (uint64, bool) {
	cfg := s.evictionCfg
	if len(s.generationStats) == 0 || (cfg.windowTTL <= 0 && cfg.maxBytes <= 0) {
		return 0, false
	}
	generations := make([]uint64, 0, len(s.generationStats))
	for generation := range s.generationStats {
		generations = append(generations, generation)
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i] > generations[j] })

	evictBefore := evictBeforeTimetick(s.snapshotCheckpointTimetick, cfg.windowTTL)
	cumulativeEntries := 0
	cumulativeBytes := 0
	var minRequired uint64
	retainedAny := false
	for _, generation := range generations {
		stat := s.generationStats[generation]
		if cfg.maxBytes > 0 && cumulativeBytes >= cfg.maxBytes {
			break
		}
		withinTTL := cfg.windowTTL <= 0 || stat.maxCommitTT >= evictBefore
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

func (s *vchannelWindow) evictForRecovery(evictBeforeTT uint64, minEntries, maxBytes int) {
	for len(s.entries) > minEntries && len(s.commitOrder) > 0 {
		key := s.commitOrder[0]
		e, ok := s.entries[key]
		if !ok {
			s.commitOrder = s.commitOrder[1:]
			continue
		}
		if e.entry.GetCommitTimetick() >= evictBeforeTT {
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
// window's byte accounting.
func (s *vchannelWindow) dropEntryBytes(key string) {
	if e, ok := s.entries[key]; ok && e.entry != nil {
		s.entryBytes -= proto.Size(e.entry)
	}
}

// evictPersisted drops every entry that has already been durably persisted (its
// generation is assigned), stopping at the first entry that is not persisted
// yet. In normal mode the recovery-side window is only a persist-staging buffer
// -- the interceptor serves live dedup from its own window -- so a persisted
// entry is no longer needed here and is dropped regardless of TTL. Un-persisted
// entries are never dropped, so no observed write can be lost before it lands in
// a chunk.
func (s *vchannelWindow) evictPersisted() {
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
// timetick. Kept in sync with the live window's evictBeforeCommitTT (idempotency
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

func (update *idempotencyWindowMetaUpdate) WithPersistedGeneration(generation uint64) *streamingpb.VChannelWindowMeta {
	if update == nil || update.meta == nil {
		return nil
	}
	meta := proto.Clone(update.meta).(*streamingpb.VChannelWindowMeta)
	meta.LatestAppliedGeneration = maxUint64(meta.GetLatestAppliedGeneration(), generation)
	if meta.GetEntryCount() == 0 {
		// Advance the boundary only when nothing pinned retention at capture
		// time — mirroring windowMetaAtGeneration. An empty staging window is
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

func sortWindowEntries(entries []*streamingpb.WindowEntry) {
	sort.Slice(entries, func(i, j int) bool {
		left, right := entries[i], entries[j]
		if left.GetCommitTimetick() != right.GetCommitTimetick() {
			return left.GetCommitTimetick() < right.GetCommitTimetick()
		}
		return left.GetKey() < right.GetKey()
	})
}
