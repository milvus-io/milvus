package recovery

import (
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
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
}

func newEmptyVChannelWindow(pchannel, vchannel string, checkpoint *WALCheckpoint) *vchannelWindow {
	state := &vchannelWindow{
		pchannel:       pchannel,
		vchannel:       vchannel,
		entries:        make(map[string]*windowEntry),
		pendingEntries: make(map[string]*streamingpb.WindowEntry),
		pendingRecords: make([]committedWriteRecord, 0),
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
		return nil, errors.New("nil idempotency window snapshot")
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
	}
	sortedEntries := append([]*streamingpb.WindowEntry(nil), snapshot.GetEntries()...)
	sortWindowEntries(sortedEntries)
	for _, entry := range sortedEntries {
		state.entries[entry.GetKey()] = &windowEntry{entry: entry}
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
		return errors.Errorf("committed write record pchannel mismatch, state %s, record %s", s.pchannel, record.SourcePChannel)
	}
	if record.VChannel != "" && s.vchannel != "" && record.VChannel != s.vchannel {
		return errors.Errorf("committed write record vchannel mismatch, state %s, record %s", s.vchannel, record.VChannel)
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
		return errors.New("committed write record has malformed idempotency info")
	}
	if _, ok := s.entries[key]; ok {
		if markDirty {
			s.pendingRecords = append(s.pendingRecords, cloneCommittedWriteRecord(record))
		}
		s.advanceCheckpointToCommittedWriteRecord(record, markDirty)
		return nil
	}
	entry := record.WindowEntry()
	if entry == nil {
		return errors.New("committed write record cannot materialize idempotency window entry")
	}
	s.entries[key] = &windowEntry{entry: entry}
	s.commitOrder = append(s.commitOrder, key)
	if markDirty {
		s.pendingEntries[key] = entry
		s.pendingRecords = append(s.pendingRecords, cloneCommittedWriteRecord(record))
	}
	s.advanceCheckpointToCommittedWriteRecord(record, markDirty)
	s.refreshMinRequiredGeneration()
	return nil
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
		if meta.GetEntryCount() == 0 {
			meta.MinRequiredGeneration = generation
		}
	}
	return meta
}

func (s *vchannelWindow) refreshMinRequiredGeneration() {
	// Only persisted entries (with an assigned generation) pin chunk retention.
	// An entry that has not been persisted yet lives in the WAL, not in any
	// chunk, and is recovered by replaying the WAL from the window source
	// checkpoint -- counting it as generation 0 would pin the chunk-retention
	// boundary at 0 forever on a busy channel. When no persisted entry remains,
	// nothing below the latest persisted generation is still needed.
	minimum := s.latestAppliedGeneration
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
	s.minRequiredGeneration = minimum
}

func (s *vchannelWindow) evictForRecovery(evictBeforeTT uint64, minEntries, maxEntries int) {
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
		delete(s.entries, key)
	}
	for maxEntries > 0 && len(s.entries) > maxEntries && len(s.commitOrder) > 0 {
		key := s.commitOrder[0]
		s.commitOrder = s.commitOrder[1:]
		delete(s.entries, key)
	}
	s.refreshEvictedWatermark()
	s.refreshMinRequiredGeneration()
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
		delete(s.entries, key)
	}
	s.refreshEvictedWatermark()
	s.refreshMinRequiredGeneration()
}

func evictBeforeTimetick(nowTT uint64, ttl time.Duration) uint64 {
	if ttl <= 0 {
		return 0
	}
	return tsoutil.AddPhysicalDurationOnTs(nowTT, -ttl)
}

func (update *idempotencyWindowMetaUpdate) WithPersistedGeneration(generation uint64) *streamingpb.VChannelWindowMeta {
	if update == nil || update.meta == nil {
		return nil
	}
	meta := proto.Clone(update.meta).(*streamingpb.VChannelWindowMeta)
	meta.LatestAppliedGeneration = maxUint64(meta.GetLatestAppliedGeneration(), generation)
	if meta.GetEntryCount() == 0 {
		meta.MinRequiredGeneration = meta.GetLatestAppliedGeneration()
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
