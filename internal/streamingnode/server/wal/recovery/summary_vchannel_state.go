package recovery

import (
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// vchannelSummary is a staging buffer, not a retention policy. It accumulates
// the committed write facts observed on one vchannel so they can be turned into
// chunks, and it materializes the record set handed to a consumer at WAL open.
//
// It deliberately tracks nothing about what a consumer will keep. Retention on
// the consumer side is the window's own byte cap; retention on the durable side
// is computed from the manifest. Neither is recorded here, and no per-vchannel
// state is persisted anywhere.
type vchannelSummary struct {
	pchannel string
	vchannel string

	// appliedTimetick is the highest timetick already folded in, and exists only
	// to make replay idempotent: recovery seeds it from the WAL consume checkpoint
	// so that chunk contents and replayed WAL messages cannot be applied twice.
	appliedTimetick uint64

	// records deduplicates by idempotency key: the same key must not be written
	// into two chunks, and a replayed key must materialize once.
	records     map[string]*SummaryRecord
	commitOrder []string
	recordBytes int

	// pendingRecords is what the next chunk will carry. It is the ONLY record of
	// dirtiness: a separate flag could disagree with it, and the way it used to
	// disagree was to report "clean" for records a failed persist had drained but
	// never made durable.
	pendingRecords []*SummaryRecord
}

func newEmptyVChannelSummary(pchannel, vchannel string, checkpoint *WALCheckpoint) *vchannelSummary {
	state := &vchannelSummary{
		pchannel:       pchannel,
		vchannel:       vchannel,
		records:        make(map[string]*SummaryRecord),
		pendingRecords: make([]*SummaryRecord, 0),
	}
	if checkpoint != nil {
		state.appliedTimetick = checkpoint.TimeTick
	}
	return state
}

func (s *vchannelSummary) observeMessage(msg message.ImmutableMessage) {
	if msg == nil {
		return
	}
	if msg.MessageType() == message.MessageTypeTimeTick {
		return
	}
	if msg.TimeTick() <= s.appliedTimetick {
		return
	}
	record, ok := newSummaryRecordFromMessage(s.pchannel, msg)
	if !ok {
		s.advanceApplied(msg.TimeTick())
		return
	}
	s.applySummaryRecord(record, true)
}

func (s *vchannelSummary) advanceApplied(timetick uint64) {
	if timetick > s.appliedTimetick {
		s.appliedTimetick = timetick
	}
}

func (s *vchannelSummary) advanceAppliedTo(checkpoint *WALCheckpoint) {
	if checkpoint != nil {
		s.advanceApplied(checkpoint.TimeTick)
	}
}

// peekPendingSummaryRecords returns what the next chunk should carry WITHOUT
// removing it.
//
// Nothing leaves the staging buffer until it is durable. Draining first and
// putting the records back on failure looks equivalent and is not: the persist
// can fail in ways that never reach a restore -- a cancelled context on close,
// a terminal fenced/corrupted error -- and the records would be gone while the
// consume checkpoint advanced past them, losing those keys with no error
// anywhere.
func (s *vchannelSummary) peekPendingSummaryRecords() []*SummaryRecord {
	if len(s.pendingRecords) == 0 {
		return nil
	}
	return sortedSummaryRecords(s.pendingRecords)
}

// dropPersistedSummaryRecords removes the first count records, which are the
// ones a completed chunk carried. Anything observed while that chunk was being
// written sits after them and stays staged.
func (s *vchannelSummary) dropPersistedSummaryRecords(count int) {
	if count <= 0 {
		return
	}
	if count >= len(s.pendingRecords) {
		s.pendingRecords = s.pendingRecords[:0]
		return
	}
	s.pendingRecords = append(s.pendingRecords[:0], s.pendingRecords[count:]...)
}

func (s *vchannelSummary) hasPendingSummaryRecords() bool {
	return len(s.pendingRecords) > 0
}

// VChannelSummarySnapshot is what recovery hands a consumer once it has replayed
// the chunks: one vchannel's records.
//
// It never leaves the process: built once at WAL open, consumed once, never
// stored or sent. That is why it is a plain Go struct rather than a message in
// streaming.proto — a proto here would advertise a wire contract that does not
// exist.
type VChannelSummarySnapshot struct {
	PChannel string
	VChannel string
	Records  []*SummaryRecord
}

func (s *vchannelSummary) snapshot() *VChannelSummarySnapshot {
	records := make([]*SummaryRecord, 0, len(s.records))
	for _, r := range s.records {
		records = append(records, r)
	}
	sortSummaryRecordsInPlace(records)
	return &VChannelSummarySnapshot{
		PChannel: s.pchannel,
		VChannel: s.vchannel,
		Records:  records,
	}
}

// applySummaryRecordsAtGeneration replays one chunk's records during recovery.
func (s *vchannelSummary) applySummaryRecordsAtGeneration(records []*SummaryRecord) {
	for _, record := range sortedSummaryRecords(records) {
		s.applySummaryRecord(record, false)
	}
	s.pendingRecords = s.pendingRecords[:0]
}

func (s *vchannelSummary) applySummaryRecord(record *SummaryRecord, stage bool) {
	if record == nil {
		return
	}
	if record.IdempotencyKey == "" {
		// A keyless write materializes nothing for any consumer, so it is never
		// staged for a chunk. It only moves the applied position, which exists to
		// keep replay idempotent.
		s.advanceApplied(record.SourceTimeTick)
		return
	}
	if stage {
		s.pendingRecords = append(s.pendingRecords, record)
	}
	if _, ok := s.records[record.IdempotencyKey]; ok {
		// Already materialized. The chunk still records the repeat, but the record
		// set keeps the first sighting.
		s.advanceApplied(record.SourceTimeTick)
		return
	}
	s.records[record.IdempotencyKey] = record
	s.recordBytes += record.Size()
	s.commitOrder = append(s.commitOrder, record.IdempotencyKey)
	s.advanceApplied(record.SourceTimeTick)
}

func (s *vchannelSummary) dropRecord(key string) {
	record, ok := s.records[key]
	if !ok {
		return
	}
	delete(s.records, key)
	s.recordBytes -= record.Size()
	for idx, ordered := range s.commitOrder {
		if ordered == key {
			s.commitOrder = append(s.commitOrder[:idx], s.commitOrder[idx+1:]...)
			break
		}
	}
}

// capRecoveryBytes bounds the memory a replay may materialize before the set is
// handed to the consumer. It is a guard against a large retained chunk set, not
// a retention policy: the consumer applies its own byte cap the moment it takes
// ownership, and it may keep less than this.
func (s *vchannelSummary) capRecoveryBytes(maxBytes int) {
	for maxBytes > 0 && s.recordBytes > maxBytes && len(s.commitOrder) > 0 {
		key := s.commitOrder[0]
		s.commitOrder = s.commitOrder[1:]
		s.dropRecord(key)
	}
}

// evictPersisted drops every record that is already carried by a written chunk.
// In normal running the recovery-side summary is only a staging buffer -- the
// interceptor serves live dedup from its own window -- so a persisted record is
// no longer needed here.
func (s *vchannelSummary) evictPersisted() {
	for _, key := range s.commitOrder {
		if record, ok := s.records[key]; ok {
			s.recordBytes -= record.Size()
			delete(s.records, key)
		}
	}
	s.commitOrder = s.commitOrder[:0]
}

// sortedSummaryRecords returns the records in chunk order: by source timetick,
// then source message id, then idempotency key. It copies the slice but never
// the records — they are shared by pointer and nothing here mutates them.
func sortedSummaryRecords(records []*SummaryRecord) []*SummaryRecord {
	if len(records) == 0 {
		return nil
	}
	sorted := make([]*SummaryRecord, 0, len(records))
	for _, record := range records {
		if record == nil {
			continue
		}
		sorted = append(sorted, record)
	}
	sortSummaryRecordsInPlace(sorted)
	return sorted
}

func sortSummaryRecordsInPlace(records []*SummaryRecord) {
	sort.Slice(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if left.SourceTimeTick != right.SourceTimeTick {
			return left.SourceTimeTick < right.SourceTimeTick
		}
		if left.SourceMessageID.GetId() != right.SourceMessageID.GetId() {
			return left.SourceMessageID.GetId() < right.SourceMessageID.GetId()
		}
		return left.IdempotencyKey < right.IdempotencyKey
	})
}
