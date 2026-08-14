package recovery

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// committedWriteRecord is the internal representation of a committed write
// fact derived from an already-landed pchannel WAL message. It is the in-memory
// model; its durable form is streamingpb.CommittedWriteRecord, converted in
// summary_store_codec.go.
type committedWriteRecord struct {
	SourcePChannel  string
	SourceMessageID *commonpb.MessageID
	SourceTimeTick  uint64
	VChannel        string
	// IdempotentResult holds the rows this write produced, in the one shape they
	// are ever used in: the result a duplicate append hands back. It is both the
	// persisted form and the served form, so nothing is projected back and forth.
	IdempotentResult       *messagespb.IdempotentInsertResult
	Idempotency            *committedWriteIdempotency
	LastConfirmedMessageID *commonpb.MessageID
}

type committedWriteIdempotency struct {
	Key string
}

func (record committedWriteRecord) intoProto() *streamingpb.CommittedWriteRecord {
	pb := &streamingpb.CommittedWriteRecord{
		SourcePchannel:         record.SourcePChannel,
		SourceMessageId:        cloneMessageIDProto(record.SourceMessageID),
		SourceTimetick:         record.SourceTimeTick,
		Vchannel:               record.VChannel,
		LastConfirmedMessageId: cloneMessageIDProto(record.LastConfirmedMessageID),
		IdempotentResult:       record.IdempotentResult,
	}
	if record.Idempotency != nil {
		pb.IdempotencyKey = record.Idempotency.Key
	}
	return pb
}

func newCommittedWriteRecordFromProto(pb *streamingpb.CommittedWriteRecord) committedWriteRecord {
	record := committedWriteRecord{
		SourcePChannel:         pb.GetSourcePchannel(),
		SourceMessageID:        cloneMessageIDProto(pb.GetSourceMessageId()),
		SourceTimeTick:         pb.GetSourceTimetick(),
		VChannel:               pb.GetVchannel(),
		LastConfirmedMessageID: cloneMessageIDProto(pb.GetLastConfirmedMessageId()),
		IdempotentResult:       pb.GetIdempotentResult(),
	}
	// An absent key and an empty key are the same thing here: a record only ever
	// carries an idempotency block when the write had a non-empty key.
	if key := pb.GetIdempotencyKey(); key != "" {
		record.Idempotency = &committedWriteIdempotency{Key: key}
	}
	return record
}

// newCommittedWriteRecordFromMessage extracts a committed write fact from an
// immutable WAL message. Callers should only pass messages observed after WAL
// append/scan has completed; inflight requests must never reach this function.
func newCommittedWriteRecordFromMessage(pchannel string, msg message.ImmutableMessage) (*committedWriteRecord, bool) {
	if txnMsg := message.AsImmutableTxnMessage(msg); txnMsg != nil {
		return newCommittedWriteRecordFromTxnMessage(pchannel, txnMsg)
	}
	if msg == nil || !msg.MessageType().IsDMLMessageType() || msg.IsPChannelLevel() {
		return nil, false
	}
	record := &committedWriteRecord{
		SourcePChannel:         pchannel,
		SourceMessageID:        safeMessageIDProto(msg.MessageID()),
		SourceTimeTick:         msg.TimeTick(),
		VChannel:               msg.VChannel(),
		LastConfirmedMessageID: safeMessageIDProto(msg.LastConfirmedMessageID()),
	}
	if record.SourcePChannel == "" {
		record.SourcePChannel = msg.PChannel()
	}

	var decodedResult *messagespb.IdempotentInsertResult
	if result, ok := idempotentInsertResultFromImmutableInsert(msg); ok {
		decodedResult = result
		record.IdempotentResult = result
	}

	if key := idempotencyKeyFromImmutableMessage(msg); key != "" {
		record.Idempotency = &committedWriteIdempotency{
			Key: key,
		}
	} else if decodedResult == nil {
		record.IdempotentResult = nil
	}
	return record, true
}

func newCommittedWriteRecordFromTxnMessage(pchannel string, msg message.ImmutableTxnMessage) (*committedWriteRecord, bool) {
	if msg == nil || msg.IsPChannelLevel() {
		return nil, false
	}
	record := &committedWriteRecord{
		SourcePChannel:         pchannel,
		SourceMessageID:        safeMessageIDProto(msg.MessageID()),
		SourceTimeTick:         msg.TimeTick(),
		VChannel:               msg.VChannel(),
		LastConfirmedMessageID: safeMessageIDProto(msg.LastConfirmedMessageID()),
	}
	if record.SourcePChannel == "" {
		record.SourcePChannel = msg.PChannel()
	}

	insertResults := make([]*messagespb.IdempotentInsertResult, 0, msg.Size())
	hasDML := false
	_ = msg.RangeOver(func(body message.ImmutableMessage) error {
		if body == nil || body.IsPChannelLevel() || !body.MessageType().IsDMLMessageType() {
			return nil
		}
		hasDML = true
		if result, ok := idempotentInsertResultFromImmutableInsert(body); ok {
			insertResults = append(insertResults, result)
		}
		return nil
	})

	if key := idempotencyKeyFromImmutableMessage(msg.Commit()); key != "" {
		record.Idempotency = &committedWriteIdempotency{
			Key: key,
		}
	}
	mergedResult, hadAny, err := message.MergeIdempotentInsertResults(insertResults...)
	if err != nil {
		// Corrupt committed-write payload (e.g. mixed id types): surface it loudly
		// instead of silently degrading to "no idempotent payload", then keep the
		// record without a duplicate response.
		mlog.Warn(context.TODO(), "failed to merge idempotent insert results for committed write record",
			mlog.String("pchannel", record.SourcePChannel),
			mlog.String("vchannel", record.VChannel),
			mlog.Err(err))
	} else if hadAny {
		record.IdempotentResult = mergedResult
	}
	if !hadAny && record.Idempotency == nil && !hasDML {
		return nil, false
	}
	return record, true
}

func idempotencyKeyFromImmutableMessage(msg message.ImmutableMessage) string {
	if msg == nil {
		return ""
	}
	// A replicated message preserves the SOURCE cluster's message properties,
	// including its idempotency key. That key must never materialize a summary
	// entry here: the local summary's key history is independent of the source's,
	// and a poisoned entry would drive replicated appends down the duplicate
	// path after a restart. Replicated writes are treated as keyless committed
	// writes (checkpoint bookkeeping only), matching the interceptor-side bypass.
	if msg.ReplicateHeader() != nil {
		return ""
	}
	// Gated to the message types the summary deduplicates, mirroring
	// getIdempotencyKey on the interceptor side: the key property alone must not
	// materialize an entry for a type the append path never dedups.
	switch msg.MessageType() {
	case message.MessageTypeInsert, message.MessageTypeCommitTxn:
		return message.IdempotencyKeyOf(msg)
	default:
		return ""
	}
}

func idempotentInsertResultFromImmutableInsert(msg message.ImmutableMessage) (*messagespb.IdempotentInsertResult, bool) {
	if msg.MessageType() != message.MessageTypeInsert {
		return nil, false
	}
	// A replicated insert inherits the SOURCE cluster's header verbatim,
	// including its IdempotentInsertResult. Its committed-write record is keyless
	// checkpoint bookkeeping only (see idempotencyKeyFromImmutableMessage), so a
	// decoded result could never be served as a duplicate response — persisting
	// its per-row PKs into the chunk would be pure write amplification.
	if msg.ReplicateHeader() != nil {
		return nil, false
	}
	insertMsg, err := message.AsImmutableInsertMessageV1(msg)
	if err != nil {
		return nil, false
	}
	return message.IdempotentInsertResultFromInsertHeader(insertMsg.Header())
}

func committedWriteRecordFromSummaryEntry(pchannel, vchannel string, entry *streamingpb.SummaryEntry) *committedWriteRecord {
	if entry == nil {
		return nil
	}
	return &committedWriteRecord{
		SourcePChannel:         pchannel,
		SourceMessageID:        cloneMessageIDProto(entry.GetMessageId()),
		SourceTimeTick:         entry.GetCommitTimetick(),
		VChannel:               vchannel,
		LastConfirmedMessageID: cloneMessageIDProto(entry.GetLastConfirmedMessageId()),
		IdempotentResult:       entry.GetIdempotentResult(),
		Idempotency: &committedWriteIdempotency{
			Key: entry.GetKey(),
		},
	}
}

func (record *committedWriteRecord) SummaryEntry() *streamingpb.SummaryEntry {
	if record == nil || record.Idempotency == nil {
		return nil
	}
	entry := &streamingpb.SummaryEntry{
		Key:                    record.Idempotency.Key,
		CommitTimetick:         record.SourceTimeTick,
		MessageId:              cloneMessageIDProto(record.SourceMessageID),
		LastConfirmedMessageId: cloneMessageIDProto(record.LastConfirmedMessageID),
	}
	entry.IdempotentResult = record.IdempotentResult
	return entry
}

func cloneCommittedWriteRecord(record committedWriteRecord) committedWriteRecord {
	record.SourceMessageID = cloneMessageIDProto(record.SourceMessageID)
	record.LastConfirmedMessageID = cloneMessageIDProto(record.LastConfirmedMessageID)
	if record.Idempotency != nil {
		idempotency := *record.Idempotency
		record.Idempotency = &idempotency
	}
	return record
}
