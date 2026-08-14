package recovery

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// A committed write fact derived from an already-landed pchannel WAL message is
// streamingpb.CommittedWriteRecord itself — there is no separate in-memory
// model. The generated type is the one representation: it is what the chunk
// stores, what recovery decodes, and what callers receive, so nothing is copied
// between shapes and the two cannot drift apart.

// newCommittedWriteRecordFromMessage extracts a committed write fact from an
// immutable WAL message. Callers should only pass messages observed after WAL
// append/scan has completed; inflight requests must never reach this function.
func newCommittedWriteRecordFromMessage(pchannel string, msg message.ImmutableMessage) (*streamingpb.CommittedWriteRecord, bool) {
	if txnMsg := message.AsImmutableTxnMessage(msg); txnMsg != nil {
		return newCommittedWriteRecordFromTxnMessage(pchannel, txnMsg)
	}
	if msg == nil || !msg.MessageType().IsDMLMessageType() || msg.IsPChannelLevel() {
		return nil, false
	}
	record := &streamingpb.CommittedWriteRecord{
		SourcePchannel:         pchannel,
		SourceMessageId:        safeMessageIDProto(msg.MessageID()),
		SourceTimetick:         msg.TimeTick(),
		Vchannel:               msg.VChannel(),
		LastConfirmedMessageId: safeMessageIDProto(msg.LastConfirmedMessageID()),
	}
	if record.GetSourcePchannel() == "" {
		record.SourcePchannel = msg.PChannel()
	}

	var decodedResult *messagespb.IdempotentInsertResult
	if result, ok := idempotentInsertResultFromImmutableInsert(msg); ok {
		decodedResult = result
		record.IdempotentResult = result
	}

	if key := idempotencyKeyFromImmutableMessage(msg); key != "" {
		record.IdempotencyKey = key
	} else if decodedResult == nil {
		record.IdempotentResult = nil
	}
	return record, true
}

func newCommittedWriteRecordFromTxnMessage(pchannel string, msg message.ImmutableTxnMessage) (*streamingpb.CommittedWriteRecord, bool) {
	if msg == nil || msg.IsPChannelLevel() {
		return nil, false
	}
	record := &streamingpb.CommittedWriteRecord{
		SourcePchannel:         pchannel,
		SourceMessageId:        safeMessageIDProto(msg.MessageID()),
		SourceTimetick:         msg.TimeTick(),
		Vchannel:               msg.VChannel(),
		LastConfirmedMessageId: safeMessageIDProto(msg.LastConfirmedMessageID()),
	}
	if record.GetSourcePchannel() == "" {
		record.SourcePchannel = msg.PChannel()
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
		record.IdempotencyKey = key
	}
	mergedResult, hadAny, err := message.MergeIdempotentInsertResults(insertResults...)
	if err != nil {
		// Corrupt committed-write payload (e.g. mixed id types): surface it loudly
		// instead of silently degrading to "no idempotent payload", then keep the
		// record without a duplicate response.
		mlog.Warn(context.TODO(), "failed to merge idempotent insert results for committed write record",
			mlog.String("pchannel", record.GetSourcePchannel()),
			mlog.String("vchannel", record.GetVchannel()),
			mlog.Err(err))
	} else if hadAny {
		record.IdempotentResult = mergedResult
	}
	if !hadAny && record.GetIdempotencyKey() == "" && !hasDML {
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

func committedWriteRecordFromSummaryEntry(pchannel, vchannel string, entry *streamingpb.SummaryEntry) *streamingpb.CommittedWriteRecord {
	if entry == nil {
		return nil
	}
	return &streamingpb.CommittedWriteRecord{
		SourcePchannel:         pchannel,
		SourceMessageId:        cloneMessageIDProto(entry.GetMessageId()),
		SourceTimetick:         entry.GetCommitTimetick(),
		Vchannel:               vchannel,
		LastConfirmedMessageId: cloneMessageIDProto(entry.GetLastConfirmedMessageId()),
		IdempotentResult:       entry.GetIdempotentResult(),
		IdempotencyKey:         entry.GetKey(),
	}
}

// summaryEntryOfCommittedWriteRecord projects a record into the summary entry an
// application view materializes from it. A keyless record has no entry: it is
// checkpoint bookkeeping only.
func summaryEntryOfCommittedWriteRecord(record *streamingpb.CommittedWriteRecord) *streamingpb.SummaryEntry {
	if record == nil || record.GetIdempotencyKey() == "" {
		return nil
	}
	return &streamingpb.SummaryEntry{
		Key:                    record.GetIdempotencyKey(),
		CommitTimetick:         record.GetSourceTimetick(),
		MessageId:              cloneMessageIDProto(record.GetSourceMessageId()),
		LastConfirmedMessageId: cloneMessageIDProto(record.GetLastConfirmedMessageId()),
		IdempotentResult:       record.GetIdempotentResult(),
	}
}
