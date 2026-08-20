package recovery

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// SummaryRecord is one committed write fact: where and when it landed in the
// WAL, plus what the idempotency view remembers about it.
//
// It is a plain Go struct, not a generated message, because it is not the stored
// shape. A chunk stores a write split across sections -- identity and primary
// keys in one, the client key and row offsets in another -- so that a consumer
// can read only the part it needs. In memory there is no such consumer: whoever
// holds a record holds all of it. Keeping one struct here and splitting only at
// the codec means the split exists exactly where it pays for itself.
type SummaryRecord struct {
	SourceMessageID *commonpb.MessageID
	SourceTimeTick  uint64

	// IdempotencyKey is empty for a write no view remembers. Such a record is
	// never staged for a chunk: it materializes nothing for any consumer, and the
	// WAL consume checkpoint -- not a chunk -- is what records how far the vchannel
	// has advanced.
	IdempotencyKey string

	// InsertResult is what a duplicate append replays back to the client. Its two
	// halves are stored in different sections: RowOffsets with the key, Ids with
	// the write. They are rejoined on read.
	InsertResult *messagespb.IdempotentInsertResult
}

// Size estimates the record's memory footprint for the byte budgets that bound
// the staging buffer and the dedup window. The primary keys dominate it by far,
// so the estimate is theirs plus a small fixed remainder.
func (r *SummaryRecord) Size() int {
	if r == nil {
		return 0
	}
	size := len(r.IdempotencyKey) + 8
	if r.SourceMessageID != nil {
		size += proto.Size(r.SourceMessageID)
	}
	if r.InsertResult != nil {
		size += proto.Size(r.InsertResult)
	}
	return size
}

// newSummaryRecordFromMessage extracts a committed write fact from an
// immutable WAL message. Callers should only pass messages observed after WAL
// append/scan has completed; inflight requests must never reach this function.
func newSummaryRecordFromMessage(pchannel string, msg message.ImmutableMessage) (*SummaryRecord, bool) {
	if txnMsg := message.AsImmutableTxnMessage(msg); txnMsg != nil {
		return newSummaryRecordFromTxnMessage(pchannel, txnMsg)
	}
	if msg == nil || !msg.MessageType().IsDMLMessageType() || msg.IsPChannelLevel() {
		return nil, false
	}
	record := &SummaryRecord{
		SourceMessageID: safeMessageIDProto(msg.MessageID()),
		SourceTimeTick:  msg.TimeTick(),
	}

	// The result is only worth storing alongside a key: without one it can never
	// be served (a keyless record materializes nothing for any view), and the
	// append path rejects an insert that carries a result but no key.
	if key := idempotencyKeyFromImmutableMessage(msg); key != "" {
		record.IdempotencyKey = key
		if result, ok := idempotentInsertResultFromImmutableInsert(msg); ok {
			record.InsertResult = result
		}
	}
	return record, true
}

func newSummaryRecordFromTxnMessage(pchannel string, msg message.ImmutableTxnMessage) (*SummaryRecord, bool) {
	if msg == nil || msg.IsPChannelLevel() {
		return nil, false
	}
	record := &SummaryRecord{
		SourceMessageID: safeMessageIDProto(msg.MessageID()),
		SourceTimeTick:  msg.TimeTick(),
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

	key := idempotencyKeyFromImmutableMessage(msg.Commit())
	mergedResult, hadAny, err := message.MergeIdempotentInsertResults(insertResults...)
	if err != nil {
		// Corrupt committed-write payload (e.g. mixed id types): surface it loudly
		// instead of silently degrading to "no idempotent payload", then keep the
		// record without a duplicate response.
		mlog.Warn(context.TODO(), "failed to merge idempotent insert results for summary record",
			mlog.String("pchannel", pchannel),
			mlog.String("vchannel", msg.VChannel()),
			mlog.Err(err))
		hadAny = false
	}
	if key != "" {
		record.IdempotencyKey = key
		if hadAny {
			record.InsertResult = mergedResult
		}
	}
	if !hadAny && key == "" && !hasDML {
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
	// record here: the local summary's key history is independent of the source's,
	// and a poisoned record would drive replicated appends down the duplicate
	// path after a restart. Replicated writes are treated as keyless committed
	// writes, matching the interceptor-side bypass.
	if msg.ReplicateHeader() != nil {
		return ""
	}
	// Gated to the message types the summary deduplicates, mirroring
	// getIdempotencyKey on the interceptor side: the key property alone must not
	// materialize a record for a type the append path never dedups.
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
	// including its IdempotentInsertResult. Its summary record is keyless
	// bookkeeping only (see idempotencyKeyFromImmutableMessage), so a decoded
	// result could never be served as a duplicate response — persisting its
	// per-row PKs into the chunk would be pure write amplification.
	if msg.ReplicateHeader() != nil {
		return nil, false
	}
	insertMsg, err := message.AsImmutableInsertMessageV1(msg)
	if err != nil {
		return nil, false
	}
	return message.IdempotentInsertResultFromInsertHeader(insertMsg.Header())
}
