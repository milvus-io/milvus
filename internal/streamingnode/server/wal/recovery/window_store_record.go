package recovery

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const (
	committedWritePrimaryKeyTypeInt64  = "int64"
	committedWritePrimaryKeyTypeString = "string"
)

// committedWriteRecord is the internal representation of a committed write
// fact derived from an already-landed pchannel WAL message.
type committedWriteRecord struct {
	SourcePChannel         string                         `json:"source_pchannel"`
	SourceMessageID        *commonpb.MessageID            `json:"source_message_id,omitempty"`
	SourceTimeTick         uint64                         `json:"source_timetick"`
	VChannel               string                         `json:"vchannel"`
	Rows                   []committedWriteRow            `json:"rows,omitempty"`
	Idempotency            *committedWriteIdempotency     `json:"idempotency,omitempty"`
	DuplicateResponse      *committedWriteDuplicateResult `json:"-"`
	LastConfirmedMessageID *commonpb.MessageID            `json:"last_confirmed_message_id,omitempty"`
}

type committedWriteRow struct {
	RowOffset             uint32 `json:"row_offset"`
	PrimaryKeyType        string `json:"primary_key_type,omitempty"`
	Int64PrimaryKeyValue  int64  `json:"int64_primary_key_value,omitempty"`
	StringPrimaryKeyValue string `json:"string_primary_key_value,omitempty"`
	RowID                 int64  `json:"row_id,omitempty"`
}

type committedWriteIdempotency struct {
	Key string `json:"key"`
}

type committedWriteDuplicateResult = messagespb.IdempotentInsertResult

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
		record.Rows = committedWriteRowsFromInsertResult(result)
		record.DuplicateResponse = result
	}

	if key := idempotencyKeyFromImmutableMessage(msg); key != "" {
		record.Idempotency = &committedWriteIdempotency{
			Key: key,
		}
	} else if decodedResult == nil {
		record.DuplicateResponse = nil
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
		record.Rows = committedWriteRowsFromInsertResult(mergedResult)
		record.DuplicateResponse = mergedResult
	}
	if !hadAny && record.Idempotency == nil && !hasDML {
		return nil, false
	}
	return record, true
}

func idempotencyKeyFromImmutableMessage(msg message.ImmutableMessage) string {
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		insertMsg, err := message.AsImmutableInsertMessageV1(msg)
		if err != nil {
			return ""
		}
		return insertMsg.Header().GetIdempotencyKey()
	case message.MessageTypeCommitTxn:
		commitMsg, err := message.AsImmutableCommitTxnMessageV2(msg)
		if err != nil {
			return ""
		}
		return commitMsg.Header().GetIdempotencyKey()
	default:
		return ""
	}
}

func idempotentInsertResultFromImmutableInsert(msg message.ImmutableMessage) (*messagespb.IdempotentInsertResult, bool) {
	if msg.MessageType() != message.MessageTypeInsert {
		return nil, false
	}
	insertMsg, err := message.AsImmutableInsertMessageV1(msg)
	if err != nil {
		return nil, false
	}
	return message.IdempotentInsertResultFromInsertHeader(insertMsg.Header())
}

func committedWriteRecordFromWindowEntry(pchannel, vchannel string, entry *streamingpb.WindowEntry) *committedWriteRecord {
	if entry == nil {
		return nil
	}
	return &committedWriteRecord{
		SourcePChannel:         pchannel,
		SourceMessageID:        cloneMessageIDProto(entry.GetMessageId()),
		SourceTimeTick:         entry.GetCommitTimetick(),
		VChannel:               vchannel,
		LastConfirmedMessageID: cloneMessageIDProto(entry.GetLastConfirmedMessageId()),
		Rows:                   committedWriteRowsFromInsertResult(entry.GetIdempotentResult()),
		Idempotency: &committedWriteIdempotency{
			Key: entry.GetKey(),
		},
		DuplicateResponse: entry.GetIdempotentResult(),
	}
}

func (record *committedWriteRecord) WindowEntry() *streamingpb.WindowEntry {
	if record == nil || record.Idempotency == nil {
		return nil
	}
	entry := &streamingpb.WindowEntry{
		Key:                    record.Idempotency.Key,
		CommitTimetick:         record.SourceTimeTick,
		MessageId:              cloneMessageIDProto(record.SourceMessageID),
		LastConfirmedMessageId: cloneMessageIDProto(record.LastConfirmedMessageID),
	}
	if record.DuplicateResponse != nil {
		entry.IdempotentResult = record.DuplicateResponse
	}
	if entry.IdempotentResult == nil && len(record.Rows) > 0 {
		entry.IdempotentResult = message.NewIdempotentInsertResult(
			committedWriteRowOffsets(record.Rows),
			committedWriteRowIDs(record.Rows),
		)
	}
	return entry
}

func committedWriteRecordCheckpointMessageID(record committedWriteRecord) *commonpb.MessageID {
	if record.LastConfirmedMessageID != nil {
		return cloneMessageIDProto(record.LastConfirmedMessageID)
	}
	return cloneMessageIDProto(record.SourceMessageID)
}

func cloneCommittedWriteRecord(record committedWriteRecord) committedWriteRecord {
	record.SourceMessageID = cloneMessageIDProto(record.SourceMessageID)
	record.LastConfirmedMessageID = cloneMessageIDProto(record.LastConfirmedMessageID)
	record.Rows = append([]committedWriteRow(nil), record.Rows...)
	if record.Idempotency != nil {
		idempotency := *record.Idempotency
		record.Idempotency = &idempotency
	}
	return record
}

func committedWriteRowsFromInsertResult(result *messagespb.IdempotentInsertResult) []committedWriteRow {
	if result == nil {
		return nil
	}
	return committedWriteRowsFromIDs(result.GetRowOffsets(), result.GetIds())
}

func committedWriteRowsFromIDs(rowOffsets []uint32, ids *schemapb.IDs) []committedWriteRow {
	if ids == nil {
		rows := make([]committedWriteRow, 0, len(rowOffsets))
		for _, offset := range rowOffsets {
			rows = append(rows, committedWriteRow{
				RowOffset: offset,
			})
		}
		return rows
	}
	if intIDs := ids.GetIntId(); intIDs != nil {
		rows := make([]committedWriteRow, 0, len(intIDs.GetData()))
		for idx, pk := range intIDs.GetData() {
			rows = append(rows, committedWriteRow{
				RowOffset:            rowOffsetAt(rowOffsets, idx),
				PrimaryKeyType:       committedWritePrimaryKeyTypeInt64,
				Int64PrimaryKeyValue: pk,
			})
		}
		return rows
	}
	if strIDs := ids.GetStrId(); strIDs != nil {
		rows := make([]committedWriteRow, 0, len(strIDs.GetData()))
		for idx, pk := range strIDs.GetData() {
			rows = append(rows, committedWriteRow{
				RowOffset:             rowOffsetAt(rowOffsets, idx),
				PrimaryKeyType:        committedWritePrimaryKeyTypeString,
				StringPrimaryKeyValue: pk,
			})
		}
		return rows
	}
	return nil
}

func rowOffsetAt(rowOffsets []uint32, idx int) uint32 {
	if idx < len(rowOffsets) {
		return rowOffsets[idx]
	}
	return uint32(idx)
}

func committedWriteRowOffsets(rows []committedWriteRow) []uint32 {
	if len(rows) == 0 {
		return nil
	}
	offsets := make([]uint32, 0, len(rows))
	for _, row := range rows {
		offsets = append(offsets, row.RowOffset)
	}
	return offsets
}

func committedWriteRowIDs(rows []committedWriteRow) *schemapb.IDs {
	if len(rows) == 0 {
		return nil
	}
	switch rows[0].PrimaryKeyType {
	case committedWritePrimaryKeyTypeInt64:
		ids := make([]int64, 0, len(rows))
		for _, row := range rows {
			if row.PrimaryKeyType != committedWritePrimaryKeyTypeInt64 {
				return nil
			}
			ids = append(ids, row.Int64PrimaryKeyValue)
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
		}
	case committedWritePrimaryKeyTypeString:
		ids := make([]string, 0, len(rows))
		for _, row := range rows {
			if row.PrimaryKeyType != committedWritePrimaryKeyTypeString {
				return nil
			}
			ids = append(ids, row.StringPrimaryKeyValue)
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids}},
		}
	default:
		return nil
	}
}
