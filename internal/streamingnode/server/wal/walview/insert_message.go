package walview

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SegmentInsertMessage is an Insert or Txn(Insert) entry selected for one segment.
type SegmentInsertMessage struct {
	Message    message.ImmutableInsertMessageV1
	Assignment *messagespb.PartitionSegmentAssignment
	TimeTick   uint64
}

// ForEachSegmentInsertMessage expands raw Insert or Txn WAL messages and visits
// only the insert assignments that belong to segmentID.
func ForEachSegmentInsertMessage(
	raw message.ImmutableMessage,
	segmentID int64,
	visit func(SegmentInsertMessage) error,
) error {
	if raw == nil {
		return merr.WrapErrServiceInternalMsg("nil insert WAL message")
	}
	switch raw.MessageType() {
	case message.MessageTypeInsert:
		return forEachSegmentInsertMessageFromInsert(
			message.MustAsImmutableInsertMessageV1(raw),
			segmentID,
			raw.TimeTick(),
			visit,
		)
	case message.MessageTypeTxn:
		txn := message.AsImmutableTxnMessage(raw)
		if txn == nil {
			return merr.WrapErrServiceInternalMsg("invalid txn WAL message")
		}
		return txn.RangeOver(func(inner message.ImmutableMessage) error {
			if inner.MessageType() != message.MessageTypeInsert {
				return nil
			}
			return forEachSegmentInsertMessageFromInsert(
				message.MustAsImmutableInsertMessageV1(inner),
				segmentID,
				raw.TimeTick(),
				visit,
			)
		})
	default:
		return nil
	}
}

func forEachSegmentInsertMessageFromInsert(
	insert message.ImmutableInsertMessageV1,
	segmentID int64,
	timeTick uint64,
	visit func(SegmentInsertMessage) error,
) error {
	for _, assignment := range insert.Header().GetPartitions() {
		if segmentID != 0 && assignment.GetSegmentAssignment().GetSegmentId() != segmentID {
			continue
		}
		if err := visit(SegmentInsertMessage{
			Message:    insert,
			Assignment: assignment,
			TimeTick:   timeTick,
		}); err != nil {
			return err
		}
	}
	return nil
}
