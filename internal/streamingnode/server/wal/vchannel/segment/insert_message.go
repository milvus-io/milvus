package segment

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// segmentInsertMessage is an Insert or Txn(Insert) entry selected for one
// segment. It is persistence-owned and intentionally does not depend on the
// persistence module.
type segmentInsertMessage struct {
	Message    message.ImmutableInsertMessageV1
	Assignment *messagespb.PartitionSegmentAssignment
	TimeTick   uint64
}

// InsertBatch is the part of one Insert or Txn WAL message assigned to a
// single segment. The vchannel layer builds batches once and each SegmentView
// consumes exactly one batch.
type InsertBatch struct {
	timeTick    uint64
	assignments []*messagespb.PartitionSegmentAssignment
	rows        uint64
	binarySize  uint64
}

func BuildInsertBatches(raw message.ImmutableMessage) (map[int64]InsertBatch, error) {
	batches := make(map[int64]InsertBatch)
	err := forEachSegmentInsertMessage(raw, 0, func(insert segmentInsertMessage) error {
		segmentID := insert.Assignment.GetSegmentAssignment().GetSegmentId()
		batch := batches[segmentID]
		batch.timeTick = insert.TimeTick
		batch.assignments = append(batch.assignments, insert.Assignment)
		batch.rows += insert.Assignment.GetRows()
		batch.binarySize += insert.Assignment.GetBinarySize()
		batches[segmentID] = batch
		return nil
	})
	return batches, err
}

func forEachSegmentInsertMessage(
	raw message.ImmutableMessage,
	segmentID int64,
	visit func(segmentInsertMessage) error,
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
	visit func(segmentInsertMessage) error,
) error {
	for _, assignment := range insert.Header().GetPartitions() {
		if segmentID != 0 && assignment.GetSegmentAssignment().GetSegmentId() != segmentID {
			continue
		}
		if err := visit(segmentInsertMessage{
			Message:    insert,
			Assignment: assignment,
			TimeTick:   timeTick,
		}); err != nil {
			return err
		}
	}
	return nil
}
