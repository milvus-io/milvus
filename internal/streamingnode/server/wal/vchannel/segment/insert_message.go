package segment

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// segmentInsertMessage is row content from Insert, Upsert, or Txn selected for one
// segment. It is persistence-owned and intentionally does not depend on the
// persistence module.
type segmentInsertMessage struct {
	RowsReader     messageutil.InsertRowsReader
	Assignment     *messagespb.PartitionSegmentAssignment
	TimeTick       uint64
	Rows           []int64
	PartitionCount int
}

// InsertBatch is the part of one Insert, Upsert, or Txn WAL message assigned to a
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
	case message.MessageTypeInsert, message.MessageTypeUpsert:
		return forEachSegmentInsertMessageFromInsert(
			raw,
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
			return forEachSegmentInsertMessageFromInsert(
				inner,
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
	raw message.ImmutableMessage,
	segmentID int64,
	timeTick uint64,
	visit func(segmentInsertMessage) error,
) error {
	reader, assignments, ok := messageutil.AsInsertRowsReader(raw)
	if !ok {
		return nil
	}
	selections, err := messageutil.InsertSelections(context.Background(), raw)
	if err != nil {
		return err
	}
	for index, assignment := range assignments {
		var rows []int64
		if selections != nil {
			rows = selections[index]
		}
		if segmentID != 0 && assignment.GetSegmentAssignment().GetSegmentId() != segmentID {
			continue
		}
		if err := visit(segmentInsertMessage{
			RowsReader:     reader,
			Assignment:     assignment,
			TimeTick:       timeTick,
			Rows:           rows,
			PartitionCount: len(assignments),
		}); err != nil {
			return err
		}
	}
	return nil
}
