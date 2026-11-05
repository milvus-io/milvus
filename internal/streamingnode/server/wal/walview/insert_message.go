package walview

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SegmentInsertMessage is row content selected for one segment from an outer WAL message.
type SegmentInsertMessage struct {
	Message    messageutil.InsertRequest
	Assignment *messagespb.PartitionSegmentAssignment
	TimeTick   uint64
	Rows       []int64
}

// ForEachSegmentInsertMessage expands Insert, Upsert, or Txn WAL messages and visits
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
	visit func(SegmentInsertMessage) error,
) error {
	insert, assignments, ok := messageutil.AsInsertRequest(raw)
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
		if err := visit(SegmentInsertMessage{
			Message:    insert,
			Assignment: assignment,
			TimeTick:   timeTick,
			Rows:       rows,
		}); err != nil {
			return err
		}
	}
	return nil
}
