package manager

import (
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
)

// AssignSegmentRequest is a request to allocate segment.
type AssignSegmentRequest struct {
	CollectionID  int64
	PartitionID   int64
	InsertMetrics stats.InsertMetrics
	TimeTick      uint64
	TxnSession    *txn.TxnSession
}

// AssignSegmentResult is a result of segment allocation.
// The sum of Results.Row is equal to InserMetrics.NumRows.
type AssignSegmentResult struct {
	SegmentID   int64
	Acknowledge *atomic.Int32 // used to ack the segment assign result has been consumed
}

// Ack acks the segment assign result has been consumed.
// Must be only call once after the segment assign result has been consumed.
func (r *AssignSegmentResult) Ack() {
	r.Acknowledge.Dec()
}
