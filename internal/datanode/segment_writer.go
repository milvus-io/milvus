package datanode

import (
	"math"

	"github.com/milvus-io/milvus/internal/datanode/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func NewSegmentDeltaWriter(segmentID, partitionID, collectionID int64) *SegmentDeltaWriter {
	return &SegmentDeltaWriter{
		segmentID:    segmentID,
		partitionID:  partitionID,
		collectionID: collectionID,
		tsFrom:       math.MaxUint64,
		tsTo:         0,

		writer: storage.NewDeleteSerializedWriter(collectionID, partitionID, segmentID),
	}
}

type SegmentDeltaWriter struct {
	segmentID    int64
	partitionID  int64
	collectionID int64

	writer *storage.DeleteSerializedWriter

	rowCount int
	memSize  int64
	tsFrom   typeutil.Timestamp
	tsTo     typeutil.Timestamp
}

func (w *SegmentDeltaWriter) GetCollectionID() int64 {
	return w.collectionID
}

func (w *SegmentDeltaWriter) GetPartitionID() int64 {
	return w.partitionID
}

func (w *SegmentDeltaWriter) GetSegmentID() int64 {
	return w.segmentID
}

func (w *SegmentDeltaWriter) GetRowNum() int64 {
	return int64(w.rowCount)
}

func (w *SegmentDeltaWriter) GetMemorySize() int64 {
	return w.memSize
}

func (w *SegmentDeltaWriter) GetTimeRange() *writebuffer.TimeRange {
	return writebuffer.NewTimeRange(w.tsFrom, w.tsTo)
}

func (w *SegmentDeltaWriter) updateRange(ts typeutil.Timestamp) {
	if ts < w.tsFrom {
		w.tsFrom = ts
	}
	if ts > w.tsTo {
		w.tsTo = ts
	}
}

func (w *SegmentDeltaWriter) WriteSerialized(serializedRow string, pk storage.PrimaryKey, ts typeutil.Timestamp) error {
	w.updateRange(ts)
	w.memSize += pk.Size() + int64(8)
	w.rowCount += 1
	return w.writer.Write(serializedRow)
}

// Finish returns serialized bytes and timestamp range of delete data
func (w *SegmentDeltaWriter) Finish() ([]byte, *writebuffer.TimeRange, error) {
	blob, err := w.writer.Finish(w.tsFrom, w.tsTo)
	if err != nil {
		return nil, nil, err
	}
	return blob, w.GetTimeRange(), nil
}
