// SegmentInsertBuffer can be reused to buffer all insert data of one segment
// buffer.Serialize will serialize the InsertBuffer and clear it
// pkstats keeps tracking pkstats of the segment until Finish

package storage

import (
	"fmt"
	"math"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TimeRange is a range of timestamp contains the min-timestamp and max-timestamp
type TimeRange struct {
	TimestampMin typeutil.Timestamp
	TimestampMax typeutil.Timestamp
}

func NewTimeRange(min, max typeutil.Timestamp) *TimeRange {
	return &TimeRange{
		TimestampMin: min,
		TimestampMax: max,
	}
}

func (tr *TimeRange) GetMinTimestamp() typeutil.Timestamp {
	return tr.TimestampMin
}

func (tr *TimeRange) GetMaxTimestamp() typeutil.Timestamp {
	return tr.TimestampMax
}

func (tr *TimeRange) Merge(other *TimeRange) {
	if other.TimestampMin < tr.TimestampMin {
		tr.TimestampMin = other.TimestampMin
	}
	if other.TimestampMax > tr.TimestampMax {
		tr.TimestampMax = other.TimestampMax
	}
}

func NewSegmentDeltaWriter(segmentID, partitionID, collectionID int64) *SegmentDeltaWriter {
	return &SegmentDeltaWriter{
		deleteData:   &DeleteData{},
		segmentID:    segmentID,
		partitionID:  partitionID,
		collectionID: collectionID,
		tsFrom:       math.MaxUint64,
		tsTo:         0,
	}
}

type SegmentDeltaWriter struct {
	deleteData   *DeleteData
	segmentID    int64
	partitionID  int64
	collectionID int64

	tsFrom typeutil.Timestamp
	tsTo   typeutil.Timestamp
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
	return w.deleteData.RowCount
}

func (w *SegmentDeltaWriter) GetTimeRange() *TimeRange {
	return NewTimeRange(w.tsFrom, w.tsTo)
}

func (w *SegmentDeltaWriter) updateRange(ts typeutil.Timestamp) {
	if ts < w.tsFrom {
		w.tsFrom = ts
	}
	if ts > w.tsTo {
		w.tsTo = ts
	}
}

func (w *SegmentDeltaWriter) Write(pk PrimaryKey, ts typeutil.Timestamp) {
	w.deleteData.Append(pk, ts)
	w.updateRange(ts)
}

func (w *SegmentDeltaWriter) WriteBatch(pks []PrimaryKey, tss []typeutil.Timestamp) {
	w.deleteData.AppendBatch(pks, tss)

	for _, ts := range tss {
		w.updateRange(ts)
	}
}

func (w *SegmentDeltaWriter) Finish() (*Blob, *TimeRange, error) {
	blob, err := NewDeleteCodec().Serialize(w.collectionID, w.partitionID, w.segmentID, w.deleteData)
	if err != nil {
		return nil, nil, err
	}

	return blob, w.GetTimeRange(), nil
}

func (w *SegmentDeltaWriter) GetDeltaData() *DeleteData {
	return w.deleteData
}

type SegmentWriter struct {
	writer  *SerializeWriter[*Value]
	closers []func() (*Blob, error)
	tsFrom  typeutil.Timestamp
	tsTo    typeutil.Timestamp

	pkstats      *PrimaryKeyStats
	segmentID    int64
	partitionID  int64
	collectionID int64
	sch          *schemapb.CollectionSchema
	rowCount     *atomic.Int64
}

func (w *SegmentWriter) GetRowNum() int64 {
	return w.rowCount.Load()
}

func (w *SegmentWriter) GetCollectionID() int64 {
	return w.collectionID
}

func (w *SegmentWriter) GetPartitionID() int64 {
	return w.partitionID
}

func (w *SegmentWriter) GetSegmentID() int64 {
	return w.segmentID
}

func (w *SegmentWriter) GetPkID() int64 {
	return w.pkstats.FieldID
}

func (w *SegmentWriter) WrittenMemorySize() uint64 {
	return w.writer.WrittenMemorySize()
}

func (w *SegmentWriter) Write(v *Value) error {
	ts := typeutil.Timestamp(v.Timestamp)
	if ts < w.tsFrom {
		w.tsFrom = ts
	}
	if ts > w.tsTo {
		w.tsTo = ts
	}

	w.pkstats.Update(v.PK)
	w.rowCount.Inc()
	return w.writer.Write(v)
}

func (w *SegmentWriter) Finish(actualRowCount int64) (*Blob, error) {
	w.writer.Flush()
	codec := NewInsertCodecWithSchema(&etcdpb.CollectionMeta{ID: w.collectionID, Schema: w.sch})
	return codec.SerializePkStats(w.pkstats, actualRowCount)
}

func (w *SegmentWriter) IsFull() bool {
	return w.writer.WrittenMemorySize() > paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64()
}

func (w *SegmentWriter) FlushAndIsFull() bool {
	w.writer.Flush()
	return w.writer.WrittenMemorySize() > paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64()
}

func (w *SegmentWriter) IsEmpty() bool {
	return w.writer.WrittenMemorySize() == 0
}

func (w *SegmentWriter) FlushAndIsEmpty() bool {
	w.writer.Flush()
	return w.writer.WrittenMemorySize() == 0
}

func (w *SegmentWriter) GetTimeRange() *TimeRange {
	return NewTimeRange(w.tsFrom, w.tsTo)
}

func (w *SegmentWriter) SerializeYield() ([]*Blob, *TimeRange, error) {
	w.writer.Flush()
	w.writer.Close()

	fieldData := make([]*Blob, len(w.closers))
	for i, f := range w.closers {
		blob, err := f()
		if err != nil {
			return nil, nil, err
		}
		fieldData[i] = blob
	}

	tr := w.GetTimeRange()
	w.clear()

	return fieldData, tr, nil
}

func (w *SegmentWriter) clear() {
	writer, closers, _ := newBinlogWriter(w.collectionID, w.partitionID, w.segmentID, w.sch)
	w.writer = writer
	w.closers = closers
	w.tsFrom = math.MaxUint64
	w.tsTo = 0
}

func NewSegmentWriter(sch *schemapb.CollectionSchema, maxCount int64, segID, partID, collID int64) (*SegmentWriter, error) {
	writer, closers, err := newBinlogWriter(collID, partID, segID, sch)
	if err != nil {
		return nil, err
	}

	var pkField *schemapb.FieldSchema
	for _, fs := range sch.GetFields() {
		if fs.GetIsPrimaryKey() && fs.GetFieldID() >= 100 && typeutil.IsPrimaryFieldType(fs.GetDataType()) {
			pkField = fs
		}
	}
	if pkField == nil {
		log.Warn("failed to get pk field from schema")
		return nil, fmt.Errorf("no pk field in schema")
	}

	stats, err := NewPrimaryKeyStats(pkField.GetFieldID(), int64(pkField.GetDataType()), maxCount)
	if err != nil {
		return nil, err
	}

	segWriter := SegmentWriter{
		writer:  writer,
		closers: closers,
		tsFrom:  math.MaxUint64,
		tsTo:    0,

		pkstats:      stats,
		sch:          sch,
		segmentID:    segID,
		partitionID:  partID,
		collectionID: collID,
		rowCount:     atomic.NewInt64(0),
	}

	return &segWriter, nil
}

func newBinlogWriter(collID, partID, segID int64, schema *schemapb.CollectionSchema,
) (writer *SerializeWriter[*Value], closers []func() (*Blob, error), err error) {
	fieldWriters := NewBinlogStreamWriters(collID, partID, segID, schema.Fields)
	closers = make([]func() (*Blob, error), 0, len(fieldWriters))
	for _, w := range fieldWriters {
		closers = append(closers, w.Finalize)
	}
	writer, err = NewBinlogSerializeWriter(schema, partID, segID, fieldWriters, 1024)
	return
}
