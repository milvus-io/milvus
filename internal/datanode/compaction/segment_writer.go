// SegmentInsertBuffer can be reused to buffer all insert data of one segment
// buffer.Serialize will serialize the InsertBuffer and clear it
// pkstats keeps tracking pkstats of the segment until Finish

package compaction

import (
	"fmt"
	"math"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/writebuffer"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func NewSegmentDeltaWriter(segmentID, partitionID, collectionID int64) *SegmentDeltaWriter {
	return &SegmentDeltaWriter{
		deleteData:   &storage.DeleteData{},
		segmentID:    segmentID,
		partitionID:  partitionID,
		collectionID: collectionID,
		tsFrom:       math.MaxUint64,
		tsTo:         0,
	}
}

type SegmentDeltaWriter struct {
	deleteData   *storage.DeleteData
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

func (w *SegmentDeltaWriter) Write(pk storage.PrimaryKey, ts typeutil.Timestamp) {
	w.deleteData.Append(pk, ts)
	w.updateRange(ts)
}

func (w *SegmentDeltaWriter) WriteBatch(pks []storage.PrimaryKey, tss []typeutil.Timestamp) {
	w.deleteData.AppendBatch(pks, tss)

	for _, ts := range tss {
		w.updateRange(ts)
	}
}

func (w *SegmentDeltaWriter) Finish() (*storage.Blob, *writebuffer.TimeRange, error) {
	blob, err := storage.NewDeleteCodec().Serialize(w.collectionID, w.partitionID, w.segmentID, w.deleteData)
	if err != nil {
		return nil, nil, err
	}

	return blob, w.GetTimeRange(), nil
}

type SegmentWriter struct {
	writer  *storage.SerializeWriter[*storage.Value]
	closers []func() (*storage.Blob, error)
	tsFrom  typeutil.Timestamp
	tsTo    typeutil.Timestamp

	pkstats      *storage.PrimaryKeyStats
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

func (w *SegmentWriter) Write(v *storage.Value) error {
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

func (w *SegmentWriter) Finish(actualRowCount int64) (*storage.Blob, error) {
	w.writer.Flush()
	codec := storage.NewInsertCodecWithSchema(&etcdpb.CollectionMeta{ID: w.collectionID, Schema: w.sch})
	return codec.SerializePkStats(w.pkstats, actualRowCount)
}

func (w *SegmentWriter) IsFull() bool {
	w.writer.Flush()
	return w.writer.WrittenMemorySize() > paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64()
}

func (w *SegmentWriter) IsEmpty() bool {
	w.writer.Flush()
	return w.writer.WrittenMemorySize() == 0
}

func (w *SegmentWriter) GetTimeRange() *writebuffer.TimeRange {
	return writebuffer.NewTimeRange(w.tsFrom, w.tsTo)
}

func (w *SegmentWriter) SerializeYield() ([]*storage.Blob, *writebuffer.TimeRange, error) {
	w.writer.Flush()
	w.writer.Close()

	fieldData := make([]*storage.Blob, len(w.closers))
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

	stats, err := storage.NewPrimaryKeyStats(pkField.GetFieldID(), int64(pkField.GetDataType()), maxCount)
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
) (writer *storage.SerializeWriter[*storage.Value], closers []func() (*storage.Blob, error), err error) {
	fieldWriters := storage.NewBinlogStreamWriters(collID, partID, segID, schema.Fields)
	closers = make([]func() (*storage.Blob, error), 0, len(fieldWriters))
	for _, w := range fieldWriters {
		closers = append(closers, w.Finalize)
	}
	writer, err = storage.NewBinlogSerializeWriter(schema, partID, segID, fieldWriters, 1024)
	return
}
