// SegmentInsertBuffer can be reused to buffer all insert data of one segment
// buffer.Serialize will serialize the InsertBuffer and clear it
// pkstats keeps tracking pkstats of the segment until Finish

package compaction

import (
	"context"
	"math"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
)

// Not concurrent safe.
type MultiSegmentWriter struct {
	binlogIO  io.BinlogIO
	allocator *compactionAlloactor

	writers []*SegmentWriter
	current int

	maxRows     int64
	segmentSize int64
	// segmentSize in Bytes
	// segmentSize might be changed dynamicly. To make sure a compaction plan is static,
	// The target segmentSize is defined when creating the compaction plan.

	schema       *schemapb.CollectionSchema
	partitionID  int64
	collectionID int64
	channel      string

	cachedMeta map[typeutil.UniqueID]map[typeutil.UniqueID]*datapb.FieldBinlog
	// segID -> fieldID -> binlogs

	res []*datapb.CompactionSegment
}

type compactionAlloactor struct {
	segmentAlloc allocator.Interface
	logIDAlloc   allocator.Interface
}

func NewCompactionAllocator(segmentAlloc, logIDAlloc allocator.Interface) *compactionAlloactor {
	return &compactionAlloactor{
		segmentAlloc: segmentAlloc,
		logIDAlloc:   logIDAlloc,
	}
}

func (alloc *compactionAlloactor) allocSegmentID() (typeutil.UniqueID, error) {
	return alloc.segmentAlloc.AllocOne()
}

func (alloc *compactionAlloactor) getLogIDAllocator() allocator.Interface {
	return alloc.logIDAlloc
}

func NewMultiSegmentWriter(binlogIO io.BinlogIO, allocator *compactionAlloactor, plan *datapb.CompactionPlan, maxRows int64, partitionID, collectionID int64) *MultiSegmentWriter {
	return &MultiSegmentWriter{
		binlogIO:  binlogIO,
		allocator: allocator,

		writers: make([]*SegmentWriter, 0),
		current: -1,

		maxRows:     maxRows, // For bloomfilter only
		segmentSize: plan.GetMaxSize(),

		schema:       plan.GetSchema(),
		partitionID:  partitionID,
		collectionID: collectionID,
		channel:      plan.GetChannel(),

		cachedMeta: make(map[typeutil.UniqueID]map[typeutil.UniqueID]*datapb.FieldBinlog),
		res:        make([]*datapb.CompactionSegment, 0),
	}
}

func (w *MultiSegmentWriter) finishCurrent() error {
	writer := w.writers[w.current]
	allBinlogs, ok := w.cachedMeta[writer.segmentID]
	if !ok {
		allBinlogs = make(map[typeutil.UniqueID]*datapb.FieldBinlog)
	}

	if !writer.FlushAndIsEmpty() {
		kvs, partialBinlogs, err := serializeWrite(context.TODO(), w.allocator.getLogIDAllocator(), writer)
		if err != nil {
			return err
		}

		if err := w.binlogIO.Upload(context.TODO(), kvs); err != nil {
			return err
		}

		mergeFieldBinlogs(allBinlogs, partialBinlogs)
	}

	sPath, err := statSerializeWrite(context.TODO(), w.binlogIO, w.allocator.getLogIDAllocator(), writer)
	if err != nil {
		return err
	}

	w.res = append(w.res, &datapb.CompactionSegment{
		SegmentID:           writer.GetSegmentID(),
		InsertLogs:          lo.Values(allBinlogs),
		Field2StatslogPaths: []*datapb.FieldBinlog{sPath},
		NumOfRows:           writer.GetRowNum(),
		Channel:             w.channel,
	})

	log.Info("Segment writer flushed a segment",
		zap.Int64("segmentID", writer.GetSegmentID()),
		zap.String("channel", w.channel),
		zap.Int64("totalRows", writer.GetRowNum()),
		zap.Int64("totalSize", writer.GetTotalSize()))

	w.cachedMeta[writer.segmentID] = nil
	return nil
}

func (w *MultiSegmentWriter) addNewWriter() error {
	newSegmentID, err := w.allocator.allocSegmentID()
	if err != nil {
		return err
	}
	writer, err := NewSegmentWriter(w.schema, w.maxRows, newSegmentID, w.partitionID, w.collectionID)
	if err != nil {
		return err
	}
	w.writers = append(w.writers, writer)
	w.current++
	return nil
}

func (w *MultiSegmentWriter) getWriter() (*SegmentWriter, error) {
	if len(w.writers) == 0 {
		if err := w.addNewWriter(); err != nil {
			return nil, err
		}

		return w.writers[w.current], nil
	}

	if w.writers[w.current].GetTotalSize() > w.segmentSize {
		if err := w.finishCurrent(); err != nil {
			return nil, err
		}
		if err := w.addNewWriter(); err != nil {
			return nil, err
		}
	}

	return w.writers[w.current], nil
}

func (w *MultiSegmentWriter) Write(v *storage.Value) error {
	writer, err := w.getWriter()
	if err != nil {
		return err
	}

	if writer.IsFull() {
		// init segment fieldBinlogs if it is not exist
		if _, ok := w.cachedMeta[writer.segmentID]; !ok {
			w.cachedMeta[writer.segmentID] = make(map[typeutil.UniqueID]*datapb.FieldBinlog)
		}

		kvs, partialBinlogs, err := serializeWrite(context.TODO(), w.allocator.getLogIDAllocator(), writer)
		if err != nil {
			return err
		}

		if err := w.binlogIO.Upload(context.TODO(), kvs); err != nil {
			return err
		}

		mergeFieldBinlogs(w.cachedMeta[writer.segmentID], partialBinlogs)
	}

	return writer.Write(v)
}

// Could return an empty list if every insert of the segment is deleted
func (w *MultiSegmentWriter) Finish() ([]*datapb.CompactionSegment, error) {
	if w.current == -1 {
		return w.res, nil
	}

	if !w.writers[w.current].FlushAndIsEmpty() {
		if err := w.finishCurrent(); err != nil {
			return nil, err
		}
	}

	return w.res, nil
}

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
	syncedSize   *atomic.Int64
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

func (w *SegmentWriter) Finish() (*storage.Blob, error) {
	w.writer.Flush()
	codec := storage.NewInsertCodecWithSchema(&etcdpb.CollectionMeta{ID: w.collectionID, Schema: w.sch})
	return codec.SerializePkStats(w.pkstats, w.GetRowNum())
}

func (w *SegmentWriter) IsFull() bool {
	return w.writer.WrittenMemorySize() > paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64()
}

func (w *SegmentWriter) FlushAndIsFull() bool {
	w.writer.Flush()
	return w.writer.WrittenMemorySize() > paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64()
}

func (w *SegmentWriter) FlushAndIsFullWithBinlogMaxSize(binLogMaxSize uint64) bool {
	w.writer.Flush()
	return w.writer.WrittenMemorySize() > binLogMaxSize
}

func (w *SegmentWriter) IsEmpty() bool {
	return w.writer.WrittenMemorySize() == 0
}

func (w *SegmentWriter) FlushAndIsEmpty() bool {
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

func (w *SegmentWriter) GetTotalSize() int64 {
	return w.syncedSize.Load() + int64(w.writer.WrittenMemorySize())
}

func (w *SegmentWriter) clear() {
	w.syncedSize.Add(int64(w.writer.WrittenMemorySize()))

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

	pkField, err := typeutil.GetPrimaryFieldSchema(sch)
	if err != nil {
		log.Warn("failed to get pk field from schema")
		return nil, err
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
		syncedSize:   atomic.NewInt64(0),
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
	writer, err = storage.NewBinlogSerializeWriter(schema, partID, segID, fieldWriters, 100)
	return
}
