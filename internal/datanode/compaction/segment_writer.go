// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compaction

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	// DONOT leave it empty of all segments are deleted, just return a segment with zero meta for datacoord
	bm25Fields []int64
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

func NewMultiSegmentWriter(binlogIO io.BinlogIO, allocator *compactionAlloactor, plan *datapb.CompactionPlan, maxRows int64, partitionID, collectionID int64, bm25Fields []int64) *MultiSegmentWriter {
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
		bm25Fields: bm25Fields,
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

	result := &datapb.CompactionSegment{
		SegmentID:           writer.GetSegmentID(),
		InsertLogs:          lo.Values(allBinlogs),
		Field2StatslogPaths: []*datapb.FieldBinlog{sPath},
		NumOfRows:           writer.GetRowNum(),
		Channel:             w.channel,
	}

	if len(w.bm25Fields) > 0 {
		bmBinlogs, err := bm25SerializeWrite(context.TODO(), w.binlogIO, w.allocator.getLogIDAllocator(), writer)
		if err != nil {
			log.Warn("compact wrong, failed to serialize write segment bm25 stats", zap.Error(err))
			return err
		}
		result.Bm25Logs = bmBinlogs
	}

	w.res = append(w.res, result)

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
	writer, err := NewSegmentWriter(w.schema, w.maxRows, compactionBatchSize, newSegmentID, w.partitionID, w.collectionID, w.bm25Fields)
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

func (w *MultiSegmentWriter) writeInternal(writer *SegmentWriter) error {
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
	return nil
}

func (w *MultiSegmentWriter) WriteRecord(r storage.Record) error {
	writer, err := w.getWriter()
	if err != nil {
		return err
	}
	if err := w.writeInternal(writer); err != nil {
		return err
	}

	return writer.WriteRecord(r)
}

func (w *MultiSegmentWriter) Write(v *storage.Value) error {
	writer, err := w.getWriter()
	if err != nil {
		return err
	}
	if err := w.writeInternal(writer); err != nil {
		return err
	}

	return writer.Write(v)
}

func (w *MultiSegmentWriter) appendEmptySegment() error {
	writer, err := w.getWriter()
	if err != nil {
		return err
	}

	w.res = append(w.res, &datapb.CompactionSegment{
		SegmentID: writer.GetSegmentID(),
		NumOfRows: 0,
		Channel:   w.channel,
	})
	return nil
}

// DONOT return an empty list if every insert of the segment is deleted,
// append an empty segment instead
func (w *MultiSegmentWriter) Finish() ([]*datapb.CompactionSegment, error) {
	if w.current == -1 {
		if err := w.appendEmptySegment(); err != nil {
			return nil, err
		}
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

	pkstats   *storage.PrimaryKeyStats
	bm25Stats map[int64]*storage.BM25Stats

	segmentID    int64
	partitionID  int64
	collectionID int64
	sch          *schemapb.CollectionSchema
	rowCount     *atomic.Int64
	syncedSize   *atomic.Int64

	batchSize     int
	maxBinlogSize uint64
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

func (w *SegmentWriter) WriteRecord(r storage.Record) error {
	tsArray := r.Column(common.TimeStampField).(*array.Int64)
	rows := r.Len()
	for i := 0; i < rows; i++ {
		ts := typeutil.Timestamp(tsArray.Value(i))
		if ts < w.tsFrom {
			w.tsFrom = ts
		}
		if ts > w.tsTo {
			w.tsTo = ts
		}

		switch schemapb.DataType(w.pkstats.PkType) {
		case schemapb.DataType_Int64:
			pkArray := r.Column(w.GetPkID()).(*array.Int64)
			pk := &storage.Int64PrimaryKey{
				Value: pkArray.Value(i),
			}
			w.pkstats.Update(pk)
		case schemapb.DataType_VarChar:
			pkArray := r.Column(w.GetPkID()).(*array.String)
			pk := &storage.VarCharPrimaryKey{
				Value: pkArray.Value(i),
			}
			w.pkstats.Update(pk)
		default:
			panic("invalid data type")
		}

		for fieldID, stats := range w.bm25Stats {
			field, ok := r.Column(fieldID).(*array.Binary)
			if !ok {
				return fmt.Errorf("bm25 field value not found")
			}
			stats.AppendBytes(field.Value(i))
		}

		w.rowCount.Inc()
	}
	return w.writer.WriteRecord(r)
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
	for fieldID, stats := range w.bm25Stats {
		data, ok := v.Value.(map[storage.FieldID]interface{})[fieldID]
		if !ok {
			return fmt.Errorf("bm25 field value not found")
		}

		bytes, ok := data.([]byte)
		if !ok {
			return fmt.Errorf("bm25 field value not sparse bytes")
		}
		stats.AppendBytes(bytes)
	}

	w.rowCount.Inc()
	return w.writer.Write(v)
}

func (w *SegmentWriter) Finish() (*storage.Blob, error) {
	w.writer.Flush()
	codec := storage.NewInsertCodecWithSchema(&etcdpb.CollectionMeta{ID: w.collectionID, Schema: w.sch})
	return codec.SerializePkStats(w.pkstats, w.GetRowNum())
}

func (w *SegmentWriter) GetBm25Stats() map[int64]*storage.BM25Stats {
	return w.bm25Stats
}

func (w *SegmentWriter) GetBm25StatsBlob() (map[int64]*storage.Blob, error) {
	result := make(map[int64]*storage.Blob)
	for fieldID, stats := range w.bm25Stats {
		bytes, err := stats.Serialize()
		if err != nil {
			return nil, err
		}
		result[fieldID] = &storage.Blob{
			Key:        fmt.Sprintf("%d", fieldID),
			Value:      bytes,
			RowNum:     stats.NumRow(),
			MemorySize: int64(len(bytes)),
		}
	}

	return result, nil
}

func (w *SegmentWriter) IsFull() bool {
	return w.writer.WrittenMemorySize() > w.maxBinlogSize
}

func (w *SegmentWriter) FlushAndIsFull() bool {
	w.writer.Flush()
	return w.writer.WrittenMemorySize() > w.maxBinlogSize
}

func (w *SegmentWriter) IsFullWithBinlogMaxSize(binLogMaxSize uint64) bool {
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

	writer, closers, _ := newBinlogWriter(w.collectionID, w.partitionID, w.segmentID, w.sch, w.batchSize)
	w.writer = writer
	w.closers = closers
	w.tsFrom = math.MaxUint64
	w.tsTo = 0
}

func NewSegmentWriter(sch *schemapb.CollectionSchema, maxCount int64, batchSize int, segID, partID, collID int64, Bm25Fields []int64) (*SegmentWriter, error) {
	writer, closers, err := newBinlogWriter(collID, partID, segID, sch, batchSize)
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
		bm25Stats:    make(map[int64]*storage.BM25Stats),
		sch:          sch,
		segmentID:    segID,
		partitionID:  partID,
		collectionID: collID,
		rowCount:     atomic.NewInt64(0),
		syncedSize:   atomic.NewInt64(0),

		batchSize:     batchSize,
		maxBinlogSize: paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64(),
	}

	for _, fieldID := range Bm25Fields {
		segWriter.bm25Stats[fieldID] = storage.NewBM25Stats()
	}
	return &segWriter, nil
}

func newBinlogWriter(collID, partID, segID int64, schema *schemapb.CollectionSchema, batchSize int,
) (writer *storage.SerializeWriter[*storage.Value], closers []func() (*storage.Blob, error), err error) {
	fieldWriters := storage.NewBinlogStreamWriters(collID, partID, segID, schema.Fields)
	closers = make([]func() (*storage.Blob, error), 0, len(fieldWriters))
	for _, w := range fieldWriters {
		closers = append(closers, w.Finalize)
	}
	writer, err = storage.NewBinlogSerializeWriter(schema, partID, segID, fieldWriters, batchSize)
	return
}
