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

package compactor

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Not concurrent safe.
type MultiSegmentWriter struct {
	ctx       context.Context
	binlogIO  io.BinlogIO
	allocator *compactionAlloactor

	writer           *storage.BinlogValueWriter
	currentSegmentID typeutil.UniqueID

	maxRows     int64
	segmentSize int64
	// segmentSize in Bytes
	// segmentSize might be changed dynamicly. To make sure a compaction plan is static,
	// The target segmentSize is defined when creating the compaction plan.

	schema       *schemapb.CollectionSchema
	partitionID  int64
	collectionID int64
	channel      string
	batchSize    int

	res []*datapb.CompactionSegment
	// DONOT leave it empty of all segments are deleted, just return a segment with zero meta for datacoord

	storageVersion int64
	rwOption       []storage.RwOption
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

func NewMultiSegmentWriter(ctx context.Context, binlogIO io.BinlogIO, allocator *compactionAlloactor, segmentSize int64,
	schema *schemapb.CollectionSchema,
	maxRows int64, partitionID, collectionID int64, channel string, batchSize int, rwOption ...storage.RwOption,
) *MultiSegmentWriter {
	storageVersion := storage.StorageV1
	if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
		storageVersion = storage.StorageV2
	}
	rwOpts := rwOption
	if len(rwOption) == 0 {
		rwOpts = make([]storage.RwOption, 0)
	}
	return &MultiSegmentWriter{
		ctx:            ctx,
		binlogIO:       binlogIO,
		allocator:      allocator,
		maxRows:        maxRows, // For bloomfilter only
		segmentSize:    segmentSize,
		schema:         schema,
		partitionID:    partitionID,
		collectionID:   collectionID,
		channel:        channel,
		batchSize:      batchSize,
		res:            make([]*datapb.CompactionSegment, 0),
		storageVersion: storageVersion,
		rwOption:       rwOpts,
	}
}

func (w *MultiSegmentWriter) closeWriter() error {
	if w.writer != nil {
		if err := w.writer.Close(); err != nil {
			return err
		}

		fieldBinlogs, statsLog, bm25Logs := w.writer.GetLogs()

		result := &datapb.CompactionSegment{
			SegmentID:           w.currentSegmentID,
			InsertLogs:          lo.Values(fieldBinlogs),
			Field2StatslogPaths: []*datapb.FieldBinlog{statsLog},
			NumOfRows:           w.writer.GetRowNum(),
			Channel:             w.channel,
			Bm25Logs:            lo.Values(bm25Logs),
			StorageVersion:      w.storageVersion,
		}

		w.res = append(w.res, result)

		log.Info("created new segment",
			zap.Int64("segmentID", w.currentSegmentID),
			zap.String("channel", w.channel),
			zap.Int64("totalRows", w.writer.GetRowNum()),
			zap.Uint64("totalSize", w.writer.GetWrittenUncompressed()),
			zap.Int64("expected segment size", w.segmentSize),
			zap.Int64("storageVersion", w.storageVersion))
	}
	return nil
}

func (w *MultiSegmentWriter) rotateWriter() error {
	if err := w.closeWriter(); err != nil {
		return err
	}

	newSegmentID, err := w.allocator.allocSegmentID()
	if err != nil {
		return err
	}
	w.currentSegmentID = newSegmentID

	chunkSize := paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64()
	rootPath := binlog.GetRootPath()

	w.rwOption = append(w.rwOption,
		storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			return w.binlogIO.Upload(ctx, kvs)
		}),
		storage.WithVersion(w.storageVersion),
	)
	rw, err := storage.NewBinlogRecordWriter(w.ctx, w.collectionID, w.partitionID, newSegmentID,
		w.schema, w.allocator.logIDAlloc, chunkSize, rootPath, w.maxRows, w.rwOption...,
	)
	if err != nil {
		return err
	}

	w.writer = storage.NewBinlogValueWriter(rw, w.batchSize)
	return nil
}

func (w *MultiSegmentWriter) GetWrittenUncompressed() uint64 {
	if w.writer == nil {
		return 0
	}
	return w.writer.GetWrittenUncompressed()
}

func (w *MultiSegmentWriter) GetBufferUncompressed() uint64 {
	if w.writer == nil {
		return 0
	}
	return w.writer.GetBufferUncompressed()
}

func (w *MultiSegmentWriter) GetCompactionSegments() []*datapb.CompactionSegment {
	return w.res
}

func (w *MultiSegmentWriter) Write(r storage.Record) error {
	if w.writer == nil || w.writer.GetWrittenUncompressed() >= uint64(w.segmentSize) {
		if err := w.rotateWriter(); err != nil {
			return err
		}
	}
	return w.writer.Write(r)
}

func (w *MultiSegmentWriter) WriteValue(v *storage.Value) error {
	if w.writer == nil || w.writer.GetWrittenUncompressed() >= uint64(w.segmentSize) {
		if err := w.rotateWriter(); err != nil {
			return err
		}
	}

	return w.writer.WriteValue(v)
}

func (w *MultiSegmentWriter) FlushChunk() error {
	if w.writer == nil {
		return nil
	}
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.writer.FlushChunk()
}

func (w *MultiSegmentWriter) Close() error {
	if w.writer != nil {
		return w.closeWriter()
	}
	return nil
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
	writer  *storage.BinlogSerializeWriter
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
	return w.writer.GetWrittenUncompressed()
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
	return w.writer.Write(r)
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
	return w.writer.WriteValue(v)
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
	return w.writer.GetWrittenUncompressed() > w.maxBinlogSize
}

func (w *SegmentWriter) FlushAndIsFull() bool {
	w.writer.Flush()
	return w.writer.GetWrittenUncompressed() > w.maxBinlogSize
}

func (w *SegmentWriter) IsFullWithBinlogMaxSize(binLogMaxSize uint64) bool {
	return w.writer.GetWrittenUncompressed() > binLogMaxSize
}

func (w *SegmentWriter) IsEmpty() bool {
	return w.writer.GetWrittenUncompressed() == 0
}

func (w *SegmentWriter) FlushAndIsEmpty() bool {
	w.writer.Flush()
	return w.writer.GetWrittenUncompressed() == 0
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
	return w.syncedSize.Load() + int64(w.writer.GetWrittenUncompressed())
}

func (w *SegmentWriter) clear() {
	w.syncedSize.Add(int64(w.writer.GetWrittenUncompressed()))

	writer, closers, _ := newBinlogWriter(w.collectionID, w.partitionID, w.segmentID, w.sch, w.batchSize)
	w.writer = writer
	w.closers = closers
	w.tsFrom = math.MaxUint64
	w.tsTo = 0
}

// deprecated: use NewMultiSegmentWriter instead
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
) (writer *storage.BinlogSerializeWriter, closers []func() (*storage.Blob, error), err error) {
	fieldWriters := storage.NewBinlogStreamWriters(collID, partID, segID, schema.Fields)
	closers = make([]func() (*storage.Blob, error), 0, len(fieldWriters))
	for _, w := range fieldWriters {
		closers = append(closers, w.Finalize)
	}
	writer, err = storage.NewBinlogSerializeWriter(schema, partID, segID, fieldWriters, batchSize)
	return
}
