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

package storage

import (
	"fmt"
	"path"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type BinlogRecordWriter interface {
	RecordWriter
	GetLogs() (
		fieldBinlogs map[FieldID]*datapb.FieldBinlog,
		statsLog *datapb.FieldBinlog,
		bm25StatsLog map[FieldID]*datapb.FieldBinlog,
		manifest string,
		expirQuantiles []int64,
	)
	GetRowNum() int64
	// GetStatsBlobSize returns the cumulative memory size of bloom-filter +
	// BM25 stat blobs produced by this writer. The value comes from a
	// counter populated by both the V2 (writeStats) and V3 (appendV3Stats)
	// paths; for V3 the statsLog / bm25StatsLog FieldBinlogs are nil
	// because stats live in the manifest, so this counter is the only
	// source of the blob footprint.
	GetStatsBlobSize() int64
	FlushChunk() error
	GetBufferUncompressed() uint64
	Schema() *schemapb.CollectionSchema
}

type packedBinlogRecordWriterBase struct {
	// attributes
	collectionID         UniqueID
	partitionID          UniqueID
	segmentID            UniqueID
	schema               *schemapb.CollectionSchema
	BlobsWriter          ChunkedBlobsWriter
	allocator            allocator.Interface
	maxRowNum            int64
	arrowSchema          *arrow.Schema
	bufferSize           int64
	multiPartUploadSize  int64
	columnGroups         []storagecommon.ColumnGroup
	storageConfig        *indexpb.StorageConfig
	storagePluginContext *indexcgopb.StoragePluginContext
	// basePath is the segment data root, populated by initWriters. The
	// underlying packed batch writers do not return a manifest path; the
	// caller builds the manifest update against this base path.
	basePath string

	pkCollector         *PkStatsCollector
	bm25Collector       *Bm25StatsCollector
	tsFrom              typeutil.Timestamp
	tsTo                typeutil.Timestamp
	rowNum              int64
	writtenUncompressed uint64

	// results
	fieldBinlogs  map[FieldID]*datapb.FieldBinlog
	statsLog      *datapb.FieldBinlog
	bm25StatsLog  map[FieldID]*datapb.FieldBinlog
	manifest      string
	statsBlobSize int64

	ttlFieldID     int64
	ttlFieldValues []int64

	// Track null counts per field for nullable fields
	nullCounts map[FieldID]int64
}

func (pw *packedBinlogRecordWriterBase) getColumnStatsFromRecord(r Record, allFields []*schemapb.FieldSchema) map[int64]storagecommon.ColumnStats {
	result := make(map[int64]storagecommon.ColumnStats)
	for _, field := range allFields {
		if arr := r.Column(field.FieldID); arr != nil {
			result[field.FieldID] = storagecommon.ColumnStats{
				AvgSize: int64(arr.Data().SizeInBytes()) / int64(arr.Len()),
			}
		}
	}
	return result
}

func (pw *packedBinlogRecordWriterBase) GetWrittenUncompressed() uint64 {
	return pw.writtenUncompressed
}

func (pw *packedBinlogRecordWriterBase) GetExpirQuantiles() []int64 {
	return calculateExpirQuantiles(pw.ttlFieldID, pw.rowNum, pw.ttlFieldValues)
}

func (pw *packedBinlogRecordWriterBase) writeStats() error {
	// Write PK stats
	pkStatsMap, err := pw.pkCollector.Digest(
		pw.collectionID,
		pw.partitionID,
		pw.segmentID,
		pw.storageConfig.GetRootPath(),
		pw.rowNum,
		pw.allocator,
		pw.BlobsWriter,
	)
	if err != nil {
		return err
	}
	// Extract single PK stats from map
	for _, statsLog := range pkStatsMap {
		pw.statsLog = statsLog
		for _, l := range statsLog.GetBinlogs() {
			pw.statsBlobSize += l.GetMemorySize()
		}
		break
	}

	// Write BM25 stats
	bm25StatsLog, err := pw.bm25Collector.Digest(
		pw.collectionID,
		pw.partitionID,
		pw.segmentID,
		pw.storageConfig.GetRootPath(),
		pw.rowNum,
		pw.allocator,
		pw.BlobsWriter,
	)
	if err != nil {
		return err
	}
	pw.bm25StatsLog = bm25StatsLog
	for _, fb := range bm25StatsLog {
		for _, l := range fb.GetBinlogs() {
			pw.statsBlobSize += l.GetMemorySize()
		}
	}

	return nil
}

func (pw *packedBinlogRecordWriterBase) GetLogs() (
	fieldBinlogs map[FieldID]*datapb.FieldBinlog,
	statsLog *datapb.FieldBinlog,
	bm25StatsLog map[FieldID]*datapb.FieldBinlog,
	manifest string,
	expirQuantiles []int64,
) {
	return pw.fieldBinlogs, pw.statsLog, pw.bm25StatsLog, pw.manifest, pw.GetExpirQuantiles()
}

func (pw *packedBinlogRecordWriterBase) GetRowNum() int64 {
	return pw.rowNum
}

func (pw *packedBinlogRecordWriterBase) fillV3ColumnGroupFormats() (string, []string) {
	writerFormat := paramtable.Get().DataNodeCfg.StorageFormat.GetValue()
	pw.columnGroups = storagecommon.FillColumnGroupFormats(pw.columnGroups, writerFormat)
	return writerFormat, storagecommon.ColumnGroupFormats(pw.columnGroups, writerFormat)
}

func (pw *packedBinlogRecordWriterBase) GetStatsBlobSize() int64 {
	return pw.statsBlobSize
}

func (pw *packedBinlogRecordWriterBase) FlushChunk() error {
	return nil // do nothing
}

func (pw *packedBinlogRecordWriterBase) Schema() *schemapb.CollectionSchema {
	return pw.schema
}

func (pw *packedBinlogRecordWriterBase) GetBufferUncompressed() uint64 {
	return uint64(pw.multiPartUploadSize)
}

func (pw *packedBinlogRecordWriterBase) collectNullCounts(r Record) {
	if pw.nullCounts == nil {
		pw.nullCounts = make(map[FieldID]int64)
	}
	allFields := typeutil.GetAllFieldSchemas(pw.schema)
	for _, field := range allFields {
		if col := r.Column(field.FieldID); col != nil {
			pw.nullCounts[field.FieldID] += int64(col.NullN())
		}
	}
}

func (pw *packedBinlogRecordWriterBase) getFieldNullCountsForColumnGroup(columnGroup storagecommon.ColumnGroup) map[int64]int64 {
	result := make(map[int64]int64, len(columnGroup.Fields))
	for _, fieldID := range columnGroup.Fields {
		if n, ok := pw.nullCounts[fieldID]; ok {
			result[fieldID] = n
		}
	}
	return result
}

var _ BinlogRecordWriter = (*PackedBinlogRecordWriter)(nil)

type PackedBinlogRecordWriter struct {
	packedBinlogRecordWriterBase
	writer *packedRecordWriter
}

func (pw *PackedBinlogRecordWriter) Write(r Record) error {
	if err := pw.initWriters(r); err != nil {
		return err
	}

	// Track timestamps
	tsArray := r.Column(common.TimeStampField).(*array.Int64)
	rows := r.Len()
	for i := 0; i < rows; i++ {
		ts := typeutil.Timestamp(tsArray.Value(i))
		if ts < pw.tsFrom {
			pw.tsFrom = ts
		}
		if ts > pw.tsTo {
			pw.tsTo = ts
		}
	}

	if pw.ttlFieldID >= common.StartOfUserFieldID {
		ttlColumn := r.Column(pw.ttlFieldID)
		// Defensive check to prevent panic
		if ttlColumn == nil {
			return merr.WrapErrServiceInternal("ttl field not found")
		}
		ttlArray, ok := ttlColumn.(*array.Int64)
		if !ok {
			return merr.WrapErrServiceInternal("ttl field is not int64")
		}
		for i := 0; i < rows; i++ {
			if ttlArray.IsNull(i) {
				continue
			}
			ttlValue := ttlArray.Value(i)
			if ttlValue <= 0 {
				continue
			}
			pw.ttlFieldValues = append(pw.ttlFieldValues, ttlValue)
		}
	}

	// Collect statistics
	if err := pw.pkCollector.Collect(r); err != nil {
		return err
	}
	if err := pw.bm25Collector.Collect(r); err != nil {
		return err
	}

	pw.collectNullCounts(r)

	err := pw.writer.Write(r)
	if err != nil {
		return merr.WrapErrServiceInternal(fmt.Sprintf("write record batch error: %s", err.Error()))
	}
	pw.writtenUncompressed = pw.writer.GetWrittenUncompressed()
	return nil
}

func (pw *PackedBinlogRecordWriter) initWriters(r Record) error {
	if pw.writer == nil {
		if len(pw.columnGroups) == 0 {
			allFields := typeutil.GetAllFieldSchemas(pw.schema)
			pw.columnGroups = storagecommon.SplitColumns(allFields, pw.getColumnStatsFromRecord(r, allFields), storagecommon.DefaultPolicies()...)
		}
		logIdStart, _, err := pw.allocator.Alloc(uint32(len(pw.columnGroups)))
		if err != nil {
			return err
		}
		paths := []string{}
		for _, columnGroup := range pw.columnGroups {
			path := metautil.BuildInsertLogPath(pw.storageConfig.GetRootPath(), pw.collectionID, pw.partitionID, pw.segmentID, columnGroup.GroupID, logIdStart)
			paths = append(paths, path)
			logIdStart++
		}
		pw.writer, err = NewPackedRecordWriter(pw.storageConfig.GetBucketName(), paths, pw.schema, pw.bufferSize, pw.multiPartUploadSize, pw.columnGroups, pw.storageConfig, pw.storagePluginContext)
		if err != nil {
			return merr.WrapErrServiceInternal(fmt.Sprintf("can not new packed record writer %s", err.Error()))
		}
	}
	return nil
}

func (pw *PackedBinlogRecordWriter) finalizeBinlogs() {
	if pw.writer == nil {
		return
	}
	pw.rowNum = pw.writer.GetWrittenRowNum()
	if pw.fieldBinlogs == nil {
		pw.fieldBinlogs = make(map[FieldID]*datapb.FieldBinlog, len(pw.columnGroups))
	}
	for _, columnGroup := range pw.columnGroups {
		columnGroupID := columnGroup.GroupID
		if _, exists := pw.fieldBinlogs[columnGroupID]; !exists {
			pw.fieldBinlogs[columnGroupID] = &datapb.FieldBinlog{
				FieldID:     columnGroupID,
				ChildFields: columnGroup.Fields,
			}
		}
		pw.fieldBinlogs[columnGroupID].Binlogs = append(pw.fieldBinlogs[columnGroupID].Binlogs, &datapb.Binlog{
			LogSize:         int64(pw.writer.GetColumnGroupWrittenCompressed(columnGroupID)),
			MemorySize:      int64(pw.writer.GetColumnGroupWrittenUncompressed(columnGroupID)),
			LogPath:         pw.writer.GetWrittenPaths(columnGroupID),
			EntriesNum:      pw.writer.GetWrittenRowNum(),
			TimestampFrom:   pw.tsFrom,
			TimestampTo:     pw.tsTo,
			FieldNullCounts: pw.getFieldNullCountsForColumnGroup(columnGroup),
		})
	}
	pw.manifest = pw.writer.GetWrittenManifest()
}

func (pw *PackedBinlogRecordWriter) Close() error {
	if pw.writer != nil {
		if err := pw.writer.Close(); err != nil {
			return err
		}
	}
	pw.finalizeBinlogs()
	if err := pw.writeStats(); err != nil {
		return err
	}
	return nil
}

func newPackedBinlogRecordWriter(collectionID, partitionID, segmentID UniqueID, schema *schemapb.CollectionSchema,
	blobsWriter ChunkedBlobsWriter, allocator allocator.Interface, maxRowNum int64, bufferSize, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
) (*PackedBinlogRecordWriter, error) {
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("convert collection schema [%s] to arrow schema error: %s", schema.Name, err.Error())
	}

	writer := &PackedBinlogRecordWriter{
		packedBinlogRecordWriterBase: packedBinlogRecordWriterBase{
			collectionID:         collectionID,
			partitionID:          partitionID,
			segmentID:            segmentID,
			schema:               schema,
			arrowSchema:          arrowSchema,
			BlobsWriter:          blobsWriter,
			allocator:            allocator,
			maxRowNum:            maxRowNum,
			bufferSize:           bufferSize,
			multiPartUploadSize:  multiPartUploadSize,
			columnGroups:         columnGroups,
			storageConfig:        storageConfig,
			storagePluginContext: storagePluginContext,
			tsFrom:               typeutil.MaxTimestamp,
			tsTo:                 0,
			ttlFieldID:           getTTLFieldID(schema),
			ttlFieldValues:       make([]int64, 0),
		},
	}

	// Create stats collectors
	writer.pkCollector, err = NewPkStatsCollector(
		collectionID,
		schema,
		maxRowNum,
	)
	if err != nil {
		return nil, err
	}

	writer.bm25Collector = NewBm25StatsCollector(schema)

	return writer, nil
}

var _ BinlogRecordWriter = (*PackedManifestRecordWriter)(nil)

type PackedManifestRecordWriter struct {
	packedBinlogRecordWriterBase
	// writer and stats generated at runtime
	writer *packedRecordBatchWriter
}

func (pw *PackedManifestRecordWriter) Write(r Record) error {
	if err := pw.initWriters(r); err != nil {
		return err
	}

	// Track timestamps
	tsArray := r.Column(common.TimeStampField).(*array.Int64)
	rows := r.Len()
	for i := 0; i < rows; i++ {
		ts := typeutil.Timestamp(tsArray.Value(i))
		if ts < pw.tsFrom {
			pw.tsFrom = ts
		}
		if ts > pw.tsTo {
			pw.tsTo = ts
		}
	}

	// Collect statistics
	if err := pw.pkCollector.Collect(r); err != nil {
		return err
	}
	if err := pw.bm25Collector.Collect(r); err != nil {
		return err
	}

	pw.collectNullCounts(r)

	err := pw.writer.Write(r)
	if err != nil {
		return merr.WrapErrServiceInternal(fmt.Sprintf("write record batch error: %s", err.Error()))
	}
	pw.writtenUncompressed = pw.writer.GetWrittenUncompressed()
	return nil
}

func (pw *PackedManifestRecordWriter) initWriters(r Record) error {
	if pw.writer == nil {
		if len(pw.columnGroups) == 0 {
			allFields := typeutil.GetAllFieldSchemas(pw.schema)
			pw.columnGroups = storagecommon.SplitColumns(allFields, pw.getColumnStatsFromRecord(r, allFields), storagecommon.DefaultPolicies()...)
		}
		writerFormat, schemaBasedFormats := pw.fillV3ColumnGroupFormats()

		var err error
		k := metautil.JoinIDPath(pw.collectionID, pw.partitionID, pw.segmentID)
		pw.basePath = path.Join(pw.storageConfig.GetRootPath(), common.SegmentInsertLogPath, k)
		pw.writer, err = NewPackedRecordBatchWriter(pw.basePath, pw.schema, pw.bufferSize, pw.multiPartUploadSize, pw.columnGroups, pw.storageConfig, pw.storagePluginContext, writerFormat, schemaBasedFormats)
		if err != nil {
			return merr.WrapErrServiceInternal(fmt.Sprintf("can not new packed record writer %s", err.Error()))
		}
	}
	return nil
}

func (pw *PackedManifestRecordWriter) finalizeBinlogs() {
	if pw.writer == nil {
		return
	}
	pw.rowNum = pw.writer.GetWrittenRowNum()
	if pw.fieldBinlogs == nil {
		pw.fieldBinlogs = make(map[FieldID]*datapb.FieldBinlog, len(pw.columnGroups))
	}
	for _, columnGroup := range pw.columnGroups {
		columnGroupID := columnGroup.GroupID
		if _, exists := pw.fieldBinlogs[columnGroupID]; !exists {
			pw.fieldBinlogs[columnGroupID] = &datapb.FieldBinlog{
				FieldID:     columnGroupID,
				ChildFields: columnGroup.Fields,
				Format:      columnGroup.Format,
			}
		}
		pw.fieldBinlogs[columnGroupID].Binlogs = append(pw.fieldBinlogs[columnGroupID].Binlogs, &datapb.Binlog{
			LogSize:         int64(pw.writer.GetColumnGroupWrittenCompressed(columnGroupID)),
			MemorySize:      int64(pw.writer.GetColumnGroupWrittenUncompressed(columnGroupID)),
			LogPath:         pw.writer.GetWrittenPaths(columnGroupID),
			EntriesNum:      pw.writer.GetWrittenRowNum(),
			TimestampFrom:   pw.tsFrom,
			TimestampTo:     pw.tsTo,
			FieldNullCounts: pw.getFieldNullCountsForColumnGroup(columnGroup),
		})
	}
}

// Close finalizes the V3 segment. It closes the underlying packed writer to
// get column groups, serializes bloom filter / BM25 stat blobs, writes
// those blobs to storage, then performs a single packed.CommitManifestUpdates
// that registers inserts + all stats atomically.
func (pw *PackedManifestRecordWriter) Close() error {
	if pw.writer == nil {
		return nil
	}
	out, err := pw.writer.Close()
	if err != nil {
		return err
	}
	if out != nil {
		defer out.Destroy()
	}
	pw.finalizeBinlogs()
	if out == nil {
		return nil
	}

	updates := &packed.ManifestUpdates{NewFiles: out}
	if err := pw.appendV3Stats(updates); err != nil {
		return err
	}
	newManifest, err := packed.CommitManifestUpdates(pw.basePath, packed.ManifestEarliest, pw.storageConfig, updates)
	if err != nil {
		return fmt.Errorf("PackedManifestRecordWriter.Close commit: %w", err)
	}
	pw.manifest = newManifest
	return nil
}

// appendV3Stats serializes bloom filter / BM25 stat blobs, writes them to
// storage, and appends StatEntry records onto updates so the surrounding
// commit registers inserts + stats atomically. Leaves pw.statsLog and
// pw.bm25StatsLog nil — stats are embedded in the manifest, not in
// statslog FieldBinlogs. The cumulative blob memory is tracked on
// pw.statsBlobSize so callers can ship a correct SegmentInfo.Stats.
func (pw *PackedManifestRecordWriter) appendV3Stats(updates *packed.ManifestUpdates) error {
	statsBlob, pkFieldID, err := pw.pkCollector.SerializeBlob(pw.rowNum)
	if err != nil {
		return err
	}
	if statsBlob != nil {
		id, err := pw.allocator.AllocOne()
		if err != nil {
			return err
		}
		fullPath := path.Join(pw.basePath, fmt.Sprintf("_stats/bloom_filter.%d/%d", pkFieldID, id))
		if err := packed.WriteFile(pw.storageConfig, fullPath, statsBlob.Value); err != nil {
			return fmt.Errorf("appendV3Stats: failed to write bloom filter stats: %w", err)
		}
		blobSize := int64(len(statsBlob.Value))
		pw.statsBlobSize += blobSize
		updates.Stats = append(updates.Stats, packed.StatEntry{
			Key:      fmt.Sprintf("bloom_filter.%d", pkFieldID),
			Files:    []string{fullPath},
			Metadata: map[string]string{"memory_size": fmt.Sprintf("%d", blobSize)},
		})
	}

	bm25Blobs, err := pw.bm25Collector.SerializeBlobs()
	if err != nil {
		return err
	}
	for fieldID, blob := range bm25Blobs {
		id, err := pw.allocator.AllocOne()
		if err != nil {
			return err
		}
		fullPath := path.Join(pw.basePath, fmt.Sprintf("_stats/bm25.%d/%d", fieldID, id))
		if err := packed.WriteFile(pw.storageConfig, fullPath, blob.Value); err != nil {
			return fmt.Errorf("appendV3Stats: failed to write bm25 stats: %w", err)
		}
		pw.statsBlobSize += blob.MemorySize
		updates.Stats = append(updates.Stats, packed.StatEntry{
			Key:      fmt.Sprintf("bm25.%d", fieldID),
			Files:    []string{fullPath},
			Metadata: map[string]string{"memory_size": fmt.Sprintf("%d", blob.MemorySize)},
		})
	}
	return nil
}

func newPackedManifestRecordWriter(collectionID, partitionID, segmentID UniqueID, schema *schemapb.CollectionSchema,
	blobsWriter ChunkedBlobsWriter, allocator allocator.Interface, maxRowNum int64, bufferSize, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
) (*PackedManifestRecordWriter, error) {
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("convert collection schema [%s] to arrow schema error: %s", schema.Name, err.Error())
	}

	writer := &PackedManifestRecordWriter{
		packedBinlogRecordWriterBase: packedBinlogRecordWriterBase{
			collectionID:         collectionID,
			partitionID:          partitionID,
			segmentID:            segmentID,
			schema:               schema,
			arrowSchema:          arrowSchema,
			BlobsWriter:          blobsWriter,
			allocator:            allocator,
			maxRowNum:            maxRowNum,
			bufferSize:           bufferSize,
			multiPartUploadSize:  multiPartUploadSize,
			columnGroups:         columnGroups,
			storageConfig:        storageConfig,
			storagePluginContext: storagePluginContext,
			tsFrom:               typeutil.MaxTimestamp,
			tsTo:                 0,
			ttlFieldID:           getTTLFieldID(schema),
			ttlFieldValues:       make([]int64, 0),
		},
	}

	// Create stats collectors
	writer.pkCollector, err = NewPkStatsCollector(
		collectionID,
		schema,
		maxRowNum,
	)
	if err != nil {
		return nil, err
	}

	writer.bm25Collector = NewBm25StatsCollector(schema)

	return writer, nil
}

var _ BinlogRecordWriter = (*PackedTextManifestRecordWriter)(nil)

// PackedTextManifestRecordWriter wraps packedTextBatchWriter for TEXT column compaction.
// this writer is used during compaction when TEXT columns need REWRITE_ALL strategy.
type PackedTextManifestRecordWriter struct {
	packedBinlogRecordWriterBase
	writer            *packedTextBatchWriter
	textColumnConfigs []packed.TextColumnConfig
}

func (pw *PackedTextManifestRecordWriter) Write(r Record) error {
	if err := pw.initWriters(r); err != nil {
		return err
	}

	// track timestamps
	tsArray := r.Column(common.TimeStampField).(*array.Int64)
	rows := r.Len()
	for i := 0; i < rows; i++ {
		ts := typeutil.Timestamp(tsArray.Value(i))
		if ts < pw.tsFrom {
			pw.tsFrom = ts
		}
		if ts > pw.tsTo {
			pw.tsTo = ts
		}
	}

	// collect statistics
	if err := pw.pkCollector.Collect(r); err != nil {
		return err
	}
	if err := pw.bm25Collector.Collect(r); err != nil {
		return err
	}

	pw.collectNullCounts(r)

	err := pw.writer.Write(r)
	if err != nil {
		return merr.WrapErrServiceInternal(fmt.Sprintf("write record batch error: %s", err.Error()))
	}
	pw.writtenUncompressed = pw.writer.GetWrittenUncompressed()
	return nil
}

func (pw *PackedTextManifestRecordWriter) initWriters(r Record) error {
	if pw.writer == nil {
		if len(pw.columnGroups) == 0 {
			allFields := typeutil.GetAllFieldSchemas(pw.schema)
			pw.columnGroups = storagecommon.SplitColumns(allFields, pw.getColumnStatsFromRecord(r, allFields), storagecommon.DefaultPolicies()...)
		}
		writerFormat, schemaBasedFormats := pw.fillV3ColumnGroupFormats()

		var err error
		k := metautil.JoinIDPath(pw.collectionID, pw.partitionID, pw.segmentID)
		pw.basePath = path.Join(pw.storageConfig.GetRootPath(), common.SegmentInsertLogPath, k)
		pw.writer, err = NewPackedTextBatchWriter(pw.storageConfig.GetBucketName(), pw.basePath, pw.schema, pw.bufferSize, pw.multiPartUploadSize, pw.columnGroups, pw.storageConfig, pw.textColumnConfigs, writerFormat, schemaBasedFormats)
		if err != nil {
			return merr.WrapErrServiceInternal(fmt.Sprintf("can not new packed text writer %s", err.Error()))
		}
	}
	return nil
}

func (pw *PackedTextManifestRecordWriter) finalizeBinlogs() {
	if pw.writer == nil {
		return
	}
	pw.rowNum = pw.writer.GetWrittenRowNum()
	if pw.fieldBinlogs == nil {
		pw.fieldBinlogs = make(map[FieldID]*datapb.FieldBinlog, len(pw.columnGroups))
	}
	for _, columnGroup := range pw.columnGroups {
		columnGroupID := columnGroup.GroupID
		if _, exists := pw.fieldBinlogs[columnGroupID]; !exists {
			pw.fieldBinlogs[columnGroupID] = &datapb.FieldBinlog{
				FieldID:     columnGroupID,
				ChildFields: columnGroup.Fields,
				Format:      columnGroup.Format,
			}
		}
		pw.fieldBinlogs[columnGroupID].Binlogs = append(pw.fieldBinlogs[columnGroupID].Binlogs, &datapb.Binlog{
			LogSize:         int64(pw.writer.GetColumnGroupWrittenCompressed(columnGroupID)),
			MemorySize:      int64(pw.writer.GetColumnGroupWrittenUncompressed(columnGroupID)),
			LogPath:         pw.writer.GetWrittenPaths(columnGroupID),
			EntriesNum:      pw.writer.GetWrittenRowNum(),
			TimestampFrom:   pw.tsFrom,
			TimestampTo:     pw.tsTo,
			FieldNullCounts: pw.getFieldNullCountsForColumnGroup(columnGroup),
		})
	}
}

// Close finalizes the text-column segment using the same do-then-commit
// pattern as PackedManifestRecordWriter.Close.
func (pw *PackedTextManifestRecordWriter) Close() error {
	if pw.writer == nil {
		return pw.writeStats()
	}
	out, err := pw.writer.Close()
	if err != nil {
		return err
	}
	if out != nil {
		defer out.Destroy()
	}
	pw.finalizeBinlogs()
	if out == nil {
		return pw.writeStats()
	}

	newManifest, err := packed.CommitManifestUpdates(pw.basePath, packed.ManifestEarliest, pw.storageConfig,
		&packed.ManifestUpdates{NewFiles: out})
	if err != nil {
		return fmt.Errorf("PackedTextManifestRecordWriter.Close commit: %w", err)
	}
	pw.manifest = newManifest
	return pw.writeStats()
}

// NewPackedTextManifestRecordWriter creates a new BinlogRecordWriter for TEXT column compaction.
// textColumnConfigs: TEXT column configurations for REWRITE_ALL fields
func NewPackedTextManifestRecordWriter(
	collectionID, partitionID, segmentID UniqueID,
	schema *schemapb.CollectionSchema,
	blobsWriter ChunkedBlobsWriter,
	allocator allocator.Interface,
	maxRowNum int64,
	bufferSize, multiPartUploadSize int64,
	columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	textColumnConfigs []packed.TextColumnConfig,
) (*PackedTextManifestRecordWriter, error) {
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("convert collection schema [%s] to arrow schema error: %s", schema.Name, err.Error())
	}

	writer := &PackedTextManifestRecordWriter{
		packedBinlogRecordWriterBase: packedBinlogRecordWriterBase{
			collectionID:        collectionID,
			partitionID:         partitionID,
			segmentID:           segmentID,
			schema:              schema,
			arrowSchema:         arrowSchema,
			BlobsWriter:         blobsWriter,
			allocator:           allocator,
			maxRowNum:           maxRowNum,
			bufferSize:          bufferSize,
			multiPartUploadSize: multiPartUploadSize,
			columnGroups:        columnGroups,
			storageConfig:       storageConfig,
			tsFrom:              typeutil.MaxTimestamp,
			tsTo:                0,
			ttlFieldID:          getTTLFieldID(schema),
			ttlFieldValues:      make([]int64, 0),
		},
		textColumnConfigs: textColumnConfigs,
	}

	// create stats collectors
	writer.pkCollector, err = NewPkStatsCollector(
		collectionID,
		schema,
		maxRowNum,
	)
	if err != nil {
		return nil, err
	}

	writer.bm25Collector = NewBm25StatsCollector(schema)

	return writer, nil
}
