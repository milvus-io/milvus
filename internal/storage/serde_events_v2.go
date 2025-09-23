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
	"io"
	"path"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type packedRecordReader struct {
	paths  [][]string
	chunk  int
	reader *packed.PackedReader

	bufferSize           int64
	arrowSchema          *arrow.Schema
	field2Col            map[FieldID]int
	storageConfig        *indexpb.StorageConfig
	storagePluginContext *indexcgopb.StoragePluginContext
}

var _ RecordReader = (*packedRecordReader)(nil)

func (pr *packedRecordReader) iterateNextBatch() error {
	if pr.reader != nil {
		if err := pr.reader.Close(); err != nil {
			return err
		}
	}

	if pr.chunk >= len(pr.paths) {
		return io.EOF
	}

	reader, err := packed.NewPackedReader(pr.paths[pr.chunk], pr.arrowSchema, pr.bufferSize, pr.storageConfig, pr.storagePluginContext)
	pr.chunk++
	if err != nil {
		return errors.Newf("New binlog record packed reader error: %w", err)
	}
	pr.reader = reader
	return nil
}

func (pr *packedRecordReader) Next() (Record, error) {
	if pr.reader == nil {
		if err := pr.iterateNextBatch(); err != nil {
			return nil, err
		}
	}

	for {
		rec, err := pr.reader.ReadNext()
		if err == io.EOF {
			if err := pr.iterateNextBatch(); err != nil {
				return nil, err
			}
			continue
		} else if err != nil {
			return nil, err
		}
		return NewSimpleArrowRecord(rec, pr.field2Col), nil
	}
}

func (pr *packedRecordReader) SetNeededFields(fields typeutil.Set[int64]) {
	// TODO, push down SetNeededFields to packedReader after implemented
	// no-op for now
}

func (pr *packedRecordReader) Close() error {
	if pr.reader != nil {
		return pr.reader.Close()
	}
	return nil
}

func newPackedRecordReader(paths [][]string, schema *schemapb.CollectionSchema, bufferSize int64, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext,
) (*packedRecordReader, error) {
	arrowSchema, err := ConvertToArrowSchema(schema)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("convert collection schema [%s] to arrow schema error: %s", schema.Name, err.Error())
	}
	field2Col := make(map[FieldID]int)
	allFields := typeutil.GetAllFieldSchemas(schema)
	for i, field := range allFields {
		field2Col[field.FieldID] = i
	}
	return &packedRecordReader{
		paths:                paths,
		bufferSize:           bufferSize,
		arrowSchema:          arrowSchema,
		field2Col:            field2Col,
		storageConfig:        storageConfig,
		storagePluginContext: storagePluginContext,
	}, nil
}

// Deprecated
func NewPackedDeserializeReader(paths [][]string, schema *schemapb.CollectionSchema,
	bufferSize int64, shouldCopy bool,
) (*DeserializeReaderImpl[*Value], error) {
	reader, err := newPackedRecordReader(paths, schema, bufferSize, nil, nil)
	if err != nil {
		return nil, err
	}
	return NewDeserializeReader(reader, func(r Record, v []*Value) error {
		return ValueDeserializerWithSchema(r, v, schema, shouldCopy)
	}), nil
}

var _ RecordWriter = (*packedRecordWriter)(nil)

type packedRecordWriter struct {
	writer                  *packed.PackedWriter
	bufferSize              int64
	columnGroups            []storagecommon.ColumnGroup
	bucketName              string
	pathsMap                map[typeutil.UniqueID]string
	schema                  *schemapb.CollectionSchema
	arrowSchema             *arrow.Schema
	rowNum                  int64
	writtenUncompressed     uint64
	columnGroupUncompressed map[typeutil.UniqueID]uint64
	columnGroupCompressed   map[typeutil.UniqueID]uint64
	storageConfig           *indexpb.StorageConfig
}

func (pw *packedRecordWriter) Write(r Record) error {
	var rec arrow.Record
	sar, ok := r.(*simpleArrowRecord)
	if !ok {
		// Get all fields including struct sub-fields
		allFields := typeutil.GetAllFieldSchemas(pw.schema)
		arrays := make([]arrow.Array, len(allFields))
		for i, field := range allFields {
			arrays[i] = r.Column(field.FieldID)
		}
		rec = array.NewRecord(pw.arrowSchema, arrays, int64(r.Len()))
	} else {
		rec = sar.r
	}
	pw.rowNum += int64(r.Len())
	for col, arr := range rec.Columns() {
		size := arr.Data().SizeInBytes()
		pw.writtenUncompressed += size
		for _, columnGroup := range pw.columnGroups {
			if lo.Contains(columnGroup.Columns, col) {
				pw.columnGroupUncompressed[columnGroup.GroupID] += size
				break
			}
		}
	}
	defer rec.Release()
	return pw.writer.WriteRecordBatch(rec)
}

func (pw *packedRecordWriter) GetWrittenUncompressed() uint64 {
	return pw.writtenUncompressed
}

func (pw *packedRecordWriter) GetColumnGroupWrittenUncompressed(columnGroup typeutil.UniqueID) uint64 {
	if size, ok := pw.columnGroupUncompressed[columnGroup]; ok {
		return size
	}
	return 0
}

func (pw *packedRecordWriter) GetColumnGroupWrittenCompressed(columnGroup typeutil.UniqueID) uint64 {
	if size, ok := pw.columnGroupCompressed[columnGroup]; ok {
		return size
	}
	return 0
}

func (pw *packedRecordWriter) GetWrittenPaths(columnGroup typeutil.UniqueID) string {
	if path, ok := pw.pathsMap[columnGroup]; ok {
		return path
	}
	return ""
}

func (pw *packedRecordWriter) GetWrittenRowNum() int64 {
	return pw.rowNum
}

func (pw *packedRecordWriter) Close() error {
	if pw.writer != nil {
		err := pw.writer.Close()
		if err != nil {
			return err
		}
		for id, fpath := range pw.pathsMap {
			truePath := path.Join(pw.bucketName, fpath)
			size, err := packed.GetFileSize(truePath, pw.storageConfig)
			if err != nil {
				return err
			}
			pw.columnGroupCompressed[id] = uint64(size)
		}
	}
	return nil
}

func NewPackedRecordWriter(bucketName string, paths []string, schema *schemapb.CollectionSchema, bufferSize int64, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) (*packedRecordWriter, error) {
	arrowSchema, err := ConvertToArrowSchema(schema)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not convert collection schema %s to arrow schema: %s", schema.Name, err.Error()))
	}
	// if storage config is not passed, use common config
	storageType := paramtable.Get().CommonCfg.StorageType.GetValue()
	if storageConfig != nil {
		storageType = storageConfig.GetStorageType()
	}
	// compose true path before create packed writer here
	// and returned writtenPaths shall remain untouched
	truePaths := lo.Map(paths, func(p string, _ int) string {
		if storageType == "local" {
			return p
		}
		return path.Join(bucketName, p)
	})
	writer, err := packed.NewPackedWriter(truePaths, arrowSchema, bufferSize, multiPartUploadSize, columnGroups, storageConfig, storagePluginContext)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not new packed record writer %s", err.Error()))
	}
	columnGroupUncompressed := make(map[typeutil.UniqueID]uint64)
	columnGroupCompressed := make(map[typeutil.UniqueID]uint64)
	pathsMap := make(map[typeutil.UniqueID]string)
	if len(paths) != len(columnGroups) {
		return nil, merr.WrapErrParameterInvalid(len(paths), len(columnGroups),
			"paths length is not equal to column groups length for packed record writer")
	}
	for i, columnGroup := range columnGroups {
		columnGroupUncompressed[columnGroup.GroupID] = 0
		columnGroupCompressed[columnGroup.GroupID] = 0
		pathsMap[columnGroup.GroupID] = paths[i]
	}
	return &packedRecordWriter{
		writer:                  writer,
		schema:                  schema,
		arrowSchema:             arrowSchema,
		bufferSize:              bufferSize,
		bucketName:              bucketName,
		pathsMap:                pathsMap,
		columnGroups:            columnGroups,
		columnGroupUncompressed: columnGroupUncompressed,
		columnGroupCompressed:   columnGroupCompressed,
		storageConfig:           storageConfig,
	}, nil
}

// Deprecated, todo remove
func NewPackedSerializeWriter(bucketName string, paths []string, schema *schemapb.CollectionSchema, bufferSize int64,
	multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, batchSize int,
) (*SerializeWriterImpl[*Value], error) {
	packedRecordWriter, err := NewPackedRecordWriter(bucketName, paths, schema, bufferSize, multiPartUploadSize, columnGroups, nil, nil)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not new packed record writer %s", err.Error()))
	}
	return NewSerializeRecordWriter(packedRecordWriter, func(v []*Value) (Record, error) {
		return ValueSerializer(v, schema)
	}, batchSize), nil
}

var _ BinlogRecordWriter = (*PackedBinlogRecordWriter)(nil)

type PackedBinlogRecordWriter struct {
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

	// writer and stats generated at runtime
	writer              *packedRecordWriter
	pkstats             *PrimaryKeyStats
	bm25Stats           map[int64]*BM25Stats
	tsFrom              typeutil.Timestamp
	tsTo                typeutil.Timestamp
	rowNum              int64
	writtenUncompressed uint64

	// results
	fieldBinlogs map[FieldID]*datapb.FieldBinlog
	statsLog     *datapb.FieldBinlog
	bm25StatsLog map[FieldID]*datapb.FieldBinlog
}

func (pw *PackedBinlogRecordWriter) Write(r Record) error {
	if err := pw.initWriters(r); err != nil {
		return err
	}

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

		switch schemapb.DataType(pw.pkstats.PkType) {
		case schemapb.DataType_Int64:
			pkArray := r.Column(pw.pkstats.FieldID).(*array.Int64)
			pk := &Int64PrimaryKey{
				Value: pkArray.Value(i),
			}
			pw.pkstats.Update(pk)
		case schemapb.DataType_VarChar:
			pkArray := r.Column(pw.pkstats.FieldID).(*array.String)
			pk := &VarCharPrimaryKey{
				Value: pkArray.Value(i),
			}
			pw.pkstats.Update(pk)
		default:
			panic("invalid data type")
		}

		for fieldID, stats := range pw.bm25Stats {
			field, ok := r.Column(fieldID).(*array.Binary)
			if !ok {
				return errors.New("bm25 field value not found")
			}
			stats.AppendBytes(field.Value(i))
		}
	}

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

func (pw *PackedBinlogRecordWriter) getColumnStatsFromRecord(r Record, allFields []*schemapb.FieldSchema) map[int64]storagecommon.ColumnStats {
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

func (pw *PackedBinlogRecordWriter) GetWrittenUncompressed() uint64 {
	return pw.writtenUncompressed
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
	if err := pw.writeBm25Stats(); err != nil {
		return err
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
			LogSize:       int64(pw.writer.GetColumnGroupWrittenCompressed(columnGroupID)),
			MemorySize:    int64(pw.writer.GetColumnGroupWrittenUncompressed(columnGroupID)),
			LogPath:       pw.writer.GetWrittenPaths(columnGroupID),
			EntriesNum:    pw.writer.GetWrittenRowNum(),
			TimestampFrom: pw.tsFrom,
			TimestampTo:   pw.tsTo,
		})
	}
}

func (pw *PackedBinlogRecordWriter) writeStats() error {
	if pw.pkstats == nil {
		return nil
	}

	id, err := pw.allocator.AllocOne()
	if err != nil {
		return err
	}

	codec := NewInsertCodecWithSchema(&etcdpb.CollectionMeta{
		ID:     pw.collectionID,
		Schema: pw.schema,
	})
	sblob, err := codec.SerializePkStats(pw.pkstats, pw.rowNum)
	if err != nil {
		return err
	}

	sblob.Key = metautil.BuildStatsLogPath(pw.storageConfig.GetRootPath(),
		pw.collectionID, pw.partitionID, pw.segmentID, pw.pkstats.FieldID, id)

	if err := pw.BlobsWriter([]*Blob{sblob}); err != nil {
		return err
	}

	pw.statsLog = &datapb.FieldBinlog{
		FieldID: pw.pkstats.FieldID,
		Binlogs: []*datapb.Binlog{
			{
				LogSize:    int64(len(sblob.GetValue())),
				MemorySize: int64(len(sblob.GetValue())),
				LogPath:    sblob.Key,
				EntriesNum: pw.rowNum,
			},
		},
	}
	return nil
}

func (pw *PackedBinlogRecordWriter) writeBm25Stats() error {
	if len(pw.bm25Stats) == 0 {
		return nil
	}
	id, _, err := pw.allocator.Alloc(uint32(len(pw.bm25Stats)))
	if err != nil {
		return err
	}

	if pw.bm25StatsLog == nil {
		pw.bm25StatsLog = make(map[FieldID]*datapb.FieldBinlog)
	}
	for fid, stats := range pw.bm25Stats {
		bytes, err := stats.Serialize()
		if err != nil {
			return err
		}
		key := metautil.BuildBm25LogPath(pw.storageConfig.GetRootPath(),
			pw.collectionID, pw.partitionID, pw.segmentID, fid, id)
		blob := &Blob{
			Key:        key,
			Value:      bytes,
			RowNum:     stats.NumRow(),
			MemorySize: int64(len(bytes)),
		}
		if err := pw.BlobsWriter([]*Blob{blob}); err != nil {
			return err
		}

		fieldLog := &datapb.FieldBinlog{
			FieldID: fid,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:    int64(len(blob.GetValue())),
					MemorySize: int64(len(blob.GetValue())),
					LogPath:    key,
					EntriesNum: pw.rowNum,
				},
			},
		}

		pw.bm25StatsLog[fid] = fieldLog
		id++
	}

	return nil
}

func (pw *PackedBinlogRecordWriter) GetLogs() (
	fieldBinlogs map[FieldID]*datapb.FieldBinlog,
	statsLog *datapb.FieldBinlog,
	bm25StatsLog map[FieldID]*datapb.FieldBinlog,
) {
	return pw.fieldBinlogs, pw.statsLog, pw.bm25StatsLog
}

func (pw *PackedBinlogRecordWriter) GetRowNum() int64 {
	return pw.rowNum
}

func (pw *PackedBinlogRecordWriter) FlushChunk() error {
	return nil // do nothing
}

func (pw *PackedBinlogRecordWriter) Schema() *schemapb.CollectionSchema {
	return pw.schema
}

func (pw *PackedBinlogRecordWriter) GetBufferUncompressed() uint64 {
	return uint64(pw.multiPartUploadSize)
}

func newPackedBinlogRecordWriter(collectionID, partitionID, segmentID UniqueID, schema *schemapb.CollectionSchema,
	blobsWriter ChunkedBlobsWriter, allocator allocator.Interface, maxRowNum int64, bufferSize, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
) (*PackedBinlogRecordWriter, error) {
	arrowSchema, err := ConvertToArrowSchema(schema)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("convert collection schema [%s] to arrow schema error: %s", schema.Name, err.Error())
	}
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		log.Warn("failed to get pk field from schema")
		return nil, err
	}
	stats, err := NewPrimaryKeyStats(pkField.GetFieldID(), int64(pkField.GetDataType()), maxRowNum)
	if err != nil {
		return nil, err
	}
	bm25FieldIDs := lo.FilterMap(schema.GetFunctions(), func(function *schemapb.FunctionSchema, _ int) (int64, bool) {
		if function.GetType() == schemapb.FunctionType_BM25 {
			return function.GetOutputFieldIds()[0], true
		}
		return 0, false
	})
	bm25Stats := make(map[int64]*BM25Stats, len(bm25FieldIDs))
	for _, fid := range bm25FieldIDs {
		bm25Stats[fid] = NewBM25Stats()
	}

	return &PackedBinlogRecordWriter{
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
		pkstats:              stats,
		bm25Stats:            bm25Stats,
		storageConfig:        storageConfig,
		storagePluginContext: storagePluginContext,

		tsFrom: typeutil.MaxTimestamp,
		tsTo:   0,
	}, nil
}
