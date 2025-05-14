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
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type packedRecordReader struct {
	paths  [][]string
	chunk  int
	reader *packed.PackedReader

	bufferSize  int64
	arrowSchema *arrow.Schema
	field2Col   map[FieldID]int
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

	reader, err := packed.NewPackedReader(pr.paths[pr.chunk], pr.arrowSchema, pr.bufferSize)
	pr.chunk++
	if err != nil {
		return merr.WrapErrParameterInvalid("New binlog record packed reader error: %s", err.Error())
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

func (pr *packedRecordReader) Close() error {
	if pr.reader != nil {
		return pr.reader.Close()
	}
	return nil
}

func newPackedRecordReader(paths [][]string, schema *schemapb.CollectionSchema, bufferSize int64,
) (*packedRecordReader, error) {
	arrowSchema, err := ConvertToArrowSchema(schema.Fields)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("convert collection schema [%s] to arrow schema error: %s", schema.Name, err.Error())
	}
	field2Col := make(map[FieldID]int)
	for i, field := range schema.Fields {
		field2Col[field.FieldID] = i
	}
	return &packedRecordReader{
		paths:       paths,
		bufferSize:  bufferSize,
		arrowSchema: arrowSchema,
		field2Col:   field2Col,
	}, nil
}

func NewPackedDeserializeReader(paths [][]string, schema *schemapb.CollectionSchema,
	bufferSize int64,
) (*DeserializeReaderImpl[*Value], error) {
	reader, err := newPackedRecordReader(paths, schema, bufferSize)
	if err != nil {
		return nil, err
	}

	return NewDeserializeReader(reader, func(r Record, v []*Value) error {
		return ValueDeserializer(r, v, schema.Fields)
	}), nil
}

var _ RecordWriter = (*packedRecordWriter)(nil)

type packedRecordWriter struct {
	writer                  *packed.PackedWriter
	bufferSize              int64
	columnGroups            []storagecommon.ColumnGroup
	bucketName              string
	paths                   []string
	schema                  *schemapb.CollectionSchema
	arrowSchema             *arrow.Schema
	rowNum                  int64
	writtenUncompressed     uint64
	columnGroupUncompressed []uint64
}

func (pw *packedRecordWriter) Write(r Record) error {
	var rec arrow.Record
	sar, ok := r.(*simpleArrowRecord)
	if !ok {
		arrays := make([]arrow.Array, len(pw.schema.Fields))
		for i, field := range pw.schema.Fields {
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
		for columnGroup, group := range pw.columnGroups {
			if lo.Contains(group.Columns, col) {
				pw.columnGroupUncompressed[columnGroup] += size
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

func (pw *packedRecordWriter) GetColumnGroupWrittenUncompressed(columnGroup int) uint64 {
	return pw.columnGroupUncompressed[columnGroup]
}

func (pw *packedRecordWriter) GetWrittenPaths() []string {
	return pw.paths
}

func (pw *packedRecordWriter) GetWrittenRowNum() int64 {
	return pw.rowNum
}

func (pw *packedRecordWriter) Close() error {
	if pw.writer != nil {
		return pw.writer.Close()
	}
	return nil
}

func NewPackedRecordWriter(bucketName string, paths []string, schema *schemapb.CollectionSchema, bufferSize int64, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup) (*packedRecordWriter, error) {
	arrowSchema, err := ConvertToArrowSchema(schema.Fields)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not convert collection schema %s to arrow schema: %s", schema.Name, err.Error()))
	}
	// compose true path before create packed writer here
	// and returned writtenPaths shall remain untouched
	truePaths := lo.Map(paths, func(p string, _ int) string {
		return path.Join(bucketName, p)
	})
	writer, err := packed.NewPackedWriter(truePaths, arrowSchema, bufferSize, multiPartUploadSize, columnGroups)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not new packed record writer %s", err.Error()))
	}
	columnGroupUncompressed := make([]uint64, len(columnGroups))
	return &packedRecordWriter{
		writer:                  writer,
		schema:                  schema,
		arrowSchema:             arrowSchema,
		bufferSize:              bufferSize,
		bucketName:              bucketName,
		paths:                   paths,
		columnGroups:            columnGroups,
		columnGroupUncompressed: columnGroupUncompressed,
	}, nil
}

func NewPackedSerializeWriter(bucketName string, paths []string, schema *schemapb.CollectionSchema, bufferSize int64,
	multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, batchSize int,
) (*SerializeWriterImpl[*Value], error) {
	PackedBinlogRecordWriter, err := NewPackedRecordWriter(bucketName, paths, schema, bufferSize, multiPartUploadSize, columnGroups)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not new packed record writer %s", err.Error()))
	}
	return NewSerializeRecordWriter(PackedBinlogRecordWriter, func(v []*Value) (Record, error) {
		return ValueSerializer(v, schema.Fields)
	}, batchSize), nil
}

var _ BinlogRecordWriter = (*PackedBinlogRecordWriter)(nil)

type PackedBinlogRecordWriter struct {
	// attributes
	collectionID        UniqueID
	partitionID         UniqueID
	segmentID           UniqueID
	schema              *schemapb.CollectionSchema
	BlobsWriter         ChunkedBlobsWriter
	allocator           allocator.Interface
	chunkSize           uint64
	bucketName          string
	rootPath            string
	maxRowNum           int64
	arrowSchema         *arrow.Schema
	bufferSize          int64
	multiPartUploadSize int64
	columnGroups        []storagecommon.ColumnGroup

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
	return nil
}

func (pw *PackedBinlogRecordWriter) splitColumnByRecord(r Record) []storagecommon.ColumnGroup {
	groups := make([]storagecommon.ColumnGroup, 0)
	shortColumnGroup := storagecommon.ColumnGroup{Columns: make([]int, 0)}
	for i, field := range pw.schema.Fields {
		arr := r.Column(field.FieldID)
		size := arr.Data().SizeInBytes()
		rows := uint64(arr.Len())
		if rows != 0 && int64(size/rows) >= packed.ColumnGroupSizeThreshold {
			groups = append(groups, storagecommon.ColumnGroup{Columns: []int{i}})
		} else {
			shortColumnGroup.Columns = append(shortColumnGroup.Columns, i)
		}
	}
	if len(shortColumnGroup.Columns) > 0 {
		groups = append(groups, shortColumnGroup)
	}
	return groups
}

func (pw *PackedBinlogRecordWriter) initWriters(r Record) error {
	if pw.writer == nil {
		if len(pw.columnGroups) == 0 {
			pw.columnGroups = pw.splitColumnByRecord(r)
		}
		logIdStart, _, err := pw.allocator.Alloc(uint32(len(pw.columnGroups)))
		if err != nil {
			return err
		}
		paths := []string{}
		for columnGroup := range pw.columnGroups {
			path := metautil.BuildInsertLogPath(pw.rootPath, pw.collectionID, pw.partitionID, pw.segmentID, typeutil.UniqueID(columnGroup), logIdStart)
			paths = append(paths, path)
			logIdStart++
		}
		pw.writer, err = NewPackedRecordWriter(pw.bucketName, paths, pw.schema, pw.bufferSize, pw.multiPartUploadSize, pw.columnGroups)
		if err != nil {
			return merr.WrapErrServiceInternal(fmt.Sprintf("can not new packed record writer %s", err.Error()))
		}
	}
	return nil
}

func (pw *PackedBinlogRecordWriter) GetWrittenUncompressed() uint64 {
	return pw.writtenUncompressed
}

func (pw *PackedBinlogRecordWriter) Close() error {
	pw.finalizeBinlogs()
	if err := pw.writeStats(); err != nil {
		return err
	}
	if err := pw.writeBm25Stats(); err != nil {
		return err
	}
	if err := pw.writer.Close(); err != nil {
		return err
	}
	return nil
}

func (pw *PackedBinlogRecordWriter) finalizeBinlogs() {
	if pw.writer == nil {
		return
	}
	pw.rowNum = pw.writer.GetWrittenRowNum()
	pw.writtenUncompressed = pw.writer.GetWrittenUncompressed()
	if pw.fieldBinlogs == nil {
		pw.fieldBinlogs = make(map[FieldID]*datapb.FieldBinlog, len(pw.columnGroups))
	}
	for columnGroup := range pw.columnGroups {
		columnGroupID := typeutil.UniqueID(columnGroup)
		if _, exists := pw.fieldBinlogs[columnGroupID]; !exists {
			pw.fieldBinlogs[columnGroupID] = &datapb.FieldBinlog{
				FieldID: columnGroupID,
			}
		}
		pw.fieldBinlogs[columnGroupID].Binlogs = append(pw.fieldBinlogs[columnGroupID].Binlogs, &datapb.Binlog{
			LogSize:       int64(pw.writer.columnGroupUncompressed[columnGroup]), // TODO: should provide the log size of each column group file in storage v2
			MemorySize:    int64(pw.writer.columnGroupUncompressed[columnGroup]),
			LogPath:       pw.writer.GetWrittenPaths()[columnGroupID],
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

	sblob.Key = metautil.BuildStatsLogPath(pw.rootPath,
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
		key := metautil.BuildBm25LogPath(pw.rootPath,
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
	blobsWriter ChunkedBlobsWriter, allocator allocator.Interface, chunkSize uint64, bucketName, rootPath string, maxRowNum int64, bufferSize, multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup,
) (*PackedBinlogRecordWriter, error) {
	arrowSchema, err := ConvertToArrowSchema(schema.Fields)
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
		collectionID:        collectionID,
		partitionID:         partitionID,
		segmentID:           segmentID,
		schema:              schema,
		arrowSchema:         arrowSchema,
		BlobsWriter:         blobsWriter,
		allocator:           allocator,
		chunkSize:           chunkSize,
		bucketName:          bucketName,
		rootPath:            rootPath,
		maxRowNum:           maxRowNum,
		bufferSize:          bufferSize,
		multiPartUploadSize: multiPartUploadSize,
		columnGroups:        columnGroups,
		pkstats:             stats,
		bm25Stats:           bm25Stats,
	}, nil
}
