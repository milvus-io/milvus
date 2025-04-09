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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ RecordReader = (*CompositeBinlogRecordReader)(nil)

// ChunkedBlobsReader returns a chunk composed of blobs, or io.EOF if no more data
type ChunkedBlobsReader func() ([]*Blob, error)

type CompositeBinlogRecordReader struct {
	BlobsReader ChunkedBlobsReader
	schema      *schemapb.CollectionSchema
	index       map[FieldID]int16

	brs []*BinlogReader
	rrs []array.RecordReader
}

func (crr *CompositeBinlogRecordReader) iterateNextBatch() error {
	if crr.brs != nil {
		for _, er := range crr.brs {
			if er != nil {
				er.Close()
			}
		}
	}
	if crr.rrs != nil {
		for _, rr := range crr.rrs {
			if rr != nil {
				rr.Release()
			}
		}
	}

	blobs, err := crr.BlobsReader()
	if err != nil {
		return err
	}

	crr.rrs = make([]array.RecordReader, len(crr.schema.Fields))
	crr.brs = make([]*BinlogReader, len(crr.schema.Fields))

	for _, b := range blobs {
		reader, err := NewBinlogReader(b.Value)
		if err != nil {
			return err
		}

		er, err := reader.NextEventReader()
		if err != nil {
			return err
		}
		i := crr.index[reader.FieldID]
		rr, err := er.GetArrowRecordReader()
		if err != nil {
			return err
		}
		crr.rrs[i] = rr
		crr.brs[i] = reader
	}
	return nil
}

func (crr *CompositeBinlogRecordReader) Next() (Record, error) {
	if crr.rrs == nil {
		if err := crr.iterateNextBatch(); err != nil {
			return nil, err
		}
	}

	composeRecord := func() (Record, error) {
		recs := make([]arrow.Array, len(crr.schema.Fields))

		for i, f := range crr.schema.Fields {
			if crr.rrs[i] != nil {
				if ok := crr.rrs[i].Next(); !ok {
					return nil, io.EOF
				}
				recs[i] = crr.rrs[i].Record().Column(0)
			} else {
				// If the field is not in the current batch, fill with null array
				// Note that we're intentionally not filling default value here, because the
				// deserializer will fill them later.
				if !f.Nullable {
					return nil, merr.WrapErrServiceInternal(fmt.Sprintf("missing field data %s", f.Name))
				}
				dim, _ := typeutil.GetDim(f)
				builder := array.NewBuilder(memory.DefaultAllocator, serdeMap[f.DataType].arrowType(int(dim)))
				builder.AppendNulls(int(crr.rrs[0].Record().NumRows()))
				recs[i] = builder.NewArray()
			}
		}
		return &compositeRecord{
			index: crr.index,
			recs:  recs,
		}, nil
	}

	// Try compose records
	r, err := composeRecord()
	if err == io.EOF {
		// if EOF, try iterate next batch (blob)
		if err := crr.iterateNextBatch(); err != nil {
			return nil, err
		}
		r, err = composeRecord() // try compose again
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (crr *CompositeBinlogRecordReader) Close() error {
	if crr.brs != nil {
		for _, er := range crr.brs {
			if er != nil {
				er.Close()
			}
		}
	}
	if crr.rrs != nil {
		for _, rr := range crr.rrs {
			if rr != nil {
				rr.Release()
			}
		}
	}
	return nil
}

func parseBlobKey(blobKey string) (colId FieldID, logId UniqueID) {
	if _, _, _, colId, logId, ok := metautil.ParseInsertLogPath(blobKey); ok {
		return colId, logId
	}
	if colId, err := strconv.ParseInt(blobKey, 10, 64); err == nil {
		// data_codec.go generate single field id as blob key.
		return colId, 0
	}
	return InvalidUniqueID, InvalidUniqueID
}

func MakeBlobsReader(blobs []*Blob) ChunkedBlobsReader {
	// sort blobs by log id
	sort.Slice(blobs, func(i, j int) bool {
		_, iLog := parseBlobKey(blobs[i].Key)
		_, jLog := parseBlobKey(blobs[j].Key)
		return iLog < jLog
	})
	var field0 FieldID
	pivots := make([]int, 0)
	for i, blob := range blobs {
		if i == 0 {
			field0, _ = parseBlobKey(blob.Key)
			pivots = append(pivots, 0)
			continue
		}
		if fieldID, _ := parseBlobKey(blob.Key); fieldID == field0 {
			pivots = append(pivots, i)
		}
	}
	pivots = append(pivots, len(blobs)) // append a pivot to the end of the slice
	chunkPos := 0
	return func() ([]*Blob, error) {
		if chunkPos >= len(pivots)-1 {
			return nil, io.EOF
		}
		chunk := blobs[pivots[chunkPos]:pivots[chunkPos+1]]
		chunkPos++
		return chunk, nil
	}
}

func newCompositeBinlogRecordReader(schema *schemapb.CollectionSchema, blobsReader ChunkedBlobsReader) (*CompositeBinlogRecordReader, error) {
	index := make(map[FieldID]int16)
	for i, f := range schema.Fields {
		index[f.FieldID] = int16(i)
	}
	return &CompositeBinlogRecordReader{
		schema:      schema,
		BlobsReader: blobsReader,
		index:       index,
	}, nil
}

func ValueDeserializer(r Record, v []*Value, fieldSchema []*schemapb.FieldSchema) error {
	pkField := func() *schemapb.FieldSchema {
		for _, field := range fieldSchema {
			if field.GetIsPrimaryKey() {
				return field
			}
		}
		return nil
	}()
	if pkField == nil {
		return merr.WrapErrServiceInternal("no primary key field found")
	}

	for i := 0; i < r.Len(); i++ {
		value := v[i]
		if value == nil {
			value = &Value{}
			value.Value = make(map[FieldID]interface{}, len(fieldSchema))
			v[i] = value
		}

		m := value.Value.(map[FieldID]interface{})
		for _, f := range fieldSchema {
			j := f.FieldID
			dt := f.DataType
			if r.Column(j).IsNull(i) {
				if f.GetDefaultValue() != nil {
					m[j] = getDefaultValue(f)
				} else {
					m[j] = nil
				}
			} else {
				d, ok := serdeMap[dt].deserialize(r.Column(j), i)
				if ok {
					m[j] = d // TODO: avoid memory copy here.
				} else {
					return merr.WrapErrServiceInternal(fmt.Sprintf("unexpected type %s", dt))
				}
			}
		}

		rowID, ok := m[common.RowIDField].(int64)
		if !ok {
			return merr.WrapErrIoKeyNotFound("no row id column found")
		}
		value.ID = rowID
		value.Timestamp = m[common.TimeStampField].(int64)

		pk, err := GenPrimaryKeyByRawData(m[pkField.FieldID], pkField.DataType)
		if err != nil {
			return err
		}

		value.PK = pk
		value.IsDeleted = false
		value.Value = m
	}
	return nil
}

func NewBinlogDeserializeReader(schema *schemapb.CollectionSchema, blobsReader ChunkedBlobsReader) (*DeserializeReaderImpl[*Value], error) {
	reader, err := newCompositeBinlogRecordReader(schema, blobsReader)
	if err != nil {
		return nil, err
	}

	return NewDeserializeReader(reader, func(r Record, v []*Value) error {
		return ValueDeserializer(r, v, schema.Fields)
	}), nil
}

func newDeltalogOneFieldReader(blobs []*Blob) (*DeserializeReaderImpl[*DeleteLog], error) {
	reader, err := newCompositeBinlogRecordReader(
		&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_VarChar,
				},
			},
		},
		MakeBlobsReader(blobs))
	if err != nil {
		return nil, err
	}
	return NewDeserializeReader(reader, func(r Record, v []*DeleteLog) error {
		for i := 0; i < r.Len(); i++ {
			if v[i] == nil {
				v[i] = &DeleteLog{}
			}
			// retrieve the only field
			a := r.(*compositeRecord).recs[0].(*array.String)
			strVal := a.Value(i)
			if err := v[i].Parse(strVal); err != nil {
				return err
			}
		}
		return nil
	}), nil
}

type BinlogStreamWriter struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	fieldSchema  *schemapb.FieldSchema

	buf bytes.Buffer
	rw  *singleFieldRecordWriter
}

func (bsw *BinlogStreamWriter) GetRecordWriter() (RecordWriter, error) {
	if bsw.rw != nil {
		return bsw.rw, nil
	}

	rw, err := newSingleFieldRecordWriter(bsw.fieldSchema, &bsw.buf, WithRecordWriterProps(getFieldWriterProps(bsw.fieldSchema)))
	if err != nil {
		return nil, err
	}
	bsw.rw = rw
	return rw, nil
}

func (bsw *BinlogStreamWriter) Finalize() (*Blob, error) {
	if bsw.rw == nil {
		return nil, io.ErrUnexpectedEOF
	}
	bsw.rw.Close()

	var b bytes.Buffer
	if err := bsw.writeBinlogHeaders(&b); err != nil {
		return nil, err
	}
	if _, err := b.Write(bsw.buf.Bytes()); err != nil {
		return nil, err
	}
	return &Blob{
		Key:        strconv.Itoa(int(bsw.fieldSchema.FieldID)),
		Value:      b.Bytes(),
		RowNum:     int64(bsw.rw.numRows),
		MemorySize: int64(bsw.rw.writtenUncompressed),
	}, nil
}

func (bsw *BinlogStreamWriter) writeBinlogHeaders(w io.Writer) error {
	// Write magic number
	if err := binary.Write(w, common.Endian, MagicNumber); err != nil {
		return err
	}
	// Write descriptor
	de := NewBaseDescriptorEvent(bsw.collectionID, bsw.partitionID, bsw.segmentID)
	de.PayloadDataType = bsw.fieldSchema.DataType
	de.FieldID = bsw.fieldSchema.FieldID
	de.descriptorEventData.AddExtra(originalSizeKey, strconv.Itoa(int(bsw.rw.writtenUncompressed)))
	de.descriptorEventData.AddExtra(nullableKey, bsw.fieldSchema.Nullable)
	if err := de.Write(w); err != nil {
		return err
	}
	// Write event header
	eh := newEventHeader(InsertEventType)
	// Write event data
	ev := newInsertEventData()
	ev.StartTimestamp = 1
	ev.EndTimestamp = 1
	eh.EventLength = int32(bsw.buf.Len()) + eh.GetMemoryUsageInBytes() + int32(binary.Size(ev))
	// eh.NextPosition = eh.EventLength + w.Offset()
	if err := eh.Write(w); err != nil {
		return err
	}
	if err := ev.WriteEventData(w); err != nil {
		return err
	}
	return nil
}

func NewBinlogStreamWriters(collectionID, partitionID, segmentID UniqueID,
	schema []*schemapb.FieldSchema,
) map[FieldID]*BinlogStreamWriter {
	bws := make(map[FieldID]*BinlogStreamWriter, len(schema))
	for _, f := range schema {
		bws[f.FieldID] = &BinlogStreamWriter{
			collectionID: collectionID,
			partitionID:  partitionID,
			segmentID:    segmentID,
			fieldSchema:  f,
		}
	}
	return bws
}

func ValueSerializer(v []*Value, fieldSchema []*schemapb.FieldSchema) (Record, error) {
	builders := make(map[FieldID]array.Builder, len(fieldSchema))
	types := make(map[FieldID]schemapb.DataType, len(fieldSchema))
	for _, f := range fieldSchema {
		dim, _ := typeutil.GetDim(f)
		builders[f.FieldID] = array.NewBuilder(memory.DefaultAllocator, serdeMap[f.DataType].arrowType(int(dim)))
		types[f.FieldID] = f.DataType
	}

	for _, vv := range v {
		m := vv.Value.(map[FieldID]any)

		for fid, e := range m {
			typeEntry, ok := serdeMap[types[fid]]
			if !ok {
				panic("unknown type")
			}
			ok = typeEntry.serialize(builders[fid], e)
			if !ok {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("serialize error on type %s", types[fid]))
			}
		}
	}
	arrays := make([]arrow.Array, len(fieldSchema))
	fields := make([]arrow.Field, len(fieldSchema))
	field2Col := make(map[FieldID]int, len(fieldSchema))
	for i, field := range fieldSchema {
		builder := builders[field.FieldID]
		arrays[i] = builder.NewArray()
		builder.Release()
		fields[i] = ConvertToArrowField(field, arrays[i].DataType())
		field2Col[field.FieldID] = i
	}
	return NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema(fields, nil), arrays, int64(len(v))), field2Col), nil
}

type BinlogRecordWriter interface {
	RecordWriter
	GetLogs() (
		fieldBinlogs map[FieldID]*datapb.FieldBinlog,
		statsLog *datapb.FieldBinlog,
		bm25StatsLog map[FieldID]*datapb.FieldBinlog,
	)
	GetRowNum() int64
	FlushChunk() error
	GetBufferUncompressed() uint64
	Schema() *schemapb.CollectionSchema
}

type ChunkedBlobsWriter func([]*Blob) error

type CompositeBinlogRecordWriter struct {
	// attributes
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	schema       *schemapb.CollectionSchema
	BlobsWriter  ChunkedBlobsWriter
	allocator    allocator.Interface
	chunkSize    uint64
	rootPath     string
	maxRowNum    int64
	pkstats      *PrimaryKeyStats
	bm25Stats    map[int64]*BM25Stats

	// writers and stats generated at runtime
	fieldWriters map[FieldID]*BinlogStreamWriter
	rw           RecordWriter
	tsFrom       typeutil.Timestamp
	tsTo         typeutil.Timestamp
	rowNum       int64

	// results
	fieldBinlogs map[FieldID]*datapb.FieldBinlog
	statsLog     *datapb.FieldBinlog
	bm25StatsLog map[FieldID]*datapb.FieldBinlog

	flushedUncompressed uint64
}

var _ BinlogRecordWriter = (*CompositeBinlogRecordWriter)(nil)

func (c *CompositeBinlogRecordWriter) Write(r Record) error {
	if err := c.initWriters(); err != nil {
		return err
	}

	tsArray := r.Column(common.TimeStampField).(*array.Int64)
	rows := r.Len()
	for i := 0; i < rows; i++ {
		ts := typeutil.Timestamp(tsArray.Value(i))
		if ts < c.tsFrom {
			c.tsFrom = ts
		}
		if ts > c.tsTo {
			c.tsTo = ts
		}

		switch schemapb.DataType(c.pkstats.PkType) {
		case schemapb.DataType_Int64:
			pkArray := r.Column(c.pkstats.FieldID).(*array.Int64)
			pk := &Int64PrimaryKey{
				Value: pkArray.Value(i),
			}
			c.pkstats.Update(pk)
		case schemapb.DataType_VarChar:
			pkArray := r.Column(c.pkstats.FieldID).(*array.String)
			pk := &VarCharPrimaryKey{
				Value: pkArray.Value(i),
			}
			c.pkstats.Update(pk)
		default:
			panic("invalid data type")
		}

		for fieldID, stats := range c.bm25Stats {
			field, ok := r.Column(fieldID).(*array.Binary)
			if !ok {
				return fmt.Errorf("bm25 field value not found")
			}
			stats.AppendBytes(field.Value(i))
		}
	}

	if err := c.rw.Write(r); err != nil {
		return err
	}
	c.rowNum += int64(rows)

	// flush if size exceeds chunk size
	if c.rw.GetWrittenUncompressed() >= c.chunkSize {
		return c.FlushChunk()
	}

	return nil
}

func (c *CompositeBinlogRecordWriter) initWriters() error {
	if c.rw == nil {
		c.fieldWriters = NewBinlogStreamWriters(c.collectionID, c.partitionID, c.segmentID, c.schema.Fields)
		rws := make(map[FieldID]RecordWriter, len(c.fieldWriters))
		for fid, w := range c.fieldWriters {
			rw, err := w.GetRecordWriter()
			if err != nil {
				return err
			}
			rws[fid] = rw
		}
		c.rw = NewCompositeRecordWriter(rws)
	}
	return nil
}

func (c *CompositeBinlogRecordWriter) resetWriters() {
	c.fieldWriters = nil
	c.rw = nil
	c.tsFrom = math.MaxUint64
	c.tsTo = 0
}

func (c *CompositeBinlogRecordWriter) Close() error {
	if err := c.writeStats(); err != nil {
		return err
	}
	if err := c.writeBm25Stats(); err != nil {
		return err
	}
	if c.rw != nil {
		// if rw is not nil, it means there is data to be flushed
		if err := c.FlushChunk(); err != nil {
			return err
		}
	}
	return nil
}

func (c *CompositeBinlogRecordWriter) GetBufferUncompressed() uint64 {
	if c.rw == nil {
		return 0
	}
	return c.rw.GetWrittenUncompressed()
}

func (c *CompositeBinlogRecordWriter) GetWrittenUncompressed() uint64 {
	return c.flushedUncompressed + c.GetBufferUncompressed()
}

func (c *CompositeBinlogRecordWriter) FlushChunk() error {
	if c.fieldWriters == nil {
		return nil
	}

	id, _, err := c.allocator.Alloc(uint32(len(c.fieldWriters)))
	if err != nil {
		return err
	}
	blobs := make(map[FieldID]*Blob, len(c.fieldWriters))
	for fid, w := range c.fieldWriters {
		b, err := w.Finalize()
		if err != nil {
			return err
		}
		// assign blob key
		b.Key = metautil.BuildInsertLogPath(c.rootPath, c.collectionID, c.partitionID, c.segmentID, fid, id)
		blobs[fid] = b
		id++
	}
	if err := c.BlobsWriter(lo.Values(blobs)); err != nil {
		return err
	}

	// attach binlogs
	if c.fieldBinlogs == nil {
		c.fieldBinlogs = make(map[FieldID]*datapb.FieldBinlog, len(c.fieldWriters))
		for fid := range c.fieldWriters {
			c.fieldBinlogs[fid] = &datapb.FieldBinlog{
				FieldID: fid,
			}
		}
	}

	for fid, b := range blobs {
		c.fieldBinlogs[fid].Binlogs = append(c.fieldBinlogs[fid].Binlogs, &datapb.Binlog{
			LogSize:       int64(len(b.Value)),
			MemorySize:    b.MemorySize,
			LogPath:       b.Key,
			EntriesNum:    b.RowNum,
			TimestampFrom: c.tsFrom,
			TimestampTo:   c.tsTo,
		})
	}

	c.flushedUncompressed += c.rw.GetWrittenUncompressed()

	// reset writers
	c.resetWriters()
	return nil
}

func (c *CompositeBinlogRecordWriter) Schema() *schemapb.CollectionSchema {
	return c.schema
}

func (c *CompositeBinlogRecordWriter) writeStats() error {
	if c.pkstats == nil {
		return nil
	}

	id, err := c.allocator.AllocOne()
	if err != nil {
		return err
	}

	codec := NewInsertCodecWithSchema(&etcdpb.CollectionMeta{
		ID:     c.collectionID,
		Schema: c.schema,
	})
	sblob, err := codec.SerializePkStats(c.pkstats, c.rowNum)
	if err != nil {
		return err
	}

	sblob.Key = metautil.BuildStatsLogPath(c.rootPath,
		c.collectionID, c.partitionID, c.segmentID, c.pkstats.FieldID, id)

	if err := c.BlobsWriter([]*Blob{sblob}); err != nil {
		return err
	}

	c.statsLog = &datapb.FieldBinlog{
		FieldID: c.pkstats.FieldID,
		Binlogs: []*datapb.Binlog{
			{
				LogSize:    int64(len(sblob.GetValue())),
				MemorySize: int64(len(sblob.GetValue())),
				LogPath:    sblob.Key,
				EntriesNum: c.rowNum,
			},
		},
	}
	return nil
}

func (c *CompositeBinlogRecordWriter) writeBm25Stats() error {
	if len(c.bm25Stats) == 0 {
		return nil
	}
	id, _, err := c.allocator.Alloc(uint32(len(c.bm25Stats)))
	if err != nil {
		return err
	}

	if c.bm25StatsLog == nil {
		c.bm25StatsLog = make(map[FieldID]*datapb.FieldBinlog)
	}
	for fid, stats := range c.bm25Stats {
		bytes, err := stats.Serialize()
		if err != nil {
			return err
		}
		key := metautil.BuildBm25LogPath(c.rootPath,
			c.collectionID, c.partitionID, c.segmentID, fid, id)
		blob := &Blob{
			Key:        key,
			Value:      bytes,
			RowNum:     stats.NumRow(),
			MemorySize: int64(len(bytes)),
		}
		if err := c.BlobsWriter([]*Blob{blob}); err != nil {
			return err
		}

		fieldLog := &datapb.FieldBinlog{
			FieldID: fid,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:    int64(len(blob.GetValue())),
					MemorySize: int64(len(blob.GetValue())),
					LogPath:    key,
					EntriesNum: c.rowNum,
				},
			},
		}

		c.bm25StatsLog[fid] = fieldLog
		id++
	}

	return nil
}

func (c *CompositeBinlogRecordWriter) GetLogs() (
	fieldBinlogs map[FieldID]*datapb.FieldBinlog,
	statsLog *datapb.FieldBinlog,
	bm25StatsLog map[FieldID]*datapb.FieldBinlog,
) {
	return c.fieldBinlogs, c.statsLog, c.bm25StatsLog
}

func (c *CompositeBinlogRecordWriter) GetRowNum() int64 {
	return c.rowNum
}

func newCompositeBinlogRecordWriter(collectionID, partitionID, segmentID UniqueID, schema *schemapb.CollectionSchema,
	blobsWriter ChunkedBlobsWriter, allocator allocator.Interface, chunkSize uint64, rootPath string, maxRowNum int64,
) (*CompositeBinlogRecordWriter, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
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

	return &CompositeBinlogRecordWriter{
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		schema:       schema,
		BlobsWriter:  blobsWriter,
		allocator:    allocator,
		chunkSize:    chunkSize,
		rootPath:     rootPath,
		maxRowNum:    maxRowNum,
		pkstats:      stats,
		bm25Stats:    bm25Stats,
	}, nil
}

// BinlogValueWriter is a BinlogRecordWriter with SerializeWriter[*Value] mixin.
type BinlogValueWriter struct {
	BinlogRecordWriter
	SerializeWriter[*Value]
}

func (b *BinlogValueWriter) Close() error {
	return b.SerializeWriter.Close()
}

func NewBinlogValueWriter(rw BinlogRecordWriter, batchSize int,
) *BinlogValueWriter {
	return &BinlogValueWriter{
		BinlogRecordWriter: rw,
		SerializeWriter: NewSerializeRecordWriter(rw, func(v []*Value) (Record, error) {
			return ValueSerializer(v, rw.Schema().Fields)
		}, batchSize),
	}
}

// deprecated, use NewBinlogValueWriter instead
type BinlogSerializeWriter struct {
	RecordWriter
	SerializeWriter[*Value]
}

func (b *BinlogSerializeWriter) Close() error {
	return b.SerializeWriter.Close()
}

func NewBinlogSerializeWriter(schema *schemapb.CollectionSchema, partitionID, segmentID UniqueID,
	eventWriters map[FieldID]*BinlogStreamWriter, batchSize int,
) (*BinlogSerializeWriter, error) {
	rws := make(map[FieldID]RecordWriter, len(eventWriters))
	for fid := range eventWriters {
		w := eventWriters[fid]
		rw, err := w.GetRecordWriter()
		if err != nil {
			return nil, err
		}
		rws[fid] = rw
	}
	compositeRecordWriter := NewCompositeRecordWriter(rws)
	return &BinlogSerializeWriter{
		RecordWriter: compositeRecordWriter,
		SerializeWriter: NewSerializeRecordWriter[*Value](compositeRecordWriter, func(v []*Value) (Record, error) {
			return ValueSerializer(v, schema.Fields)
		}, batchSize),
	}, nil
}

type DeltalogStreamWriter struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	fieldSchema  *schemapb.FieldSchema

	buf bytes.Buffer
	rw  *singleFieldRecordWriter
}

func (dsw *DeltalogStreamWriter) GetRecordWriter() (RecordWriter, error) {
	if dsw.rw != nil {
		return dsw.rw, nil
	}
	rw, err := newSingleFieldRecordWriter(dsw.fieldSchema, &dsw.buf, WithRecordWriterProps(getFieldWriterProps(dsw.fieldSchema)))
	if err != nil {
		return nil, err
	}
	dsw.rw = rw
	return rw, nil
}

func (dsw *DeltalogStreamWriter) Finalize() (*Blob, error) {
	if dsw.rw == nil {
		return nil, io.ErrUnexpectedEOF
	}
	dsw.rw.Close()

	var b bytes.Buffer
	if err := dsw.writeDeltalogHeaders(&b); err != nil {
		return nil, err
	}
	if _, err := b.Write(dsw.buf.Bytes()); err != nil {
		return nil, err
	}
	return &Blob{
		Value:      b.Bytes(),
		RowNum:     int64(dsw.rw.numRows),
		MemorySize: int64(dsw.rw.writtenUncompressed),
	}, nil
}

func (dsw *DeltalogStreamWriter) writeDeltalogHeaders(w io.Writer) error {
	// Write magic number
	if err := binary.Write(w, common.Endian, MagicNumber); err != nil {
		return err
	}
	// Write descriptor
	de := NewBaseDescriptorEvent(dsw.collectionID, dsw.partitionID, dsw.segmentID)
	de.PayloadDataType = dsw.fieldSchema.DataType
	de.descriptorEventData.AddExtra(originalSizeKey, strconv.Itoa(int(dsw.rw.writtenUncompressed)))
	if err := de.Write(w); err != nil {
		return err
	}
	// Write event header
	eh := newEventHeader(DeleteEventType)
	// Write event data
	ev := newDeleteEventData()
	ev.StartTimestamp = 1
	ev.EndTimestamp = 1
	eh.EventLength = int32(dsw.buf.Len()) + eh.GetMemoryUsageInBytes() + int32(binary.Size(ev))
	// eh.NextPosition = eh.EventLength + w.Offset()
	if err := eh.Write(w); err != nil {
		return err
	}
	if err := ev.WriteEventData(w); err != nil {
		return err
	}
	return nil
}

func newDeltalogStreamWriter(collectionID, partitionID, segmentID UniqueID) *DeltalogStreamWriter {
	return &DeltalogStreamWriter{
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		fieldSchema: &schemapb.FieldSchema{
			FieldID:  common.RowIDField,
			Name:     "delta",
			DataType: schemapb.DataType_String,
		},
	}
}

func newDeltalogSerializeWriter(eventWriter *DeltalogStreamWriter, batchSize int) (*SerializeWriterImpl[*DeleteLog], error) {
	rws := make(map[FieldID]RecordWriter, 1)
	rw, err := eventWriter.GetRecordWriter()
	if err != nil {
		return nil, err
	}
	rws[0] = rw
	compositeRecordWriter := NewCompositeRecordWriter(rws)
	return NewSerializeRecordWriter(compositeRecordWriter, func(v []*DeleteLog) (Record, error) {
		builder := array.NewBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String)

		for _, vv := range v {
			strVal, err := json.Marshal(vv)
			if err != nil {
				return nil, err
			}

			builder.AppendValueFromString(string(strVal))
		}
		arr := []arrow.Array{builder.NewArray()}
		field := []arrow.Field{{
			Name:     "delta",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
		}}
		field2Col := map[FieldID]int{
			0: 0,
		}
		return NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema(field, nil), arr, int64(len(v))), field2Col), nil
	}, batchSize), nil
}

var _ RecordReader = (*simpleArrowRecordReader)(nil)

type simpleArrowRecordReader struct {
	blobs []*Blob

	blobPos int
	rr      array.RecordReader
	closer  func()

	r simpleArrowRecord
}

func (crr *simpleArrowRecordReader) iterateNextBatch() error {
	if crr.closer != nil {
		crr.closer()
	}

	crr.blobPos++
	if crr.blobPos >= len(crr.blobs) {
		return io.EOF
	}

	reader, err := NewBinlogReader(crr.blobs[crr.blobPos].Value)
	if err != nil {
		return err
	}

	er, err := reader.NextEventReader()
	if err != nil {
		return err
	}
	rr, err := er.GetArrowRecordReader()
	if err != nil {
		return err
	}
	crr.rr = rr
	crr.closer = func() {
		crr.rr.Release()
		er.Close()
		reader.Close()
	}

	return nil
}

func (crr *simpleArrowRecordReader) Next() (Record, error) {
	if crr.rr == nil {
		if len(crr.blobs) == 0 {
			return nil, io.EOF
		}
		crr.blobPos = -1
		crr.r = simpleArrowRecord{
			field2Col: make(map[FieldID]int),
		}
		if err := crr.iterateNextBatch(); err != nil {
			return nil, err
		}
	}

	composeRecord := func() bool {
		if ok := crr.rr.Next(); !ok {
			return false
		}
		record := crr.rr.Record()
		for i := range record.Schema().Fields() {
			crr.r.field2Col[FieldID(i)] = i
		}
		crr.r.r = record
		return true
	}

	if ok := composeRecord(); !ok {
		if err := crr.iterateNextBatch(); err != nil {
			return nil, err
		}
		if ok := composeRecord(); !ok {
			return nil, io.EOF
		}
	}
	return &crr.r, nil
}

func (crr *simpleArrowRecordReader) Close() error {
	if crr.closer != nil {
		crr.closer()
	}
	return nil
}

func newSimpleArrowRecordReader(blobs []*Blob) (*simpleArrowRecordReader, error) {
	return &simpleArrowRecordReader{
		blobs: blobs,
	}, nil
}

func newMultiFieldDeltalogStreamWriter(collectionID, partitionID, segmentID UniqueID, pkType schemapb.DataType) *MultiFieldDeltalogStreamWriter {
	return &MultiFieldDeltalogStreamWriter{
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		pkType:       pkType,
	}
}

type MultiFieldDeltalogStreamWriter struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	pkType       schemapb.DataType

	buf bytes.Buffer
	rw  *multiFieldRecordWriter
}

func (dsw *MultiFieldDeltalogStreamWriter) GetRecordWriter() (RecordWriter, error) {
	if dsw.rw != nil {
		return dsw.rw, nil
	}

	fieldIDs := []FieldID{common.RowIDField, common.TimeStampField} // Not used.
	fields := []arrow.Field{
		{
			Name:     "pk",
			Type:     serdeMap[dsw.pkType].arrowType(0),
			Nullable: false,
		},
		{
			Name:     "ts",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
		},
	}

	rw, err := newMultiFieldRecordWriter(fieldIDs, fields, &dsw.buf)
	if err != nil {
		return nil, err
	}
	dsw.rw = rw
	return rw, nil
}

func (dsw *MultiFieldDeltalogStreamWriter) Finalize() (*Blob, error) {
	if dsw.rw == nil {
		return nil, io.ErrUnexpectedEOF
	}
	dsw.rw.Close()

	var b bytes.Buffer
	if err := dsw.writeDeltalogHeaders(&b); err != nil {
		return nil, err
	}
	if _, err := b.Write(dsw.buf.Bytes()); err != nil {
		return nil, err
	}
	return &Blob{
		Value:      b.Bytes(),
		RowNum:     int64(dsw.rw.numRows),
		MemorySize: int64(dsw.rw.writtenUncompressed),
	}, nil
}

func (dsw *MultiFieldDeltalogStreamWriter) writeDeltalogHeaders(w io.Writer) error {
	// Write magic number
	if err := binary.Write(w, common.Endian, MagicNumber); err != nil {
		return err
	}
	// Write descriptor
	de := NewBaseDescriptorEvent(dsw.collectionID, dsw.partitionID, dsw.segmentID)
	de.PayloadDataType = schemapb.DataType_Int64
	de.descriptorEventData.AddExtra(originalSizeKey, strconv.Itoa(int(dsw.rw.writtenUncompressed)))
	de.descriptorEventData.AddExtra(version, MultiField)
	if err := de.Write(w); err != nil {
		return err
	}
	// Write event header
	eh := newEventHeader(DeleteEventType)
	// Write event data
	ev := newDeleteEventData()
	ev.StartTimestamp = 1
	ev.EndTimestamp = 1
	eh.EventLength = int32(dsw.buf.Len()) + eh.GetMemoryUsageInBytes() + int32(binary.Size(ev))
	// eh.NextPosition = eh.EventLength + w.Offset()
	if err := eh.Write(w); err != nil {
		return err
	}
	if err := ev.WriteEventData(w); err != nil {
		return err
	}
	return nil
}

func newDeltalogMultiFieldWriter(eventWriter *MultiFieldDeltalogStreamWriter, batchSize int) (*SerializeWriterImpl[*DeleteLog], error) {
	rw, err := eventWriter.GetRecordWriter()
	if err != nil {
		return nil, err
	}
	return NewSerializeRecordWriter[*DeleteLog](rw, func(v []*DeleteLog) (Record, error) {
		fields := []arrow.Field{
			{
				Name:     "pk",
				Type:     serdeMap[schemapb.DataType(v[0].PkType)].arrowType(0),
				Nullable: false,
			},
			{
				Name:     "ts",
				Type:     arrow.PrimitiveTypes.Int64,
				Nullable: false,
			},
		}
		arrowSchema := arrow.NewSchema(fields, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer builder.Release()

		pkType := schemapb.DataType(v[0].PkType)
		switch pkType {
		case schemapb.DataType_Int64:
			pb := builder.Field(0).(*array.Int64Builder)
			for _, vv := range v {
				pk := vv.Pk.GetValue().(int64)
				pb.Append(pk)
			}
		case schemapb.DataType_VarChar:
			pb := builder.Field(0).(*array.StringBuilder)
			for _, vv := range v {
				pk := vv.Pk.GetValue().(string)
				pb.Append(pk)
			}
		default:
			return nil, fmt.Errorf("unexpected pk type %v", v[0].PkType)
		}

		for _, vv := range v {
			builder.Field(1).(*array.Int64Builder).Append(int64(vv.Ts))
		}

		arr := []arrow.Array{builder.Field(0).NewArray(), builder.Field(1).NewArray()}

		field2Col := map[FieldID]int{
			common.RowIDField:     0,
			common.TimeStampField: 1,
		}
		return NewSimpleArrowRecord(array.NewRecord(arrowSchema, arr, int64(len(v))), field2Col), nil
	}, batchSize), nil
}

func newDeltalogMultiFieldReader(blobs []*Blob) (*DeserializeReaderImpl[*DeleteLog], error) {
	reader, err := newSimpleArrowRecordReader(blobs)
	if err != nil {
		return nil, err
	}
	return NewDeserializeReader(reader, func(r Record, v []*DeleteLog) error {
		rec, ok := r.(*simpleArrowRecord)
		if !ok {
			return fmt.Errorf("can not cast to simple arrow record")
		}
		fields := rec.r.Schema().Fields()
		switch fields[0].Type.ID() {
		case arrow.INT64:
			arr := r.Column(0).(*array.Int64)
			for j := 0; j < r.Len(); j++ {
				if v[j] == nil {
					v[j] = &DeleteLog{}
				}
				v[j].Pk = NewInt64PrimaryKey(arr.Value(j))
			}
		case arrow.STRING:
			arr := r.Column(0).(*array.String)
			for j := 0; j < r.Len(); j++ {
				if v[j] == nil {
					v[j] = &DeleteLog{}
				}
				v[j].Pk = NewVarCharPrimaryKey(arr.Value(j))
			}
		default:
			return fmt.Errorf("unexpected delta log pkType %v", fields[0].Type.Name())
		}

		arr := r.Column(1).(*array.Int64)
		for j := 0; j < r.Len(); j++ {
			v[j].Ts = uint64(arr.Value(j))
		}
		return nil
	}), nil
}

// NewDeltalogDeserializeReader is the entry point for the delta log reader.
// It includes NewDeltalogOneFieldReader, which uses the existing log format with only one column in a log file,
// and NewDeltalogMultiFieldReader, which uses the new format and supports multiple fields in a log file.
func newDeltalogDeserializeReader(blobs []*Blob) (*DeserializeReaderImpl[*DeleteLog], error) {
	if supportMultiFieldFormat(blobs) {
		return newDeltalogMultiFieldReader(blobs)
	}
	return newDeltalogOneFieldReader(blobs)
}

// check delta log description data to see if it is the format with
// pk and ts column separately
func supportMultiFieldFormat(blobs []*Blob) bool {
	if len(blobs) > 0 {
		reader, err := NewBinlogReader(blobs[0].Value)
		if err != nil {
			return false
		}
		defer reader.Close()
		version := reader.descriptorEventData.Extras[version]
		return version != nil && version.(string) == MultiField
	}
	return false
}

func CreateDeltalogReader(blobs []*Blob) (*DeserializeReaderImpl[*DeleteLog], error) {
	return newDeltalogDeserializeReader(blobs)
}

func CreateDeltalogWriter(collectionID, partitionID, segmentID UniqueID, pkType schemapb.DataType, batchSize int,
) (*SerializeWriterImpl[*DeleteLog], func() (*Blob, error), error) {
	format := paramtable.Get().DataNodeCfg.DeltalogFormat.GetValue()
	if format == "json" {
		eventWriter := newDeltalogStreamWriter(collectionID, partitionID, segmentID)
		writer, err := newDeltalogSerializeWriter(eventWriter, batchSize)
		return writer, eventWriter.Finalize, err
	} else if format == "parquet" {
		eventWriter := newMultiFieldDeltalogStreamWriter(collectionID, partitionID, segmentID, pkType)
		writer, err := newDeltalogMultiFieldWriter(eventWriter, batchSize)
		return writer, eventWriter.Finalize, err
	}
	return nil, nil, merr.WrapErrParameterInvalid("unsupported deltalog format %s", format)
}
