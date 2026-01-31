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
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

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

func newCompositeBinlogRecordReader(
	schema *schemapb.CollectionSchema,
	neededFields typeutil.Set[int64],
	blobs []*Blob,
) (*CompositeBinlogRecordReader, error) {
	allFields := typeutil.GetAllFieldSchemas(schema)
	if neededFields != nil {
		allFields = lo.Filter(allFields, func(field *schemapb.FieldSchema, _ int) bool {
			return neededFields.Contain(field.GetFieldID())
		})
	}

	idx := 0
	index := make(map[FieldID]int16)
	fields := make(map[FieldID]*schemapb.FieldSchema)
	for _, f := range allFields {
		index[f.FieldID] = int16(idx)
		fields[f.FieldID] = f
		idx++
	}

	rrs := make([]array.RecordReader, len(allFields))
	brs := make([]*BinlogReader, len(allFields))
	for _, b := range blobs {
		reader, err := NewBinlogReader(b.Value)
		if err != nil {
			return nil, err
		}

		er, err := reader.NextEventReader()
		if err != nil {
			return nil, err
		}
		i := index[reader.FieldID]
		rr, err := er.GetArrowRecordReader()
		if err != nil {
			return nil, err
		}
		rrs[i] = rr
		brs[i] = reader
	}

	return &CompositeBinlogRecordReader{
		fields: fields,
		index:  index,
		rrs:    rrs,
		brs:    brs,
	}, nil
}

func newIterativeCompositeBinlogRecordReader(
	schema *schemapb.CollectionSchema,
	neededFields typeutil.Set[int64],
	chunkedBlobs ChunkedBlobsReader,
) *IterativeRecordReader {
	return &IterativeRecordReader{
		iterate: func() (RecordReader, error) {
			blobs, err := chunkedBlobs()
			if err != nil {
				return nil, err
			}
			return newCompositeBinlogRecordReader(schema, neededFields, blobs)
		},
	}
}

func ValueDeserializerWithSelectedFields(r Record, v []*Value, fieldSchema []*schemapb.FieldSchema, shouldCopy bool) error {
	return valueDeserializer(r, v, fieldSchema, shouldCopy)
}

func ValueDeserializerWithSchema(r Record, v []*Value, schema *schemapb.CollectionSchema, shouldCopy bool) error {
	allFields := typeutil.GetAllFieldSchemas(schema)
	return valueDeserializer(r, v, allFields, shouldCopy)
}

func valueDeserializer(r Record, v []*Value, fields []*schemapb.FieldSchema, shouldCopy bool) error {
	pkField := func() *schemapb.FieldSchema {
		for _, field := range fields {
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
			value.Value = make(map[FieldID]interface{}, len(fields))
			v[i] = value
		}

		m := value.Value.(map[FieldID]interface{})
		for _, f := range fields {
			j := f.FieldID
			dt := f.DataType
			if r.Column(j).IsNull(i) {
				if f.GetDefaultValue() != nil {
					m[j] = GetDefaultValue(f)
				} else {
					m[j] = nil
				}
			} else {
				// Get element type and dim for ArrayOfVector, otherwise use defaults
				elementType := schemapb.DataType_None
				dim := 0
				if typeutil.IsVectorType(dt) {
					dimValue, _ := typeutil.GetDim(f)
					dim = int(dimValue)
				}
				if dt == schemapb.DataType_ArrayOfVector {
					elementType = f.GetElementType()
				}

				d, err := serdeMap[dt].deserialize(r.Column(j), i, elementType, dim, shouldCopy)
				if err != nil {
					return merr.WrapErrServiceInternal(fmt.Sprintf("deserialize error on type %s: %v", dt, err))
				}
				m[j] = d // TODO: avoid memory copy here.
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

func NewBinlogDeserializeReader(schema *schemapb.CollectionSchema, blobsReader ChunkedBlobsReader, shouldCopy bool,
) (*DeserializeReaderImpl[*Value], error) {
	reader := newIterativeCompositeBinlogRecordReader(schema, nil, blobsReader)
	return NewDeserializeReader(reader, func(r Record, v []*Value) error {
		return ValueDeserializerWithSchema(r, v, schema, shouldCopy)
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

	eh := newEventHeader(InsertEventType)

	ev := newInsertEventData()
	ev.StartTimestamp = 1
	ev.EndTimestamp = 1
	eh.EventLength = int32(bsw.buf.Len()) + eh.GetMemoryUsageInBytes() + int32(binary.Size(ev))
	// eh.NextPosition = eh.EventLength + w.Offset()

	// Write event header
	if err := eh.Write(&b); err != nil {
		return nil, err
	}

	// Write event data, which ic startTs and endTs for insert event data
	if err := ev.WriteEventData(&b); err != nil {
		return nil, err
	}

	if err := binary.Write(&b, common.Endian, bsw.buf.Bytes()); err != nil {
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
	return nil
}

func newBinlogWriter(collectionID, partitionID, segmentID UniqueID,
	field *schemapb.FieldSchema,
) *BinlogStreamWriter {
	return &BinlogStreamWriter{
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		fieldSchema:  field,
	}
}

func NewBinlogStreamWriters(collectionID, partitionID, segmentID UniqueID,
	schema *schemapb.CollectionSchema,
) map[FieldID]*BinlogStreamWriter {
	bws := make(map[FieldID]*BinlogStreamWriter)

	for _, f := range schema.Fields {
		writer := newBinlogWriter(collectionID, partitionID, segmentID, f)
		bws[f.FieldID] = writer
	}

	for _, structField := range schema.StructArrayFields {
		for _, subField := range structField.Fields {
			writer := newBinlogWriter(collectionID, partitionID, segmentID, subField)
			bws[subField.FieldID] = writer
		}
	}

	return bws
}

func ValueSerializer(v []*Value, schema *schemapb.CollectionSchema) (Record, error) {
	allFieldsSchema := schema.Fields
	for _, structField := range schema.StructArrayFields {
		allFieldsSchema = append(allFieldsSchema, structField.Fields...)
	}

	builders := make(map[FieldID]array.Builder, len(allFieldsSchema))
	types := make(map[FieldID]schemapb.DataType, len(allFieldsSchema))
	elementTypes := make(map[FieldID]schemapb.DataType, len(allFieldsSchema)) // For ArrayOfVector
	for _, f := range allFieldsSchema {
		dim, _ := typeutil.GetDim(f)

		elementType := schemapb.DataType_None
		if f.DataType == schemapb.DataType_ArrayOfVector {
			elementType = f.GetElementType()
			elementTypes[f.FieldID] = elementType
		}

		arrowType := serdeMap[f.DataType].arrowType(int(dim), elementType)
		builders[f.FieldID] = array.NewBuilder(memory.DefaultAllocator, arrowType)
		builders[f.FieldID].Reserve(len(v)) // reserve space to avoid copy
		types[f.FieldID] = f.DataType
	}

	for _, vv := range v {
		m := vv.Value.(map[FieldID]any)

		for fid, e := range m {
			typeEntry, ok := serdeMap[types[fid]]
			if !ok {
				panic("unknown type")
			}

			// Get element type for ArrayOfVector, otherwise use None
			elementType := schemapb.DataType_None
			if types[fid] == schemapb.DataType_ArrayOfVector {
				elementType = elementTypes[fid]
			}

			if err := typeEntry.serialize(builders[fid], e, elementType); err != nil {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("serialize error on type %s: %v", types[fid], err))
			}
		}
	}
	arrays := make([]arrow.Array, len(allFieldsSchema))
	fields := make([]arrow.Field, len(allFieldsSchema))
	field2Col := make(map[FieldID]int, len(allFieldsSchema))
	for i, field := range allFieldsSchema {
		builder := builders[field.FieldID]
		arrays[i] = builder.NewArray()
		builder.Release()
		fields[i] = ConvertToArrowField(field, arrays[i].DataType(), false)
		field2Col[field.FieldID] = i
	}
	return NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema(fields, nil), arrays, int64(len(v))), field2Col), nil
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

	// writers and stats generated at runtime
	fieldWriters  map[FieldID]*BinlogStreamWriter
	rw            RecordWriter
	pkCollector   *PkStatsCollector
	bm25Collector *Bm25StatsCollector
	tsFrom        typeutil.Timestamp
	tsTo          typeutil.Timestamp
	rowNum        int64

	// results
	fieldBinlogs map[FieldID]*datapb.FieldBinlog
	statsLog     *datapb.FieldBinlog
	bm25StatsLog map[FieldID]*datapb.FieldBinlog

	flushedUncompressed uint64

	ttlFieldID     int64
	ttlFieldValues []int64
}

var _ BinlogRecordWriter = (*CompositeBinlogRecordWriter)(nil)

func (c *CompositeBinlogRecordWriter) Write(r Record) error {
	if err := c.initWriters(); err != nil {
		return err
	}

	// Track timestamps
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
	}

	// not system column
	if c.ttlFieldID >= common.StartOfUserFieldID {
		// Defensive check to prevent panic
		ttlColumn := r.Column(c.ttlFieldID)
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
			c.ttlFieldValues = append(c.ttlFieldValues, ttlValue)
		}
	}

	// Collect statistics
	if err := c.pkCollector.Collect(r); err != nil {
		return err
	}
	if err := c.bm25Collector.Collect(r); err != nil {
		return err
	}

	if err := c.rw.Write(r); err != nil {
		return err
	}
	c.rowNum += int64(r.Len())

	// flush if size exceeds chunk size
	if c.rw.GetWrittenUncompressed() >= c.chunkSize {
		return c.FlushChunk()
	}

	return nil
}

func (c *CompositeBinlogRecordWriter) initWriters() error {
	if c.rw == nil {
		c.fieldWriters = NewBinlogStreamWriters(c.collectionID, c.partitionID, c.segmentID, c.schema)
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
	if c.rw != nil {
		// if rw is not nil, it means there is data to be flushed
		if err := c.FlushChunk(); err != nil {
			return err
		}
	}
	if err := c.writeStats(); err != nil {
		return err
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
	// Write PK stats
	pkStatsMap, err := c.pkCollector.Digest(
		c.collectionID,
		c.partitionID,
		c.segmentID,
		c.rootPath,
		c.rowNum,
		c.allocator,
		c.BlobsWriter,
	)
	if err != nil {
		return err
	}
	// Extract single PK stats from map
	for _, statsLog := range pkStatsMap {
		c.statsLog = statsLog
		break
	}

	// Write BM25 stats
	bm25StatsLog, err := c.bm25Collector.Digest(
		c.collectionID,
		c.partitionID,
		c.segmentID,
		c.rootPath,
		c.rowNum,
		c.allocator,
		c.BlobsWriter,
	)
	if err != nil {
		return err
	}
	c.bm25StatsLog = bm25StatsLog

	return nil
}

func (c *CompositeBinlogRecordWriter) GetExpirQuantiles() []int64 {
	return calculateExpirQuantiles(c.ttlFieldID, c.rowNum, c.ttlFieldValues)
}

func (c *CompositeBinlogRecordWriter) GetLogs() (
	fieldBinlogs map[FieldID]*datapb.FieldBinlog,
	statsLog *datapb.FieldBinlog,
	bm25StatsLog map[FieldID]*datapb.FieldBinlog,
	manifest string,
	expirQuantiles []int64,
) {
	return c.fieldBinlogs, c.statsLog, c.bm25StatsLog, "", c.GetExpirQuantiles()
}

func (c *CompositeBinlogRecordWriter) GetRowNum() int64 {
	return c.rowNum
}

func newCompositeBinlogRecordWriter(collectionID, partitionID, segmentID UniqueID, schema *schemapb.CollectionSchema,
	blobsWriter ChunkedBlobsWriter, allocator allocator.Interface, chunkSize uint64, rootPath string, maxRowNum int64,
) (*CompositeBinlogRecordWriter, error) {
	writer := &CompositeBinlogRecordWriter{
		collectionID:   collectionID,
		partitionID:    partitionID,
		segmentID:      segmentID,
		schema:         schema,
		BlobsWriter:    blobsWriter,
		allocator:      allocator,
		chunkSize:      chunkSize,
		rootPath:       rootPath,
		maxRowNum:      maxRowNum,
		tsFrom:         math.MaxUint64,
		tsTo:           0,
		ttlFieldID:     getTTLFieldID(schema),
		ttlFieldValues: make([]int64, 0),
	}

	// Create stats collectors
	var err error
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
			return ValueSerializer(v, rw.Schema())
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
			return ValueSerializer(v, schema)
		}, batchSize),
	}, nil
}
