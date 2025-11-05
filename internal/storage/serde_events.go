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
	"path"
	"sort"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func NewRecordReaderFromBinlogs(fieldBinlogs []*datapb.FieldBinlog,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
) (RecordReader, error) {
	// check legacy or import binlog struct
	for _, fieldBinlog := range fieldBinlogs {
		if len(fieldBinlog.ChildFields) == 0 {
			binlogLists := lo.Map(fieldBinlogs, func(fieldBinlog *datapb.FieldBinlog, _ int) []*datapb.Binlog {
				return fieldBinlog.GetBinlogs()
			})
			bucketName := storageConfig.BucketName
			paths := make([][]string, len(binlogLists[0]))
			for _, binlogs := range binlogLists {
				for j, binlog := range binlogs {
					logPath := binlog.GetLogPath()
					if storageConfig.StorageType != "local" {
						logPath = path.Join(bucketName, logPath)
					}
					paths[j] = append(paths[j], logPath)
				}
			}
			return newIterativePackedRecordReader(paths, schema, bufferSize, storageConfig, storagePluginContext), nil
		}
	}
	return NewManifestReaderFromBinlogs(fieldBinlogs, schema, bufferSize, storageConfig, storagePluginContext)
}

var _ RecordReader = (*IterativeRecordReader)(nil)

type IterativeRecordReader struct {
	cur     RecordReader
	iterate func() (RecordReader, error)
}

// Close implements RecordReader.
func (ir *IterativeRecordReader) Close() error {
	if ir.cur != nil {
		return ir.cur.Close()
	}
	return nil
}

func (ir *IterativeRecordReader) Next() (Record, error) {
	if ir.cur == nil {
		r, err := ir.iterate()
		if err != nil {
			return nil, err
		}
		ir.cur = r
	}
	rec, err := ir.cur.Next()
	if err == io.EOF {
		closeErr := ir.cur.Close()
		if closeErr != nil {
			return nil, closeErr
		}
		ir.cur, err = ir.iterate()
		if err != nil {
			return nil, err
		}
		rec, err = ir.cur.Next()
	}
	return rec, err
}

type Manifest struct {
	Version      int            `json:"version"`
	ColumnGroups []*ColumnGroup `json:"column_groups"`
}

type ColumnGroup struct {
	Columns []string `json:"columns"`
	Format  string   `json:"format"`
	Paths   []string `json:"paths"`
}

type ManifestReader struct {
	fieldBinlogs []*datapb.FieldBinlog
	reader       *packed.FFIPackedReader

	bufferSize           int64
	arrowSchema          *arrow.Schema
	schema               *schemapb.CollectionSchema
	schemaHelper         *typeutil.SchemaHelper
	field2Col            map[FieldID]int
	storageConfig        *indexpb.StorageConfig
	storagePluginContext *indexcgopb.StoragePluginContext

	neededColumns []string
}

// NewManifestReaderFromBinlogs creates a ManifestReader from binlogs
func NewManifestReaderFromBinlogs(fieldBinlogs []*datapb.FieldBinlog,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
) (*ManifestReader, error) {
	arrowSchema, err := ConvertToArrowSchema(schema)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("convert collection schema [%s] to arrow schema error: %s", schema.Name, err.Error())
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, err
	}
	field2Col := make(map[FieldID]int)
	allFields := typeutil.GetAllFieldSchemas(schema)
	neededColumns := make([]string, 0, len(allFields))
	for i, field := range allFields {
		field2Col[field.FieldID] = i
		neededColumns = append(neededColumns, field.Name)
	}
	prr := &ManifestReader{
		fieldBinlogs:         fieldBinlogs,
		bufferSize:           bufferSize,
		arrowSchema:          arrowSchema,
		schema:               schema,
		schemaHelper:         schemaHelper,
		field2Col:            field2Col,
		storageConfig:        storageConfig,
		storagePluginContext: storagePluginContext,

		neededColumns: neededColumns,
	}

	err = prr.init()
	if err != nil {
		return nil, err
	}

	return prr, nil
}

func (mr *ManifestReader) generateManifest() (string, error) {
	m := &Manifest{
		Version: 0,
		ColumnGroups: lo.Map(mr.fieldBinlogs, func(binlog *datapb.FieldBinlog, _ int) *ColumnGroup {
			return &ColumnGroup{
				Columns: lo.Map(binlog.ChildFields, func(fieldID int64, _ int) string {
					field, err := mr.schemaHelper.GetFieldFromID(fieldID)
					if err != nil {
						// return empty string if field not found
						return ""
					}
					return field.GetName()
				}),
				Format: "parquet",
				Paths: lo.Map(binlog.Binlogs, func(binlog *datapb.Binlog, _ int) string {
					p := binlog.GetLogPath()
					if mr.storageConfig.StorageType != "local" {
						p = path.Join(mr.storageConfig.BucketName, p)
					}
					return p
				}),
			}
		}),
	}
	bs, err := json.Marshal(m)
	return string(bs), err
}

func (mr *ManifestReader) init() error {
	manifest, err := mr.generateManifest()
	if err != nil {
		return err
	}
	// TODO add needed column option

	reader, err := packed.NewFFIPackedReader(manifest, mr.arrowSchema, mr.neededColumns, mr.bufferSize, mr.storageConfig, mr.storagePluginContext)
	if err != nil {
		return err
	}
	mr.reader = reader
	return nil
}

func (mr ManifestReader) Next() (Record, error) {
	rec, err := mr.reader.ReadNext()
	if err != nil {
		return nil, err
	}
	return NewSimpleArrowRecord(rec, mr.field2Col), nil
}

func (mr ManifestReader) Close() error {
	if mr.reader != nil {
		return mr.reader.Close()
	}
	return nil
}

// ChunkedBlobsReader returns a chunk composed of blobs, or io.EOF if no more data
type ChunkedBlobsReader func() ([]*Blob, error)

type CompositeBinlogRecordReader struct {
	fields map[FieldID]*schemapb.FieldSchema
	index  map[FieldID]int16
	brs    []*BinlogReader
	rrs    []array.RecordReader
}

var _ RecordReader = (*CompositeBinlogRecordReader)(nil)

func (crr *CompositeBinlogRecordReader) Next() (Record, error) {
	recs := make([]arrow.Array, len(crr.fields))
	nonExistingFields := make([]*schemapb.FieldSchema, 0)
	nRows := 0
	for _, f := range crr.fields {
		idx := crr.index[f.FieldID]
		if crr.rrs[idx] != nil {
			if ok := crr.rrs[idx].Next(); !ok {
				return nil, io.EOF
			}
			r := crr.rrs[idx].Record()
			recs[idx] = r.Column(0)
			if nRows == 0 {
				nRows = int(r.NumRows())
			}
			if nRows != int(r.NumRows()) {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("number of rows mismatch for field %d", f.FieldID))
			}
		} else {
			nonExistingFields = append(nonExistingFields, f)
		}
	}
	for _, f := range nonExistingFields {
		// If the field is not in the current batch, fill with null array
		arr, err := GenerateEmptyArrayFromSchema(f, nRows)
		if err != nil {
			return nil, err
		}
		recs[crr.index[f.FieldID]] = arr
	}
	return &compositeRecord{
		index: crr.index,
		recs:  recs,
	}, nil
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

func newCompositeBinlogRecordReader(
	schema *schemapb.CollectionSchema,
	neededFields typeutil.Set[int64],
	blobs []*Blob,
	opts ...BinlogReaderOption,
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
		reader, err := NewBinlogReader(b.Value, opts...)
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
	opts ...BinlogReaderOption,
) *IterativeRecordReader {
	return &IterativeRecordReader{
		iterate: func() (RecordReader, error) {
			blobs, err := chunkedBlobs()
			if err != nil {
				return nil, err
			}
			return newCompositeBinlogRecordReader(schema, neededFields, blobs, opts...)
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

				d, ok := serdeMap[dt].deserialize(r.Column(j), i, elementType, dim, shouldCopy)
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

func NewBinlogDeserializeReader(schema *schemapb.CollectionSchema, blobsReader ChunkedBlobsReader, shouldCopy bool,
) (*DeserializeReaderImpl[*Value], error) {
	reader := newIterativeCompositeBinlogRecordReader(schema, nil, blobsReader)
	return NewDeserializeReader(reader, func(r Record, v []*Value) error {
		return ValueDeserializerWithSchema(r, v, schema, shouldCopy)
	}), nil
}

type HeaderExtraWriterOption func(header *descriptorEvent)

func WithEncryptionKey(ezID int64, edek []byte) HeaderExtraWriterOption {
	return func(header *descriptorEvent) {
		header.AddExtra(edekKey, string(edek))
		header.AddExtra(ezIDKey, ezID)
	}
}

type StreamWriterOption func(*BinlogStreamWriter)

func WithEncryptor(encryptor hook.Encryptor) StreamWriterOption {
	return func(w *BinlogStreamWriter) {
		w.encryptor = encryptor
	}
}

func WithHeaderExtraOptions(headerOpt HeaderExtraWriterOption) StreamWriterOption {
	return func(w *BinlogStreamWriter) {
		w.headerOpt = headerOpt
	}
}

func GetEncryptionOptions(ezID int64, edek []byte, encryptor hook.Encryptor) []StreamWriterOption {
	return []StreamWriterOption{
		WithEncryptor(encryptor),
		WithHeaderExtraOptions(WithEncryptionKey(ezID, edek)),
	}
}

type BinlogStreamWriter struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	fieldSchema  *schemapb.FieldSchema

	buf       bytes.Buffer
	rw        *singleFieldRecordWriter
	headerOpt HeaderExtraWriterOption
	encryptor hook.Encryptor
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

	// Everything but descryptor event is encrypted
	tmpBuf := &b
	if bsw.encryptor != nil {
		tmpBuf = &bytes.Buffer{}
	}

	eh := newEventHeader(InsertEventType)

	ev := newInsertEventData()
	ev.StartTimestamp = 1
	ev.EndTimestamp = 1
	eh.EventLength = int32(bsw.buf.Len()) + eh.GetMemoryUsageInBytes() + int32(binary.Size(ev))
	// eh.NextPosition = eh.EventLength + w.Offset()

	// Write event header
	if err := eh.Write(tmpBuf); err != nil {
		return nil, err
	}

	// Write event data, which ic startTs and endTs for insert event data
	if err := ev.WriteEventData(tmpBuf); err != nil {
		return nil, err
	}

	if err := binary.Write(tmpBuf, common.Endian, bsw.buf.Bytes()); err != nil {
		return nil, err
	}

	// tmpBuf could be "b" or new "tmpBuf"
	// if encryptor is not nil, tmpBuf is new tmpBuf, which need to be written into
	// b after encryption
	if bsw.encryptor != nil {
		cipherText, err := bsw.encryptor.Encrypt(tmpBuf.Bytes())
		if err != nil {
			return nil, err
		}
		log.Debug("Binlog stream writer encrypted cipher text",
			zap.Int64("collectionID", bsw.collectionID),
			zap.Int64("segmentID", bsw.segmentID),
			zap.Int64("fieldID", bsw.fieldSchema.FieldID),
			zap.Int("plain size", tmpBuf.Len()),
			zap.Int("cipher size", len(cipherText)),
		)
		if err := binary.Write(&b, common.Endian, cipherText); err != nil {
			return nil, err
		}
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
	// Additional head options
	if bsw.headerOpt != nil {
		bsw.headerOpt(de)
	}
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
	writerOptions ...StreamWriterOption,
) map[FieldID]*BinlogStreamWriter {
	bws := make(map[FieldID]*BinlogStreamWriter)

	for _, f := range schema.Fields {
		writer := newBinlogWriter(collectionID, partitionID, segmentID, f)
		for _, writerOption := range writerOptions {
			writerOption(writer)
		}
		bws[f.FieldID] = writer
	}

	for _, structField := range schema.StructArrayFields {
		for _, subField := range structField.Fields {
			writer := newBinlogWriter(collectionID, partitionID, segmentID, subField)
			for _, writerOption := range writerOptions {
				writerOption(writer)
			}
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

			ok = typeEntry.serialize(builders[fid], e, elementType)
			if !ok {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("serialize error on type %s", types[fid]))
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
	options             []StreamWriterOption
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
		c.fieldWriters = NewBinlogStreamWriters(c.collectionID, c.partitionID, c.segmentID, c.schema, c.options...)
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
	options ...StreamWriterOption,
) (*CompositeBinlogRecordWriter, error) {
	writer := &CompositeBinlogRecordWriter{
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		schema:       schema,
		BlobsWriter:  blobsWriter,
		allocator:    allocator,
		chunkSize:    chunkSize,
		rootPath:     rootPath,
		maxRowNum:    maxRowNum,
		options:      options,
		tsFrom:       math.MaxUint64,
		tsTo:         0,
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
