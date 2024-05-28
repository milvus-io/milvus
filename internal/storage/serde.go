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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Record interface {
	Schema() map[FieldID]schemapb.DataType
	Column(i FieldID) arrow.Array
	Len() int
	Release()
}

type RecordReader interface {
	Next() error
	Record() Record
	Close()
}

type RecordWriter interface {
	Write(r Record) error
	Close()
}

type (
	Serializer[T any]   func([]T) (Record, uint64, error)
	Deserializer[T any] func(Record, []T) error
)

// compositeRecord is a record being composed of multiple records, in which each only have 1 column
type compositeRecord struct {
	recs   map[FieldID]arrow.Record
	schema map[FieldID]schemapb.DataType
}

func (r *compositeRecord) Column(i FieldID) arrow.Array {
	return r.recs[i].Column(0)
}

func (r *compositeRecord) Len() int {
	for _, rec := range r.recs {
		return rec.Column(0).Len()
	}
	return 0
}

func (r *compositeRecord) Release() {
	for _, rec := range r.recs {
		rec.Release()
	}
}

func (r *compositeRecord) Schema() map[FieldID]schemapb.DataType {
	return r.schema
}

var _ RecordReader = (*compositeRecordReader)(nil)

type compositeRecordReader struct {
	blobs [][]*Blob

	blobPos int
	rrs     []array.RecordReader
	closers []func()
	fields  []FieldID

	r compositeRecord
}

func (crr *compositeRecordReader) iterateNextBatch() error {
	if crr.closers != nil {
		for _, close := range crr.closers {
			if close != nil {
				close()
			}
		}
	}
	crr.blobPos++
	if crr.blobPos >= len(crr.blobs[0]) {
		return io.EOF
	}

	for i, b := range crr.blobs {
		reader, err := NewBinlogReader(b[crr.blobPos].Value)
		if err != nil {
			return err
		}

		crr.fields[i] = reader.FieldID
		// TODO: assert schema being the same in every blobs
		crr.r.schema[reader.FieldID] = reader.PayloadDataType
		er, err := reader.NextEventReader()
		if err != nil {
			return err
		}
		rr, err := er.GetArrowRecordReader()
		if err != nil {
			return err
		}
		crr.rrs[i] = rr
		crr.closers[i] = func() {
			rr.Release()
			er.Close()
			reader.Close()
		}
	}
	return nil
}

func (crr *compositeRecordReader) Next() error {
	if crr.rrs == nil {
		if crr.blobs == nil || len(crr.blobs) == 0 {
			return io.EOF
		}
		crr.rrs = make([]array.RecordReader, len(crr.blobs))
		crr.closers = make([]func(), len(crr.blobs))
		crr.blobPos = -1
		crr.fields = make([]FieldID, len(crr.rrs))
		crr.r = compositeRecord{
			recs:   make(map[FieldID]arrow.Record, len(crr.rrs)),
			schema: make(map[FieldID]schemapb.DataType, len(crr.rrs)),
		}
		if err := crr.iterateNextBatch(); err != nil {
			return err
		}
	}

	composeRecord := func() bool {
		for i, rr := range crr.rrs {
			if ok := rr.Next(); !ok {
				return false
			}
			// compose record
			crr.r.recs[crr.fields[i]] = rr.Record()
		}
		return true
	}

	// Try compose records
	if ok := composeRecord(); !ok {
		// If failed the first time, try iterate next batch (blob), the error may be io.EOF
		if err := crr.iterateNextBatch(); err != nil {
			return err
		}
		// If iterate next batch success, try compose again
		if ok := composeRecord(); !ok {
			// If the next blob is empty, return io.EOF (it's rare).
			return io.EOF
		}
	}
	return nil
}

func (crr *compositeRecordReader) Record() Record {
	return &crr.r
}

func (crr *compositeRecordReader) Close() {
	for _, close := range crr.closers {
		if close != nil {
			close()
		}
	}
}

type serdeEntry struct {
	// arrowType returns the arrow type for the given dimension
	arrowType func(int) arrow.DataType
	// deserialize deserializes the i-th element in the array, returns the value and ok.
	//	null is deserialized to nil without checking the type nullability.
	deserialize func(arrow.Array, int) (any, bool)
	// serialize serializes the value to the builder, returns ok.
	// 	nil is serialized to null without checking the type nullability.
	serialize func(array.Builder, any) bool
	// sizeof returns the size in bytes of the value
	sizeof func(any) uint64
}

var serdeMap = func() map[schemapb.DataType]serdeEntry {
	m := make(map[schemapb.DataType]serdeEntry)
	m[schemapb.DataType_Bool] = serdeEntry{
		func(i int) arrow.DataType {
			return arrow.FixedWidthTypes.Boolean
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Boolean); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.BooleanBuilder); ok {
				if v, ok := v.(bool); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
		func(any) uint64 {
			return 1
		},
	}
	m[schemapb.DataType_Int8] = serdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Int8
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int8); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Int8Builder); ok {
				if v, ok := v.(int8); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
		func(any) uint64 {
			return 1
		},
	}
	m[schemapb.DataType_Int16] = serdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Int16
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int16); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Int16Builder); ok {
				if v, ok := v.(int16); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
		func(any) uint64 {
			return 2
		},
	}
	m[schemapb.DataType_Int32] = serdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Int32
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int32); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Int32Builder); ok {
				if v, ok := v.(int32); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
		func(any) uint64 {
			return 4
		},
	}
	m[schemapb.DataType_Int64] = serdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Int64
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int64); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Int64Builder); ok {
				if v, ok := v.(int64); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
		func(any) uint64 {
			return 8
		},
	}
	m[schemapb.DataType_Float] = serdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Float32
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Float32); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Float32Builder); ok {
				if v, ok := v.(float32); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
		func(any) uint64 {
			return 4
		},
	}
	m[schemapb.DataType_Double] = serdeEntry{
		func(i int) arrow.DataType {
			return arrow.PrimitiveTypes.Float64
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Float64); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.Float64Builder); ok {
				if v, ok := v.(float64); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
		func(any) uint64 {
			return 8
		},
	}
	stringEntry := serdeEntry{
		func(i int) arrow.DataType {
			return arrow.BinaryTypes.String
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.String); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.StringBuilder); ok {
				if v, ok := v.(string); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
		func(v any) uint64 {
			if v == nil {
				return 8
			}
			return uint64(len(v.(string)))
		},
	}

	m[schemapb.DataType_VarChar] = stringEntry
	m[schemapb.DataType_String] = stringEntry
	m[schemapb.DataType_Array] = serdeEntry{
		func(i int) arrow.DataType {
			return arrow.BinaryTypes.Binary
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Binary); ok && i < arr.Len() {
				v := &schemapb.ScalarField{}
				if err := proto.Unmarshal(arr.Value(i), v); err == nil {
					return v, true
				}
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.BinaryBuilder); ok {
				if vv, ok := v.(*schemapb.ScalarField); ok {
					if bytes, err := proto.Marshal(vv); err == nil {
						builder.Append(bytes)
						return true
					}
				}
			}
			return false
		},
		func(v any) uint64 {
			if v == nil {
				return 8
			}
			return uint64(v.(*schemapb.ScalarField).XXX_Size())
		},
	}

	sizeOfBytes := func(v any) uint64 {
		if v == nil {
			return 8
		}
		return uint64(len(v.([]byte)))
	}

	byteEntry := serdeEntry{
		func(i int) arrow.DataType {
			return arrow.BinaryTypes.Binary
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Binary); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.BinaryBuilder); ok {
				if v, ok := v.([]byte); ok {
					builder.Append(v)
					return true
				}
			}
			return false
		},
		sizeOfBytes,
	}

	m[schemapb.DataType_JSON] = byteEntry

	fixedSizeDeserializer := func(a arrow.Array, i int) (any, bool) {
		if a.IsNull(i) {
			return nil, true
		}
		if arr, ok := a.(*array.FixedSizeBinary); ok && i < arr.Len() {
			return arr.Value(i), true
		}
		return nil, false
	}
	fixedSizeSerializer := func(b array.Builder, v any) bool {
		if v == nil {
			b.AppendNull()
			return true
		}
		if builder, ok := b.(*array.FixedSizeBinaryBuilder); ok {
			if v, ok := v.([]byte); ok {
				builder.Append(v)
				return true
			}
		}
		return false
	}

	m[schemapb.DataType_BinaryVector] = serdeEntry{
		func(i int) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: (i + 7) / 8}
		},
		fixedSizeDeserializer,
		fixedSizeSerializer,
		sizeOfBytes,
	}
	m[schemapb.DataType_Float16Vector] = serdeEntry{
		func(i int) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: i * 2}
		},
		fixedSizeDeserializer,
		fixedSizeSerializer,
		sizeOfBytes,
	}
	m[schemapb.DataType_BFloat16Vector] = serdeEntry{
		func(i int) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: i * 2}
		},
		fixedSizeDeserializer,
		fixedSizeSerializer,
		sizeOfBytes,
	}
	m[schemapb.DataType_FloatVector] = serdeEntry{
		func(i int) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: i * 4}
		},
		func(a arrow.Array, i int) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.FixedSizeBinary); ok && i < arr.Len() {
				return arrow.Float32Traits.CastFromBytes(arr.Value(i)), true
			}
			return nil, false
		},
		func(b array.Builder, v any) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.FixedSizeBinaryBuilder); ok {
				if vv, ok := v.([]float32); ok {
					dim := len(vv)
					byteLength := dim * 4
					bytesData := make([]byte, byteLength)
					for i, vec := range vv {
						bytes := math.Float32bits(vec)
						common.Endian.PutUint32(bytesData[i*4:], bytes)
					}
					builder.Append(bytesData)
					return true
				}
			}
			return false
		},
		func(v any) uint64 {
			if v == nil {
				return 8
			}
			return uint64(len(v.([]float32)) * 4)
		},
	}
	m[schemapb.DataType_SparseFloatVector] = byteEntry
	return m
}()

func parseBlobKey(bolbKey string) (colId FieldID, logId UniqueID) {
	if _, _, _, colId, logId, ok := metautil.ParseInsertLogPath(bolbKey); ok {
		return colId, logId
	}
	if colId, err := strconv.ParseInt(bolbKey, 10, 64); err == nil {
		// data_codec.go generate single field id as blob key.
		return colId, 0
	}
	return -1, -1
}

func newCompositeRecordReader(blobs []*Blob) (*compositeRecordReader, error) {
	sort.Slice(blobs, func(i, j int) bool {
		iCol, iLog := parseBlobKey(blobs[i].Key)
		jCol, jLog := parseBlobKey(blobs[j].Key)

		if iCol == jCol {
			return iLog < jLog
		}
		return iCol < jCol
	})

	blobm := make([][]*Blob, 0)
	var fieldId FieldID = -1
	var currentCol []*Blob

	for _, blob := range blobs {
		colId, _ := parseBlobKey(blob.Key)
		if colId != fieldId {
			if currentCol != nil {
				blobm = append(blobm, currentCol)
			}
			currentCol = make([]*Blob, 0)
			fieldId = colId
		}
		currentCol = append(currentCol, blob)
	}
	if currentCol != nil {
		blobm = append(blobm, currentCol)
	}

	return &compositeRecordReader{
		blobs: blobm,
	}, nil
}

type DeserializeReader[T any] struct {
	rr           RecordReader
	deserializer Deserializer[T]
	rec          Record
	values       []T
	pos          int
}

// Iterate to next value, return error or EOF if no more value.
func (deser *DeserializeReader[T]) Next() error {
	if deser.rec == nil || deser.pos >= deser.rec.Len()-1 {
		if err := deser.rr.Next(); err != nil {
			return err
		}
		deser.pos = 0
		deser.rec = deser.rr.Record()

		// allocate new slice preventing overwrite previous batch
		deser.values = make([]T, deser.rec.Len())
		if err := deser.deserializer(deser.rec, deser.values); err != nil {
			return err
		}
	} else {
		deser.pos++
	}

	return nil
}

func (deser *DeserializeReader[T]) Value() T {
	return deser.values[deser.pos]
}

func (deser *DeserializeReader[T]) Close() {
	if deser.rec != nil {
		deser.rec.Release()
	}
	if deser.rr != nil {
		deser.rr.Close()
	}
}

func NewDeserializeReader[T any](rr RecordReader, deserializer Deserializer[T]) *DeserializeReader[T] {
	return &DeserializeReader[T]{
		rr:           rr,
		deserializer: deserializer,
	}
}

func NewBinlogDeserializeReader(blobs []*Blob, PKfieldID UniqueID) (*DeserializeReader[*Value], error) {
	reader, err := newCompositeRecordReader(blobs)
	if err != nil {
		return nil, err
	}

	return NewDeserializeReader(reader, func(r Record, v []*Value) error {
		// Note: the return value `Value` is reused.
		for i := 0; i < r.Len(); i++ {
			value := v[i]
			if value == nil {
				value = &Value{}
				m := make(map[FieldID]interface{}, len(r.Schema()))
				value.Value = m
				v[i] = value
			}

			m := value.Value.(map[FieldID]interface{})
			for j, dt := range r.Schema() {
				if r.Column(j).IsNull(i) {
					m[j] = nil
				} else {
					d, ok := serdeMap[dt].deserialize(r.Column(j), i)
					if ok {
						m[j] = d // TODO: avoid memory copy here.
					} else {
						return errors.New(fmt.Sprintf("unexpected type %s", dt))
					}
				}
			}

			if _, ok := m[common.RowIDField]; !ok {
				panic("no row id column found")
			}
			value.ID = m[common.RowIDField].(int64)
			value.Timestamp = m[common.TimeStampField].(int64)

			pk, err := GenPrimaryKeyByRawData(m[PKfieldID], r.Schema()[PKfieldID])
			if err != nil {
				return err
			}

			value.PK = pk
			value.IsDeleted = false
			value.Value = m
		}
		return nil
	}), nil
}

var _ Record = (*selectiveRecord)(nil)

// selectiveRecord is a Record that only contains a single field, reusing existing Record.
type selectiveRecord struct {
	r               Record
	selectedFieldId FieldID

	schema map[FieldID]schemapb.DataType
}

func (r *selectiveRecord) Schema() map[FieldID]schemapb.DataType {
	return r.schema
}

func (r *selectiveRecord) Column(i FieldID) arrow.Array {
	if i == r.selectedFieldId {
		return r.r.Column(i)
	}
	return nil
}

func (r *selectiveRecord) Len() int {
	return r.r.Len()
}

func (r *selectiveRecord) Release() {
	// do nothing.
}

func newSelectiveRecord(r Record, selectedFieldId FieldID) *selectiveRecord {
	dt, ok := r.Schema()[selectedFieldId]
	if !ok {
		return nil
	}
	schema := make(map[FieldID]schemapb.DataType, 1)
	schema[selectedFieldId] = dt
	return &selectiveRecord{
		r:               r,
		selectedFieldId: selectedFieldId,
		schema:          schema,
	}
}

var _ RecordWriter = (*compositeRecordWriter)(nil)

type compositeRecordWriter struct {
	writers map[FieldID]RecordWriter
}

func (crw *compositeRecordWriter) Write(r Record) error {
	if len(r.Schema()) != len(crw.writers) {
		return fmt.Errorf("schema length mismatch %d, expected %d", len(r.Schema()), len(crw.writers))
	}
	for fieldId, w := range crw.writers {
		sr := newSelectiveRecord(r, fieldId)
		if err := w.Write(sr); err != nil {
			return err
		}
	}
	return nil
}

func (crw *compositeRecordWriter) Close() {
	if crw != nil {
		for _, w := range crw.writers {
			if w != nil {
				w.Close()
			}
		}
	}
}

func newCompositeRecordWriter(writers map[FieldID]RecordWriter) *compositeRecordWriter {
	return &compositeRecordWriter{
		writers: writers,
	}
}

var _ RecordWriter = (*singleFieldRecordWriter)(nil)

type singleFieldRecordWriter struct {
	fw      *pqarrow.FileWriter
	fieldId FieldID
	schema  *arrow.Schema

	numRows int
}

func (sfw *singleFieldRecordWriter) Write(r Record) error {
	sfw.numRows += r.Len()
	a := r.Column(sfw.fieldId)
	rec := array.NewRecord(sfw.schema, []arrow.Array{a}, int64(r.Len()))
	defer rec.Release()
	return sfw.fw.WriteBuffered(rec)
}

func (sfw *singleFieldRecordWriter) Close() {
	sfw.fw.Close()
}

func newSingleFieldRecordWriter(fieldId FieldID, field arrow.Field, writer io.Writer) (*singleFieldRecordWriter, error) {
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	fw, err := pqarrow.NewFileWriter(schema, writer,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(math.MaxInt64)), // No additional grouping for now.
		pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	return &singleFieldRecordWriter{
		fw:      fw,
		fieldId: fieldId,
		schema:  schema,
	}, nil
}

type SerializeWriter[T any] struct {
	rw         RecordWriter
	serializer Serializer[T]
	batchSize  int

	buffer            []T
	pos               int
	writtenMemorySize uint64
}

func (sw *SerializeWriter[T]) Flush() error {
	if sw.pos == 0 {
		return nil
	}
	buf := sw.buffer[:sw.pos]
	r, size, err := sw.serializer(buf)
	if err != nil {
		return err
	}
	defer r.Release()
	if err := sw.rw.Write(r); err != nil {
		return err
	}
	sw.pos = 0
	sw.writtenMemorySize += size
	return nil
}

func (sw *SerializeWriter[T]) Write(value T) error {
	if sw.buffer == nil {
		sw.buffer = make([]T, sw.batchSize)
	}
	sw.buffer[sw.pos] = value
	sw.pos++
	if sw.pos == sw.batchSize {
		if err := sw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (sw *SerializeWriter[T]) WrittenMemorySize() uint64 {
	return sw.writtenMemorySize
}

func (sw *SerializeWriter[T]) Close() error {
	if err := sw.Flush(); err != nil {
		return err
	}
	sw.rw.Close()
	return nil
}

func NewSerializeRecordWriter[T any](rw RecordWriter, serializer Serializer[T], batchSize int) *SerializeWriter[T] {
	return &SerializeWriter[T]{
		rw:         rw,
		serializer: serializer,
		batchSize:  batchSize,
	}
}

type simpleArrowRecord struct {
	Record

	r      arrow.Record
	schema map[FieldID]schemapb.DataType

	field2Col map[FieldID]int
}

func (sr *simpleArrowRecord) Schema() map[FieldID]schemapb.DataType {
	return sr.schema
}

func (sr *simpleArrowRecord) Column(i FieldID) arrow.Array {
	colIdx, ok := sr.field2Col[i]
	if !ok {
		panic("no such field")
	}
	return sr.r.Column(colIdx)
}

func (sr *simpleArrowRecord) Len() int {
	return int(sr.r.NumRows())
}

func (sr *simpleArrowRecord) Release() {
	sr.r.Release()
}

func newSimpleArrowRecord(r arrow.Record, schema map[FieldID]schemapb.DataType, field2Col map[FieldID]int) *simpleArrowRecord {
	return &simpleArrowRecord{
		r:         r,
		schema:    schema,
		field2Col: field2Col,
	}
}

type BinlogStreamWriter struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	fieldSchema  *schemapb.FieldSchema

	memorySize int // To be updated on the fly

	buf bytes.Buffer
	rw  *singleFieldRecordWriter
}

func (bsw *BinlogStreamWriter) GetRecordWriter() (RecordWriter, error) {
	if bsw.rw != nil {
		return bsw.rw, nil
	}

	fid := bsw.fieldSchema.FieldID
	dim, _ := typeutil.GetDim(bsw.fieldSchema)
	rw, err := newSingleFieldRecordWriter(fid, arrow.Field{
		Name:     strconv.Itoa(int(fid)),
		Type:     serdeMap[bsw.fieldSchema.DataType].arrowType(int(dim)),
		Nullable: true, // No nullable check here.
	}, &bsw.buf)
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
		MemorySize: int64(bsw.memorySize),
	}, nil
}

func (bsw *BinlogStreamWriter) writeBinlogHeaders(w io.Writer) error {
	// Write magic number
	if err := binary.Write(w, common.Endian, MagicNumber); err != nil {
		return err
	}
	// Write descriptor
	de := newDescriptorEvent()
	de.PayloadDataType = bsw.fieldSchema.DataType
	de.CollectionID = bsw.collectionID
	de.PartitionID = bsw.partitionID
	de.SegmentID = bsw.segmentID
	de.FieldID = bsw.fieldSchema.FieldID
	de.StartTimestamp = 0
	de.EndTimestamp = 0
	de.descriptorEventData.AddExtra(originalSizeKey, strconv.Itoa(bsw.memorySize)) // FIXME: enable original size
	if err := de.Write(w); err != nil {
		return err
	}
	// Write event header
	eh := newEventHeader(InsertEventType)
	// Write event data
	ev := newInsertEventData()
	ev.StartTimestamp = 1 // Fixme: enable start/end timestamp
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

func NewBinlogSerializeWriter(schema *schemapb.CollectionSchema, partitionID, segmentID UniqueID,
	writers map[FieldID]*BinlogStreamWriter, batchSize int,
) (*SerializeWriter[*Value], error) {
	rws := make(map[FieldID]RecordWriter, len(writers))
	for fid := range writers {
		w := writers[fid]
		rw, err := w.GetRecordWriter()
		if err != nil {
			return nil, err
		}
		rws[fid] = rw
	}
	compositeRecordWriter := newCompositeRecordWriter(rws)
	return NewSerializeRecordWriter[*Value](compositeRecordWriter, func(v []*Value) (Record, uint64, error) {
		builders := make(map[FieldID]array.Builder, len(schema.Fields))
		types := make(map[FieldID]schemapb.DataType, len(schema.Fields))
		for _, f := range schema.Fields {
			dim, _ := typeutil.GetDim(f)
			builders[f.FieldID] = array.NewBuilder(memory.DefaultAllocator, serdeMap[f.DataType].arrowType(int(dim)))
			types[f.FieldID] = f.DataType
		}

		var memorySize uint64
		for _, vv := range v {
			m := vv.Value.(map[FieldID]any)

			for fid, e := range m {
				typeEntry, ok := serdeMap[types[fid]]
				if !ok {
					panic("unknown type")
				}
				ok = typeEntry.serialize(builders[fid], e)
				if !ok {
					return nil, 0, errors.New(fmt.Sprintf("serialize error on type %s", types[fid]))
				}
				writers[fid].memorySize += int(typeEntry.sizeof(e))
				memorySize += typeEntry.sizeof(e)
			}
		}
		arrays := make([]arrow.Array, len(types))
		fields := make([]arrow.Field, len(types))
		field2Col := make(map[FieldID]int, len(types))
		i := 0
		for fid, builder := range builders {
			arrays[i] = builder.NewArray()
			builder.Release()
			fields[i] = arrow.Field{
				Name:     strconv.Itoa(int(fid)),
				Type:     arrays[i].DataType(),
				Nullable: true, // No nullable check here.
			}
			field2Col[fid] = i
			i++
		}
		return newSimpleArrowRecord(array.NewRecord(arrow.NewSchema(fields, nil), arrays, int64(len(v))), types, field2Col), memorySize, nil
	}, batchSize), nil
}
