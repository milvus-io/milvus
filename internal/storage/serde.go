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
	"math"
	"sync"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

type Record interface {
	Schema() map[FieldID]schemapb.DataType
	ArrowSchema() *arrow.Schema
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

func (r *compositeRecord) ArrowSchema() *arrow.Schema {
	var fields []arrow.Field
	for _, rec := range r.recs {
		fields = append(fields, rec.Schema().Field(0))
	}
	return arrow.NewSchema(fields, nil)
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
			return uint64(proto.Size(v.(*schemapb.ScalarField)))
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

// Since parquet does not support custom fallback encoding for now,
// we disable dict encoding for primary key.
// It can be scale to all fields once parquet fallback encoding is available.
func getFieldWriterProps(field *schemapb.FieldSchema) *parquet.WriterProperties {
	if field.GetIsPrimaryKey() {
		return parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Zstd),
			parquet.WithCompressionLevel(3),
			parquet.WithDictionaryDefault(false),
		)
	}
	return parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
	)
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

func (r *selectiveRecord) ArrowSchema() *arrow.Schema {
	return r.r.ArrowSchema()
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

type RecordWriterOptions func(*singleFieldRecordWriter)

func WithRecordWriterProps(writerProps *parquet.WriterProperties) RecordWriterOptions {
	return func(w *singleFieldRecordWriter) {
		w.writerProps = writerProps
	}
}

type singleFieldRecordWriter struct {
	fw      *pqarrow.FileWriter
	fieldId FieldID
	schema  *arrow.Schema

	numRows     int
	writerProps *parquet.WriterProperties
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

func newSingleFieldRecordWriter(fieldId FieldID, field arrow.Field, writer io.Writer, opts ...RecordWriterOptions) (*singleFieldRecordWriter, error) {
	w := &singleFieldRecordWriter{
		fieldId: fieldId,
		schema:  arrow.NewSchema([]arrow.Field{field}, nil),
		writerProps: parquet.NewWriterProperties(
			parquet.WithMaxRowGroupLength(math.MaxInt64), // No additional grouping for now.
			parquet.WithCompression(compress.Codecs.Zstd),
			parquet.WithCompressionLevel(3)),
	}
	for _, o := range opts {
		o(w)
	}
	fw, err := pqarrow.NewFileWriter(w.schema, writer, w.writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	w.fw = fw
	return w, nil
}

var _ RecordWriter = (*multiFieldRecordWriter)(nil)

type multiFieldRecordWriter struct {
	fw       *pqarrow.FileWriter
	fieldIds []FieldID
	schema   *arrow.Schema

	numRows int
}

func (mfw *multiFieldRecordWriter) Write(r Record) error {
	mfw.numRows += r.Len()
	columns := make([]arrow.Array, len(mfw.fieldIds))
	for i, fieldId := range mfw.fieldIds {
		columns[i] = r.Column(fieldId)
	}
	rec := array.NewRecord(mfw.schema, columns, int64(r.Len()))
	defer rec.Release()
	return mfw.fw.WriteBuffered(rec)
}

func (mfw *multiFieldRecordWriter) Close() {
	mfw.fw.Close()
}

func newMultiFieldRecordWriter(fieldIds []FieldID, fields []arrow.Field, writer io.Writer) (*multiFieldRecordWriter, error) {
	schema := arrow.NewSchema(fields, nil)
	fw, err := pqarrow.NewFileWriter(schema, writer,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(math.MaxInt64)), // No additional grouping for now.
		pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	return &multiFieldRecordWriter{
		fw:       fw,
		fieldIds: fieldIds,
		schema:   schema,
	}, nil
}

type SerializeWriter[T any] struct {
	rw         RecordWriter
	serializer Serializer[T]
	batchSize  int
	mu         sync.Mutex

	buffer            []T
	pos               int
	writtenMemorySize atomic.Uint64
}

func (sw *SerializeWriter[T]) Flush() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
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
	sw.writtenMemorySize.Add(size)
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
	return sw.writtenMemorySize.Load()
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

func (sr *simpleArrowRecord) ArrowSchema() *arrow.Schema {
	return sr.r.Schema()
}

func newSimpleArrowRecord(r arrow.Record, schema map[FieldID]schemapb.DataType, field2Col map[FieldID]int) *simpleArrowRecord {
	return &simpleArrowRecord{
		r:         r,
		schema:    schema,
		field2Col: field2Col,
	}
}
