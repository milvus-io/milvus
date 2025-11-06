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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/bitutil"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type Record interface {
	Column(i FieldID) arrow.Array
	Len() int
	Release()
	Retain()
}

type RecordReader interface {
	Next() (Record, error)
	Close() error
}

type RecordWriter interface {
	Write(r Record) error
	GetWrittenUncompressed() uint64
	Close() error
}

type (
	Serializer[T any]   func([]T) (Record, error)
	Deserializer[T any] func(Record, []T) error
)

// compositeRecord is a record being composed of multiple records, in which each only have 1 column
type compositeRecord struct {
	index map[FieldID]int16
	recs  []arrow.Array
}

var _ Record = (*compositeRecord)(nil)

func (r *compositeRecord) Column(i FieldID) arrow.Array {
	if _, ok := r.index[i]; !ok {
		return nil
	}
	return r.recs[r.index[i]]
}

func (r *compositeRecord) Len() int {
	return r.recs[0].Len()
}

func (r *compositeRecord) Release() {
	for _, rec := range r.recs {
		rec.Release()
	}
}

func (r *compositeRecord) Retain() {
	for _, rec := range r.recs {
		rec.Retain()
	}
}

type serdeEntry struct {
	// arrowType returns the arrow type for the given dimension and element type
	// elementType is only used for ArrayOfVector
	arrowType func(dim int, elementType schemapb.DataType) arrow.DataType
	// deserialize deserializes the i-th element in the array, returns the value and ok.
	//	null is deserialized to nil without checking the type nullability.
	//	if shouldCopy is true, the returned value is copied rather than referenced from arrow array.
	//	elementType is only used for ArrayOfVector
	deserialize func(a arrow.Array, i int, elementType schemapb.DataType, dim int, shouldCopy bool) (any, bool)
	// serialize serializes the value to the builder, returns ok.
	// 	nil is serialized to null without checking the type nullability.
	//	elementType is only used for ArrayOfVector
	serialize func(b array.Builder, v any, elementType schemapb.DataType) bool
}

var serdeMap = func() map[schemapb.DataType]serdeEntry {
	m := make(map[schemapb.DataType]serdeEntry)
	m[schemapb.DataType_Bool] = serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.FixedWidthTypes.Boolean
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Boolean); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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
	}
	m[schemapb.DataType_Int8] = serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.PrimitiveTypes.Int8
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int8); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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
	}
	m[schemapb.DataType_Int16] = serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.PrimitiveTypes.Int16
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int16); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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
	}
	m[schemapb.DataType_Int32] = serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.PrimitiveTypes.Int32
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int32); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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
	}
	m[schemapb.DataType_Int64] = serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.PrimitiveTypes.Int64
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int64); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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
	}
	m[schemapb.DataType_Float] = serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.PrimitiveTypes.Float32
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Float32); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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
	}
	m[schemapb.DataType_Double] = serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.PrimitiveTypes.Float64
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Float64); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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
	}
	m[schemapb.DataType_Timestamptz] = serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.PrimitiveTypes.Int64
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, _ int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Int64); ok && i < arr.Len() {
				return arr.Value(i), true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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
	}
	stringEntry := serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.BinaryTypes.String
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.String); ok && i < arr.Len() {
				value := arr.Value(i)
				if shouldCopy {
					return strings.Clone(value), true
				}
				return value, true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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
	}

	m[schemapb.DataType_VarChar] = stringEntry
	m[schemapb.DataType_String] = stringEntry
	m[schemapb.DataType_Text] = stringEntry

	// We're not using the deserialized data in go, so we can skip the heavy pb serde.
	// If there is need in the future, just assign it to m[schemapb.DataType_Array]
	eagerArrayEntry := serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.BinaryTypes.Binary
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
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
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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
	}
	_ = eagerArrayEntry

	byteEntry := serdeEntry{
		arrowType: func(_ int, _ schemapb.DataType) arrow.DataType {
			return arrow.BinaryTypes.Binary
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.Binary); ok && i < arr.Len() {
				value := arr.Value(i)
				if shouldCopy {
					result := make([]byte, len(value))
					copy(result, value)
					return result, true
				}
				return value, true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.BinaryBuilder); ok {
				if vv, ok := v.([]byte); ok {
					builder.Append(vv)
					return true
				}
				if vv, ok := v.(*schemapb.ScalarField); ok {
					if bytes, err := proto.Marshal(vv); err == nil {
						builder.Append(bytes)
						return true
					}
				}
				if vv, ok := v.(*schemapb.VectorField); ok {
					if bytes, err := proto.Marshal(vv); err == nil {
						builder.Append(bytes)
						return true
					}
				}
			}
			return false
		},
	}

	m[schemapb.DataType_Array] = eagerArrayEntry
	m[schemapb.DataType_JSON] = byteEntry
	m[schemapb.DataType_Geometry] = byteEntry

	// ArrayOfVector now implements the standard interface with elementType parameter
	m[schemapb.DataType_ArrayOfVector] = serdeEntry{
		arrowType: func(dim int, elementType schemapb.DataType) arrow.DataType {
			return getArrayOfVectorArrowType(elementType, dim)
		},
		deserialize: func(a arrow.Array, i int, elementType schemapb.DataType, dim int, shouldCopy bool) (any, bool) {
			return deserializeArrayOfVector(a, i, elementType, int64(dim), shouldCopy)
		},
		serialize: func(b array.Builder, v any, elementType schemapb.DataType) bool {
			vf, ok := v.(*schemapb.VectorField)
			if !ok {
				return false
			}

			if vf == nil {
				b.AppendNull()
				return true
			}

			builder, ok := b.(*array.ListBuilder)
			if !ok {
				return false
			}

			builder.Append(true)
			valueBuilder := builder.ValueBuilder().(*array.FixedSizeBinaryBuilder)
			dim := vf.GetDim()

			appendVectorChunks := func(data []byte, bytesPerVector int) bool {
				numVectors := len(data) / bytesPerVector
				for i := 0; i < numVectors; i++ {
					start := i * bytesPerVector
					end := start + bytesPerVector
					valueBuilder.Append(data[start:end])
				}
				return true
			}

			switch elementType {
			case schemapb.DataType_FloatVector:
				if vf.GetFloatVector() == nil {
					return false
				}
				floatData := vf.GetFloatVector().GetData()
				numVectors := len(floatData) / int(dim)
				// Convert float data to binary
				for i := 0; i < numVectors; i++ {
					start := i * int(dim)
					end := start + int(dim)
					vectorSlice := floatData[start:end]

					bytes := make([]byte, dim*4)
					for j, f := range vectorSlice {
						binary.LittleEndian.PutUint32(bytes[j*4:], math.Float32bits(f))
					}
					valueBuilder.Append(bytes)
				}
				return true

			case schemapb.DataType_BinaryVector:
				if vf.GetBinaryVector() == nil {
					return false
				}
				return appendVectorChunks(vf.GetBinaryVector(), int((dim+7)/8))

			case schemapb.DataType_Float16Vector:
				if vf.GetFloat16Vector() == nil {
					return false
				}
				return appendVectorChunks(vf.GetFloat16Vector(), int(dim)*2)

			case schemapb.DataType_BFloat16Vector:
				if vf.GetBfloat16Vector() == nil {
					return false
				}
				return appendVectorChunks(vf.GetBfloat16Vector(), int(dim)*2)

			case schemapb.DataType_Int8Vector:
				if vf.GetInt8Vector() == nil {
					return false
				}
				return appendVectorChunks(vf.GetInt8Vector(), int(dim))

			case schemapb.DataType_SparseFloatVector:
				panic("SparseFloatVector in VectorArray not implemented yet")
			default:
				return false
			}
		},
	}

	fixedSizeDeserializer := func(a arrow.Array, i int, _ schemapb.DataType, _ int, shouldCopy bool) (any, bool) {
		if a.IsNull(i) {
			return nil, true
		}
		if arr, ok := a.(*array.FixedSizeBinary); ok && i < arr.Len() {
			value := arr.Value(i)
			if shouldCopy {
				result := make([]byte, len(value))
				copy(result, value)
				return result, true
			}
			return value, true
		}
		return nil, false
	}
	fixedSizeSerializer := func(b array.Builder, v any, _ schemapb.DataType) bool {
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
		arrowType: func(dim int, _ schemapb.DataType) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: (dim + 7) / 8}
		},
		deserialize: fixedSizeDeserializer,
		serialize:   fixedSizeSerializer,
	}
	m[schemapb.DataType_Float16Vector] = serdeEntry{
		arrowType: func(dim int, _ schemapb.DataType) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: dim * 2}
		},
		deserialize: fixedSizeDeserializer,
		serialize:   fixedSizeSerializer,
	}
	m[schemapb.DataType_BFloat16Vector] = serdeEntry{
		arrowType: func(dim int, _ schemapb.DataType) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: dim * 2}
		},
		deserialize: fixedSizeDeserializer,
		serialize:   fixedSizeSerializer,
	}
	m[schemapb.DataType_Int8Vector] = serdeEntry{
		arrowType: func(dim int, _ schemapb.DataType) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: dim}
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, _ int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.FixedSizeBinary); ok && i < arr.Len() {
				// convert to []int8
				bytes := arr.Value(i)
				int8s := make([]int8, len(bytes))
				for i, b := range bytes {
					int8s[i] = int8(b)
				}
				return int8s, true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
			if v == nil {
				b.AppendNull()
				return true
			}
			if builder, ok := b.(*array.FixedSizeBinaryBuilder); ok {
				if vv, ok := v.([]byte); ok {
					builder.Append(vv)
					return true
				} else if vv, ok := v.([]int8); ok {
					builder.Append(arrow.Int8Traits.CastToBytes(vv))
					return true
				}
			}
			return false
		},
	}
	m[schemapb.DataType_FloatVector] = serdeEntry{
		arrowType: func(dim int, _ schemapb.DataType) arrow.DataType {
			return &arrow.FixedSizeBinaryType{ByteWidth: dim * 4}
		},
		deserialize: func(a arrow.Array, i int, _ schemapb.DataType, _ int, shouldCopy bool) (any, bool) {
			if a.IsNull(i) {
				return nil, true
			}
			if arr, ok := a.(*array.FixedSizeBinary); ok && i < arr.Len() {
				vector := arrow.Float32Traits.CastFromBytes(arr.Value(i))
				if shouldCopy {
					vectorCopy := make([]float32, len(vector))
					copy(vectorCopy, vector)
					return vectorCopy, true
				}
				return vector, true
			}
			return nil, false
		},
		serialize: func(b array.Builder, v any, _ schemapb.DataType) bool {
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

type DeserializeReader[T any] interface {
	NextValue() (*T, error)
	Close() error
}

type DeserializeReaderImpl[T any] struct {
	rr           RecordReader
	deserializer Deserializer[T]
	rec          Record
	values       []T
	pos          int
}

// Iterate to next value, return error or EOF if no more value.
func (deser *DeserializeReaderImpl[T]) NextValue() (*T, error) {
	if deser.pos == 0 || deser.pos >= len(deser.values) {
		r, err := deser.rr.Next()
		if err != nil {
			return nil, err
		}
		deser.pos = 0
		deser.rec = r

		deser.values = make([]T, deser.rec.Len())

		if err := deser.deserializer(deser.rec, deser.values); err != nil {
			return nil, err
		}
	}
	ret := &deser.values[deser.pos]
	deser.pos++
	return ret, nil
}

func (deser *DeserializeReaderImpl[T]) Close() error {
	return deser.rr.Close()
}

func NewDeserializeReader[T any](rr RecordReader, deserializer Deserializer[T]) *DeserializeReaderImpl[T] {
	return &DeserializeReaderImpl[T]{
		rr:           rr,
		deserializer: deserializer,
	}
}

var _ Record = (*selectiveRecord)(nil)

// selectiveRecord is a Record that only contains a single field, reusing existing Record.
type selectiveRecord struct {
	r       Record
	fieldId FieldID
}

func (r *selectiveRecord) Column(i FieldID) arrow.Array {
	if i == r.fieldId {
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

func (r *selectiveRecord) Retain() {
	// do nothing
}

func newSelectiveRecord(r Record, selectedFieldId FieldID) Record {
	return &selectiveRecord{
		r:       r,
		fieldId: selectedFieldId,
	}
}

var _ RecordWriter = (*CompositeRecordWriter)(nil)

type CompositeRecordWriter struct {
	writers map[FieldID]RecordWriter
}

func (crw *CompositeRecordWriter) GetWrittenUncompressed() uint64 {
	s := uint64(0)
	for _, w := range crw.writers {
		s += w.GetWrittenUncompressed()
	}
	return s
}

func (crw *CompositeRecordWriter) Write(r Record) error {
	for fieldId, w := range crw.writers {
		sr := newSelectiveRecord(r, fieldId)
		if err := w.Write(sr); err != nil {
			return err
		}
	}
	return nil
}

func (crw *CompositeRecordWriter) Close() error {
	if crw != nil {
		for _, w := range crw.writers {
			if w != nil {
				if err := w.Close(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func NewCompositeRecordWriter(writers map[FieldID]RecordWriter) *CompositeRecordWriter {
	return &CompositeRecordWriter{
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
	fw          *pqarrow.FileWriter
	fieldId     FieldID
	schema      *arrow.Schema
	writerProps *parquet.WriterProperties

	numRows              int
	writtenUncompressed  uint64
	memoryExpansionRatio int
}

func (sfw *singleFieldRecordWriter) Write(r Record) error {
	sfw.numRows += r.Len()
	a := r.Column(sfw.fieldId)

	sfw.writtenUncompressed += calculateActualDataSize(a)
	rec := array.NewRecord(sfw.schema, []arrow.Array{a}, int64(r.Len()))
	defer rec.Release()
	return sfw.fw.WriteBuffered(rec)
}

func (sfw *singleFieldRecordWriter) GetWrittenUncompressed() uint64 {
	return sfw.writtenUncompressed * uint64(sfw.memoryExpansionRatio)
}

func (sfw *singleFieldRecordWriter) Close() error {
	return sfw.fw.Close()
}

// getArrayOfVectorArrowType returns the appropriate Arrow type for ArrayOfVector based on element type
func getArrayOfVectorArrowType(elementType schemapb.DataType, dim int) arrow.DataType {
	switch elementType {
	case schemapb.DataType_FloatVector:
		return arrow.ListOf(&arrow.FixedSizeBinaryType{ByteWidth: dim * 4})
	case schemapb.DataType_BinaryVector:
		return arrow.ListOf(&arrow.FixedSizeBinaryType{ByteWidth: (dim + 7) / 8})
	case schemapb.DataType_Float16Vector:
		return arrow.ListOf(&arrow.FixedSizeBinaryType{ByteWidth: dim * 2})
	case schemapb.DataType_BFloat16Vector:
		return arrow.ListOf(&arrow.FixedSizeBinaryType{ByteWidth: dim * 2})
	case schemapb.DataType_Int8Vector:
		return arrow.ListOf(&arrow.FixedSizeBinaryType{ByteWidth: dim})
	case schemapb.DataType_SparseFloatVector:
		return arrow.ListOf(arrow.BinaryTypes.Binary)
	default:
		panic(fmt.Sprintf("unsupported element type for ArrayOfVector: %s", elementType.String()))
	}
}

// deserializeArrayOfVector deserializes ArrayOfVector data with known element type
func deserializeArrayOfVector(a arrow.Array, i int, elementType schemapb.DataType, dim int64, shouldCopy bool) (any, bool) {
	if a.IsNull(i) {
		return nil, true
	}

	arr, ok := a.(*array.List)
	if !ok || i >= arr.Len() {
		return nil, false
	}

	start, end := arr.ValueOffsets(i)
	totalElements := end - start
	if totalElements == 0 {
		return nil, false
	}

	valuesArray := arr.ListValues()
	binaryArray, ok := valuesArray.(*array.FixedSizeBinary)
	if !ok {
		// empty array
		return nil, true
	}

	numVectors := int(totalElements)

	// Helper function to extract byte vectors from FixedSizeBinary array
	extractByteVectors := func(bytesPerVector int64) []byte {
		totalBytes := numVectors * int(bytesPerVector)
		data := make([]byte, totalBytes)
		for j := 0; j < numVectors; j++ {
			vectorIndex := int(start) + j
			vectorData := binaryArray.Value(vectorIndex)
			copy(data[j*int(bytesPerVector):], vectorData)
		}
		return data
	}

	switch elementType {
	case schemapb.DataType_FloatVector:
		totalFloats := numVectors * int(dim)
		floatData := make([]float32, totalFloats)
		for j := 0; j < numVectors; j++ {
			vectorIndex := int(start) + j
			binaryData := binaryArray.Value(vectorIndex)
			vectorFloats := arrow.Float32Traits.CastFromBytes(binaryData)
			copy(floatData[j*int(dim):], vectorFloats)
		}

		return &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{
					Data: floatData,
				},
			},
		}, true

	case schemapb.DataType_BinaryVector:
		return &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_BinaryVector{
				BinaryVector: extractByteVectors((dim + 7) / 8),
			},
		}, true

	case schemapb.DataType_Float16Vector:
		return &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_Float16Vector{
				Float16Vector: extractByteVectors(dim * 2),
			},
		}, true

	case schemapb.DataType_BFloat16Vector:
		return &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_Bfloat16Vector{
				Bfloat16Vector: extractByteVectors(dim * 2),
			},
		}, true

	case schemapb.DataType_Int8Vector:
		return &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_Int8Vector{
				Int8Vector: extractByteVectors(dim),
			},
		}, true
	case schemapb.DataType_SparseFloatVector:
		panic("SparseFloatVector in VectorArray deserialization not implemented yet")
	default:
		panic(fmt.Sprintf("unsupported element type for ArrayOfVector deserialization: %s", elementType.String()))
	}
}

func newSingleFieldRecordWriter(field *schemapb.FieldSchema, writer io.Writer, opts ...RecordWriterOptions) (*singleFieldRecordWriter, error) {
	// calculate memory expansion ratio
	// arrays are serialized by protobuf, where int values may be compacted, see https://protobuf.dev/reference/go/size
	// to correct the actual size, we need to multiply the memory expansion ratio accordingly.
	determineMemoryExpansionRatio := func(field *schemapb.FieldSchema) int {
		if field.DataType == schemapb.DataType_Array {
			switch field.GetElementType() {
			case schemapb.DataType_Int16:
				return 2
			case schemapb.DataType_Int32:
				return 4
			case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
				return 8
			}
		}
		return 1
	}
	dim, _ := typeutil.GetDim(field)

	var fieldMetadata arrow.Metadata
	var arrowType arrow.DataType
	elementType := schemapb.DataType_None

	if field.DataType == schemapb.DataType_ArrayOfVector {
		elementType = field.GetElementType()
		fieldMetadata = arrow.NewMetadata(
			[]string{"elementType", "dim"},
			[]string{fmt.Sprintf("%d", int32(elementType)), fmt.Sprintf("%d", dim)},
		)
	}
	arrowType = serdeMap[field.DataType].arrowType(int(dim), elementType)

	w := &singleFieldRecordWriter{
		fieldId: field.FieldID,
		schema: arrow.NewSchema([]arrow.Field{
			{
				Name:     strconv.Itoa(int(field.FieldID)),
				Type:     arrowType,
				Nullable: true, // No nullable check here.
				Metadata: fieldMetadata,
			},
		}, nil),
		writerProps: parquet.NewWriterProperties(
			parquet.WithMaxRowGroupLength(math.MaxInt64), // No additional grouping for now.
			parquet.WithCompression(compress.Codecs.Zstd),
			parquet.WithCompressionLevel(3)),
		memoryExpansionRatio: determineMemoryExpansionRatio(field),
	}
	for _, o := range opts {
		o(w)
	}

	// Use appropriate Arrow writer properties for ArrayOfVector
	arrowWriterProps := pqarrow.DefaultWriterProps()
	if field.DataType == schemapb.DataType_ArrayOfVector {
		// Ensure schema metadata is preserved for ArrayOfVector
		arrowWriterProps = pqarrow.NewArrowWriterProperties(
			pqarrow.WithStoreSchema(),
		)
	}

	fw, err := pqarrow.NewFileWriter(w.schema, writer, w.writerProps, arrowWriterProps)
	if err != nil {
		return nil, err
	}
	w.fw = fw
	return w, nil
}

var _ RecordWriter = (*multiFieldRecordWriter)(nil)

type multiFieldRecordWriter struct {
	fw       *pqarrow.FileWriter
	fieldIDs []FieldID
	schema   *arrow.Schema

	numRows             int
	writtenUncompressed uint64
}

func (mfw *multiFieldRecordWriter) Write(r Record) error {
	mfw.numRows += r.Len()
	columns := make([]arrow.Array, len(mfw.fieldIDs))
	for i, fieldId := range mfw.fieldIDs {
		columns[i] = r.Column(fieldId)
		mfw.writtenUncompressed += calculateActualDataSize(columns[i])
	}
	rec := array.NewRecord(mfw.schema, columns, int64(r.Len()))
	defer rec.Release()
	return mfw.fw.WriteBuffered(rec)
}

func (mfw *multiFieldRecordWriter) GetWrittenUncompressed() uint64 {
	return mfw.writtenUncompressed
}

func (mfw *multiFieldRecordWriter) Close() error {
	return mfw.fw.Close()
}

func newMultiFieldRecordWriter(fieldIDs []FieldID, fields []arrow.Field, writer io.Writer) (*multiFieldRecordWriter, error) {
	schema := arrow.NewSchema(fields, nil)
	fw, err := pqarrow.NewFileWriter(schema, writer,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(math.MaxInt64)), // No additional grouping for now.
		pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	return &multiFieldRecordWriter{
		fw:       fw,
		fieldIDs: fieldIDs,
		schema:   schema,
	}, nil
}

type SerializeWriter[T any] interface {
	WriteValue(value T) error
	Flush() error
	Close() error
}

type SerializeWriterImpl[T any] struct {
	rw         RecordWriter
	serializer Serializer[T]
	batchSize  int

	buffer []T
	pos    int
}

func (sw *SerializeWriterImpl[T]) Flush() error {
	if sw.pos == 0 {
		return nil
	}
	buf := sw.buffer[:sw.pos]
	r, err := sw.serializer(buf)
	if err != nil {
		return err
	}
	defer r.Release()
	if err := sw.rw.Write(r); err != nil {
		return err
	}
	sw.pos = 0
	return nil
}

func (sw *SerializeWriterImpl[T]) WriteValue(value T) error {
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

func (sw *SerializeWriterImpl[T]) Close() error {
	if err := sw.Flush(); err != nil {
		return err
	}
	return sw.rw.Close()
}

func NewSerializeRecordWriter[T any](rw RecordWriter, serializer Serializer[T], batchSize int) *SerializeWriterImpl[T] {
	return &SerializeWriterImpl[T]{
		rw:         rw,
		serializer: serializer,
		batchSize:  batchSize,
	}
}

type simpleArrowRecord struct {
	r arrow.Record

	field2Col map[FieldID]int
}

var _ Record = (*simpleArrowRecord)(nil)

func (sr *simpleArrowRecord) Column(i FieldID) arrow.Array {
	colIdx, ok := sr.field2Col[i]
	if !ok {
		panic(fmt.Sprintf("no such field: %d, having %v", i, sr.field2Col))
	}
	return sr.r.Column(colIdx)
}

func (sr *simpleArrowRecord) Len() int {
	return int(sr.r.NumRows())
}

func (sr *simpleArrowRecord) Release() {
	sr.r.Release()
}

func (sr *simpleArrowRecord) Retain() {
	sr.r.Retain()
}

func (sr *simpleArrowRecord) ArrowSchema() *arrow.Schema {
	return sr.r.Schema()
}

func NewSimpleArrowRecord(r arrow.Record, field2Col map[FieldID]int) *simpleArrowRecord {
	return &simpleArrowRecord{
		r:         r,
		field2Col: field2Col,
	}
}

func BuildRecord(b *array.RecordBuilder, data *InsertData, schema *schemapb.CollectionSchema) error {
	if data == nil {
		return nil
	}
	idx := 0
	serializeField := func(field *schemapb.FieldSchema) error {
		fBuilder := b.Field(idx)
		idx++
		typeEntry, ok := serdeMap[field.DataType]
		if !ok {
			panic("unknown type")
		}
		fieldData, exists := data.Data[field.FieldID]
		if !exists {
			return merr.WrapErrFieldNotFound(field.FieldID, fmt.Sprintf("field %s not found", field.Name))
		}

		if fieldData.RowNum() == 0 {
			return merr.WrapErrServiceInternal(fmt.Sprintf("row num is 0 for field %s", field.Name))
		}

		// Get element type for ArrayOfVector, otherwise use None
		elementType := schemapb.DataType_None
		if field.DataType == schemapb.DataType_ArrayOfVector {
			elementType = field.GetElementType()
		}

		for j := 0; j < fieldData.RowNum(); j++ {
			ok = typeEntry.serialize(fBuilder, fieldData.GetRow(j), elementType)
			if !ok {
				return merr.WrapErrServiceInternal(fmt.Sprintf("serialize error on type %s", field.DataType.String()))
			}
		}
		return nil
	}
	for _, field := range schema.GetFields() {
		if err := serializeField(field); err != nil {
			return err
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			if err := serializeField(field); err != nil {
				return err
			}
		}
	}
	return nil
}

func calculateActualDataSize(a arrow.Array) uint64 {
	data := a.Data()
	if data == nil {
		return 0
	}

	return ActualSizeInBytes(data)
}

// calculate preciese data size of sliced ArrayData
func ActualSizeInBytes(data arrow.ArrayData) uint64 {
	var size uint64
	dt := data.DataType()
	length := data.Len()
	offset := data.Offset()
	buffers := data.Buffers()

	switch dt.ID() {
	case arrow.NULL:
		return 0

	case arrow.BOOL:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}

	case arrow.UINT8, arrow.INT8:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length)
		}

	case arrow.UINT16, arrow.INT16, arrow.FLOAT16:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length * 2)
		}

	case arrow.UINT32, arrow.INT32, arrow.FLOAT32, arrow.DATE32, arrow.TIME32,
		arrow.INTERVAL_MONTHS:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length * 4)
		}

	case arrow.UINT64, arrow.INT64, arrow.FLOAT64, arrow.DATE64, arrow.TIME64,
		arrow.TIMESTAMP, arrow.DURATION, arrow.INTERVAL_DAY_TIME:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length * 8)
		}

	case arrow.INTERVAL_MONTH_DAY_NANO:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length * 16)
		}

	case arrow.DECIMAL128:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length * 16)
		}

	case arrow.DECIMAL256:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length * 32)
		}

	case arrow.FIXED_SIZE_BINARY:
		fsbType := dt.(*arrow.FixedSizeBinaryType)
		byteWidth := fsbType.ByteWidth
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length * byteWidth)
		}

	case arrow.STRING, arrow.BINARY:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil && buffers[2] != nil {
			size += uint64((length + 1) * 4)
			offsets := arrow.Int32Traits.CastFromBytes(buffers[1].Bytes())
			if offset+length < len(offsets) {
				dataStart := offsets[offset]
				dataEnd := offsets[offset+length]
				size += uint64(dataEnd - dataStart)
			}
		}

	case arrow.LARGE_STRING, arrow.LARGE_BINARY:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil && buffers[2] != nil {
			size += uint64((length + 1) * 8)
			offsets := arrow.Int64Traits.CastFromBytes(buffers[1].Bytes())
			if offset+length < len(offsets) {
				dataStart := offsets[offset]
				dataEnd := offsets[offset+length]
				size += uint64(dataEnd - dataStart)
			}
		}

	case arrow.STRING_VIEW, arrow.BINARY_VIEW:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length * arrow.ViewHeaderSizeBytes)
		}
		for i := 2; i < len(buffers); i++ {
			if buffers[i] != nil {
				size += uint64(buffers[i].Len())
			}
		}

	case arrow.LIST, arrow.MAP:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64((length + 1) * 4)
		}
		for _, child := range data.Children() {
			size += ActualSizeInBytes(child)
		}

	case arrow.LARGE_LIST:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64((length + 1) * 8)
		}
		for _, child := range data.Children() {
			size += ActualSizeInBytes(child)
		}

	case arrow.LIST_VIEW:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length * 4)
		}
		if buffers[2] != nil {
			size += uint64(length * 4)
		}
		for _, child := range data.Children() {
			size += ActualSizeInBytes(child)
		}

	case arrow.LARGE_LIST_VIEW:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		if buffers[1] != nil {
			size += uint64(length * 8)
		}
		if buffers[2] != nil {
			size += uint64(length * 8)
		}
		for _, child := range data.Children() {
			size += ActualSizeInBytes(child)
		}

	case arrow.FIXED_SIZE_LIST:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		for _, child := range data.Children() {
			size += ActualSizeInBytes(child)
		}

	case arrow.STRUCT:
		if buffers[0] != nil {
			size += uint64(bitutil.BytesForBits(int64(length)))
		}
		for _, child := range data.Children() {
			size += ActualSizeInBytes(child)
		}

	case arrow.SPARSE_UNION:
		if buffers[0] != nil {
			size += uint64(length)
		}
		for _, child := range data.Children() {
			size += ActualSizeInBytes(child)
		}

	case arrow.DENSE_UNION:
		if buffers[0] != nil {
			size += uint64(length)
		}
		if buffers[1] != nil {
			size += uint64(length * 4)
		}
		for _, child := range data.Children() {
			size += ActualSizeInBytes(child)
		}

	case arrow.DICTIONARY:
		for _, buf := range buffers {
			if buf != nil {
				size += uint64(buf.Len())
			}
		}
		if dict := data.Dictionary(); dict != nil {
			size += ActualSizeInBytes(dict)
		}

	case arrow.RUN_END_ENCODED:
		for _, child := range data.Children() {
			size += ActualSizeInBytes(child)
		}

	case arrow.EXTENSION:
		extType := dt.(arrow.ExtensionType)
		storageData := array.NewData(extType.StorageType(), length, buffers, data.Children(), data.NullN(), offset)
		size = ActualSizeInBytes(storageData)
		storageData.Release()

	default:
		for _, buf := range buffers {
			if buf != nil {
				size += uint64(buf.Len())
			}
		}
		for _, child := range data.Children() {
			size += ActualSizeInBytes(child)
		}
	}

	return size
}
