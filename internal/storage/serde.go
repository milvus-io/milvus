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
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

type Record interface {
	Schema() *map[FieldID]schemapb.DataType
	Column(i FieldID) arrow.Array
	Len() int
	Release()
}

type RecordReader interface {
	Next() error
	Record() Record
	Close()
}

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

func (r *compositeRecord) Schema() *map[FieldID]schemapb.DataType {
	return &r.schema
}

type compositeRecordReader struct {
	RecordReader
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
		crr.iterateNextBatch()
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
		close()
	}
}

func getBlobColLog(blob *Blob) (FieldID, UniqueID) {
	// Blob key format (ref: data_codec.go):
	// ${tenant}/insert_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/${log_idx}
	parts := strings.Split(blob.Key, "/")

	id, _ := strconv.ParseInt(parts[len(parts)-1], 0, 10)
	if len(parts) >= 2 {
		colId, _ := strconv.ParseInt(parts[len(parts)-2], 0, 10)
		return colId, id
	}
	return id, 0
}

func newCompositeRecordReader(blobs []*Blob) (*compositeRecordReader, error) {
	sort.Slice(blobs, func(i, j int) bool {
		iCol, iLog := getBlobColLog(blobs[i])
		jCol, jLog := getBlobColLog(blobs[j])

		if iCol == jCol {
			return iLog < jLog
		}
		return iCol < jCol
	})

	blobm := make([][]*Blob, 0)
	var fieldId FieldID = -1
	var currentCol []*Blob

	for _, blob := range blobs {
		colId, _ := getBlobColLog(blob)
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
	deserializer func(Record, *[]T) error
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
		if deser.rec != nil {
			deser.rec.Release()
		}
		deser.rec = deser.rr.Record()

		if deser.values == nil {
			deser.values = make([]T, deser.rec.Len())
		}
		if err := deser.deserializer(deser.rec, &deser.values); err != nil {
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

func NewDeserializeReader[T any](rr RecordReader, deserializer func(Record, *[]T) error) *DeserializeReader[T] {
	return &DeserializeReader[T]{
		rr:           rr,
		deserializer: deserializer,
	}
}

func deserializeCell(col arrow.Array, dataType schemapb.DataType, i int) (interface{}, bool) {
	switch dataType {
	case schemapb.DataType_Bool:
		arr, ok := col.(*array.Boolean)
		if !ok {
			return nil, false
		}
		return arr.Value(i), true

	case schemapb.DataType_Int8:
		arr, ok := col.(*array.Int8)
		if !ok {
			return nil, false
		}
		return arr.Value(i), true

	case schemapb.DataType_Int16:
		arr, ok := col.(*array.Int16)
		if !ok {
			return nil, false
		}
		return arr.Value(i), true

	case schemapb.DataType_Int32:
		arr, ok := col.(*array.Int32)
		if !ok {
			return nil, false
		}
		return arr.Value(i), true

	case schemapb.DataType_Int64:
		arr, ok := col.(*array.Int64)
		if !ok {
			return nil, false
		}
		return arr.Value(i), true

	case schemapb.DataType_Float:
		arr, ok := col.(*array.Float32)
		if !ok {
			return nil, false
		}
		return arr.Value(i), true

	case schemapb.DataType_Double:
		arr, ok := col.(*array.Float64)
		if !ok {
			return nil, false
		}
		return arr.Value(i), true

	case schemapb.DataType_String, schemapb.DataType_VarChar:
		arr, ok := col.(*array.String)
		if !ok {
			return nil, false
		}
		return arr.Value(i), true

	case schemapb.DataType_Array:
		arr, ok := col.(*array.Binary)
		if !ok {
			return nil, false
		}
		v := &schemapb.ScalarField{}
		proto.Unmarshal(arr.Value(i), v)
		return v, true

	case schemapb.DataType_JSON:
		arr, ok := col.(*array.Binary)
		if !ok {
			return nil, false
		}
		return arr.Value(i), true

	case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		arr, ok := col.(*array.FixedSizeBinary)
		if !ok {
			return nil, false
		}
		return arr.Value(i), true

	case schemapb.DataType_FloatVector:
		arr, ok := col.(*array.FixedSizeBinary)
		if !ok {
			return nil, false
		}
		return arrow.Float32Traits.CastFromBytes(arr.Value(i)), true

	default:
		panic(fmt.Sprintf("unsupported type %s", dataType))
	}
}

func NewBinlogDeserializeReader(blobs []*Blob, PKfieldID UniqueID) (*DeserializeReader[*Value], error) {
	reader, err := newCompositeRecordReader(blobs)
	if err != nil {
		return nil, err
	}

	return NewDeserializeReader(reader, func(r Record, v *[]*Value) error {
		// Note: the return value `Value` is reused.
		for i := 0; i < r.Len(); i++ {
			value := (*v)[i]
			if value == nil {
				value = &Value{}
				(*v)[i] = value
			}

			m := make(map[FieldID]interface{})
			for j, dt := range *r.Schema() {
				d, ok := deserializeCell(r.Column(j), dt, i)
				if ok {
					m[j] = d
				} else {
					return errors.New(fmt.Sprintf("unexpected type %s", dt))
				}
			}

			value.ID = m[common.RowIDField].(int64)
			value.Timestamp = m[common.TimeStampField].(int64)

			pk, err := GenPrimaryKeyByRawData(m[PKfieldID], (*r.Schema())[PKfieldID])
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
