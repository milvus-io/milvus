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

package fastpb

import (
	"encoding/binary"
	"math"
	"reflect"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// InsertRequestViewEncoder serializes a row selection from Source directly into
// the protobuf wire representation of a msgpb.InsertRequest. It deliberately
// does not materialize destination RowIDs, Timestamps, or FieldData columns.
//
// Template supplies the output message metadata (base, collection, partition,
// segment, shard, version, and namespace). Its row-bearing fields are ignored.
// Source supplies RowIDs, Timestamps, and FieldsData. Rows is borrowed rather
// than copied and must be strictly increasing.
//
// For a cursor-owned encoder, Template and Rows must remain immutable until its
// single MarshalTo call returns, while Source and every object reachable from
// it must remain immutable for the cursor's entire lifetime. A standalone
// encoder may be marshaled repeatedly, so all of its borrowed inputs must stay
// immutable for the encoder's entire lifetime. This is the same ownership rule
// as an Arrow array view: the encoder owns only the view, not the referenced
// data. Direct proto3 strings are a trusted internal input; the encoder
// deliberately does not rescan UTF-8 payloads.
type InsertRequestViewEncoder struct {
	template              *msgpb.InsertRequest
	source                *msgpb.InsertRequest
	rows                  []int
	firstFieldDataIndices []int64
	sizePlan              []int
	fieldSizeStates       []insertFieldSizeState
	arrayCellPayloads     []int
	arrayFieldCount       int
	size                  int
	owner                 *InsertRequestViewCursor
	consumed              bool
}

// BodyType binds this encoder to InsertRequest message builders at compile
// time. The returned value is a type marker and is never inspected at runtime.
func (*InsertRequestViewEncoder) BodyType() *msgpb.InsertRequest {
	return nil
}

// InsertRequestViewCursor binds compact nullable-vector prefix state and
// reusable encoding scratch to one source request. Source and every object
// reachable from it must remain immutable from cursor creation until the cursor
// is discarded. Successive calls to NextEncoder must use globally increasing
// row selections. The returned encoder borrows the cursor scratch until
// MarshalTo returns, so callers must consume it synchronously before requesting
// the next view.
type InsertRequestViewCursor struct {
	source                       *msgpb.InsertRequest
	scanRow                      int
	lastSelectedRow              int
	fieldDataIndices             []int64
	nextFieldDataIndices         []int64
	rowFieldDataIndices          []int64
	encoderFirstFieldDataIndices []int64
	pendingFieldDataIndices      []int64
	pendingFieldDeltas           []insertFieldRowDelta
	pendingRow                   int
	sizeState                    insertRequestSizeState
	sizePlan                     []int
	activeEncoder                *InsertRequestViewEncoder
}

// NewInsertRequestViewCursor creates a source-bound cursor for a sequential
// message splitter.
func NewInsertRequestViewCursor(source *msgpb.InsertRequest) (*InsertRequestViewCursor, error) {
	if source == nil {
		return nil, insertViewInternal("nil source request")
	}
	if len(source.GetRowData()) != 0 {
		return nil, insertViewInternal("row-based InsertRequest is not supported")
	}
	return &InsertRequestViewCursor{
		source:                       source,
		lastSelectedRow:              -1,
		fieldDataIndices:             make([]int64, len(source.GetFieldsData())),
		nextFieldDataIndices:         make([]int64, len(source.GetFieldsData())),
		rowFieldDataIndices:          make([]int64, len(source.GetFieldsData())),
		encoderFirstFieldDataIndices: make([]int64, len(source.GetFieldsData())),
		pendingFieldDataIndices:      make([]int64, len(source.GetFieldsData())),
		pendingFieldDeltas:           make([]insertFieldRowDelta, len(source.GetFieldsData())),
		pendingRow:                   -1,
	}, nil
}

// newEncoder creates an unbounded borrowed view for package tests. Production
// splitters must pass an explicit size limit through NextEncoder.
func (c *InsertRequestViewCursor) newEncoder(template *msgpb.InsertRequest, rows []int) (*InsertRequestViewEncoder, error) {
	encoder, consumed, err := c.NextEncoder(template, rows, 0)
	if err != nil {
		return nil, err
	}
	if consumed != len(rows) {
		return nil, insertViewInternal("unbounded encoder consumed %d of %d rows", consumed, len(rows))
	}
	return encoder, nil
}

// NextEncoder returns an encoder over the largest non-empty prefix of rows
// whose exact plaintext protobuf body size is below sizeLimit. A single row is
// always returned even when it reaches or exceeds the limit, matching the
// existing insert split behavior. A non-positive limit disables splitting.
//
// The exact sizing pass and MarshalTo share a replay plan plus row-major ARRAY
// scratch. Callers must synchronously marshal the returned encoder before
// requesting the next one.
func (c *InsertRequestViewCursor) NextEncoder(template *msgpb.InsertRequest, rows []int, sizeLimit int) (*InsertRequestViewEncoder, int, error) {
	if c == nil || c.source == nil {
		return nil, 0, insertViewInternal("nil insert request view cursor")
	}
	if c.activeEncoder != nil {
		return nil, 0, insertViewInternal("previous cursor encoder has not been marshaled")
	}
	if len(rows) == 0 {
		return nil, 0, insertViewInternal("row selection is empty")
	}
	fieldCount := len(c.source.GetFieldsData())
	if len(c.fieldDataIndices) != fieldCount || len(c.nextFieldDataIndices) != fieldCount ||
		len(c.rowFieldDataIndices) != fieldCount || len(c.encoderFirstFieldDataIndices) != fieldCount ||
		len(c.pendingFieldDataIndices) != fieldCount || len(c.pendingFieldDeltas) != fieldCount {
		return nil, 0, insertViewInternal("source FieldsData count changed while cursor was in use")
	}
	if err := c.sizeState.reset(template, c.source, len(rows)); err != nil {
		return nil, 0, err
	}

	copy(c.nextFieldDataIndices, c.fieldDataIndices)
	planScanRow := c.scanRow
	previous := c.lastSelectedRow
	consumed := 0
	aggregatedRows, err := c.sizeState.aggregateSimpleRows(rows, previous, sizeLimit)
	if err != nil {
		return nil, 0, err
	}
	if aggregatedRows < 0 || aggregatedRows > len(rows) {
		return nil, 0, insertViewInternal("aggregated row count %d is out of range for %d selected rows", aggregatedRows, len(rows))
	}
	if aggregatedRows > 0 {
		aggregated := rows[:aggregatedRows]
		firstRow := aggregated[0]
		lastRow := aggregated[len(aggregated)-1]
		for fieldIndex := range c.sizeState.fields {
			c.encoderFirstFieldDataIndices[fieldIndex] = int64(firstRow)
			c.nextFieldDataIndices[fieldIndex] = int64(lastRow + 1)
		}
		c.pendingRow = -1
		planScanRow = lastRow + 1
		previous = lastRow
		consumed = aggregatedRows
	}
	for i := aggregatedRows; i < len(rows); i++ {
		row := rows[i]
		if row < 0 {
			return nil, 0, insertViewInternal("row offset at selection index %d is negative: %d", i, row)
		}
		if row <= previous {
			return nil, 0, insertViewInternal("cursor row offsets must be globally increasing: rows[%d]=%d after %d", i, row, previous)
		}
		usePending := i == 0 && row == c.pendingRow
		var encodedSize int
		var err error
		if usePending {
			copy(c.rowFieldDataIndices, c.pendingFieldDataIndices)
			encodedSize, err = c.sizeState.previewCachedRow(row, c.pendingFieldDeltas)
		} else {
			if i == 0 {
				c.pendingRow = -1
			}
			if err := c.resolveRowFieldDataIndices(planScanRow, row); err != nil {
				return nil, 0, err
			}
			encodedSize, err = c.sizeState.previewRow(row, c.rowFieldDataIndices)
		}
		if err != nil {
			return nil, 0, err
		}
		if consumed > 0 && sizeLimit > 0 && encodedSize >= sizeLimit {
			c.pendingRow = row
			copy(c.pendingFieldDataIndices, c.rowFieldDataIndices)
			copy(c.pendingFieldDeltas, c.sizeState.rowDeltas)
			break
		}
		if consumed == 0 {
			copy(c.encoderFirstFieldDataIndices, c.rowFieldDataIndices)
		}
		c.sizeState.commitRow(row, encodedSize)
		if usePending {
			c.pendingRow = -1
		}
		for fieldIndex := range c.sizeState.fields {
			if c.sizeState.fields[fieldIndex].compactVector {
				nextIndex := c.rowFieldDataIndices[fieldIndex]
				if c.sizeState.fields[fieldIndex].validData[row] {
					nextIndex++
				}
				c.nextFieldDataIndices[fieldIndex] = nextIndex
			} else {
				c.nextFieldDataIndices[fieldIndex] = int64(row + 1)
			}
		}
		planScanRow = row + 1
		previous = row
		consumed++
	}
	if consumed == 0 {
		return nil, 0, insertViewInternal("exact insert splitter produced an empty selection")
	}

	sizePlan, encodedSize, err := c.sizeState.buildPlan(c.sizePlan[:0])
	if err != nil {
		return nil, 0, err
	}
	encoder := &InsertRequestViewEncoder{
		template:              template,
		source:                c.source,
		rows:                  rows[:consumed],
		firstFieldDataIndices: c.encoderFirstFieldDataIndices,
		sizePlan:              sizePlan,
		fieldSizeStates:       c.sizeState.fields,
		arrayCellPayloads:     c.sizeState.arrayCellPayloads,
		arrayFieldCount:       c.sizeState.arrayFieldCount,
		size:                  encodedSize,
	}

	c.fieldDataIndices, c.nextFieldDataIndices = c.nextFieldDataIndices, c.fieldDataIndices
	c.scanRow = planScanRow
	c.lastSelectedRow = rows[consumed-1]
	c.sizePlan = sizePlan
	encoder.owner = c
	c.activeEncoder = encoder
	return encoder, consumed, nil
}

func (c *InsertRequestViewCursor) resolveRowFieldDataIndices(scanRow, row int) error {
	if scanRow < 0 || scanRow > row {
		return insertViewInternal("invalid compact-vector scan range [%d:%d]", scanRow, row)
	}
	for fieldIndex := range c.sizeState.fields {
		state := &c.sizeState.fields[fieldIndex]
		if !state.compactVector {
			c.rowFieldDataIndices[fieldIndex] = int64(row)
			continue
		}
		valid := state.validData
		if row >= len(valid) {
			return insertViewInternal("row offset %d exceeds ValidData length %d for field %q (%d)", row, len(valid), state.field.GetFieldName(), state.field.GetFieldId())
		}
		if scanRow > len(valid) {
			return insertViewInternal("compact-vector scan row %d exceeds ValidData length %d for field %q (%d)", scanRow, len(valid), state.field.GetFieldName(), state.field.GetFieldId())
		}
		dataIndex := c.nextFieldDataIndices[fieldIndex]
		for _, isValid := range valid[scanRow:row] {
			if isValid {
				dataIndex++
			}
		}
		c.rowFieldDataIndices[fieldIndex] = dataIndex
	}
	return nil
}

// NewInsertRequestViewEncoder creates a borrowed row-selection view over source.
// Invalid offsets and malformed source column shapes are Milvus-internal
// contract violations, so they intentionally use ErrServiceInternal rather
// than an input-error code.
func NewInsertRequestViewEncoder(template, source *msgpb.InsertRequest, rows []int) (*InsertRequestViewEncoder, error) {
	cursor, err := NewInsertRequestViewCursor(source)
	if err != nil {
		return nil, err
	}
	encoder, consumed, err := cursor.NextEncoder(template, rows, 0)
	if err != nil {
		return nil, err
	}
	if consumed != len(rows) {
		return nil, insertViewInternal("unbounded encoder consumed %d of %d rows", consumed, len(rows))
	}
	// Standalone encoders own their scratch through the slices above and may be
	// marshaled more than once. Cursor encoders remain single-use so their
	// scratch can be safely recycled by the next exact split.
	encoder.owner = nil
	cursor.activeEncoder = nil
	return encoder, nil
}

// EncodedSize returns the exact number of bytes MarshalTo writes.
func (e *InsertRequestViewEncoder) EncodedSize() (int, error) {
	if e == nil {
		return 0, insertViewInternal("nil encoder")
	}
	if err := e.checkCursorOwnership(); err != nil {
		return 0, err
	}
	return e.size, nil
}

// MarshalTo writes the selected request into dst without constructing target
// protobuf columns. dst may be larger than EncodedSize; only its prefix is used.
func (e *InsertRequestViewEncoder) MarshalTo(dst []byte) (int, error) {
	if e == nil {
		return 0, insertViewInternal("nil encoder")
	}
	if err := e.checkCursorOwnership(); err != nil {
		return 0, err
	}
	if len(dst) < e.size {
		return 0, insertViewInternal("destination buffer has %d bytes, need %d", len(dst), e.size)
	}

	w := newInsertViewMarshalWriter(dst[:0:e.size], &e.sizePlan)
	if err := e.appendRequest(w); err != nil {
		return 0, err
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.planIndex != len(e.sizePlan) {
		return 0, insertViewInternal("encoded size plan changed while borrowed data was in use: consumed %d entries, expected %d", w.planIndex, len(e.sizePlan))
	}
	if w.n != e.size || len(w.out) != e.size {
		return 0, insertViewInternal("encoded size changed while borrowed data was in use: expected %d, wrote %d", e.size, w.n)
	}
	if e.size > 0 && &w.out[0] != &dst[0] {
		return 0, insertViewInternal("encoder exceeded the caller-provided destination buffer")
	}
	if e.owner != nil {
		e.consumed = true
		e.owner.activeEncoder = nil
	}
	return e.size, nil
}

func (e *InsertRequestViewEncoder) checkCursorOwnership() error {
	if e.owner == nil {
		return nil
	}
	if e.consumed {
		return insertViewInternal("encoder has already been marshaled")
	}
	// Pointer identity also rejects value-copied encoders. Without it, a stale
	// copy could release scratch that a newer encoder currently borrows.
	if e.owner.activeEncoder != e {
		return insertViewInternal("encoder is no longer active for its cursor")
	}
	return nil
}

func (e *InsertRequestViewEncoder) appendRequest(w *insertViewWriter) error {
	t := e.template
	if t.GetBase() != nil {
		if err := w.protoMessage(1, t.GetBase(), "insert base"); err != nil {
			return err
		}
	}
	if err := w.proto3String(2, t.GetShardName(), "shard name"); err != nil {
		return err
	}
	if err := w.proto3String(3, t.GetDbName(), "database name"); err != nil {
		return err
	}
	if err := w.proto3String(4, t.GetCollectionName(), "collection name"); err != nil {
		return err
	}
	if err := w.proto3String(5, t.GetPartitionName(), "partition name"); err != nil {
		return err
	}
	w.proto3Varint(6, uint64(t.GetDbID()))
	w.proto3Varint(7, uint64(t.GetCollectionID()))
	w.proto3Varint(8, uint64(t.GetPartitionID()))
	w.proto3Varint(9, uint64(t.GetSegmentID()))

	if err := e.appendSelectedUint64(w, 10, e.source.GetTimestamps()); err != nil {
		return err
	}
	if err := e.appendSelectedInt64(w, 11, e.source.GetRowIDs()); err != nil {
		return err
	}

	for i, field := range e.source.GetFieldsData() {
		if field == nil {
			return insertViewInternal("source FieldsData[%d] is nil", i)
		}
		if field.GetStructArrays() != nil {
			return insertViewInternal("source field %q (%d) still contains StructArrays; proxy must flatten struct fields before repacking", field.GetFieldName(), field.GetFieldId())
		}
		fieldSize, err := w.plannedInt()
		if err != nil {
			return err
		}
		if err := w.message(13, fieldSize, func() error {
			return e.appendFieldData(w, i, field)
		}); err != nil {
			return err
		}
	}

	w.proto3Varint(14, uint64(len(e.rows)))
	w.proto3Varint(15, uint64(t.GetVersion()))
	if t.Namespace != nil {
		if err := w.string(16, *t.Namespace, true, "namespace"); err != nil {
			return err
		}
	}

	// A shallow-copy-and-overwrite implementation would retain template unknown
	// fields. Preserve them here as well so future metadata remains wire-safe.
	w.raw(t.ProtoReflect().GetUnknown())
	return w.err
}

func (e *InsertRequestViewEncoder) appendSelectedUint64(w *insertViewWriter, fieldNumber protowire.Number, values []uint64) error {
	if len(e.rows) == 0 {
		return nil
	}
	payloadSize, err := w.plannedInt()
	if err != nil {
		return err
	}
	return w.message(fieldNumber, payloadSize, func() error {
		for _, row := range e.rows {
			w.varint(values[row])
		}
		return w.err
	})
}

func (e *InsertRequestViewEncoder) appendSelectedInt64(w *insertViewWriter, fieldNumber protowire.Number, values []int64) error {
	if len(e.rows) == 0 {
		return nil
	}
	payloadSize, err := w.plannedInt()
	if err != nil {
		return err
	}
	return w.message(fieldNumber, payloadSize, func() error {
		for _, row := range e.rows {
			w.varint(uint64(values[row]))
		}
		return w.err
	})
}

func (e *InsertRequestViewEncoder) appendFieldData(w *insertViewWriter, fieldIndex int, field *schemapb.FieldData) error {
	w.proto3Varint(1, uint64(field.GetType()))
	if err := w.proto3String(2, field.GetFieldName(), "field name"); err != nil {
		return err
	}

	switch value := field.Field.(type) {
	case nil:
		// Match AppendFieldData: metadata survives even if the source has no
		// scalar/vector payload.
	case *schemapb.FieldData_Scalars:
		if value.Scalars == nil {
			return insertViewInternal("scalar field %q (%d) has a nil ScalarField", field.GetFieldName(), field.GetFieldId())
		}
		scalarSize, err := w.plannedInt()
		if err != nil {
			return err
		}
		if err := w.message(3, scalarSize, func() error {
			return e.appendScalarField(w, fieldIndex, value.Scalars)
		}); err != nil {
			return err
		}
	case *schemapb.FieldData_Vectors:
		if value.Vectors == nil {
			return insertViewInternal("vector field %q (%d) has a nil VectorField", field.GetFieldName(), field.GetFieldId())
		}
		vectorSize, err := w.plannedInt()
		if err != nil {
			return err
		}
		if err := w.message(4, vectorSize, func() error {
			return e.appendVectorField(w, fieldIndex, field, value.Vectors)
		}); err != nil {
			return err
		}
	case *schemapb.FieldData_StructArrays:
		return insertViewInternal("source field %q (%d) still contains StructArrays; proxy must flatten struct fields before repacking", field.GetFieldName(), field.GetFieldId())
	default:
		return insertViewInternal("source field %q (%d) has unsupported FieldData oneof %T", field.GetFieldName(), field.GetFieldId(), value)
	}

	w.proto3Varint(5, uint64(field.GetFieldId()))
	if field.GetIsDynamic() {
		w.varintField(6, 1)
	}
	return w.err
}

// appendFieldValidData writes the row-selected validity mask into the nested
// ScalarField (field 17) / VectorField (field 9) message, the field-specific
// location #52203 moved it to. It must run after every oneof value write so
// the planned-slot consumption order matches appendPlan.
func (e *InsertRequestViewEncoder) appendFieldValidData(w *insertViewWriter, fieldIndex int, fieldNumber protowire.Number) error {
	if fieldIndex < 0 || fieldIndex >= len(e.fieldSizeStates) {
		return insertViewInternal("validity payload field index %d is out of range", fieldIndex)
	}
	valid := e.fieldSizeStates[fieldIndex].validData
	if len(valid) == 0 || len(e.rows) == 0 {
		return w.err
	}
	validSize, err := w.plannedInt()
	if err != nil {
		return err
	}
	return w.message(fieldNumber, validSize, func() error {
		for _, row := range e.rows {
			if valid[row] {
				w.varint(1)
			} else {
				w.varint(0)
			}
		}
		return w.err
	})
}

func (e *InsertRequestViewEncoder) appendScalarField(w *insertViewWriter, fieldIndex int, field *schemapb.ScalarField) error {
	var err error
	switch value := field.Data.(type) {
	case nil:
	case *schemapb.ScalarField_BoolData:
		err = e.appendBoolArray(w, 1, value.BoolData)
	case *schemapb.ScalarField_IntData:
		err = e.appendInt32Array(w, 2, value.IntData, "int scalar")
	case *schemapb.ScalarField_LongData:
		err = e.appendInt64Array(w, 3, value.LongData, "long scalar")
	case *schemapb.ScalarField_FloatData:
		err = e.appendFloat32Array(w, 4, value.FloatData)
	case *schemapb.ScalarField_DoubleData:
		err = e.appendFloat64Array(w, 5, value.DoubleData)
	case *schemapb.ScalarField_StringData:
		err = e.appendStringArray(w, 6, value.StringData, "string scalar")
	// These cold oneofs are declared by the current schema but are not handled
	// by the old row-at-a-time AppendFieldData oracle. Encoding them here is a
	// forward-safe row selection; the differential test covers the old oracle's
	// real support set separately.
	case *schemapb.ScalarField_BytesData:
		err = e.appendBytesArray(w, 7, value.BytesData, "bytes scalar")
	case *schemapb.ScalarField_ArrayData:
		err = e.appendArrayArray(w, fieldIndex, 8, value.ArrayData)
	case *schemapb.ScalarField_JsonData:
		err = e.appendRawBytesRows(w, 9, value.JsonData.GetData(), "JSON scalar", value.JsonData == nil)
	case *schemapb.ScalarField_GeometryData:
		err = e.appendRawBytesRows(w, 10, value.GeometryData.GetData(), "geometry scalar", value.GeometryData == nil)
	case *schemapb.ScalarField_TimestamptzData:
		err = e.appendInt64Values(w, 11, value.TimestamptzData.GetData(), "timestamptz scalar", value.TimestamptzData == nil)
	case *schemapb.ScalarField_GeometryWktData:
		err = e.appendStringValues(w, 12, value.GeometryWktData.GetData(), "geometry WKT scalar", value.GeometryWktData == nil)
	case *schemapb.ScalarField_MolData:
		err = e.appendRawBytesRows(w, 13, value.MolData.GetData(), "molecular scalar", value.MolData == nil)
	case *schemapb.ScalarField_MolSmilesData:
		err = e.appendStringValues(w, 14, value.MolSmilesData.GetData(), "molecular SMILES scalar", value.MolSmilesData == nil)
	case *schemapb.ScalarField_DateData:
		err = e.appendInt32Values(w, 15, value.DateData.GetData(), "date scalar", value.DateData == nil)
	case *schemapb.ScalarField_TimeData:
		err = e.appendInt64Values(w, 16, value.TimeData.GetData(), "time scalar", value.TimeData == nil)
	default:
		return insertViewInternal("unsupported ScalarField oneof %T", value)
	}
	if err != nil {
		return err
	}
	return e.appendFieldValidData(w, fieldIndex, 17)
}

func (e *InsertRequestViewEncoder) appendBoolArray(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.BoolArray) error {
	if values == nil {
		return insertViewInternal("nil bool scalar array")
	}
	payloadSize, err := w.plannedInt()
	if err != nil {
		return err
	}
	arraySize := 0
	if payloadSize > 0 {
		arraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(fieldNumber, arraySize, func() error {
		if payloadSize == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			for _, row := range e.rows {
				if values.GetData()[row] {
					w.varint(1)
				} else {
					w.varint(0)
				}
			}
			return w.err
		})
	})
}

func (e *InsertRequestViewEncoder) appendInt32Array(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.IntArray, label string) error {
	if values == nil {
		return insertViewInternal("nil %s array", label)
	}
	return e.appendInt32Values(w, fieldNumber, values.GetData(), label, false)
}

func (e *InsertRequestViewEncoder) appendInt64Array(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.LongArray, label string) error {
	if values == nil {
		return insertViewInternal("nil %s array", label)
	}
	return e.appendInt64Values(w, fieldNumber, values.GetData(), label, false)
}

func (e *InsertRequestViewEncoder) appendInt32Values(w *insertViewWriter, fieldNumber protowire.Number, values []int32, label string, nilArray bool) error {
	if nilArray {
		return insertViewInternal("nil %s array", label)
	}
	payloadSize, err := w.plannedInt()
	if err != nil {
		return err
	}
	arraySize := 0
	if len(e.rows) > 0 {
		arraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(fieldNumber, arraySize, func() error {
		if len(e.rows) == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			for _, row := range e.rows {
				w.varint(uint64(values[row]))
			}
			return w.err
		})
	})
}

func (e *InsertRequestViewEncoder) appendInt64Values(w *insertViewWriter, fieldNumber protowire.Number, values []int64, label string, nilArray bool) error {
	if nilArray {
		return insertViewInternal("nil %s array", label)
	}
	payloadSize, err := w.plannedInt()
	if err != nil {
		return err
	}
	arraySize := 0
	if len(e.rows) > 0 {
		arraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(fieldNumber, arraySize, func() error {
		if len(e.rows) == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			for _, row := range e.rows {
				w.varint(uint64(values[row]))
			}
			return w.err
		})
	})
}

func (e *InsertRequestViewEncoder) appendFloat32Array(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.FloatArray) error {
	if values == nil {
		return insertViewInternal("nil float scalar array")
	}
	payloadSize, err := w.plannedInt()
	if err != nil {
		return err
	}
	arraySize := 0
	if payloadSize > 0 {
		arraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(fieldNumber, arraySize, func() error {
		if payloadSize == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			for _, row := range e.rows {
				w.fixed32(math.Float32bits(values.GetData()[row]))
			}
			return w.err
		})
	})
}

func (e *InsertRequestViewEncoder) appendFloat64Array(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.DoubleArray) error {
	if values == nil {
		return insertViewInternal("nil double scalar array")
	}
	payloadSize, err := w.plannedInt()
	if err != nil {
		return err
	}
	arraySize := 0
	if payloadSize > 0 {
		arraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(fieldNumber, arraySize, func() error {
		if payloadSize == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			for _, row := range e.rows {
				w.fixed64(math.Float64bits(values.GetData()[row]))
			}
			return w.err
		})
	})
}

func (e *InsertRequestViewEncoder) appendStringArray(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.StringArray, label string) error {
	if values == nil {
		return insertViewInternal("nil %s array", label)
	}
	return e.appendStringValues(w, fieldNumber, values.GetData(), label, false)
}

func (e *InsertRequestViewEncoder) appendStringValues(w *insertViewWriter, fieldNumber protowire.Number, values []string, label string, nilArray bool) error {
	if nilArray {
		return insertViewInternal("nil %s array", label)
	}
	arraySize, err := w.plannedInt()
	if err != nil {
		return err
	}
	return w.message(fieldNumber, arraySize, func() error {
		for _, row := range e.rows {
			w.stringBytes(1, values[row])
		}
		return w.err
	})
}

func (e *InsertRequestViewEncoder) appendBytesArray(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.BytesArray, label string) error {
	if values == nil {
		return insertViewInternal("nil %s array", label)
	}
	return e.appendRawBytesRows(w, fieldNumber, values.GetData(), label, false)
}

func (e *InsertRequestViewEncoder) appendRawBytesRows(w *insertViewWriter, fieldNumber protowire.Number, values [][]byte, label string, nilArray bool) error {
	if nilArray {
		return insertViewInternal("nil %s array", label)
	}
	arraySize, err := w.plannedInt()
	if err != nil {
		return err
	}
	return w.message(fieldNumber, arraySize, func() error {
		for _, row := range e.rows {
			w.bytes(1, values[row])
		}
		return w.err
	})
}

func (e *InsertRequestViewEncoder) appendArrayArray(w *insertViewWriter, fieldIndex int, fieldNumber protowire.Number, values *schemapb.ArrayArray) error {
	if values == nil {
		return insertViewInternal("nil array scalar payload")
	}
	if fieldIndex < 0 || fieldIndex >= len(e.fieldSizeStates) {
		return insertViewInternal("array cell payload field index %d is out of range", fieldIndex)
	}
	arrayOrdinal := e.fieldSizeStates[fieldIndex].arrayOrdinal
	if arrayOrdinal < 0 || arrayOrdinal >= e.arrayFieldCount {
		return insertViewInternal("array field %d has invalid payload ordinal %d of %d", fieldIndex, arrayOrdinal, e.arrayFieldCount)
	}
	expectedPayloads, err := checkedProduct(len(e.rows), e.arrayFieldCount, "array cell payload plan")
	if err != nil {
		return err
	}
	if len(e.arrayCellPayloads) != expectedPayloads {
		return insertViewInternal("array cell payload plan has %d entries, expected %d", len(e.arrayCellPayloads), expectedPayloads)
	}
	arraySize, err := w.plannedInt()
	if err != nil {
		return err
	}
	return w.message(fieldNumber, arraySize, func() error {
		for i, row := range e.rows {
			payload := e.arrayCellPayloads[i*e.arrayFieldCount+arrayOrdinal]
			if err := appendArrayCell(w, values.GetData()[row], payload); err != nil {
				return err
			}
		}
		w.proto3Varint(2, uint64(values.GetElementType()))
		return w.err
	})
}

// appendArrayCell writes one ArrayArray element. The sizing pass stores one
// payload token per selected cell in cursor scratch: a non-negative arithmetic
// payload size or scalarCellProtoFallbackPayload. Replaying that token removes
// the second O(elements) sizing traversal while the writer's byte-count checks
// detect borrowed-data mutation.
func appendArrayCell(w *insertViewWriter, cell *schemapb.ScalarField, payload int) error {
	plan, ok := classifyScalarCell(cell)
	if payload == scalarCellProtoFallbackPayload {
		if ok {
			return insertViewInternal("array cell classification changed after planning: expected protobuf fallback")
		}
		return w.protoMessage(1, cell, "array scalar row")
	}
	if payload < 0 {
		return insertViewInternal("array cell plan contains invalid payload size %d", payload)
	}
	if !ok {
		return insertViewInternal("array cell classification changed after planning: expected arithmetic path")
	}
	plan.payload = payload
	return w.message(1, plan.scalarCellSize(), func() error {
		return appendScalarCell(w, cell, plan)
	})
}

func (e *InsertRequestViewEncoder) appendVectorField(w *insertViewWriter, fieldIndex int, field *schemapb.FieldData, vector *schemapb.VectorField) error {
	var err error
	switch value := vector.Data.(type) {
	case nil:
		w.proto3Varint(1, uint64(vector.GetDim()))
		err = w.err
	case *schemapb.VectorField_BinaryVector:
		err = e.appendDenseBytesVector(w, fieldIndex, field, vector.GetDim(), 3, value.BinaryVector, 8, "binary vector")
	case *schemapb.VectorField_FloatVector:
		err = e.appendFloatVector(w, fieldIndex, field, vector.GetDim(), value.FloatVector)
	case *schemapb.VectorField_Float16Vector:
		err = e.appendDenseBytesVector(w, fieldIndex, field, vector.GetDim(), 4, value.Float16Vector, 2, "float16 vector")
	case *schemapb.VectorField_Bfloat16Vector:
		err = e.appendDenseBytesVector(w, fieldIndex, field, vector.GetDim(), 5, value.Bfloat16Vector, 2, "bfloat16 vector")
	case *schemapb.VectorField_SparseFloatVector:
		err = e.appendSparseVector(w, fieldIndex, field, vector, value.SparseFloatVector)
	case *schemapb.VectorField_Int8Vector:
		err = e.appendDenseBytesVector(w, fieldIndex, field, vector.GetDim(), 7, value.Int8Vector, 1, "int8 vector")
	case *schemapb.VectorField_VectorArray:
		err = e.appendVectorArray(w, fieldIndex, vector, value.VectorArray)
	default:
		return insertViewInternal("field %q (%d) has unsupported VectorField oneof %T", field.GetFieldName(), field.GetFieldId(), value)
	}
	if err != nil {
		return err
	}
	return e.appendFieldValidData(w, fieldIndex, 9)
}

// appendDenseBytesVector handles byte-backed dense vectors. unit denotes bytes
// per dimension, except binary vectors where unit=8 means dimensions per byte.
func (e *InsertRequestViewEncoder) appendDenseBytesVector(w *insertViewWriter, fieldIndex int, field *schemapb.FieldData, dim int64, fieldNumber protowire.Number, data []byte, unit int, label string) error {
	stride, err := denseVectorStride(dim, unit, label)
	if err != nil {
		return err
	}
	selected, err := w.plannedInt()
	if err != nil {
		return err
	}
	w.proto3Varint(1, uint64(dim))
	if selected == 0 {
		return w.err
	}
	payloadSize, err := checkedProduct(selected, stride, label+" payload")
	if err != nil {
		return err
	}
	return w.message(fieldNumber, payloadSize, func() error {
		_, err := e.countSelectedVectorRows(fieldIndex, field, func(dataIndex int) error {
			start, end, err := vectorBounds(dataIndex, stride, len(data), field, label)
			if err != nil {
				return err
			}
			w.raw(data[start:end])
			return w.err
		})
		return err
	})
}

func (e *InsertRequestViewEncoder) appendFloatVector(w *insertViewWriter, fieldIndex int, field *schemapb.FieldData, dim int64, values *schemapb.FloatArray) error {
	if values == nil {
		return insertViewInternal("field %q (%d) has a nil float vector payload", field.GetFieldName(), field.GetFieldId())
	}
	stride, err := nonNegativeDim(dim, "float vector")
	if err != nil {
		return err
	}
	selected, err := w.plannedInt()
	if err != nil {
		return err
	}
	w.proto3Varint(1, uint64(dim))
	if selected == 0 {
		return w.err
	}
	elementCount, err := checkedProduct(selected, stride, "float vector element count")
	if err != nil {
		return err
	}
	payloadSize, err := checkedProduct(elementCount, 4, "float vector payload")
	if err != nil {
		return err
	}
	floatArraySize := 0
	if payloadSize > 0 {
		floatArraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(2, floatArraySize, func() error {
		if payloadSize == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			_, err := e.countSelectedVectorRows(fieldIndex, field, func(dataIndex int) error {
				start, end, err := vectorBounds(dataIndex, stride, len(values.GetData()), field, "float vector")
				if err != nil {
					return err
				}
				w.raw(f32ReadOnlyBytes(values.GetData()[start:end]))
				return w.err
			})
			return err
		})
	})
}

func (e *InsertRequestViewEncoder) appendSparseVector(w *insertViewWriter, fieldIndex int, field *schemapb.FieldData, vector *schemapb.VectorField, values *schemapb.SparseFloatArray) error {
	if values == nil {
		return insertViewInternal("field %q (%d) has a nil sparse vector payload", field.GetFieldName(), field.GetFieldId())
	}
	selected, err := w.plannedInt()
	if err != nil {
		return err
	}
	sparseSize, err := w.plannedInt()
	if err != nil {
		return err
	}
	selectedDimSize, err := w.plannedInt()
	if err != nil {
		return err
	}
	selectedDim := int64(selectedDimSize)

	outerDim := vector.GetDim()
	if selected > 0 {
		// This intentionally matches AppendFieldData's sparse-vector behavior:
		// once data is present, VectorField.Dim comes from SparseFloatArray.Dim.
		outerDim = values.GetDim()
	}
	w.proto3Varint(1, uint64(outerDim))
	if selected == 0 {
		return w.err
	}
	if selectedDim != 0 {
		sparseSize += protowire.SizeTag(2) + protowire.SizeVarint(uint64(selectedDim))
	}
	return w.message(6, sparseSize, func() error {
		_, err := e.countSelectedVectorRows(fieldIndex, field, func(dataIndex int) error {
			w.bytes(1, values.GetContents()[dataIndex])
			return w.err
		})
		if err != nil {
			return err
		}
		w.proto3Varint(2, uint64(selectedDim))
		return w.err
	})
}

func (e *InsertRequestViewEncoder) appendVectorArray(w *insertViewWriter, fieldIndex int, vector *schemapb.VectorField, values *schemapb.VectorArray) error {
	if values == nil {
		return insertViewInternal("nil ArrayOfVector payload")
	}
	if fieldIndex < 0 || fieldIndex >= len(e.fieldSizeStates) {
		return insertViewInternal("ArrayOfVector cell payload field index %d is out of range", fieldIndex)
	}
	arrayOrdinal := e.fieldSizeStates[fieldIndex].arrayOrdinal
	if arrayOrdinal < 0 || arrayOrdinal >= e.arrayFieldCount {
		return insertViewInternal("ArrayOfVector field %d has invalid payload ordinal %d of %d", fieldIndex, arrayOrdinal, e.arrayFieldCount)
	}
	expectedPayloads, err := checkedProduct(len(e.rows), e.arrayFieldCount, "nested cell payload plan")
	if err != nil {
		return err
	}
	if len(e.arrayCellPayloads) != expectedPayloads {
		return insertViewInternal("nested cell payload plan has %d entries, expected %d", len(e.arrayCellPayloads), expectedPayloads)
	}
	w.proto3Varint(1, uint64(vector.GetDim()))
	arraySize, err := w.plannedInt()
	if err != nil {
		return err
	}
	return w.message(8, arraySize, func() error {
		w.proto3Varint(1, uint64(values.GetDim()))
		for i, row := range e.rows {
			payload := e.arrayCellPayloads[i*e.arrayFieldCount+arrayOrdinal]
			if err := appendVectorArrayCell(w, values.GetData()[row], payload); err != nil {
				return err
			}
		}
		w.proto3Varint(3, uint64(values.GetElementType()))
		return w.err
	})
}

func appendVectorArrayCell(w *insertViewWriter, cell *schemapb.VectorField, payload int) error {
	plan, ok, err := classifyVectorArrayCell(cell)
	if err != nil {
		return err
	}
	if payload == vectorArrayCellProtoFallback {
		if ok {
			return insertViewInternal("ArrayOfVector cell classification changed after planning: expected protobuf fallback")
		}
		return w.protoMessage(2, cell, "ArrayOfVector row")
	}
	if payload < 0 {
		return insertViewInternal("ArrayOfVector cell plan contains invalid payload size %d", payload)
	}
	if !ok {
		return insertViewInternal("ArrayOfVector cell classification changed after planning: expected arithmetic path")
	}
	if payload != plan.payloadSize {
		return insertViewInternal("ArrayOfVector cell payload changed after planning: expected %d, got %d", payload, plan.payloadSize)
	}
	return w.message(2, plan.cellSize, func() error {
		if isNilProto(cell) {
			return nil
		}
		w.proto3Varint(1, uint64(cell.GetDim()))
		if plan.kind != vectorArrayCellPlanFloat {
			return w.err
		}
		values := cell.GetFloatVector().GetData()
		return w.message(2, plan.floatArraySize, func() error {
			if payload == 0 {
				return nil
			}
			return w.message(1, payload, func() error {
				w.raw(f32ReadOnlyBytes(values))
				return w.err
			})
		})
	})
}

// countSelectedVectorRows converts logical row offsets into physical compact
// vector indices. Nullable scalar columns and ArrayOfVector remain dense; this
// helper is used only for the regular vector oneofs whose null payload is
// compacted according to ValidData.
func (e *InsertRequestViewEncoder) countSelectedVectorRows(fieldIndex int, field *schemapb.FieldData, visit func(dataIndex int) error) (int, error) {
	valid := insertFieldValidData(field)
	if len(valid) == 0 {
		for _, row := range e.rows {
			if err := visit(row); err != nil {
				return 0, err
			}
		}
		return len(e.rows), nil
	}

	dataIndex := 0
	scanRow := 0
	if len(e.rows) > 0 && len(e.firstFieldDataIndices) == len(e.source.GetFieldsData()) {
		if fieldIndex < 0 || fieldIndex >= len(e.firstFieldDataIndices) {
			return 0, insertViewInternal("field index %d is outside the borrowed source request", fieldIndex)
		}
		dataIndex = int(e.firstFieldDataIndices[fieldIndex])
		scanRow = e.rows[0]
	}
	selected := 0
	for _, row := range e.rows {
		if row >= len(valid) {
			return 0, insertViewInternal("row offset %d exceeds ValidData length %d for field %q (%d)", row, len(valid), field.GetFieldName(), field.GetFieldId())
		}
		for scanRow < row {
			if valid[scanRow] {
				dataIndex++
			}
			scanRow++
		}
		if valid[row] {
			if err := visit(dataIndex); err != nil {
				return 0, err
			}
			selected++
			dataIndex++
		}
		scanRow = row + 1
	}
	return selected, nil
}

func denseVectorStride(dim int64, unit int, label string) (int, error) {
	d, err := nonNegativeDim(dim, label)
	if err != nil {
		return 0, err
	}
	if unit == 8 {
		if d%8 != 0 {
			return 0, insertViewInternal("%s dimension %d is not divisible by 8", label, dim)
		}
		return d / 8, nil
	}
	return checkedProduct(d, unit, label+" row width")
}

func nonNegativeDim(dim int64, label string) (int, error) {
	if dim < 0 || uint64(dim) > uint64(math.MaxInt) {
		return 0, insertViewInternal("%s has invalid dimension %d", label, dim)
	}
	return int(dim), nil
}

func vectorBounds(dataIndex, stride, dataLength int, field *schemapb.FieldData, label string) (int, int, error) {
	if dataIndex < 0 {
		return 0, 0, insertViewInternal("negative compact %s index %d for field %q (%d)", label, dataIndex, field.GetFieldName(), field.GetFieldId())
	}
	if stride == 0 {
		return 0, 0, nil
	}
	if dataIndex > dataLength/stride {
		return 0, 0, insertViewInternal("compact %s index %d exceeds payload length %d for field %q (%d)", label, dataIndex, dataLength, field.GetFieldName(), field.GetFieldId())
	}
	start := dataIndex * stride
	if start > dataLength-stride {
		return 0, 0, insertViewInternal("compact %s row %d needs [%d:%d], payload length is %d for field %q (%d)", label, dataIndex, start, start+stride, dataLength, field.GetFieldName(), field.GetFieldId())
	}
	return start, start + stride, nil
}

func sparseRowDim(row []byte) (int64, error) {
	if len(row) == 0 {
		return 0, nil
	}
	if len(row)%8 != 0 {
		return 0, insertViewInternal("sparse row length %d is not a multiple of 8", len(row))
	}
	lastIndex := binary.LittleEndian.Uint32(row[len(row)-8:])
	return int64(lastIndex) + 1, nil
}

func checkedProduct(a, b int, label string) (int, error) {
	if a < 0 || b < 0 || (a != 0 && b > math.MaxInt/a) {
		return 0, insertViewInternal("%s exceeds addressable memory", label)
	}
	return a * b, nil
}

func prepareSizePlan(plan []int, fieldCount int) []int {
	capacity, err := checkedProduct(fieldCount, 6, "encoded size plan capacity")
	if err != nil || capacity > math.MaxInt-8 {
		return plan[:0]
	}
	capacity += 8
	if cap(plan) < capacity {
		return make([]int, 0, capacity)
	}
	return plan[:0]
}

func nullableProtoSize(message proto.Message, useCachedSize bool) int {
	if isNilProto(message) {
		return 0
	}
	return nestedMessageMarshalOptions(useCachedSize).Size(message)
}

func nestedMessageMarshalOptions(useCachedSize bool) proto.MarshalOptions {
	return proto.MarshalOptions{
		AllowPartial:  true,
		UseCachedSize: useCachedSize,
	}
}

func isNilProto(message proto.Message) bool {
	if message == nil {
		return true
	}
	switch message := message.(type) {
	case *schemapb.ScalarField:
		return message == nil
	case *schemapb.VectorField:
		return message == nil
	}
	value := reflect.ValueOf(message)
	return value.Kind() == reflect.Ptr && value.IsNil()
}

func insertViewInternal(format string, args ...any) error {
	return merr.WrapErrServiceInternalMsg("insert request view: "+format, args...)
}

type insertViewWriter struct {
	out       []byte
	n         int
	err       error
	sizePlan  *[]int
	planIndex int
}

func newInsertViewMarshalWriter(dst []byte, sizePlan *[]int) *insertViewWriter {
	return &insertViewWriter{out: dst, sizePlan: sizePlan}
}

// plannedInt replays aggregate sizes produced by the incremental exact splitter
// in the same pre-order used by MarshalTo.
func (w *insertViewWriter) plannedInt() (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.sizePlan == nil {
		return 0, insertViewInternal("missing encoded size plan")
	}
	if w.planIndex >= len(*w.sizePlan) {
		return 0, insertViewInternal("encoded size plan exhausted at entry %d", w.planIndex)
	}
	size := (*w.sizePlan)[w.planIndex]
	w.planIndex++
	return size, nil
}

// appendBulk writes a run of values whose combined size is already known,
// charging the writer once instead of per value. The per-value helpers below
// each recompute the value's size to advance w.n; for a packed array that size
// was already computed during sizing, so replaying it here would double the
// varint work on the hot path. appendValues must append exactly size bytes --
// verified rather than trusted, since a mismatch would silently desynchronize
// the length prefix from the payload.
func (w *insertViewWriter) appendBulk(size int, appendValues func([]byte) []byte) {
	if w.err != nil {
		return
	}
	start := len(w.out)
	w.out = appendValues(w.out)
	if actual := len(w.out) - start; actual != size {
		w.err = insertViewInternal("bulk append wrote %d bytes, expected %d", actual, size)
		return
	}
	w.add(size)
}

func (w *insertViewWriter) add(n int) {
	if w.err != nil {
		return
	}
	if n < 0 || w.n > math.MaxInt-n {
		w.err = insertViewInternal("protobuf size exceeds addressable memory")
		return
	}
	w.n += n
}

func (w *insertViewWriter) tag(number protowire.Number, typ protowire.Type) {
	if w.err != nil {
		return
	}
	w.add(protowire.SizeTag(number))
	w.out = protowire.AppendTag(w.out, number, typ)
}

func (w *insertViewWriter) varint(value uint64) {
	if w.err != nil {
		return
	}
	w.add(protowire.SizeVarint(value))
	w.out = protowire.AppendVarint(w.out, value)
}

func (w *insertViewWriter) fixed32(value uint32) {
	if w.err != nil {
		return
	}
	w.add(4)
	w.out = protowire.AppendFixed32(w.out, value)
}

func (w *insertViewWriter) fixed64(value uint64) {
	if w.err != nil {
		return
	}
	w.add(8)
	w.out = protowire.AppendFixed64(w.out, value)
}

func (w *insertViewWriter) raw(value []byte) {
	if w.err != nil {
		return
	}
	w.add(len(value))
	w.out = append(w.out, value...)
}

func (w *insertViewWriter) varintField(number protowire.Number, value uint64) {
	w.tag(number, protowire.VarintType)
	w.varint(value)
}

func (w *insertViewWriter) proto3Varint(number protowire.Number, value uint64) {
	if value != 0 {
		w.varintField(number, value)
	}
}

func (w *insertViewWriter) proto3String(number protowire.Number, value, label string) error {
	return w.string(number, value, false, label)
}

func (w *insertViewWriter) string(number protowire.Number, value string, force bool, label string) error {
	if !force && value == "" {
		return nil
	}
	w.stringBytes(number, value)
	return w.err
}

func (w *insertViewWriter) stringBytes(number protowire.Number, value string) {
	w.tag(number, protowire.BytesType)
	w.varint(uint64(len(value)))
	if w.err != nil {
		return
	}
	w.add(len(value))
	w.out = append(w.out, value...)
}

func (w *insertViewWriter) bytes(number protowire.Number, value []byte) {
	w.tag(number, protowire.BytesType)
	w.varint(uint64(len(value)))
	w.raw(value)
}

func (w *insertViewWriter) message(number protowire.Number, size int, appendBody func() error) error {
	if size < 0 {
		return insertViewInternal("negative nested protobuf size %d", size)
	}
	w.tag(number, protowire.BytesType)
	w.varint(uint64(size))
	if w.err != nil {
		return w.err
	}
	start := w.n
	if err := appendBody(); err != nil {
		return err
	}
	if w.err != nil {
		return w.err
	}
	if actual := w.n - start; actual != size {
		return insertViewInternal("nested protobuf size changed while borrowed data was in use: expected %d, wrote %d", size, actual)
	}
	return nil
}

func (w *insertViewWriter) protoMessage(number protowire.Number, message proto.Message, label string) error {
	if isNilProto(message) {
		return w.message(number, 0, func() error { return nil })
	}
	// The size pass seeds protobuf's per-message size cache. Reuse it through
	// protobuf's public API during MarshalTo; with the current generated runtime,
	// this avoids recursively traversing every ARRAY/ArrayOfVector row again.
	options := nestedMessageMarshalOptions(true)
	size := options.Size(message)
	return w.message(number, size, func() error {
		if size == 0 {
			return nil
		}
		startLen := len(w.out)
		out, err := options.MarshalAppend(w.out, message)
		if err != nil {
			return merr.WrapErrServiceInternalErr(err, "failed to marshal %s", label)
		}
		actual := len(out) - startLen
		w.out = out
		w.add(actual)
		if actual != size {
			return insertViewInternal("%s size changed while borrowed data was in use: expected %d, wrote %d", label, size, actual)
		}
		return w.err
	})
}
