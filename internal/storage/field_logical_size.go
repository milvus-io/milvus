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
	"math"
	"math/bits"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/bitutil"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type logicalSizeColumn struct {
	id         int64
	nativeText bool // the segment writer reports this field's payload at Close
	logicalValueType
}

type logicalSizeLayout struct {
	columns []logicalSizeColumn
	parents []*schemapb.StructArrayFieldSchema
}

// Only projected fields are written. A partial writer can project a flat,
// interleaved subset; use target solely to discover their shared struct masks.
func newLogicalSizeLayout(projected, target *schemapb.CollectionSchema, nativeText typeutil.UniqueSet) *logicalSizeLayout {
	parentByChild := make(map[int64]*schemapb.StructArrayFieldSchema)
	for _, parent := range target.GetStructArrayFields() {
		for _, child := range parent.GetFields() {
			parentByChild[child.GetFieldID()] = parent
		}
	}
	fields := typeutil.GetAllFieldSchemas(projected)
	layout := &logicalSizeLayout{columns: make([]logicalSizeColumn, 0, len(fields))}
	seen := make(map[int64]bool)
	for _, f := range fields {
		id := f.GetFieldID()
		rowNullable := f.GetNullable()
		if parent := parentByChild[id]; parent != nil {
			rowNullable = false
			if !seen[parent.GetFieldID()] {
				layout.parents = append(layout.parents, parent)
				seen[parent.GetFieldID()] = true
			}
		} else if typeutil.IsStructSubField(f.GetName()) {
			// In a flat projection without parent metadata the leaf still
			// doesn't own the parent's bit; don't guess the parent ID.
			rowNullable = false
		}
		layout.columns = append(layout.columns, logicalSizeColumn{
			id:               id,
			nativeText:       f.GetDataType() == schemapb.DataType_Text && nativeText.Contain(id),
			logicalValueType: logicalTypeForField(f, rowNullable),
		})
	}
	return layout
}

func (l *logicalSizeLayout) newBatch(rows int64) *datapb.FieldLogicalSizes {
	batch := &datapb.FieldLogicalSizes{
		Version: FieldLogicalSizeVersion, RowCount: rows,
		Fields: make(map[int64]*datapb.FieldLogicalSize, len(l.columns)+len(l.parents)),
	}
	for _, parent := range l.parents {
		size := &datapb.FieldLogicalSize{}
		if parent.GetNullable() {
			size.ValidityBits = uint64(rows)
		}
		batch.Fields[parent.GetFieldID()] = size
	}
	return batch
}

func (c *logicalSizeColumn) measure(batch *datapb.FieldLogicalSizes, a arrow.Array) {
	if c.nativeText {
		return // Close supplies the payload and derives validity from the accepted rows.
	}
	size := &datapb.FieldLogicalSize{}
	if c.addArrow(size, a, 0, int(batch.RowCount)) {
		batch.Fields[c.id] = size
	}
}

// logicalValueType describes a value as stored by the target schema. Arrow
// alone cannot distinguish a legacy protobuf Array, a TEXT reference and WKB
// (all Binary), or decide whether a list child logically owns a validity bit.
type logicalValueType struct {
	kind     schemapb.DataType
	nullable bool
	width    uint64 // nonzero for fixed-width scalar and dense-vector values
	element  *logicalValueType
}

func logicalScalarType(dt schemapb.DataType, nullable bool, field *schemapb.FieldSchema) logicalValueType {
	t := logicalValueType{kind: dt, nullable: nullable}
	switch dt {
	case schemapb.DataType_Bool, schemapb.DataType_Int8:
		t.width = 1
	case schemapb.DataType_Int16:
		t.width = 2
	case schemapb.DataType_Int32, schemapb.DataType_Float:
		t.width = 4
	case schemapb.DataType_Int64, schemapb.DataType_Double, schemapb.DataType_Timestamptz:
		t.width = 8
	case schemapb.DataType_BinaryVector, schemapb.DataType_FloatVector, schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector:
		dim, err := typeutil.GetDim(field)
		if err != nil || dim <= 0 {
			break
		}
		switch dt {
		case schemapb.DataType_BinaryVector:
			t.width = (uint64(dim) + 7) / 8
		case schemapb.DataType_Int8Vector:
			t.width = uint64(dim)
		case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
			t.width = uint64(dim) * 2 // dim is a positive int64
		case schemapb.DataType_FloatVector:
			if uint64(dim) <= math.MaxUint64/4 {
				t.width = uint64(dim) * 4
			}
		}
	}
	return t
}

func logicalTypeForField(field *schemapb.FieldSchema, rowNullable bool) logicalValueType {
	dt := field.GetDataType()
	if dt != schemapb.DataType_Array && dt != schemapb.DataType_ArrayOfVector {
		return logicalScalarType(dt, rowNullable, field)
	}
	leaf, leafNullable, depth, err := typeutil.GetFieldArrayLeaf(field)
	if err != nil || depth < 1 || depth > 2 {
		return logicalValueType{}
	}
	elem := logicalScalarType(leaf, leafNullable, field)
	if depth == 2 {
		inner := logicalValueType{kind: schemapb.DataType_Array, nullable: field.GetElementNullable(), element: &elem}
		return logicalValueType{kind: dt, nullable: rowNullable, element: &inner}
	}
	return logicalValueType{kind: dt, nullable: rowNullable, element: &elem}
}

func addLogicalCount(dst *uint64, n uint64) bool {
	if n > math.MaxUint64-*dst {
		return false
	}
	*dst += n
	return true
}

func addLogicalProduct(dst *uint64, count, width uint64) bool {
	hi, lo := bits.Mul64(count, width)
	return hi == 0 && addLogicalCount(dst, lo)
}

func validLogicalValues(col arrow.Array, start, end int) int {
	if start == 0 && end == col.Len() {
		return end - col.NullN()
	}
	if bitmap := col.NullBitmapBytes(); len(bitmap) != 0 {
		return bitutil.CountSetBits(bitmap, col.Data().Offset()+start, end-start)
	}
	return end - start
}

// logicalByteSpan measures non-NULL values, not buffer capacity or even the
// full offset span: a NULL in an Arrow array can own ignored physical bytes.
func logicalByteSpan(col array.BinaryLike, start, end int) uint64 {
	if start == end {
		return 0
	}
	if col.NullN() == 0 {
		last := end - 1
		return uint64(col.ValueOffset64(last)-col.ValueOffset64(start)) + uint64(col.ValueLen(last))
	}
	var n uint64
	for i := start; i < end; i++ {
		if col.IsValid(i) {
			n += uint64(col.ValueLen(i))
		}
	}
	return n // the disjoint spans fit in one Arrow value buffer
}

// TextLogicalPayloadBytes returns the UTF-8 payload of column[start:end]. A
// binary TEXT column contains LOB references, not raw strings. Only inline
// references (tag 0) and NULLs can be measured without reading the LOB. A
// false result means the whole range is unknown, not a partial subtotal.
func TextLogicalPayloadBytes(column arrow.Array, start, end int) (uint64, bool) {
	if column == nil || start < 0 || end < start || end > column.Len() {
		return 0, false
	}
	switch col := column.(type) {
	case *array.String:
		return logicalByteSpan(col, start, end), true
	case *array.Binary:
		var n uint64
		for i := start; i < end; i++ {
			if col.IsNull(i) {
				continue
			}
			ref := col.Value(i)
			if len(ref) == 0 || ref[0] != 0 {
				return 0, false
			}
			n += uint64(len(ref) - 1)
		}
		return n, true
	default:
		return 0, false
	}
}

// addArrow accounts a range of logical positions. Descend only through valid
// parent rows; neither a NULL list's child span nor its child validity counts.
func (t *logicalValueType) addArrow(size *datapb.FieldLogicalSize, col arrow.Array, start, end int) bool {
	if col == nil || start < 0 || end < start || end > col.Len() || t.kind == schemapb.DataType_None {
		return false
	}
	if t.nullable && !addLogicalCount(&size.ValidityBits, uint64(end-start)) {
		return false
	}
	if start == end || col.NullN() == col.Len() {
		return true
	}
	if t.width != 0 {
		return addLogicalProduct(&size.PayloadBytes, uint64(validLogicalValues(col, start, end)), t.width)
	}
	switch t.kind {
	case schemapb.DataType_Array, schemapb.DataType_ArrayOfVector:
		switch a := col.(type) {
		case *array.List:
			// In the common all-valid case one span covers every child,
			// including for sliced arrays. Don't allocate an Arrow slice per row.
			values := a.ListValues()
			if a.NullN() == 0 {
				lo, _ := a.ValueOffsets(start)
				_, hi := a.ValueOffsets(end - 1)
				return t.element.addArrow(size, values, int(lo), int(hi))
			}
			for i := start; i < end; i++ {
				if a.IsValid(i) {
					lo, hi := a.ValueOffsets(i)
					if !t.element.addArrow(size, values, int(lo), int(hi)) {
						return false
					}
				}
			}
			return true
		case *array.Binary:
			if t.kind != schemapb.DataType_Array {
				return false
			}
			for i := start; i < end; i++ {
				if a.IsNull(i) {
					continue
				}
				var row schemapb.ScalarField
				if err := proto.Unmarshal(a.Value(i), &row); err != nil || !t.element.addProtoElements(size, &row) {
					return false
				}
			}
			return true
		}
	case schemapb.DataType_Text:
		n, known := TextLogicalPayloadBytes(col, start, end)
		return known && addLogicalCount(&size.PayloadBytes, n)
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_JSON, schemapb.DataType_Geometry,
		schemapb.DataType_SparseFloatVector:
		// Sparse rows are already uint32/float32 pairs, JSON is the written
		// text (not a padded parser view), and Geometry is already WKB.
		if a, ok := col.(array.BinaryLike); ok {
			return addLogicalCount(&size.PayloadBytes, logicalByteSpan(a, start, end))
		}
	}
	return false
}

// addProtoElements counts the elements inside a binary ScalarField row. The
// integer wire representation is int32 even when each logical value is Int8
// or Int16. Never charge protobuf framing or NULL placeholders.
func (t *logicalValueType) addProtoElements(size *datapb.FieldLogicalSize, row *schemapb.ScalarField) bool {
	var n int
	switch {
	case row == nil:
		return false
	case row.GetData() == nil && len(row.ProtoReflect().GetUnknown()) == 0:
		// An empty row or an all-NULL compact payload.
	case t.kind == schemapb.DataType_Array && row.GetArrayData() != nil:
		n = len(row.GetArrayData().GetData())
	default:
		leaf := t.kind
		if leaf == schemapb.DataType_String {
			leaf = schemapb.DataType_VarChar
		}
		var err error
		n, err = nativeArrayLength(row, leaf)
		if err != nil {
			return false
		}
	}
	valid := typeutil.GetArrayElementValidData(row)
	nonNull := n
	if len(valid) > 0 {
		if !t.nullable {
			return false
		}
		nonNull = 0
		for _, present := range valid {
			if present {
				nonNull++
			}
		}
		if n != len(valid) && n != nonNull {
			return false
		}
	}
	if t.nullable && !addLogicalCount(&size.ValidityBits, uint64(max(n, len(valid)))) {
		return false
	}
	if t.width != 0 {
		return addLogicalProduct(&size.PayloadBytes, uint64(nonNull), t.width)
	}
	for i := 0; i < n; i++ {
		if n == len(valid) && !valid[i] {
			continue // dense representation: skip the placeholder
		}
		switch t.kind {
		case schemapb.DataType_VarChar, schemapb.DataType_String:
			if !addLogicalCount(&size.PayloadBytes, uint64(len(row.GetStringData().GetData()[i]))) {
				return false
			}
		case schemapb.DataType_Array:
			if !t.element.addProtoElements(size, row.GetArrayData().GetData()[i]) {
				return false
			}
		default:
			return false
		}
	}
	return true
}
