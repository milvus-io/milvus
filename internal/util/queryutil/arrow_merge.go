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

package queryutil

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/bitutil"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// gatherColumns materializes ONLY the named columns of a selection into a
// record.
//
// Gathering is the slow path and exists for one case: a nullable column whose
// Arrow payload is COMPACTED (physical values for valid rows only), which
// materializeColumn cannot address by logical row index. Everything else --
// every fixed-width type, and VARCHAR/JSON/geometry -- has a one-pass writer in
// arrow_materialize.go and must bypass this, because gathering builds an
// intermediate Arrow array that the FieldData build then copies out of again:
// the second payload pass the lazy selection exists to avoid.
//
// ARRAY and sparse vectors never reach here at all; WorthCarryingAsArrow
// (segment_c.cpp) keeps them out of the record entirely.
//
// cols must be ascending and within range; the returned record's schema is the
// template's restricted to those columns, metadata included.
func gatherColumns(sel *ArrowSelection, cols []int) (arrow.Record, error) {
	if sel.Empty() || len(cols) == 0 {
		return nil, nil
	}
	return buildMergedArrowColumns(sel.Records, sel.Rows, cols)
}

// buildMergedArrowColumns gathers the rows named by selectedRows out of the
// per-segment Arrow records into a single record.
//
// cols selects which columns to build and must be non-empty; the caller always
// has a specific subset in mind.
func buildMergedArrowColumns(
	records []arrow.Record,
	selectedRows []rowRef,
	cols []int,
) (arrow.Record, error) {
	if len(selectedRows) == 0 {
		return nil, nil
	}

	var template arrow.Record
	for _, r := range records {
		if r != nil {
			template = r
			break
		}
	}
	if template == nil {
		return nil, nil
	}
	numCols := int(template.NumCols())

	// Positional consistency: the merged record indexes columns by position, so
	// a record with a different column count would silently misattribute data.
	// buildMergedRetrieveResults makes the same check on FieldsData.
	for i, r := range records {
		if r != nil && int(r.NumCols()) != numCols {
			return nil, merr.WrapErrServiceInternalMsg(
				"arrow column count mismatch: record[%d] has %d columns, expected %d",
				i, r.NumCols(), numCols)
		}
	}

	// Range-check every reference before any gathering: gatherFixedWidth
	// indexes records and rows directly and would panic rather than return.
	// This pass allocates nothing.
	for _, ref := range selectedRows {
		if ref.resultIdx < 0 || ref.resultIdx >= len(records) {
			return nil, merr.WrapErrServiceInternalMsg(
				"selected row references result %d, have %d records",
				ref.resultIdx, len(records))
		}
		r := records[ref.resultIdx]
		if r == nil || ref.rowIdx < 0 || ref.rowIdx >= r.NumRows() {
			return nil, merr.WrapErrServiceInternalMsg(
				"selected row (%d,%d) is out of range for its record",
				ref.resultIdx, ref.rowIdx)
		}
	}

	for _, c := range cols {
		if c < 0 || c >= numCols {
			return nil, merr.WrapErrServiceInternalMsg(
				"column %d requested but the record has %d columns", c, numCols)
		}
	}

	merged := make([]arrow.Array, len(cols))
	// On any failure every array built so far must be released, or the C-backed
	// buffers behind them leak.
	built := 0
	defer func() {
		if built != len(cols) {
			for _, a := range merged {
				if a != nil {
					a.Release()
				}
			}
		}
	}()

	// First pass: the fixed-width gather, which is one pass and one copy
	// straight from each source row into the output buffer, and covers every
	// fixed-width type including the dense vectors that dominate a requery
	// payload. Columns it declines are collected for the fallback below.
	//
	// Splitting the passes is what keeps the fallback's setup off the fast
	// path: the take indices are an n-element Int64 array and the per-column
	// `parts` slices are one allocation each, and NONE of it is read unless a
	// column actually needs concat+take. Building them unconditionally -- as
	// this did -- charged every all-fixed-width result, which is the common
	// one, for machinery it never used.
	var fallbackCols []int
	for out, col := range cols {
		if gathered, ok := gatherFixedWidth(records, selectedRows, col); ok {
			merged[out] = gathered
			built++
			continue
		}
		if gathered, ok := gatherVarLen(records, selectedRows, col); ok {
			merged[out] = gathered
			built++
			continue
		}
		fallbackCols = append(fallbackCols, col)
	}

	if len(fallbackCols) > 0 {
		// Nothing should reach here. gatherFixedWidth takes every fixed-width
		// type, gatherVarLen takes utf8/binary/bool, and WorthCarryingAsArrow (segment_c.cpp)
		// keeps the nested types out of the record entirely -- so a column
		// arriving here means a new Arrow type started being exported without
		// a gather for it.
		//
		// This used to concatenate every record and then take the selection
		// out of the result, which copied every row of every record to serve
		// what turned out to be a single column kind. An error is better than
		// quietly paying that: it is unreachable by construction, and if the
		// construction changes a reviewer should hear about it.
		return nil, merr.WrapErrServiceInternalMsg(
			"arrow column %d has type %s, which no gather handles",
			fallbackCols[0], template.Column(fallbackCols[0]).DataType())
	}

	outSchema := template.Schema()
	if len(cols) != numCols {
		fields := make([]arrow.Field, len(cols))
		for i, c := range cols {
			fields[i] = outSchema.Field(c)
		}
		md := outSchema.Metadata()
		outSchema = arrow.NewSchema(fields, &md)
	}
	out := array.NewRecord(outSchema, merged, int64(len(selectedRows)))
	// NewRecord retains each array; drop our own references so the record is the
	// only owner and releasing it frees everything.
	for _, a := range merged {
		a.Release()
	}
	return out, nil
}

// gatherVarLen gathers the selected rows of a variable-length column directly
// into a new array, touching only the rows that were selected.
//
// Returns ok=false for types it does not handle; the caller then has no gather
// for that column and returns an error, so anything the export can emit must
// be handled here or in gatherFixedWidth.
func gatherVarLen(
	records []arrow.Record,
	selectedRows []rowRef,
	col int,
) (arrow.Array, bool) {
	var dt arrow.DataType
	for _, r := range records {
		if r != nil && r.NumRows() > 0 {
			dt = r.Column(col).DataType()
			break
		}
	}
	// Only the types the retrieve export actually produces for a column that
	// can reach a gather: VARCHAR becomes utf8, and JSON and geometry become
	// binary. The Large variants are not emitted, so they are deliberately not
	// handled -- a type that starts arriving here must be added explicitly
	// rather than silently returning ok=false.
	// BOOL is here, not in gatherFixedWidth, because Arrow bit-packs it so it
	// has no byte width. Before it was handled, a nullable BOOL column was the
	// ONLY thing still reaching the concatenate-then-take fallback that used to
	// live below -- which copied every row of every record to serve one column
	// kind. That fallback is now an error instead.
	switch dt.ID() {
	case arrow.STRING, arrow.BINARY, arrow.BOOL:
	default:
		return nil, false
	}

	// Resolve each source column once rather than once per row.
	sources := make([]arrow.Array, len(records))
	for i, r := range records {
		if r == nil || r.NumRows() == 0 {
			continue
		}
		sources[i] = r.Column(col)
	}

	bldr := array.NewBuilder(memory.DefaultAllocator, dt)
	defer bldr.Release()
	bldr.Reserve(len(selectedRows))
	// Reserve covers the offset and validity buffers only; without ReserveData
	// the value buffer grows by doubling, which for 10000 rows of 200 bytes
	// churned 4.33MB for 2MB of payload. The length pre-pass costs far less
	// than the reallocations it avoids.
	// Boolean has no value buffer to reserve; the assertion below simply fails
	// for it.
	if bd, ok := bldr.(interface{ ReserveData(int) }); ok {
		// Offsets resolved once per record, not per row -- same reason as
		// varLenArrowCol in helpers.go, which shares varLenOffsets.
		offs := make([][]int32, len(sources))
		for i, src := range sources {
			if src != nil {
				offs[i] = varLenOffsets(src)
			}
		}
		total := 0
		for _, ref := range selectedRows {
			src := sources[ref.resultIdx]
			if src == nil || src.IsNull(int(ref.rowIdx)) || offs[ref.resultIdx] == nil {
				continue
			}
			o := offs[ref.resultIdx]
			total += int(o[ref.rowIdx+1] - o[ref.rowIdx])
		}
		bd.ReserveData(total)
	}

	for _, ref := range selectedRows {
		src := sources[ref.resultIdx]
		row := int(ref.rowIdx)
		if src.IsNull(row) {
			bldr.AppendNull()
			continue
		}
		switch b := bldr.(type) {
		case *array.StringBuilder:
			b.Append(src.(*array.String).Value(row))
		case *array.BinaryBuilder:
			b.Append(src.(*array.Binary).Value(row))
		case *array.BooleanBuilder:
			b.Append(src.(*array.Boolean).Value(row))
		default:
			// A builder kind this switch does not know must not silently
			// produce a short column.
			return nil, false
		}
	}
	return bldr.NewArray(), true
}

// fixedByteWidth reports the per-element byte width of a fixed-width Arrow
// type, and whether it has one.
//
// Boolean is deliberately excluded: its bit width is 1, so a per-row byte copy
// does not apply and it falls back to the generic path.
func fixedByteWidth(dt arrow.DataType) (int, bool) {
	fw, ok := dt.(arrow.FixedWidthDataType)
	if !ok {
		return 0, false
	}
	bits := fw.BitWidth()
	if bits < 8 || bits%8 != 0 {
		return 0, false
	}
	return bits / 8, true
}

// gatherFixedWidth builds one output column by copying each selected row once,
// directly from its source record into a freshly allocated value buffer.
//
// Returns ok=false for types it cannot handle; gatherVarLen is tried next.
//
// One pass, one copy per row. The obvious alternative -- concatenate every
// record then take the selection -- copies the whole source and then the
// selection out of it, which measured slower than the per-row AppendFieldData
// it replaced.
func gatherFixedWidth(
	records []arrow.Record,
	selectedRows []rowRef,
	col int,
) (arrow.Array, bool) {
	var dt arrow.DataType
	for _, r := range records {
		if r != nil && r.NumRows() > 0 {
			dt = r.Column(col).DataType()
			break
		}
	}
	width, ok := fixedByteWidth(dt)
	if !ok {
		return nil, false
	}

	// Resolve each source record's column ONCE, not once per row. The inner
	// loop runs per selected row and every one of these was an interface call
	// through arrow.Record/arrow.Array that returns the same thing for every
	// row of the same record.
	// Storing the null bitmap bytes rather than a bound IsNull method value
	// matters twice: a method value is a heap allocation per record, and
	// calling it per row is an un-inlinable indirect call -- measured 5x the
	// cost of testing the bit inline.
	type source struct {
		bytes      []byte // the value buffer, already offset-adjusted
		nullBitmap []byte // nil when the column has no nulls
		nullOffset int    // the array's own offset into that bitmap
	}
	sources := make([]source, len(records))
	anyNulls := false
	for i, r := range records {
		// A nil slot, or one with no rows, can never be referenced: the
		// caller's range check rejects any ref whose rowIdx is not below its
		// record's row count. Skipping them also keeps an empty record's
		// possibly-absent value buffer from forcing a needless fallback.
		if r == nil || r.NumRows() == 0 {
			continue
		}
		arr := r.Column(col)
		buf := arr.Data().Buffers()[1]
		if buf == nil {
			return nil, false
		}
		// Data().Offset() is non-zero for a sliced array; the record's columns
		// come straight from IPC or cdata where it is 0, but honoring it keeps
		// this correct if a slice ever reaches here.
		off := arr.Data().Offset() * width
		sources[i] = source{bytes: buf.Bytes()[off:]}
		if arr.NullN() != 0 {
			anyNulls = true
			sources[i].nullBitmap = arr.NullBitmapBytes()
			sources[i].nullOffset = arr.Data().Offset()
		}
	}

	n := len(selectedRows)
	values := memory.NewResizableBuffer(memory.DefaultAllocator)
	values.Resize(n * width)
	dst := values.Bytes()

	// No null in any source: Arrow represents that as a nil validity buffer, so
	// the bitmap allocation and the per-row SetBit both go away. This is the
	// overwhelmingly common case on a retrieve -- a nullable column only gets a
	// bitmap when the field is declared nullable or the ORDER BY pipeline
	// allocated one.
	if !anyNulls {
		for i, ref := range selectedRows {
			src := sources[ref.resultIdx].bytes
			row := int(ref.rowIdx)
			copy(dst[i*width:(i+1)*width], src[row*width:(row+1)*width])
		}
		data := array.NewData(dt, n, []*memory.Buffer{nil, values}, nil, 0, 0)
		values.Release()
		out := array.MakeFromData(data)
		data.Release()
		return out, true
	}

	validity := memory.NewResizableBuffer(memory.DefaultAllocator)
	validity.Resize(int(bitutil.BytesForBits(int64(n))))
	valid := validity.Bytes()
	nulls := 0
	for i, ref := range selectedRows {
		src := &sources[ref.resultIdx]
		row := int(ref.rowIdx)
		if src.nullBitmap != nil && !bitutil.BitIsSet(src.nullBitmap, src.nullOffset+row) {
			nulls++
			// Leave the value slot zeroed; Resize already did that.
			continue
		}
		bitutil.SetBit(valid, i)
		copy(dst[i*width:(i+1)*width], src.bytes[row*width:(row+1)*width])
	}

	data := array.NewData(dt, n,
		[]*memory.Buffer{validity, values}, nil, nulls, 0)
	values.Release()
	validity.Release()
	out := array.MakeFromData(data)
	data.Release()
	return out, true
}
