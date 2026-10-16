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
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/arrowconv"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// MaterializeArrowSelection turns an unmaterialized reduce result into
// FieldsData, leaving `header` indistinguishable from what the protobuf path
// would have produced.
//
// This is the single payload-touching step of the Arrow path, and the reason the
// selection is carried rather than gathered: the chosen rows are copied ONCE,
// straight from each segment's Arrow buffers into the protobuf column they end
// up in. Gathering first would build a merged Arrow record that nothing reads
// and then copy out of it again.
//
// It must run while the selection's records are still alive -- on the query path
// that is before QueryTask.Execute's deferred ReleaseRecords.
//
// A nil or empty selection is a no-op, so it can be called unconditionally.
func MaterializeArrowSelection(
	header *segcorepb.RetrieveResults,
	sel *ArrowSelection,
	schema *schemapb.CollectionSchema,
) error {
	if sel.Empty() {
		return nil
	}
	template := sel.Template()
	if template == nil {
		return nil
	}

	// Every record must agree with the template on BOTH metadata keys, not just
	// on column count. The reading below comes from the template alone and is
	// then applied to every record's rows, so a disagreement would silently
	// misinterpret the others. No divergence is reachable today -- see
	// SchemaMetadataFingerprint -- so this is defense in depth over an
	// assumption that is otherwise invisible here.
	want := arrowconv.SchemaMetadataFingerprint(template)
	for i, r := range sel.Records {
		if r == nil {
			continue
		}
		if got := arrowconv.SchemaMetadataFingerprint(r); got != want {
			return merr.WrapErrServiceInternalMsg(
				"arrow record[%d] disagrees with the template on retrieve "+
					"metadata; columns cannot be reassembled from a single "+
					"reading (template %q, record %q)", i, want, got)
		}
	}

	order, err := arrowconv.FieldOrder(template)
	if err != nil {
		return err
	}
	// Duplicate ids mean the columns are identified positionally rather than by
	// id (an aggregation result), which this reassembly cannot do. Aggregation
	// is excluded from the Arrow routing, so reaching here is a routing bug.
	seen := make(map[int64]struct{}, len(order))
	for _, id := range order {
		if _, dup := seen[id]; dup {
			return merr.WrapErrServiceInternalMsg(
				"arrow selection has duplicate field id %d; such a result must "+
					"take the protobuf path", id)
		}
		seen[id] = struct{}{}
	}

	userIDs := arrowconv.UserColumnIDs(order, header.GetFieldsData())
	if len(userIDs) != int(template.NumCols()) {
		return merr.WrapErrServiceInternalMsg(
			"arrow selection has %d columns but %d user field ids",
			template.NumCols(), len(userIDs))
	}
	fieldSchemaMap := lo.SliceToMap(typeutil.GetAllFieldSchemas(schema),
		func(f *schemapb.FieldSchema) (int64, *schemapb.FieldSchema) {
			return f.GetFieldID(), f
		})

	// Every record must have the same column count as the template. Without
	// this, selectedSources' r.Column(col) is an unguarded slice index on
	// arrow-go's simpleRecord, so a short record panics the QueryNode.
	// buildMergedArrowColumns makes the same check, but only reaches it for
	// columns the one-pass path declined.
	for i, r := range sel.Records {
		if r == nil {
			continue
		}
		if r.NumCols() != template.NumCols() {
			return merr.WrapErrServiceInternalMsg(
				"arrow column count mismatch: record[%d] has %d columns, expected %d",
				i, r.NumCols(), template.NumCols())
		}
	}

	// Range-check every reference before any column indexes one directly.
	// materializeColumn reads cols[ref.resultIdx].Value(int(ref.rowIdx)) with
	// no bound of its own, so a bad ref is a panic rather than an error --
	// and selectedSources leaves a nil entry for a nil or zero-row record, so
	// the panic would be a nil dereference. buildMergedArrowColumns makes the
	// same pass for the same reason. It allocates nothing.
	for _, ref := range sel.Rows {
		if ref.resultIdx < 0 || ref.resultIdx >= len(sel.Records) {
			return merr.WrapErrServiceInternalMsg(
				"arrow selection references result %d, have %d records",
				ref.resultIdx, len(sel.Records))
		}
		r := sel.Records[ref.resultIdx]
		if r == nil || ref.rowIdx < 0 || ref.rowIdx >= r.NumRows() {
			return merr.WrapErrServiceInternalMsg(
				"arrow selection row (%d,%d) is out of range for its record",
				ref.resultIdx, ref.rowIdx)
		}
	}

	numRows := len(sel.Rows)
	userCols := make([]*schemapb.FieldData, len(userIDs))
	// Columns the straight-line writer declines: variable-length and nested
	// types, and anything with nulls (whose protobuf payload is compacted, so
	// the logical and physical indices diverge).
	var slowCols []int
	for i := range userIDs {
		fs, ok := fieldSchemaMap[userIDs[i]]
		if !ok {
			return merr.WrapErrServiceInternalMsg(
				"arrow selection column %d has field id %d, not in the schema",
				i, userIDs[i])
		}
		if err := checkVectorWidth(sel, i, fs); err != nil {
			return err
		}
		fd, ok := materializeColumn(sel, i, fs)
		if !ok {
			slowCols = append(slowCols, i)
			continue
		}
		userCols[i] = fd
	}

	if len(slowCols) > 0 {
		rec, err := gatherColumns(sel, slowCols)
		if err != nil {
			return err
		}
		// A nil record here would leave userCols holes that the reassembly
		// below dereferences. gatherColumns only returns nil for an empty
		// selection or no columns, both already excluded, so this is an error
		// rather than a skip: a defensive branch that leaves an invariant
		// broken is worse than no branch.
		if rec == nil {
			return merr.WrapErrServiceInternalMsg(
				"gather returned no record for %d columns", len(slowCols))
		}
		defer rec.Release()
		ids := lo.Map(slowCols, func(i int, _ int) int64 { return userIDs[i] })
		converted, err := arrowconv.ArrowFieldsToProtoOrdered(rec, ids, fieldSchemaMap)
		if err != nil {
			return err
		}
		if len(converted) != len(slowCols) {
			return merr.WrapErrServiceInternalMsg(
				"gathered %d columns but converted %d", len(slowCols), len(converted))
		}
		for j, i := range slowCols {
			userCols[i] = converted[j]
		}
	}

	if err := arrowconv.ReconcileValidData(template, userCols, numRows); err != nil {
		return err
	}
	// The protobuf path leaves FieldName empty and the proxy fills it in
	// complement_fields; keeping the two byte-identical matters because results
	// from both can meet in one reduce during a rolling config change.
	for _, fd := range userCols {
		fd.FieldName = ""
	}

	byID := make(map[int64]*schemapb.FieldData, len(order))
	for _, fd := range header.GetFieldsData() {
		byID[fd.GetFieldId()] = fd
	}
	for _, fd := range userCols {
		byID[fd.GetFieldId()] = fd
	}
	if len(byID) != len(order) {
		return merr.WrapErrServiceInternalMsg(
			"arrow selection field order lists %d ids but %d columns are present",
			len(order), len(byID))
	}
	merged := make([]*schemapb.FieldData, 0, len(order))
	for _, id := range order {
		fd, ok := byID[id]
		if !ok {
			return merr.WrapErrServiceInternalMsg(
				"arrow selection field order references missing field id %d", id)
		}
		merged = append(merged, fd)
	}
	header.FieldsData = merged
	return nil
}

// selectedSources resolves, once per source record, the state the per-row loop
// needs. Every lookup here was otherwise an interface call per row returning the
// same answer for every row of the same record.
//
// Returns ok=false when any contributing column has a null, because a nullable
// column's protobuf payload holds only the valid rows; that case goes to the
// gather path, which already handles the index divergence.
func selectedSources(sel *ArrowSelection, col int) ([]arrow.Array, bool) {
	out := make([]arrow.Array, len(sel.Records))
	for i, r := range sel.Records {
		if r == nil || r.NumRows() == 0 {
			continue
		}
		c := r.Column(col)
		if c.NullN() != 0 {
			return nil, false
		}
		out[i] = c
	}
	return out, true
}

// materializeColumn writes the selected rows of one column straight into a
// freshly built FieldData, with no Arrow intermediate.
//
// Only the types whose protobuf payload is a flat slice of the Arrow values are
// handled; everything else returns ok=false. That set is deliberately the one
// that carries the bytes -- the dense vectors and the fixed-width scalars --
// because the whole point is to avoid a second copy of the payload.
func materializeColumn(
	sel *ArrowSelection,
	col int,
	fs *schemapb.FieldSchema,
) (*schemapb.FieldData, bool) {
	srcs, ok := selectedSources(sel, col)
	if !ok {
		return nil, false
	}
	rows := sel.Rows
	n := len(rows)
	fd := &schemapb.FieldData{
		Type:    fs.GetDataType(),
		FieldId: fs.GetFieldID(),
	}

	switch fs.GetDataType() {
	case schemapb.DataType_Bool:
		dst, ok := gatherValues[bool, *array.Boolean](srcs, rows)
		if !ok {
			return nil, false
		}
		fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: dst}},
		}}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		// protobuf carries all three widths in int_data, and the export
		// widens all three to int32.
		dst, ok := gatherNumeric(srcs, rows, (*array.Int32).Int32Values)
		if !ok {
			return nil, false
		}
		fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: dst}},
		}}
	case schemapb.DataType_Int64:
		dst, ok := gatherNumeric(srcs, rows, (*array.Int64).Int64Values)
		if !ok {
			return nil, false
		}
		fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: dst}},
		}}
	case schemapb.DataType_Timestamptz:
		// Exported as int64, same as INT64, but protobuf keeps it in its own
		// oneof. Without this case it is the one null-free type that still
		// pays for an intermediate Arrow record.
		dst, ok := gatherNumeric(srcs, rows, (*array.Int64).Int64Values)
		if !ok {
			return nil, false
		}
		fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_TimestamptzData{
				TimestamptzData: &schemapb.TimestamptzArray{Data: dst},
			},
		}}
	case schemapb.DataType_Float:
		dst, ok := gatherNumeric(srcs, rows, (*array.Float32).Float32Values)
		if !ok {
			return nil, false
		}
		fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: dst}},
		}}
	case schemapb.DataType_Double:
		dst, ok := gatherNumeric(srcs, rows, (*array.Float64).Float64Values)
		if !ok {
			return nil, false
		}
		fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: dst}},
		}}
	case schemapb.DataType_FloatVector:
		width, ok := vectorWidth(srcs)
		if !ok || width%4 != 0 {
			return nil, false
		}
		dim, err := typeutil.GetDim(fs)
		if err != nil {
			return nil, false
		}
		// width/dim agreement is checked once, before dispatch, by
		// checkVectorWidth -- declining here would only route the column to a
		// gather that does not check either.
		dst := make([]float32, n*(width/4))
		// One copy of the whole row as bytes rather than a per-row
		// CastFromBytes: the destination is the same layout.
		cols, isFSB := concrete[*array.FixedSizeBinary](srcs)
		if !isFSB {
			return nil, false
		}
		dstBytes := arrow.Float32Traits.CastToBytes(dst)
		for i, ref := range rows {
			copy(dstBytes[i*width:(i+1)*width], cols[ref.resultIdx].Value(int(ref.rowIdx)))
		}
		fd.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: dst}},
		}}
	case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector:
		width, ok := vectorWidth(srcs)
		if !ok {
			return nil, false
		}
		dim, err := typeutil.GetDim(fs)
		if err != nil {
			return nil, false
		}
		// See the FloatVector branch: checkVectorWidth owns width/dim agreement
		// for every dense vector type, before this dispatch.
		cols, isFSB := concrete[*array.FixedSizeBinary](srcs)
		if !isFSB {
			return nil, false
		}
		dst := make([]byte, n*width)
		for i, ref := range rows {
			copy(dst[i*width:(i+1)*width], cols[ref.resultIdx].Value(int(ref.rowIdx)))
		}
		vf := &schemapb.VectorField{Dim: dim}
		switch fs.GetDataType() {
		case schemapb.DataType_BinaryVector:
			vf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: dst}
		case schemapb.DataType_Float16Vector:
			vf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: dst}
		case schemapb.DataType_BFloat16Vector:
			vf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: dst}
		case schemapb.DataType_Int8Vector:
			vf.Data = &schemapb.VectorField_Int8Vector{Int8Vector: dst}
		}
		fd.Field = &schemapb.FieldData_Vectors{Vectors: vf}
	case schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Text:
		cols, isStr := concrete[*array.String](srcs)
		if !isStr {
			return nil, false
		}
		total := 0
		for _, ref := range rows {
			total += cols[ref.resultIdx].ValueLen(int(ref.rowIdx))
		}
		// Arrow's Value is a view into its buffer, which does not outlive the
		// records, so the bytes have to be copied out. Copying into one builder
		// and slicing it costs 3 allocations instead of one per row; substrings
		// of a Go string are zero-copy and immutable, so the rows stay
		// independent. They do share a backing array, which is fine: a result's
		// rows are built, marshaled and freed together.
		var sb strings.Builder
		sb.Grow(total)
		ends := make([]int, n)
		for i, ref := range rows {
			sb.WriteString(cols[ref.resultIdx].Value(int(ref.rowIdx)))
			ends[i] = sb.Len()
		}
		joined := sb.String()
		dst := make([]string, n)
		off := 0
		for i := range rows {
			dst[i] = joined[off:ends[i]]
			off = ends[i]
		}
		fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: dst}},
		}}
	case schemapb.DataType_JSON, schemapb.DataType_Geometry:
		dst, ok := materializeBytes(srcs, rows, n)
		if !ok {
			return nil, false
		}
		sf := &schemapb.ScalarField{}
		if fs.GetDataType() == schemapb.DataType_JSON {
			sf.Data = &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: dst}}
		} else {
			sf.Data = &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: dst}}
		}
		fd.Field = &schemapb.FieldData_Scalars{Scalars: sf}
	default:
		return nil, false
	}
	return fd, true
}

// materializeBytes copies the selected rows of a binary column into one slice
// per row, which is the layout every protobuf bytes-valued column uses.
//
// The rows share one backing array. Appending per row instead costs an
// allocation per row -- measured at 10001 allocations and 2x the wall time for
// 10000 rows -- and buys nothing here, because every row of a result is built,
// marshaled and freed together. The three-index slice is what keeps them
// independent: without it a later append to one row would overwrite the next.
func materializeBytes(srcs []arrow.Array, rows []rowRef, n int) ([][]byte, bool) {
	cols, ok := concrete[*array.Binary](srcs)
	if !ok {
		return nil, false
	}
	total := 0
	for _, ref := range rows {
		total += cols[ref.resultIdx].ValueLen(int(ref.rowIdx))
	}
	arena := make([]byte, 0, total)
	dst := make([][]byte, n)
	for i, ref := range rows {
		off := len(arena)
		arena = append(arena, cols[ref.resultIdx].Value(int(ref.rowIdx))...)
		dst[i] = arena[off:len(arena):len(arena)]
	}
	return dst, true
}

// concrete resolves the per-record type assertion ONCE rather than once per
// row. There are a handful of records and up to tens of thousands of rows, so
// the assertion belongs outside the loop; it also turns the dense-vector
// branches' unchecked assertions into a checked fallback.
func concrete[A arrow.Array](srcs []arrow.Array) ([]A, bool) {
	out := make([]A, len(srcs))
	for i, a := range srcs {
		if a == nil {
			continue
		}
		c, ok := a.(A)
		if !ok {
			return nil, false
		}
		out[i] = c
	}
	return out, true
}

// gatherValues copies one scalar value per selected row out of pre-resolved
// columns.
//
// Use gatherNumeric instead wherever arrow-go exposes a typed backing slice.
// `A` here is a type parameter, so cols[...].Value(...) resolves through the
// shape dictionary on EVERY row -- concrete hoists the type assertion but not
// the call. Measured ~2.2x slower than indexing the backing slice over 10k rows
// in a standalone harness during this change; there is no in-tree benchmark, so
// treat the ratio as indicative rather than reproducible.
// This form is still right for *array.Boolean, which is bit-packed and has no
// backing slice to index.
func gatherValues[T any, A interface {
	arrow.Array
	Value(int) T
}](srcs []arrow.Array, rows []rowRef) ([]T, bool) {
	cols, ok := concrete[A](srcs)
	if !ok {
		return nil, false
	}
	dst := make([]T, len(rows))
	for i, ref := range rows {
		dst[i] = cols[ref.resultIdx].Value(int(ref.rowIdx))
	}
	return dst, true
}

// gatherNumeric copies one value per selected row by indexing each record's
// typed backing slice, so the row loop is two slice indexes and no call.
//
// values is the arrow-go accessor for that slice (Int64Values, Float32Values,
// ...). It is resolved once per record, like the type assertion in concrete.
func gatherNumeric[T any, A interface {
	arrow.Array
	Value(int) T
}](srcs []arrow.Array, rows []rowRef, values func(A) []T) ([]T, bool) {
	cols, ok := concrete[A](srcs)
	if !ok {
		return nil, false
	}
	backing := make([][]T, len(cols))
	for i, src := range srcs {
		if src == nil {
			continue
		}
		backing[i] = values(cols[i])
	}
	dst := make([]T, len(rows))
	for i, ref := range rows {
		dst[i] = backing[ref.resultIdx][ref.rowIdx]
	}
	return dst, true
}

// checkVectorWidth rejects a vector column whose Arrow byte width disagrees
// with the dim its schema declares.
//
// This runs BEFORE the fast/slow dispatch deliberately. Returning ok=false from
// materializeColumn is not a guard: it only routes the column to gatherColumns,
// and that path does not check either -- resolveVectorDim takes the dim from the
// schema without comparing ByteWidth, and compactFloatVector then allocates
// numRows*dim floats and copies numRows*width bytes, so Go's copy() silently
// drops the excess or zero-fills the shortfall. Its nullable branch is worse:
// rows are written at stride dim and read at stride width, so they misalign.
// Both paths would produce the same self-inconsistent column, just by different
// routes, which is why the check belongs here rather than in either of them.
//
// Not reachable today: the exporter derives fixed_size_binary(dim*elemSize) from
// the same schema dim, so the two agree by construction. It is here so that if
// that ever stops being true the result is a loud error rather than a column
// whose payload and declared Dim disagree.
//
// A schema with no resolvable dim is left alone: arrowconv has a documented
// width-derived fallback for it, and there is nothing to compare against.
func checkVectorWidth(sel *ArrowSelection, col int, fs *schemapb.FieldSchema) error {
	var elemBits int
	switch fs.GetDataType() {
	case schemapb.DataType_FloatVector:
		elemBits = 32
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		elemBits = 16
	case schemapb.DataType_Int8Vector:
		elemBits = 8
	case schemapb.DataType_BinaryVector:
		elemBits = 1
	default:
		return nil // not a dense vector; nothing to reconcile
	}
	dim, err := typeutil.GetDim(fs)
	if err != nil {
		return nil
	}
	// Deliberately NOT selectedSources: that declines a column with nulls, and
	// a nullable column is exactly the one that reaches the gather. Only the
	// column's Arrow type is needed here, which nulls do not affect.
	width, ok := vectorWidth(rawSources(sel, col))
	if !ok {
		return nil // no contributing record to measure
	}
	wantBits := dim * int64(elemBits)
	if int64(width)*8 != wantBits {
		return merr.WrapErrServiceInternalMsg(
			"arrow vector column %d (field %d, %s) has byte width %d but its "+
				"schema declares dim %d (%d bits/element); payload and Dim "+
				"would disagree",
			col, fs.GetFieldID(), fs.GetDataType(), width, dim, elemBits)
	}
	return nil
}

// rawSources is selectedSources without the no-nulls requirement, for checks
// that must look at a column the fast path declined.
func rawSources(sel *ArrowSelection, col int) []arrow.Array {
	out := make([]arrow.Array, len(sel.Records))
	for i, r := range sel.Records {
		if r == nil || r.NumRows() == 0 {
			continue
		}
		out[i] = r.Column(col)
	}
	return out
}

// vectorWidth reads the FixedSizeBinary byte width every contributing record
// must agree on.
func vectorWidth(srcs []arrow.Array) (width int, ok bool) {
	for _, a := range srcs {
		if a == nil {
			continue
		}
		fsb, isFSB := a.DataType().(*arrow.FixedSizeBinaryType)
		if !isFSB {
			return 0, false
		}
		if width == 0 {
			width = fsb.ByteWidth
		} else if width != fsb.ByteWidth {
			return 0, false
		}
	}
	return width, width > 0
}
