package materialize

import (
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/bitutil"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/arrowconv"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RowRef names one row of one batch.
type RowRef struct {
	Batch, Row int32
}

// Gather copies the rows, in order, out of the batches (all read with s)
// into the result's ids and FieldsData: the system columns and the user
// columns in the worker's field order, exactly what
// queryutil.MaterializeArrowSelection makes of the same rows decoded as Arrow
// records (design doc 20261019 §7). Each selected value is copied once,
// straight from the entry into the final FieldData; a value that straddles
// two received pieces is copied in two parts. Nothing returned points into
// the entries.
//
// The FieldData follow the protobuf path's conventions: FieldName empty;
// valid_data exactly for the fields the worker's retrieve metadata lists,
// all true when no row is null; a zero value (false, 0, "", empty bytes)
// under a null scalar; and, for a dense vector, only the non-null rows'
// vectors when a gathered row is null. fields resolves the user columns'
// field ids against the collection schema.
func (s *Schema) Gather(batches []*Batch, rows []RowRef, fields *typeutil.SchemaHelper) (*schemapb.IDs, []*schemapb.FieldData, error) {
	if s.orderErr != nil {
		return nil, nil, s.orderErr
	}
	for _, r := range rows {
		if r.Batch < 0 || int(r.Batch) >= len(batches) || r.Row < 0 || int(r.Row) >= batches[r.Batch].Rows {
			return nil, nil, merr.WrapErrServiceInternalMsg("materialize: row (%d,%d) is out of range", r.Batch, r.Row)
		}
	}
	byID := make(map[int64]*schemapb.FieldData, len(s.order))
	for j, id := range s.sysIDs {
		data := pick(batches, rows, func(b *Batch) []int64 { return b.Sys[j] })
		byID[id] = &schemapb.FieldData{
			FieldId: id, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
			}},
		}
	}
	for col, id := range s.userIDs {
		fs, err := fields.GetFieldFromID(id)
		if err != nil {
			return nil, nil, merr.WrapErrServiceInternalMsg("materialize: column %d has field id %d, not in the schema", col, id)
		}
		g := gatherer{batches: batches, rows: rows, col: col, dt: s.arrow.Field(s.lead() + col).Type}
		fd, err := g.fieldData(fs)
		if err != nil {
			return nil, nil, err
		}
		if _, want := s.validData[id]; want {
			typeutil.SetFieldDataValidData(fd, g.validity())
		}
		byID[id] = fd
	}
	if len(byID) != len(s.order) {
		return nil, nil, merr.WrapErrServiceInternalMsg("materialize: field order lists %d ids but %d columns are present", len(s.order), len(byID))
	}
	out := make([]*schemapb.FieldData, 0, len(s.order))
	for _, id := range s.order {
		fd, ok := byID[id]
		if !ok {
			return nil, nil, merr.WrapErrServiceInternalMsg("materialize: field order references missing field id %d", id)
		}
		out = append(out, fd)
	}

	var ids *schemapb.IDs
	if s.StringPK() {
		// The batch's pks are already copies.
		data := pick(batches, rows, func(b *Batch) []string { return b.StrPKs })
		ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: data}}}
	} else {
		data := pick(batches, rows, func(b *Batch) []int64 { return b.IntPKs })
		ids = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data}}}
	}
	return ids, out, nil
}

// pick gathers the rows of a column the batches hold as Go values (col).
func pick[T any](batches []*Batch, rows []RowRef, col func(*Batch) []T) []T {
	cols := make([][]T, len(batches))
	for b, batch := range batches {
		cols[b] = col(batch)
	}
	out := make([]T, len(rows))
	for i, r := range rows {
		out[i] = cols[r.Batch][r.Row]
	}
	return out
}

// gatherer gathers one user column.
type gatherer struct {
	batches []*Batch
	rows    []RowRef
	col     int
	dt      arrow.DataType

	valid []bool // per gathered row, nil until computed
	nulls int
}

func (g *gatherer) column(r RowRef) *column { return &g.batches[r.Batch].cols[g.col] }

// computeValidity fills valid and nulls when any batch has a null in the
// column; valid stays nil otherwise.
func (g *gatherer) computeValidity() {
	hasNulls := false
	for _, b := range g.batches {
		if b.cols[g.col].nulls > 0 {
			hasNulls = true
			break
		}
	}
	if !hasNulls {
		return
	}
	g.valid = make([]bool, len(g.rows))
	for i, r := range g.rows {
		g.valid[i] = g.column(r).isValid(int(r.Row))
		if !g.valid[i] {
			g.nulls++
		}
	}
}

// validity is the gathered rows' valid_data: per row, all true without
// nulls.
func (g *gatherer) validity() []bool {
	if g.valid != nil {
		return g.valid
	}
	out := make([]bool, len(g.rows))
	for i := range out {
		out[i] = true
	}
	return out
}

func (g *gatherer) isNull(i int) bool { return g.valid != nil && !g.valid[i] }

func (g *gatherer) fieldData(fs *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	want, ok := carried[fs.GetDataType()]
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("materialize: field %d has type %s, which Materialize does not carry",
			fs.GetFieldID(), fs.GetDataType())
	}
	if g.dt.ID() != want {
		return nil, merr.WrapErrServiceInternalMsg("materialize: field %d (%s) arrives as Arrow %s, expected %s",
			fs.GetFieldID(), fs.GetDataType(), g.dt, want)
	}
	g.computeValidity()
	fd := &schemapb.FieldData{Type: fs.GetDataType(), FieldId: fs.GetFieldID()}
	scalars := func(sf *schemapb.ScalarField) {
		fd.Field = &schemapb.FieldData_Scalars{Scalars: sf}
	}
	switch fs.GetDataType() {
	case schemapb.DataType_Bool:
		data := make([]bool, len(g.rows))
		for i, r := range g.rows {
			if !g.isNull(i) {
				data[i] = bitutil.BitIsSet(g.column(r).bits, int(r.Row))
			}
		}
		scalars(&schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}}})
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		// protobuf carries all three widths in int_data, and the export
		// widens all three to int32.
		data := make([]int32, len(g.rows))
		g.fixed(arrow.Int32Traits.CastToBytes(data), 4)
		scalars(&schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}}})
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		data := make([]int64, len(g.rows))
		g.fixed(arrow.Int64Traits.CastToBytes(data), 8)
		if fs.GetDataType() == schemapb.DataType_Timestamptz {
			scalars(&schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: data}}})
		} else {
			scalars(&schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}})
		}
	case schemapb.DataType_Float:
		data := make([]float32, len(g.rows))
		g.fixed(arrow.Float32Traits.CastToBytes(data), 4)
		scalars(&schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}}})
	case schemapb.DataType_Double:
		data := make([]float64, len(g.rows))
		g.fixed(arrow.Float64Traits.CastToBytes(data), 8)
		scalars(&schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}}})
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		scalars(&schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: g.texts()}}})
	case schemapb.DataType_JSON:
		scalars(&schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: g.binaries()}}})
	case schemapb.DataType_Geometry:
		scalars(&schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: g.binaries()}}})
	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector:
		vf, err := g.vector(fs)
		if err != nil {
			return nil, err
		}
		fd.Field = &schemapb.FieldData_Vectors{Vectors: vf}
	}
	return fd, nil
}

// fixed copies width bytes per gathered row into dst, leaving null rows
// zero.
func (g *gatherer) fixed(dst []byte, width int) {
	// Each batch's column, as one slice when it lies in a single piece;
	// cursors for the others.
	views := make([][]byte, len(g.batches))
	curs := make([]cursor, len(g.batches))
	for b, batch := range g.batches {
		c := &batch.cols[g.col]
		if v, ok := c.values.view(); ok {
			views[b] = v
		} else {
			curs[b] = cursor{p: c.values.p}
		}
	}
	for i, r := range g.rows {
		if g.isNull(i) {
			continue
		}
		row := int(r.Row)
		out := dst[i*width : (i+1)*width]
		if v := views[r.Batch]; v != nil {
			copy(out, v[row*width:(row+1)*width])
			continue
		}
		c := g.column(r)
		curs[r.Batch].read(out, c.values.off+row*width)
	}
}

// varLen returns the total bytes of the gathered rows' var-len values and a
// cursor per batch over its values.
func (g *gatherer) varLen() (int, []cursor) {
	total := 0
	for i, r := range g.rows {
		if !g.isNull(i) {
			c := g.column(r)
			total += int(c.offsets[r.Row+1] - c.offsets[r.Row])
		}
	}
	curs := make([]cursor, len(g.batches))
	for b, batch := range g.batches {
		curs[b] = cursor{p: batch.cols[g.col].values.p}
	}
	return total, curs
}

// texts copies the gathered rows' values into one string that the
// returned values slice; a null row is "".
func (g *gatherer) texts() []string {
	total, curs := g.varLen()
	var sb strings.Builder
	sb.Grow(total)
	ends := make([]int, len(g.rows))
	for i, r := range g.rows {
		if !g.isNull(i) {
			c := g.column(r)
			start := int(c.offsets[r.Row])
			curs[r.Batch].writeString(&sb, c.values.off+start, int(c.offsets[r.Row+1])-start)
		}
		ends[i] = sb.Len()
	}
	joined := sb.String()
	out := make([]string, len(g.rows))
	prev := 0
	for i, end := range ends {
		out[i] = joined[prev:end]
		prev = end
	}
	return out
}

// binaries copies the gathered rows' values into one arena, one capped slice
// per row; a null row is empty.
func (g *gatherer) binaries() [][]byte {
	total, curs := g.varLen()
	arena := make([]byte, total)
	out := make([][]byte, len(g.rows))
	pos := 0
	for i, r := range g.rows {
		n := 0
		if !g.isNull(i) {
			c := g.column(r)
			start := int(c.offsets[r.Row])
			n = int(c.offsets[r.Row+1]) - start
			curs[r.Batch].read(arena[pos:pos+n], c.values.off+start)
		}
		out[i] = arena[pos : pos+n : pos+n]
		pos += n
	}
	return out
}

// vectorDim is the dim of a dense vector field whose values are width bytes
// each: the schema's, or when the schema carries none, arrowconv's fallback
// from the width. Either way width must hold exactly dim elements of the
// type, and dim must be positive.
func vectorDim(fs *schemapb.FieldSchema, width int) (int64, error) {
	var elemBits int64
	switch fs.GetDataType() {
	case schemapb.DataType_FloatVector:
		elemBits = 32
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		elemBits = 16
	case schemapb.DataType_Int8Vector:
		elemBits = 8
	case schemapb.DataType_BinaryVector:
		elemBits = 1
	}
	dim, err := typeutil.GetDim(fs)
	if err != nil {
		dim = arrowconv.VectorDimFromByteWidth(fs.GetDataType(), width)
	}
	if dim <= 0 || int64(width)*8 != dim*elemBits {
		return 0, merr.WrapErrServiceInternalMsg(
			"materialize: vector field %d (%s) has byte width %d, which does not hold dim %d",
			fs.GetFieldID(), fs.GetDataType(), width, dim)
	}
	return dim, nil
}

// vector gathers a dense vector column. With a null among the gathered
// rows, only the non-null rows' vectors are kept, as the protobuf payload of
// a nullable vector holds.
func (g *gatherer) vector(fs *schemapb.FieldSchema) (*schemapb.VectorField, error) {
	width := g.dt.(*arrow.FixedSizeBinaryType).ByteWidth
	dim, err := vectorDim(fs, width)
	if err != nil {
		return nil, err
	}

	rows := g.rows
	if g.nulls > 0 {
		kept := make([]RowRef, 0, len(rows)-g.nulls)
		for i, r := range rows {
			if g.valid[i] {
				kept = append(kept, r)
			}
		}
		rows = kept
	}
	dense := gatherer{batches: g.batches, rows: rows, col: g.col, dt: g.dt}
	vf := &schemapb.VectorField{Dim: dim}
	if fs.GetDataType() == schemapb.DataType_FloatVector {
		data := make([]float32, len(rows)*width/4)
		dense.fixed(arrow.Float32Traits.CastToBytes(data), width)
		vf.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}}
		return vf, nil
	}
	data := make([]byte, len(rows)*width)
	dense.fixed(data, width)
	switch fs.GetDataType() {
	case schemapb.DataType_BinaryVector:
		vf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: data}
	case schemapb.DataType_Float16Vector:
		vf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: data}
	case schemapb.DataType_BFloat16Vector:
		vf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: data}
	case schemapb.DataType_Int8Vector:
		vf.Data = &schemapb.VectorField_Int8Vector{Int8Vector: data}
	}
	return vf, nil
}
