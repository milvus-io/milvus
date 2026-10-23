package materialize

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/float16"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

// fcol is one user column of a differential test record.
type fcol struct {
	dt       arrow.DataType
	nullRate float64 // per-row null probability; 1 makes every row null
}

var fusedTypes = []arrow.DataType{
	arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64, arrow.FixedWidthTypes.Boolean,
	arrow.BinaryTypes.String, arrow.BinaryTypes.Binary,
	&arrow.FixedSizeBinaryType{ByteWidth: 16}, &arrow.FixedSizeBinaryType{ByteWidth: 3},
}

// userFieldID is the field id of user column i.
func userFieldID(i int) int64 { return 101 + int64(i) }

// fusedSchema is a retrieve record's schema for cols, with the retrieve
// metadata the export writes: field order (system fields first) and the
// nullable fields.
func fusedSchema(cols []fcol, sysIDs []int64) *arrow.Schema {
	order := make([]string, 0, len(sysIDs)+len(cols))
	for _, id := range sysIDs {
		order = append(order, strconv.FormatInt(id, 10))
	}
	var valid []string
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		id := strconv.FormatInt(userFieldID(i), 10)
		order = append(order, id)
		nullable := c.nullRate > 0
		if nullable {
			valid = append(valid, id)
		}
		fields[i] = arrow.Field{Name: id, Type: c.dt, Nullable: nullable}
	}
	md := arrow.NewMetadata(
		[]string{"milvus.field_order", "milvus.valid_data_fields"},
		[]string{strings.Join(order, ","), strings.Join(valid, ",")},
	)
	return arrow.NewSchema(fields, &md)
}

// fusedRecord builds a record of n rows with random values; with slice it
// is a slice of a larger record, so every column has a non-zero offset.
func fusedRecord(r *rand.Rand, schema *arrow.Schema, cols []fcol, n int, slice bool) arrow.Record {
	pre, total := 0, n
	if slice {
		pre = 1 + r.Intn(5)
		total = pre + n + 2
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i, c := range cols {
		fb := b.Field(i)
		for j := 0; j < total; j++ {
			if c.nullRate >= 1 || (c.nullRate > 0 && r.Float64() < c.nullRate) {
				fb.AppendNull()
				continue
			}
			switch bb := fb.(type) {
			case *array.Int8Builder:
				bb.Append(int8(r.Int()))
			case *array.Int16Builder:
				bb.Append(int16(r.Int()))
			case *array.Int32Builder:
				bb.Append(r.Int31())
			case *array.Int64Builder:
				bb.Append(r.Int63())
			case *array.Uint8Builder:
				bb.Append(uint8(r.Int()))
			case *array.Uint16Builder:
				bb.Append(uint16(r.Int()))
			case *array.Uint32Builder:
				bb.Append(r.Uint32())
			case *array.Uint64Builder:
				bb.Append(r.Uint64())
			case *array.Float16Builder:
				bb.Append(float16.New(r.Float32()))
			case *array.Float32Builder:
				bb.Append(r.Float32())
			case *array.Float64Builder:
				bb.Append(r.Float64())
			case *array.BooleanBuilder:
				bb.Append(r.Intn(2) == 1)
			case *array.Date32Builder:
				bb.Append(arrow.Date32(r.Int31()))
			case *array.StringBuilder:
				bb.Append(strings.Repeat("x", r.Intn(12)) + strconv.Itoa(j))
			case *array.BinaryBuilder:
				v := make([]byte, r.Intn(9))
				r.Read(v)
				bb.Append(v)
			case *array.FixedSizeBinaryBuilder:
				v := make([]byte, c.dt.(*arrow.FixedSizeBinaryType).ByteWidth)
				r.Read(v)
				bb.Append(v)
			default:
				panic(fmt.Sprintf("no generator for %T", fb))
			}
		}
	}
	rec := b.NewRecord()
	if !slice {
		return rec
	}
	out := rec.NewSlice(int64(pre), int64(pre+n))
	rec.Release()
	return out
}

type fusedCase struct {
	name     string
	cols     []fcol
	recRows  []int // rows per record; -1 is a nil record
	slice    bool
	strPK    bool
	sysIDs   []int64
	refs     [][2]int64 // nil: random
	nrows    int        // random refs
	contig   bool       // random refs as runs of consecutive rows
	declined bool       // the fused writer must decline it
}

// buildFusedInput builds the selection and the header for c.
func buildFusedInput(t testing.TB, r *rand.Rand, c fusedCase) (*segcorepb.RetrieveResults, *queryutil.ArrowSelection, func()) {
	schema := fusedSchema(c.cols, c.sysIDs)
	recs := make([]arrow.Record, len(c.recRows))
	for i, n := range c.recRows {
		if n >= 0 {
			recs[i] = fusedRecord(r, schema, c.cols, n, c.slice)
		}
	}
	refs := c.refs
	if refs == nil {
		var live []int
		for i, n := range c.recRows {
			if n > 0 {
				live = append(live, i)
			}
		}
		for len(refs) < c.nrows {
			ri := live[r.Intn(len(live))]
			row := r.Intn(c.recRows[ri])
			run := 1
			if c.contig {
				run = 1 + r.Intn(8)
			}
			for k := 0; k < run && row+k < c.recRows[ri] && len(refs) < c.nrows; k++ {
				refs = append(refs, [2]int64{int64(ri), int64(row + k)})
			}
		}
	}
	sel := &queryutil.ArrowSelection{Records: recs, Rows: queryutil.RowRefsForTest(refs)}
	n := len(refs)
	header := &segcorepb.RetrieveResults{}
	if c.strPK {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = "pk" + strings.Repeat("k", r.Intn(5)) + strconv.Itoa(i)
		}
		header.Ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids}}}
	} else {
		ids := make([]int64, n)
		for i := range ids {
			ids[i] = r.Int63()
		}
		header.Ids = intIDs(ids...)
	}
	for _, id := range c.sysIDs {
		vals := make([]int64, n)
		for i := range vals {
			vals[i] = r.Int63()
		}
		fd := tsField(vals...)
		fd.FieldId = id
		header.FieldsData = append(header.FieldsData, fd)
	}
	return header, sel, func() {
		for _, rec := range recs {
			if rec != nil {
				rec.Release()
			}
		}
	}
}

// twoStep is the oracle: BuildBatch, then EncodeBatch(CodecNone).
func twoStep(t testing.TB, header *segcorepb.RetrieveResults, sel *queryutil.ArrowSelection) Encoded {
	rec, err := BuildBatch(header, sel)
	require.NoError(t, err)
	require.NotNil(t, rec)
	defer rec.Release()
	schemaMsg, entry, err := EncodeBatch(rec, CodecNone)
	require.NoError(t, err)
	return Encoded{SchemaMsg: schemaMsg, Entry: entry, RawSize: RawSize(rec), ResultSize: resultSize(rec, 1+len(header.GetFieldsData()))}
}

func checkFused(t *testing.T, r *rand.Rand, c fusedCase) {
	header, sel, release := buildFusedInput(t, r, c)
	defer release()
	want := twoStep(t, header, sel)
	got, ok, err := encodeFused(header, sel)
	require.NoError(t, err)
	if c.declined {
		require.False(t, ok)
	} else {
		require.True(t, ok)
		require.Equal(t, want.SchemaMsg, got.SchemaMsg, "schema message")
		require.Equal(t, want.Entry, got.Entry, "record batch message")
		require.Equal(t, want.RawSize, got.RawSize, "raw size")
		require.Equal(t, want.ResultSize, got.ResultSize, "result size")
		// ResultSize is what the reader charges the rows (Batch.RowSize).
		_, batches := readBatches(t, want.SchemaMsg, want.Entry)
		var charged int64
		for row := 0; row < batches[0].Rows; row++ {
			charged += batches[0].RowSize(row)
		}
		require.Equal(t, charged, want.ResultSize, "result size against Batch.RowSize")
	}
	// Encode is the same whichever path it takes.
	enc, err := encodeWith(header, sel, CodecNone)
	require.NoError(t, err)
	require.Equal(t, want, enc)
}

func allTypes(nullRate float64) []fcol {
	cols := make([]fcol, len(fusedTypes))
	for i, dt := range fusedTypes {
		cols[i] = fcol{dt: dt, nullRate: nullRate}
	}
	return cols
}

func TestEncodeFused_MatchesTwoStep(t *testing.T) {
	ts := []int64{1}
	both := []int64{0, 1}
	cases := []fusedCase{
		{name: "one row", cols: allTypes(0), recRows: []int{3}, sysIDs: ts, refs: [][2]int64{{0, 1}}},
		{name: "one row null", cols: allTypes(1), recRows: []int{3}, sysIDs: ts, refs: [][2]int64{{0, 2}}},
		{name: "non-null", cols: allTypes(0), recRows: []int{40, 25}, sysIDs: ts, nrows: 50},
		{name: "nullable", cols: allTypes(0.3), recRows: []int{40, 25, 9}, sysIDs: ts, nrows: 60},
		{name: "all null", cols: allTypes(1), recRows: []int{40, 25}, sysIDs: ts, nrows: 33},
		{name: "nullable source no null selected", cols: allTypes(0.02), recRows: []int{200}, sysIDs: ts, refs: nil, nrows: 2},
		{name: "sliced", cols: allTypes(0.3), recRows: []int{30, 30}, slice: true, sysIDs: ts, nrows: 45},
		{name: "sliced non-null", cols: allTypes(0), recRows: []int{30, 30}, slice: true, sysIDs: ts, nrows: 45, contig: true},
		{name: "nil and empty records", cols: allTypes(0.3), recRows: []int{-1, 0, 20, -1, 15}, sysIDs: ts, nrows: 30},
		{name: "runs", cols: allTypes(0), recRows: []int{50, 50, 50}, sysIDs: ts, nrows: 120, contig: true},
		{name: "repeated rows", cols: allTypes(0.3), recRows: []int{4}, sysIDs: ts, refs: [][2]int64{{0, 1}, {0, 1}, {0, 3}, {0, 1}, {0, 0}, {0, 0}}},
		{name: "string pk", cols: allTypes(0.3), recRows: []int{20, 20}, strPK: true, sysIDs: ts, nrows: 25},
		{name: "string pk one row", cols: allTypes(0), recRows: []int{2}, strPK: true, sysIDs: ts, refs: [][2]int64{{0, 0}}},
		{name: "no sys columns", cols: allTypes(0.3), recRows: []int{20}, nrows: 10},
		{name: "rowid and ts", cols: allTypes(0.3), recRows: []int{20}, sysIDs: both, nrows: 10},
		{name: "no user rows over 64", cols: allTypes(0.5), recRows: []int{100, 100}, sysIDs: ts, nrows: 1500},
		{name: "text only", cols: []fcol{{dt: arrow.BinaryTypes.String}}, recRows: []int{10}, sysIDs: ts, nrows: 3},
		{name: "vector only", cols: []fcol{{dt: &arrow.FixedSizeBinaryType{ByteWidth: 3072}}}, recRows: []int{50, 50}, sysIDs: ts, nrows: 100, contig: true},
		{name: "unsupported type", cols: []fcol{{dt: arrow.PrimitiveTypes.Int64}, {dt: arrow.FixedWidthTypes.Date32}}, recRows: []int{10}, sysIDs: ts, nrows: 5, declined: true},
		// Types the export never produces, which the reader rejects too.
		{name: "uint8", cols: []fcol{{dt: arrow.PrimitiveTypes.Uint8}}, recRows: []int{10}, sysIDs: ts, nrows: 5, declined: true},
		{name: "float16", cols: []fcol{{dt: arrow.FixedWidthTypes.Float16}}, recRows: []int{10}, sysIDs: ts, nrows: 5, declined: true},
	}
	r := rand.New(rand.NewSource(7))
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			checkFused(t, r, c)
		})
	}
}

// Every row count from 1 to 130, then a few larger, crosses each bitmap
// rounding the stock writer and the builders apply.
func TestEncodeFused_RowCounts(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	counts := []int{}
	for n := 1; n <= 130; n++ {
		counts = append(counts, n)
	}
	counts = append(counts, 255, 256, 257, 511, 512, 513, 1000, 1024, 1500, 2049)
	for _, n := range counts {
		for _, rate := range []float64{0, 0.4, 1} {
			c := fusedCase{cols: allTypes(rate), recRows: []int{n, 7}, sysIDs: []int64{1}, nrows: n, strPK: n%2 == 0}
			t.Run(fmt.Sprintf("n=%d/nulls=%v", n, rate), func(t *testing.T) { checkFused(t, r, c) })
		}
	}
}

func TestEncodeFused_Randomized(t *testing.T) {
	r := rand.New(rand.NewSource(13))
	for i := 0; i < 300; i++ {
		ncols := 1 + r.Intn(5)
		cols := make([]fcol, ncols)
		for j := range cols {
			cols[j] = fcol{dt: fusedTypes[r.Intn(len(fusedTypes))], nullRate: []float64{0, 0, 0.2, 0.9, 1}[r.Intn(5)]}
		}
		nrec := 1 + r.Intn(4)
		recRows := make([]int, nrec)
		live := false
		for j := range recRows {
			recRows[j] = []int{-1, 0, 1, 5, 40, 130}[r.Intn(6)]
			live = live || recRows[j] > 0
		}
		if !live {
			recRows[0] = 3
		}
		sys := [][]int64{nil, {1}, {0, 1}}[r.Intn(3)]
		c := fusedCase{
			cols: cols, recRows: recRows, slice: r.Intn(2) == 0, strPK: r.Intn(2) == 0, sysIDs: sys,
			nrows: 1 + r.Intn(200), contig: r.Intn(2) == 0,
		}
		t.Run(strconv.Itoa(i), func(t *testing.T) { checkFused(t, r, c) })
	}
}

// Inputs BuildBatch rejects reach it: the fused writer declines them and
// Encode reports BuildBatch's error.
func TestEncode_DeclinesWhatBuildBatchRejects(t *testing.T) {
	r := rand.New(rand.NewSource(17))
	c := fusedCase{cols: allTypes(0), recRows: []int{5}, sysIDs: []int64{1}, nrows: 3}
	mutate := map[string]func(h *segcorepb.RetrieveResults, s *queryutil.ArrowSelection){
		"element level": func(h *segcorepb.RetrieveResults, _ *queryutil.ArrowSelection) { h.ElementLevel = true },
		"user field":    func(h *segcorepb.RetrieveResults, _ *queryutil.ArrowSelection) { h.FieldsData[0].FieldId = 101 },
		"short sys":     func(h *segcorepb.RetrieveResults, _ *queryutil.ArrowSelection) { h.FieldsData[0] = tsField(1) },
		"rows mismatch": func(h *segcorepb.RetrieveResults, s *queryutil.ArrowSelection) { s.Rows = s.Rows[:2] },
		"row out of range": func(_ *segcorepb.RetrieveResults, s *queryutil.ArrowSelection) {
			s.Rows = queryutil.RowRefsForTest([][2]int64{{0, 0}, {0, 9}, {0, 1}})
		},
		"record out of range": func(_ *segcorepb.RetrieveResults, s *queryutil.ArrowSelection) {
			s.Rows = queryutil.RowRefsForTest([][2]int64{{0, 0}, {3, 0}, {0, 1}})
		},
	}
	for name, m := range mutate {
		t.Run(name, func(t *testing.T) {
			header, sel, release := buildFusedInput(t, r, c)
			defer release()
			m(header, sel)
			_, ok, err := encodeFused(header, sel)
			require.NoError(t, err)
			require.False(t, ok)
			_, wantErr := BuildBatch(header, sel)
			require.Error(t, wantErr)
			_, err = encodeWith(header, sel, CodecNone)
			require.Error(t, err)
			require.Equal(t, wantErr.Error(), err.Error())
		})
	}
	t.Run("empty", func(t *testing.T) {
		enc, err := encodeWith(&segcorepb.RetrieveResults{}, nil, CodecNone)
		require.NoError(t, err)
		require.Nil(t, enc.Entry)
	})
}

// A record whose columns disagree with the template's types, or whose
// retrieve metadata differs, is left to the two-step path.
func TestEncodeFused_DeclinesDisagreeingRecords(t *testing.T) {
	r := rand.New(rand.NewSource(19))
	a := []fcol{{dt: arrow.PrimitiveTypes.Int64}, {dt: arrow.BinaryTypes.String}}
	b := []fcol{{dt: arrow.PrimitiveTypes.Int32}, {dt: arrow.BinaryTypes.String}}
	ra := fusedRecord(r, fusedSchema(a, []int64{1}), a, 4, false)
	defer ra.Release()
	rb := fusedRecord(r, fusedSchema(b, []int64{1}), b, 4, false)
	defer rb.Release()
	sel := &queryutil.ArrowSelection{Records: []arrow.Record{ra, rb}, Rows: queryutil.RowRefsForTest([][2]int64{{0, 0}, {1, 1}})}
	header := &segcorepb.RetrieveResults{Ids: intIDs(1, 2), FieldsData: []*schemapb.FieldData{tsField(1, 2)}}
	_, ok, err := encodeFused(header, sel)
	require.NoError(t, err)
	require.False(t, ok)

	c := []fcol{{dt: arrow.PrimitiveTypes.Int64}, {dt: arrow.BinaryTypes.String, nullRate: 0.5}}
	rc := fusedRecord(r, fusedSchema(c, []int64{1}), c, 4, false) // valid_data_fields differs
	defer rc.Release()
	sel.Records[1] = rc
	_, ok, err = encodeFused(header, sel)
	require.NoError(t, err)
	require.False(t, ok)
}

// The fused batch reads back through the proxy's reader exactly as the
// two-step batch does.
func TestEncodeFused_ReaderRoundTrip(t *testing.T) {
	cols := []fcol{
		{dt: arrow.FixedWidthTypes.Boolean, nullRate: 0.3},
		{dt: arrow.PrimitiveTypes.Int32},
		{dt: arrow.PrimitiveTypes.Int64, nullRate: 0.3},
		{dt: arrow.PrimitiveTypes.Float32},
		{dt: arrow.PrimitiveTypes.Float64, nullRate: 1},
		{dt: arrow.BinaryTypes.String, nullRate: 0.3},
		{dt: arrow.BinaryTypes.Binary},
		{dt: &arrow.FixedSizeBinaryType{ByteWidth: 16}, nullRate: 0.3},
	}
	types := []schemapb.DataType{
		schemapb.DataType_Bool, schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_Float,
		schemapb.DataType_Double, schemapb.DataType_VarChar, schemapb.DataType_JSON, schemapb.DataType_FloatVector,
	}
	coll := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	for i, dt := range types {
		fs := &schemapb.FieldSchema{FieldID: userFieldID(i), Name: "f" + strconv.Itoa(i), DataType: dt, Nullable: cols[i].nullRate > 0}
		if dt == schemapb.DataType_FloatVector {
			fs.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}
		}
		coll.Fields = append(coll.Fields, fs)
	}
	r := rand.New(rand.NewSource(23))
	for _, slice := range []bool{false, true} {
		header, sel, release := buildFusedInput(t, r, fusedCase{cols: cols, recRows: []int{30, 30}, slice: slice, sysIDs: []int64{1}, nrows: 40})
		want := twoStep(t, header, sel)
		got, ok, err := encodeFused(header, sel)
		require.NoError(t, err)
		require.True(t, ok)
		release()

		read := func(enc Encoded) (*schemapb.IDs, []*schemapb.FieldData) {
			s, batches := readBatches(t, enc.SchemaMsg, enc.Entry)
			refs := make([]RowRef, batches[0].Rows)
			for i := range refs {
				refs[i] = RowRef{Batch: 0, Row: int32(len(refs) - 1 - i)}
			}
			ids, fields, err := s.Gather(batches, refs, fieldsOf(t, coll))
			require.NoError(t, err)
			return ids, fields
		}
		wantIDs, wantFields := read(want)
		gotIDs, gotFields := read(got)
		require.Equal(t, wantIDs, gotIDs)
		require.Equal(t, len(wantFields), len(gotFields))
		for i := range wantFields {
			require.Equal(t, wantFields[i].String(), gotFields[i].String())
		}
	}
}

// Allocations per call do not grow with the row count: the message buffer,
// and nothing per row.
func TestEncodeFused_AllocsIndependentOfRows(t *testing.T) {
	cols := []fcol{{dt: arrow.BinaryTypes.String, nullRate: 0.3}, {dt: &arrow.FixedSizeBinaryType{ByteWidth: 64}}, {dt: arrow.FixedWidthTypes.Boolean}}
	r := rand.New(rand.NewSource(29))
	var allocs []float64
	for _, n := range []int{3, 100, 1000} {
		for _, strPK := range []bool{false, true} {
			header, sel, release := buildFusedInput(t, r, fusedCase{cols: cols, recRows: []int{1000, 1000}, strPK: strPK, sysIDs: []int64{1}, nrows: n})
			_, ok, err := encodeFused(header, sel) // warm the layout cache
			require.NoError(t, err)
			require.True(t, ok)
			a := testing.AllocsPerRun(50, func() {
				_, _, _ = encodeFused(header, sel)
			})
			release()
			allocs = append(allocs, a)
		}
	}
	for _, a := range allocs {
		require.LessOrEqual(t, a, 2.0, "allocs per call: %v", allocs)
	}
}

func TestReplyCodec(t *testing.T) {
	str := []fcol{{dt: arrow.BinaryTypes.String, nullRate: 0.3}}
	vec := []fcol{{dt: arrow.BinaryTypes.String}, {dt: &arrow.FixedSizeBinaryType{ByteWidth: 16}}}
	i64 := []fcol{{dt: arrow.PrimitiveTypes.Int64}}
	// An int64 pk and one int64 column: 16 bytes a row.
	edge := zstdMinBody / 16
	cases := []struct {
		name  string
		c     fusedCase
		codec Codec
		want  Codec
	}{
		{"none stays none", fusedCase{cols: str, recRows: []int{3000}, nrows: 3000}, CodecNone, CodecNone},
		{"vector small", fusedCase{cols: vec, recRows: []int{30}, nrows: 10}, CodecZstd, CodecNone},
		{"vector large", fusedCase{cols: vec, recRows: []int{3000}, nrows: 3000}, CodecZstd, CodecNone},
		{"text small", fusedCase{cols: str, recRows: []int{30, -1, 30}, nrows: 20, sysIDs: []int64{1}}, CodecZstd, CodecNone},
		{"text large", fusedCase{cols: str, recRows: []int{3000, -1, 3000}, nrows: 3000, sysIDs: []int64{1}}, CodecZstd, CodecZstd},
		{"varchar pk large", fusedCase{cols: i64, recRows: []int{3000}, nrows: 1000, strPK: true}, CodecZstd, CodecZstd},
		{"just below threshold", fusedCase{cols: i64, recRows: []int{3 * edge}, nrows: edge - 1}, CodecZstd, CodecNone},
		{"at threshold", fusedCase{cols: i64, recRows: []int{3 * edge}, nrows: edge}, CodecZstd, CodecZstd},
	}
	coll := func(cols []fcol, strPK bool) *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
		if strPK {
			pk.DataType = schemapb.DataType_VarChar
		}
		s := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{pk}}
		for i, c := range cols {
			fs := &schemapb.FieldSchema{FieldID: userFieldID(i), Name: "f" + strconv.Itoa(i), Nullable: c.nullRate > 0}
			switch c.dt.ID() {
			case arrow.STRING:
				fs.DataType = schemapb.DataType_VarChar
			case arrow.INT64:
				fs.DataType = schemapb.DataType_Int64
			default:
				fs.DataType = schemapb.DataType_FloatVector
				fs.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}
			}
			s.Fields = append(s.Fields, fs)
		}
		return s
	}
	r := rand.New(rand.NewSource(29))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			header, sel, release := buildFusedInput(t, r, tc.c)
			defer release()
			got := ReplyCodec(header, sel, tc.codec)
			require.Equal(t, tc.want, got)

			// Round trip through the proxy reader: the reply reads the same
			// as the uncompressed one, and its schema bytes do not depend on
			// the codec, so the delegator merges mixed replies.
			want := twoStep(t, header, sel)
			enc, err := encodeWith(header, sel, got)
			require.NoError(t, err)
			require.Equal(t, want.SchemaMsg, enc.SchemaMsg)
			require.Equal(t, want.RawSize, enc.RawSize)
			if got == CodecNone {
				require.Equal(t, want.Entry, enc.Entry)
			} else {
				// Compressed framing, even where a buffer goes raw.
				require.NotEqual(t, want.Entry, enc.Entry)
			}
			read := func(e Encoded) (*schemapb.IDs, []*schemapb.FieldData) {
				s, batches := readBatches(t, e.SchemaMsg, e.Entry)
				refs := make([]RowRef, batches[0].Rows)
				for i := range refs {
					refs[i] = RowRef{Batch: 0, Row: int32(i)}
				}
				ids, fields, err := s.Gather(batches, refs, fieldsOf(t, coll(tc.c.cols, tc.c.strPK)))
				require.NoError(t, err)
				return ids, fields
			}
			wantIDs, wantFields := read(want)
			gotIDs, gotFields := read(enc)
			require.Equal(t, wantIDs, gotIDs)
			require.Equal(t, len(wantFields), len(gotFields))
			for i := range wantFields {
				require.Equal(t, wantFields[i].String(), gotFields[i].String())
			}
		})
	}

	// One reply raw, one compressed, under one schema: the proxy reads both.
	t.Run("mixed replies", func(t *testing.T) {
		c := fusedCase{cols: str, recRows: []int{3000}, nrows: 2000}
		header, sel, release := buildFusedInput(t, r, c)
		defer release()
		raw, err := encodeWith(header, sel, CodecNone)
		require.NoError(t, err)
		z, err := encodeWith(header, sel, CodecZstd)
		require.NoError(t, err)
		require.Equal(t, raw.SchemaMsg, z.SchemaMsg)
		s, batches := readBatches(t, raw.SchemaMsg, raw.Entry, z.Entry)
		refs := make([]RowRef, 0, 2*batches[0].Rows)
		for b := range batches {
			for i := 0; i < batches[b].Rows; i++ {
				refs = append(refs, RowRef{Batch: int32(b), Row: int32(i)})
			}
		}
		ids, fields, err := s.Gather(batches, refs, fieldsOf(t, coll(c.cols, false)))
		require.NoError(t, err)
		n := batches[0].Rows
		require.Equal(t, ids.GetIntId().GetData()[:n], ids.GetIntId().GetData()[n:])
		v := fields[0].GetScalars().GetStringData().GetData()
		require.Equal(t, v[:n], v[n:])
	})

	t.Run("empty selection", func(t *testing.T) {
		require.Equal(t, CodecZstd, ReplyCodec(&segcorepb.RetrieveResults{}, &queryutil.ArrowSelection{}, CodecZstd))
		require.Equal(t, CodecZstd, ReplyCodec(&segcorepb.RetrieveResults{}, nil, CodecZstd))
	})
}

// The Query-measured size every writer reports, and the reader charges, is
// queryutil's rowSizeCalculator on the same rows: every carried Arrow type,
// nullable and all-null columns, vectors, both pk types, both codecs.
func TestResultSize_MatchesQueryCalculator(t *testing.T) {
	ts := []int64{1}
	cases := []fusedCase{
		{name: "non-null", cols: allTypes(0), recRows: []int{40, 25}, sysIDs: ts, nrows: 50},
		{name: "nullable", cols: allTypes(0.3), recRows: []int{40, 25, 9}, sysIDs: ts, nrows: 60},
		{name: "all null", cols: allTypes(1), recRows: []int{40, 25}, sysIDs: ts, nrows: 33},
		{name: "sliced", cols: allTypes(0.3), recRows: []int{30, 30}, slice: true, sysIDs: ts, nrows: 45},
		{name: "string pk", cols: allTypes(0.3), recRows: []int{20, 20}, strPK: true, sysIDs: ts, nrows: 25},
		{name: "rowid and ts", cols: allTypes(0.3), recRows: []int{20}, sysIDs: []int64{0, 1}, nrows: 10},
		{name: "no sys columns", cols: allTypes(0.3), recRows: []int{20}, nrows: 10},
		{name: "nullable vector", cols: []fcol{{dt: &arrow.FixedSizeBinaryType{ByteWidth: 512}, nullRate: 0.5}}, recRows: []int{30}, sysIDs: ts, nrows: 30},
		{name: "large text", cols: []fcol{{dt: arrow.BinaryTypes.String, nullRate: 0.1}}, recRows: []int{3000}, sysIDs: ts, nrows: 3000, contig: true},
	}
	r := rand.New(rand.NewSource(17))
	for _, c := range cases {
		for _, codec := range []Codec{CodecNone, CodecZstd} {
			t.Run(c.name+"/"+string(codec), func(t *testing.T) {
				header, sel, release := buildFusedInput(t, r, c)
				defer release()
				user, err := queryutil.GatherArrowSelection(sel)
				require.NoError(t, err)
				defer user.Release()
				want := queryutil.ResultSizeForTest(header.GetFieldsData(), user)

				enc, err := encodeWith(header, sel, codec)
				require.NoError(t, err)
				require.Equal(t, want, enc.ResultSize, "writer")
				_, batches := readBatches(t, enc.SchemaMsg, enc.Entry)
				var charged int64
				for row := 0; row < batches[0].Rows; row++ {
					charged += batches[0].RowSize(row)
				}
				require.Equal(t, want, charged, "reader")
			})
		}
	}
}
