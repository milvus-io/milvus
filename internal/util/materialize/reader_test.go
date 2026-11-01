package materialize

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/materialize/internal/flatbuf"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var readerTypes = []arrow.DataType{
	arrow.FixedWidthTypes.Boolean,
	arrow.PrimitiveTypes.Int8,
	arrow.PrimitiveTypes.Int16,
	arrow.PrimitiveTypes.Int32,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Float32,
	arrow.PrimitiveTypes.Float64,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.Binary,
	&arrow.FixedSizeBinaryType{ByteWidth: 12},
}

// allTypesRecord has one column per readerTypes entry; nullEvery > 0 makes
// every nullEvery-th row of every column null.
func allTypesRecord(t testing.TB, rows, nullEvery int, seed int64) arrow.Record {
	t.Helper()
	fields := make([]arrow.Field, len(readerTypes))
	for i, dt := range readerTypes {
		fields[i] = arrow.Field{Name: fmt.Sprintf("c%d", i), Type: dt, Nullable: nullEvery > 0}
	}
	md := arrow.NewMetadata([]string{"milvus.field_order"}, []string{"1,2,3"})
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, &md))
	defer b.Release()
	r := rand.New(rand.NewSource(seed))
	for row := 0; row < rows; row++ {
		for i := range readerTypes {
			fb := b.Field(i)
			if nullEvery > 0 && (row+i)%nullEvery == 0 {
				fb.AppendNull()
				continue
			}
			switch fb := fb.(type) {
			case *array.BooleanBuilder:
				fb.Append(r.Intn(2) == 0)
			case *array.Int8Builder:
				fb.Append(int8(r.Int()))
			case *array.Int16Builder:
				fb.Append(int16(r.Int()))
			case *array.Int32Builder:
				fb.Append(r.Int31())
			case *array.Int64Builder:
				fb.Append(r.Int63())
			case *array.Float32Builder:
				fb.Append(r.Float32())
			case *array.Float64Builder:
				fb.Append(r.Float64())
			case *array.StringBuilder:
				fb.Append(strings.Repeat("s", r.Intn(40)) + "!")
			case *array.BinaryBuilder:
				v := make([]byte, r.Intn(40))
				r.Read(v)
				fb.Append(v)
			case *array.FixedSizeBinaryBuilder:
				v := make([]byte, 12)
				r.Read(v)
				fb.Append(v)
			default:
				t.Fatalf("unexpected builder %T", fb)
			}
		}
	}
	return b.NewRecord()
}

func releaseAll(recs []arrow.Record) {
	for _, r := range recs {
		r.Release()
	}
}

// splitAt cuts e at the given offsets into fresh, separate buffers.
func splitAt(e []byte, cuts ...int) [][]byte {
	var out [][]byte
	prev := 0
	for _, c := range append(cuts, len(e)) {
		if c < prev || c > len(e) {
			continue
		}
		out = append(out, bytes.Clone(e[prev:c]))
		prev = c
	}
	return out
}

func everyN(e []byte, n int) [][]byte {
	var cuts []int
	for c := n; c < len(e); c += n {
		cuts = append(cuts, c)
	}
	return splitAt(e, cuts...)
}

// splits are the ways an entry is cut into pieces for a test: whole, gRPC's
// 16 KB frames, small odd frames, 1-byte frames, and random cuts.
func splits(e []byte, seed int64) map[string][][]byte {
	r := rand.New(rand.NewSource(seed))
	var cuts []int
	for c := r.Intn(64) + 1; c < len(e); c += r.Intn(300) + 1 {
		cuts = append(cuts, c)
	}
	return map[string][][]byte{
		"whole":  {e},
		"16KB":   everyN(e, 16<<10),
		"7B":     everyN(e, 7),
		"1B":     everyN(e, 1),
		"random": splitAt(e, cuts...),
	}
}

// columnValue reads row of c from the entry, as bytes.
func columnValue(c *column, row int) []byte {
	cur := cursor{p: c.values.p}
	switch c.typ {
	case arrow.BOOL:
		if c.bits[row/8]&(1<<(row%8)) != 0 {
			return []byte{1}
		}
		return []byte{0}
	case arrow.STRING, arrow.BINARY:
		start, end := int(c.offsets[row]), int(c.offsets[row+1])
		out := make([]byte, end-start)
		cur.read(out, c.values.off+start)
		return out
	default:
		out := make([]byte, c.width)
		cur.read(out, c.values.off+row*c.width)
		return out
	}
}

// stockValue reads row of a, as bytes.
func stockValue(a arrow.Array, row int) []byte {
	switch a := a.(type) {
	case *array.Boolean:
		if a.Value(row) {
			return []byte{1}
		}
		return []byte{0}
	case *array.String:
		return []byte(a.Value(row))
	case *array.Binary:
		return a.Value(row)
	case *array.FixedSizeBinary:
		return a.Value(row)
	default:
		w := a.DataType().(arrow.FixedWidthDataType).BitWidth() / 8
		off := (a.Data().Offset() + row) * w
		return a.Data().Buffers()[1].Bytes()[off : off+w]
	}
}

// requireSameColumns checks every value of every column against the stock
// reader's record.
func requireSameColumns(t *testing.T, want arrow.Record, cols []column, rows int) {
	t.Helper()
	require.EqualValues(t, want.NumRows(), rows)
	require.Len(t, cols, int(want.NumCols()))
	for i := range cols {
		a, c := want.Column(i), &cols[i]
		require.Equal(t, a.NullN(), c.nulls, "column %d nulls", i)
		for row := 0; row < rows; row++ {
			require.Equal(t, a.IsValid(row), c.isValid(row), "column %d row %d validity", i, row)
			if a.IsValid(row) {
				require.Equal(t, stockValue(a, row), columnValue(c, row), "column %d row %d", i, row)
			}
		}
	}
}

func TestReadColumns_MatchesStockReader(t *testing.T) {
	full := allTypesRecord(t, 300, 0, 1)
	defer full.Release()
	fullNulls := allTypesRecord(t, 300, 4, 2)
	defer fullNulls.Release()
	cases := map[string]arrow.Record{
		"empty":            allTypesRecord(t, 0, 0, 3),
		"one":              allTypesRecord(t, 1, 0, 4),
		"one null":         allTypesRecord(t, 1, 1, 5),
		"no nulls":         allTypesRecord(t, 300, 0, 6),
		"nulls":            allTypesRecord(t, 300, 4, 7),
		"all nulls":        allTypesRecord(t, 9, 1, 8),
		"sliced":           full.NewSlice(13, 211),
		"sliced nulls":     fullNulls.NewSlice(7, 99),
		"odd bool padding": allTypesRecord(t, 13, 3, 9),
		"large":            allTypesRecord(t, 3000, 5, 10),
	}
	for name, rec := range cases {
		for _, codec := range []Codec{CodecNone, CodecZstd} {
			s, e, err := EncodeBatch(rec, codec)
			require.NoError(t, err)
			schema, err := readSchema(s)
			require.NoError(t, err)
			stock, err := decodeBody(s, [][]byte{e})
			require.NoError(t, err)
			for split, parts := range splits(e, int64(len(name))) {
				if split == "1B" && len(e) > 1<<16 {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", name, codec, split), func(t *testing.T) {
					cols, rows, err := readColumns(schema, parts, 0, 0)
					require.NoError(t, err)
					requireSameColumns(t, stock[0], cols, rows)
				})
			}
			releaseAll(stock)
		}
		rec.Release()
	}
}

// Two pieces cut at every offset of a small entry: inside the prefix, the
// metadata, each buffer and each row.
func TestReadColumns_EveryCut(t *testing.T) {
	rec := allTypesRecord(t, 5, 3, 11)
	defer rec.Release()
	s, e, err := EncodeBatch(rec, CodecNone)
	require.NoError(t, err)
	schema, err := readSchema(s)
	require.NoError(t, err)
	for c := 1; c < len(e); c++ {
		cols, rows, err := readColumns(schema, splitAt(e, c), 0, 0)
		require.NoError(t, err, "cut %d", c)
		requireSameColumns(t, rec, cols, rows)
	}
}

// The reader copies O(rows) state, not the values: its allocation for a
// vector-heavy entry is a small fraction of the entry.
func TestReadEntry_DoesNotCopyValues(t *testing.T) {
	fx := newGatherFixture(t, gatherShape{rows: 2000, vecDim: 768})
	defer fx.release()
	s, e := fx.encode(t, CodecNone)[0], fx.encode(t, CodecNone)[1]
	schema, err := ParseSchema(s)
	require.NoError(t, err)
	parts := everyN(e, 16<<10)
	allocs := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := schema.ReadEntry(parts, 0, 0); err != nil {
				b.Fatal(err)
			}
		}
	}).AllocedBytesPerOp()
	t.Logf("ReadEntry of a %d-byte entry allocates %d bytes", len(e), allocs)
	require.Less(t, allocs, int64(len(e)/20))
}

// recordBatch returns the RecordBatch metadata of entry, as a view whose
// Mutate methods write into entry.
func recordBatch(t testing.TB, entry []byte) *flatbuf.RecordBatch {
	t.Helper()
	metaEnd := 8 + int(binary.LittleEndian.Uint32(entry[4:]))
	msg := flatbuf.GetRootAsMessage(entry[8:metaEnd], 0)
	var tab flatbuffers.Table
	require.True(t, msg.Header(&tab))
	md := &flatbuf.RecordBatch{}
	md.Init(tab.Bytes, tab.Pos)
	return md
}

// mutateMeta returns a copy of entry whose RecordBatch metadata fn changed.
func mutateMeta(t *testing.T, entry []byte, fn func(md *flatbuf.RecordBatch)) []byte {
	t.Helper()
	out := append([]byte(nil), entry...)
	fn(recordBatch(t, out))
	return out
}

func TestReadColumns_Malformed(t *testing.T) {
	rec := allTypesRecord(t, 40, 3, 1)
	defer rec.Release()
	s, e, err := EncodeBatch(rec, CodecNone)
	require.NoError(t, err)
	schema, err := readSchema(s)
	require.NoError(t, err)
	_, ze, err := EncodeBatch(rec, CodecZstd)
	require.NoError(t, err)
	bodyLen := int64(len(e) - (8 + int(binary.LittleEndian.Uint32(e[4:]))))

	setBuffer := func(i int, off, n int64) func(*flatbuf.RecordBatch) {
		return func(md *flatbuf.RecordBatch) {
			var b flatbuf.Buffer
			require.True(t, md.Buffers(&b, i))
			b.MutateOffset(off)
			b.MutateLength(n)
		}
	}
	setNode := func(i int, length, nulls int64) func(*flatbuf.RecordBatch) {
		return func(md *flatbuf.RecordBatch) {
			var n flatbuf.FieldNode
			require.True(t, md.Nodes(&n, i))
			n.MutateLength(length)
			n.MutateNullCount(nulls)
		}
	}
	// Column 7 is utf8: buffers 2*7 = 14 (validity), 15 (offsets), 16 (values).
	var offsetsBuf flatbuf.Buffer
	require.True(t, recordBatch(t, e).Buffers(&offsetsBuf, 15))
	offOff := offsetsBuf.Offset()
	metaEnd := int64(8 + int(binary.LittleEndian.Uint32(e[4:])))
	withBody := func(src []byte, pos int64, v uint64, width int) []byte {
		out := append([]byte(nil), src...)
		if width == 4 {
			binary.LittleEndian.PutUint32(out[metaEnd+pos:], uint32(v))
		} else {
			binary.LittleEndian.PutUint64(out[metaEnd+pos:], v)
		}
		return out
	}
	// A compressed buffer's uncompressed-length prefix.
	var zbuf flatbuf.Buffer
	require.True(t, recordBatch(t, ze).Buffers(&zbuf, 16))
	zMetaEnd := int64(8 + int(binary.LittleEndian.Uint32(ze[4:])))
	withZ := func(v uint64) []byte {
		out := append([]byte(nil), ze...)
		binary.LittleEndian.PutUint64(out[zMetaEnd+zbuf.Offset():], v)
		return out
	}

	cases := map[string][]byte{
		"empty":                  {},
		"truncated":              e[:len(e)-8],
		"truncated meta":         e[:12],
		"bad continuation":       append([]byte{1, 2, 3, 4}, e[4:]...),
		"buffer past body":       mutateMeta(t, e, setBuffer(1, bodyLen-4, 64)),
		"buffer negative offset": mutateMeta(t, e, setBuffer(1, -8, 8)),
		"buffer huge length":     mutateMeta(t, e, setBuffer(3, 0, 1<<62)),
		"short values":           mutateMeta(t, e, setBuffer(9, 0, 8)),
		"short validity":         mutateMeta(t, e, setBuffer(0, 0, 1)),
		"node length mismatch":   mutateMeta(t, e, setNode(2, 39, 0)),
		"node nulls over length": mutateMeta(t, e, setNode(2, 40, 41)),
		"node negative nulls":    mutateMeta(t, e, setNode(2, 40, -1)),
		"string offsets short":   mutateMeta(t, e, setBuffer(15, 0, 8)),
		"string offset decrease": withBody(e, offOff+4*5, 0, 4),
		"string offset negative": withBody(e, offOff, 0x80000000, 4),
		"string offset too far":  withBody(e, offOff+4*40, 1<<20, 4),
		"row count":              mutateMeta(t, e, func(md *flatbuf.RecordBatch) { md.MutateLength(41) }),
		"zstd size too large":    withZ(1 << 40),
		"zstd size wrong":        withZ(3),
		"zstd size negative":     withZ(uint64(1) << 63),
		"zstd frame corrupt":     withBody(ze, zbuf.Offset()+8+zMetaEnd-metaEnd, 0xdeadbeefdeadbeef, 8),
	}
	for name, entry := range cases {
		for _, parts := range [][][]byte{{entry}, everyN(entry, 3)} {
			t.Run(name, func(t *testing.T) {
				require.NotPanics(t, func() {
					_, _, err := readColumns(schema, parts, 0, 0)
					require.ErrorIs(t, err, merr.ErrServiceInternal)
				})
			})
		}
	}
}

func FuzzReadColumns(f *testing.F) {
	rec := allTypesRecord(f, 20, 3, 1)
	defer rec.Release()
	s, e, err := EncodeBatch(rec, CodecNone)
	if err != nil {
		f.Fatal(err)
	}
	_, ze, err := EncodeBatch(rec, CodecZstd)
	if err != nil {
		f.Fatal(err)
	}
	schema, err := readSchema(s)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(e, uint16(0))
	f.Add(e[:len(e)/2], uint16(5))
	f.Add(ze, uint16(13))
	f.Fuzz(func(t *testing.T, entry []byte, cut uint16) {
		parts := [][]byte{entry}
		if int(cut) < len(entry) {
			parts = splitAt(entry, int(cut))
		}
		cols, rows, err := readColumns(schema, parts, 0, 0)
		if err != nil {
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			return
		}
		// Every value must be readable without a panic.
		for i := range cols {
			for row := 0; row < rows; row++ {
				_ = columnValue(&cols[i], row)
			}
		}
	})
}

// ---- Gather against the stock reader plus MaterializeArrowSelection ----

// gatherField is one output field of the gather fixture: its schema and the
// Arrow type segcore exports it as.
type gatherField struct {
	fs *schemapb.FieldSchema
	dt arrow.DataType
}

type gatherShape struct {
	rows     int
	strPK    bool
	nullable bool // every user field nullable, every 3rd value null
	vecDim   int  // > 0: only a float vector of this dim
}

func gatherFields(s gatherShape) []gatherField {
	dim := func(d int) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.Itoa(d)}}
	}
	if s.vecDim > 0 {
		return []gatherField{{&schemapb.FieldSchema{FieldID: 111, DataType: schemapb.DataType_FloatVector, TypeParams: dim(s.vecDim)}, &arrow.FixedSizeBinaryType{ByteWidth: 4 * s.vecDim}}}
	}
	fields := []gatherField{
		{&schemapb.FieldSchema{FieldID: 101, DataType: schemapb.DataType_Bool}, arrow.FixedWidthTypes.Boolean},
		{&schemapb.FieldSchema{FieldID: 102, DataType: schemapb.DataType_Int8}, arrow.PrimitiveTypes.Int32},
		{&schemapb.FieldSchema{FieldID: 103, DataType: schemapb.DataType_Int16}, arrow.PrimitiveTypes.Int32},
		{&schemapb.FieldSchema{FieldID: 104, DataType: schemapb.DataType_Int32}, arrow.PrimitiveTypes.Int32},
		{&schemapb.FieldSchema{FieldID: 105, DataType: schemapb.DataType_Int64}, arrow.PrimitiveTypes.Int64},
		{&schemapb.FieldSchema{FieldID: 106, DataType: schemapb.DataType_Float}, arrow.PrimitiveTypes.Float32},
		{&schemapb.FieldSchema{FieldID: 107, DataType: schemapb.DataType_Double}, arrow.PrimitiveTypes.Float64},
		{&schemapb.FieldSchema{FieldID: 108, DataType: schemapb.DataType_VarChar}, arrow.BinaryTypes.String},
		{&schemapb.FieldSchema{FieldID: 109, DataType: schemapb.DataType_JSON}, arrow.BinaryTypes.Binary},
		{&schemapb.FieldSchema{FieldID: 110, DataType: schemapb.DataType_Geometry}, arrow.BinaryTypes.Binary},
		{&schemapb.FieldSchema{FieldID: 111, DataType: schemapb.DataType_FloatVector, TypeParams: dim(4)}, &arrow.FixedSizeBinaryType{ByteWidth: 16}},
		{&schemapb.FieldSchema{FieldID: 112, DataType: schemapb.DataType_BinaryVector, TypeParams: dim(16)}, &arrow.FixedSizeBinaryType{ByteWidth: 2}},
		{&schemapb.FieldSchema{FieldID: 113, DataType: schemapb.DataType_Float16Vector, TypeParams: dim(4)}, &arrow.FixedSizeBinaryType{ByteWidth: 8}},
		{&schemapb.FieldSchema{FieldID: 114, DataType: schemapb.DataType_BFloat16Vector, TypeParams: dim(4)}, &arrow.FixedSizeBinaryType{ByteWidth: 8}},
		{&schemapb.FieldSchema{FieldID: 115, DataType: schemapb.DataType_Int8Vector, TypeParams: dim(8)}, &arrow.FixedSizeBinaryType{ByteWidth: 8}},
		{&schemapb.FieldSchema{FieldID: 116, DataType: schemapb.DataType_Timestamptz}, arrow.PrimitiveTypes.Int64},
	}
	if s.nullable {
		for _, f := range fields {
			f.fs.Nullable = true
		}
	}
	return fields
}

// gatherFixture is three workers' batches over the same layout, built as
// the worker builds them: a segment record of the output fields, a
// selection of all its rows in a shuffled order, BuildBatch.
type gatherFixture struct {
	shape  gatherShape
	coll   *schemapb.CollectionSchema
	fields []gatherField
	recs   []arrow.Record // per worker, the batch record
}

func newGatherFixture(t testing.TB, s gatherShape) *gatherFixture {
	r := rand.New(rand.NewSource(int64(s.rows)*7 + 3))
	fx := &gatherFixture{shape: s, fields: gatherFields(s)}
	pkType := schemapb.DataType_Int64
	if s.strPK {
		pkType = schemapb.DataType_VarChar
	}
	fx.coll = &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: pkType, IsPrimaryKey: true}}}
	order := []string{"1"}
	var nullable []string
	afs := make([]arrow.Field, len(fx.fields))
	for i, f := range fx.fields {
		f.fs.Name = fmt.Sprintf("f%d", f.fs.GetFieldID())
		fx.coll.Fields = append(fx.coll.Fields, f.fs)
		id := strconv.FormatInt(f.fs.GetFieldID(), 10)
		order = append(order, id)
		if f.fs.GetNullable() {
			nullable = append(nullable, id)
		}
		afs[i] = arrow.Field{Name: id, Type: f.dt, Nullable: f.fs.GetNullable()}
	}
	md := arrow.NewMetadata([]string{"milvus.field_order", "milvus.valid_data_fields"},
		[]string{strings.Join(order, ","), strings.Join(nullable, ",")})
	schema := arrow.NewSchema(afs, &md)
	for w := 0; w < 3; w++ {
		rows := s.rows + w // workers differ in size
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for row := 0; row < rows; row++ {
			for i, f := range fx.fields {
				fb := b.Field(i)
				if s.nullable && (row+i+w)%3 == 0 {
					fb.AppendNull()
					continue
				}
				switch fb := fb.(type) {
				case *array.BooleanBuilder:
					fb.Append(r.Intn(2) == 0)
				case *array.Int32Builder:
					fb.Append(r.Int31() >> (8 * (1 + f.fs.GetFieldID() - 102) % 32))
				case *array.Int64Builder:
					fb.Append(r.Int63())
				case *array.Float32Builder:
					fb.Append(r.Float32())
				case *array.Float64Builder:
					fb.Append(r.Float64())
				case *array.StringBuilder:
					fb.Append(strings.Repeat("v", r.Intn(30)))
				case *array.BinaryBuilder:
					fb.Append([]byte(fmt.Sprintf(`{"k":%d,"pad":%q}`, r.Intn(100), strings.Repeat("x", r.Intn(30)))))
				case *array.FixedSizeBinaryBuilder:
					v := make([]byte, f.dt.(*arrow.FixedSizeBinaryType).ByteWidth)
					r.Read(v)
					fb.Append(v)
				}
			}
		}
		seg := b.NewRecord()
		b.Release()
		perm := r.Perm(rows)
		pairs := make([][2]int64, rows)
		ts := make([]int64, rows)
		var ids *schemapb.IDs
		if s.strPK {
			pks := make([]string, rows)
			for i := range pks {
				pks[i] = fmt.Sprintf("pk-%d-%d", w, i)
			}
			ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: pks}}}
		} else {
			pks := make([]int64, rows)
			for i := range pks {
				pks[i] = int64(w*100000 + i)
			}
			ids = intIDs(pks...)
		}
		for i, p := range perm {
			pairs[i] = [2]int64{0, int64(p)}
			ts[i] = int64(1000 + i)
		}
		sel := &queryutil.ArrowSelection{Records: []arrow.Record{seg}, Rows: queryutil.RowRefsForTest(pairs)}
		rec, err := BuildBatch(&segcorepb.RetrieveResults{Ids: ids, FieldsData: []*schemapb.FieldData{tsField(ts...)}}, sel)
		seg.Release()
		require.NoError(t, err)
		fx.recs = append(fx.recs, rec)
	}
	return fx
}

func (fx *gatherFixture) release() { releaseAll(fx.recs) }

// encode returns the schema message, then each worker's entry.
func (fx *gatherFixture) encode(t testing.TB, codec Codec) [][]byte {
	var out [][]byte
	for _, rec := range fx.recs {
		s, e, err := EncodeBatch(rec, codec)
		require.NoError(t, err)
		if out == nil {
			out = append(out, s)
		}
		out = append(out, e)
	}
	return out
}

// referenceGather is the previous proxy path: the stock reader's records,
// split into pk, system and user columns, then MaterializeArrowSelection.
func referenceGather(t *testing.T, schemaMsg []byte, entries [][]byte, rows []RowRef, coll *schemapb.CollectionSchema) (*schemapb.IDs, []*schemapb.FieldData) {
	t.Helper()
	recs, err := decodeBody(schemaMsg, entries)
	require.NoError(t, err)
	defer releaseAll(recs)
	s, err := ParseSchema(schemaMsg)
	require.NoError(t, err)
	lead := s.lead()
	userMD := s.arrow.Metadata()
	userSchema := arrow.NewSchema(s.arrow.Fields()[lead:], &userMD)
	users := make([]arrow.Record, len(recs))
	for i, rec := range recs {
		users[i] = array.NewRecord(userSchema, rec.Columns()[lead:], rec.NumRows())
	}
	defer releaseAll(users)

	header := &segcorepb.RetrieveResults{}
	refs := make([][2]int64, len(rows))
	var intPKs []int64
	var strPKs []string
	for i, r := range rows {
		refs[i] = [2]int64{int64(r.Batch), int64(r.Row)}
		switch pk := recs[r.Batch].Column(0).(type) {
		case *array.Int64:
			intPKs = append(intPKs, pk.Value(int(r.Row)))
		case *array.String:
			strPKs = append(strPKs, strings.Clone(pk.Value(int(r.Row))))
		}
	}
	if s.StringPK() {
		header.Ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: strPKs}}}
	} else {
		header.Ids = intIDs(intPKs...)
	}
	for j, id := range s.sysIDs {
		data := make([]int64, len(rows))
		for i, r := range rows {
			data[i] = recs[r.Batch].Column(1 + j).(*array.Int64).Value(int(r.Row))
		}
		header.FieldsData = append(header.FieldsData, &schemapb.FieldData{
			FieldId: id, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
			}},
		})
	}
	sel := &queryutil.ArrowSelection{Records: users, Rows: queryutil.RowRefsForTest(refs)}
	require.NoError(t, queryutil.MaterializeArrowSelection(header, sel, coll))
	return header.GetIds(), header.GetFieldsData()
}

// The gather over entries in pieces returns exactly what the previous path
// -- the stock reader's records and MaterializeArrowSelection -- returns,
// for every carried type, int64 and string pks, nullable fields with nulls,
// rows from several batches in any order, repeated, and both codecs.
func TestGather_MatchesMaterializeArrowSelection(t *testing.T) {
	shapes := []gatherShape{
		{rows: 40},
		{rows: 40, strPK: true},
		{rows: 40, nullable: true},
		{rows: 40, nullable: true, strPK: true},
		{rows: 1, nullable: true},
		{rows: 300, vecDim: 768},
	}
	for _, shape := range shapes {
		fx := newGatherFixture(t, shape)
		for _, codec := range []Codec{CodecNone, CodecZstd} {
			enc := fx.encode(t, codec)
			schemaMsg, entries := enc[0], enc[1:]
			r := rand.New(rand.NewSource(int64(shape.rows)))
			var all, picked, nullsOnly []RowRef
			for b, rec := range fx.recs {
				for row := 0; row < int(rec.NumRows()); row++ {
					ref := RowRef{int32(b), int32(row)}
					all = append(all, ref)
					if r.Intn(3) == 0 {
						picked = append(picked, ref, ref) // repeated
					}
					// Null in the first user column (see newGatherFixture).
					if shape.nullable && (row+0+b)%3 == 0 {
						nullsOnly = append(nullsOnly, ref)
					}
				}
			}
			r.Shuffle(len(picked), func(i, j int) { picked[i], picked[j] = picked[j], picked[i] })
			selections := map[string][]RowRef{"all": all, "picked": picked, "one": all[len(all)-1:]}
			if len(nullsOnly) > 0 {
				selections["first column null"] = nullsOnly
			}
			for selName, rows := range selections {
				if len(rows) == 0 {
					continue
				}
				wantIDs, wantFields := referenceGather(t, schemaMsg, entries, rows, fx.coll)
				for split := range splits(entries[0], 1) {
					if split == "1B" && len(entries[0]) > 1<<16 {
						continue
					}
					name := fmt.Sprintf("rows=%d/strPK=%v/nullable=%v/vec=%d/%s/%s/%s", shape.rows, shape.strPK, shape.nullable, shape.vecDim, codec, selName, split)
					t.Run(name, func(t *testing.T) {
						s, err := ParseSchema(schemaMsg)
						require.NoError(t, err)
						batches := make([]*Batch, len(entries))
						for i, e := range entries {
							batches[i], err = s.ReadEntry(splits(e, int64(i))[split], i, 0)
							require.NoError(t, err)
						}
						ids, fields, err := s.Gather(batches, rows, fieldsOf(t, fx.coll))
						require.NoError(t, err)
						require.True(t, proto.Equal(wantIDs, ids), "ids")
						require.Len(t, fields, len(wantFields))
						for i := range wantFields {
							if !proto.Equal(wantFields[i], fields[i]) {
								w, g := proto.Clone(wantFields[i]).(*schemapb.FieldData), proto.Clone(fields[i]).(*schemapb.FieldData)
								require.Equal(t, typeutil.GetFieldDataValidData(w), typeutil.GetFieldDataValidData(g), "field %d valid_data", w.GetFieldId())
								typeutil.SetFieldDataValidData(w, nil)
								typeutil.SetFieldDataValidData(g, nil)
								require.Equal(t, w.String(), g.String(), "field %d", w.GetFieldId())
							}
						}
					})
				}
			}
		}
		fx.release()
	}
}

// Nothing the gather returns points into the entries: overwriting them
// afterwards changes no value.
func TestGather_CopiesOut(t *testing.T) {
	fx := newGatherFixture(t, gatherShape{rows: 30, strPK: true, nullable: true})
	defer fx.release()
	enc := fx.encode(t, CodecNone)
	s, err := ParseSchema(enc[0])
	require.NoError(t, err)
	var parts [][][]byte
	var batches []*Batch
	var rows []RowRef
	for i, e := range enc[1:] {
		p := everyN(e, 64)
		parts = append(parts, p)
		b, err := s.ReadEntry(p, i, 0)
		require.NoError(t, err)
		batches = append(batches, b)
		for row := 0; row < b.Rows; row++ {
			rows = append(rows, RowRef{int32(i), int32(row)})
		}
	}
	ids, fields, err := s.Gather(batches, rows, fieldsOf(t, fx.coll))
	require.NoError(t, err)
	want := proto.Clone(&schemapb.SearchResultData{Ids: ids, FieldsData: fields})
	for _, p := range parts {
		for _, piece := range p {
			for i := range piece {
				piece[i] = 0xAA
			}
		}
	}
	require.True(t, proto.Equal(want, &schemapb.SearchResultData{Ids: ids, FieldsData: fields}))
}

func TestGather_Errors(t *testing.T) {
	fx := newGatherFixture(t, gatherShape{rows: 3})
	defer fx.release()
	enc := fx.encode(t, CodecNone)
	s, batches := readBatches(t, enc[0], enc[1:]...)
	_, _, err := s.Gather(batches, []RowRef{{0, 99}}, fieldsOf(t, fx.coll))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	_, _, err = s.Gather(batches, []RowRef{{7, 0}}, fieldsOf(t, fx.coll))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	// A field the collection schema does not know.
	short := proto.Clone(fx.coll).(*schemapb.CollectionSchema)
	short.Fields = short.Fields[:2]
	_, _, err = s.Gather(batches, []RowRef{{0, 0}}, fieldsOf(t, short))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	// A field whose schema type disagrees with its Arrow type.
	wrong := proto.Clone(fx.coll).(*schemapb.CollectionSchema)
	wrong.Fields[1].DataType = schemapb.DataType_Double
	_, _, err = s.Gather(batches, []RowRef{{0, 0}}, fieldsOf(t, wrong))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	// A vector whose schema dim disagrees with its byte width.
	badDim := proto.Clone(fx.coll).(*schemapb.CollectionSchema)
	for _, f := range badDim.Fields {
		if f.GetDataType() == schemapb.DataType_FloatVector {
			f.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: "5"}}
		}
	}
	_, _, err = s.Gather(batches, []RowRef{{0, 0}}, fieldsOf(t, badDim))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

// A vector's dim comes from its schema, or, when the schema has none, from
// the byte width; either way the width must hold dim whole elements.
func TestVectorDim(t *testing.T) {
	dim := func(d string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{{Key: "dim", Value: d}}
	}
	cases := []struct {
		name    string
		dt      schemapb.DataType
		params  []*commonpb.KeyValuePair
		width   int
		want    int64
		wantErr bool
	}{
		{name: "float schema", dt: schemapb.DataType_FloatVector, params: dim("4"), width: 16, want: 4},
		{name: "float from width", dt: schemapb.DataType_FloatVector, width: 16, want: 4},
		{name: "float width not whole", dt: schemapb.DataType_FloatVector, width: 6, wantErr: true},
		{name: "float zero dim", dt: schemapb.DataType_FloatVector, width: 2, wantErr: true},
		{name: "float schema disagrees", dt: schemapb.DataType_FloatVector, params: dim("5"), width: 16, wantErr: true},
		{name: "binary from width", dt: schemapb.DataType_BinaryVector, width: 2, want: 16},
		{name: "float16 from width", dt: schemapb.DataType_Float16Vector, width: 8, want: 4},
		{name: "float16 width not whole", dt: schemapb.DataType_Float16Vector, width: 7, wantErr: true},
		{name: "bfloat16 from width", dt: schemapb.DataType_BFloat16Vector, width: 8, want: 4},
		{name: "int8 schema", dt: schemapb.DataType_Int8Vector, params: dim("8"), width: 8, want: 8},
		{name: "int8 from width", dt: schemapb.DataType_Int8Vector, width: 8, want: 8},
		{name: "not a dense vector", dt: schemapb.DataType_Int64, width: 8, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := vectorDim(&schemapb.FieldSchema{FieldID: 111, DataType: c.dt, TypeParams: c.params}, c.width)
			if c.wantErr {
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}
