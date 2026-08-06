package datacoord

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/sbinet/npyio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func itoa(i int) string { return strconv.Itoa(i) }

func schemaVec(dim int, extraScalars int) *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		{
			FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: itoa(dim)}},
		},
	}
	for i := 0; i < extraScalars; i++ {
		fields = append(fields, &schemapb.FieldSchema{FieldID: int64(200 + i), Name: "s" + itoa(i), DataType: schemapb.DataType_Int64})
	}
	return &schemapb.CollectionSchema{Fields: fields}
}

func Test_minRowTextBytes(t *testing.T) {
	// dim=768 float vector: >= 768 numeric chars (conservative lower bound); scalars add >= 1 each.
	got, err := minRowTextBytes(schemaVec(768, 1), importutilv2.CSV)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, got, int64(768))
	assert.LessOrEqual(t, got, int64(768+16)) // still a tight-ish floor
}

func schemaBM25AutoID() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "512"}},
			},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}},
		},
	}
}

func Test_rowByteHelpers_skipFunctionOutput(t *testing.T) {
	// sparse (102) is a function output and the autoID pk (100) is generated, so
	// only the VarChar text field remains -- and VarChar may be empty, so the floor
	// falls through to 1 rather than erroring on the sparse field.
	m, err := minRowTextBytes(schemaBM25AutoID(), importutilv2.CSV)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), m)
}

func Test_computeFileRowUpperBound(t *testing.T) {
	ctx := context.Background()

	t.Run("text json", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Size(mock.Anything, "a.json").Return(int64(768*100), nil)
		file := &internalpb.ImportFile{Paths: []string{"a.json"}}
		// A JSON row of this schema floors at 776: 768 for the dim-768 vector plus
		// 8 for the braces and "vec": around it. 768*100 / 776 + 1 == 99.
		bound, exact, err := computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file)
		assert.NoError(t, err)
		assert.Equal(t, int64(99), bound)
		assert.False(t, exact, "a byte-derived text bound is an estimate")
	})

	t.Run("numpy mocked", func(t *testing.T) {
		// The row count comes from the .npy header shape, so it is exact and does
		// not depend on file size or on any schema-derived per-row width.
		mk := mockey.Mock(numpyNumRows).Return(int64(50), nil).Build()
		defer mk.UnPatch()
		cm := mocks.NewChunkManager(t)
		file := &internalpb.ImportFile{Paths: []string{"a.npy"}}
		bound, exact, err := computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file)
		assert.NoError(t, err)
		assert.Equal(t, int64(50), bound)
		assert.True(t, exact)
	})

	t.Run("parquet mocked", func(t *testing.T) {
		mk := mockey.Mock(parquetNumRows).Return(int64(123), nil).Build()
		defer mk.UnPatch()
		cm := mocks.NewChunkManager(t)
		file := &internalpb.ImportFile{Paths: []string{"a.parquet"}}
		bound, exact, err := computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file)
		assert.NoError(t, err)
		assert.Equal(t, int64(123), bound)
		assert.True(t, exact)
	})
}

func Test_assignPKRangesToFiles(t *testing.T) {
	schema := schemaVec(768, 0) // minRowTextBytes == 768
	cm := mocks.NewChunkManager(t)
	// A JSON row floors at 776 for this schema: 768 for the vector, 8 for the
	// braces and "vec": around it.
	cm.EXPECT().Size(mock.Anything, "a.json").Return(int64(768*10), nil) // 7680/776 + 1 = 10
	cm.EXPECT().Size(mock.Anything, "b.json").Return(int64(768*20), nil) // 15360/776 + 1 = 20

	files := []*internalpb.ImportFile{
		{Paths: []string{"a.json"}},
		{Paths: []string{"b.json"}},
	}
	// fake allocator hands out [1000, 1000+n)
	alloc := func(n int64) (int64, int64, error) { return 1000, 1000 + n, nil }

	err := assignPKRangesToFiles(context.TODO(), cm, schema, files, alloc, 1 /*clusterID*/)
	assert.NoError(t, err)
	// each file's range width equals its own bound
	assert.Equal(t, int64(10), files[0].GetPkIdEnd()-files[0].GetPkIdBegin())
	assert.Equal(t, int64(20), files[1].GetPkIdEnd()-files[1].GetPkIdBegin())
	// files are contiguous, second begins where first ends
	assert.Equal(t, files[0].GetPkIdEnd(), files[1].GetPkIdBegin())
	// cluster bits are applied to the high bits
	assert.NotZero(t, files[0].GetPkIdBegin())
}

func Test_assignPKRangesToFiles_zeroTotal(t *testing.T) {
	// no files -> nothing to allocate, no allocator call
	err := assignPKRangesToFiles(context.TODO(), mocks.NewChunkManager(t), schemaVec(8, 0),
		nil, func(int64) (int64, int64, error) { t.Fatal("allocN must not be called"); return 0, 0, nil }, 1)
	assert.NoError(t, err)
}

func Test_minRowTextBytes_skipsNullableAndDefault(t *testing.T) {
	// Nullable / defaulted scalars may be omitted from a JSON row (0 bytes), so they
	// must NOT count toward the per-row lower bound. Only the required dim-2 vector does.
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
			{FieldID: 102, Name: "n1", DataType: schemapb.DataType_Int64, Nullable: true},
			{FieldID: 103, Name: "n2", DataType: schemapb.DataType_Int64, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 7}}},
		},
	}
	got, err := minRowTextBytes(schema, importutilv2.CSV)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), got) // was 4 before the fix (nullable+default counted)
}

func withExpansionFactor(t *testing.T, factor string) {
	paramtable.Init()
	key := paramtable.Get().DataCoordCfg.ImportPreAllocIDExpansionFactor.Key
	paramtable.Get().Save(key, factor)
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

func Test_sizeReservations(t *testing.T) {
	t.Run("exact gets the expansion factor, estimate does not", func(t *testing.T) {
		withExpansionFactor(t, "10")
		bounds := []int64{100, 100}
		require.NoError(t, sizeReservations(bounds, []bool{true, false}))
		assert.Equal(t, []int64{1000, 100}, bounds)
	})

	t.Run("a zero bound is floored to one id", func(t *testing.T) {
		withExpansionFactor(t, "10")
		// An empty range reads as "no range" on the datanode and silently falls back
		// to the local allocator, which is the divergence this mechanism prevents.
		bounds := []int64{0, 0}
		require.NoError(t, sizeReservations(bounds, []bool{true, false}))
		assert.Equal(t, []int64{1, 1}, bounds)
	})

	t.Run("bounds are never shrunk to fit an allocation ceiling", func(t *testing.T) {
		withExpansionFactor(t, "1")
		// Scaling an estimate down would break the upper-bound guarantee that makes
		// it usable at all; reserveRanges splits the allocation instead.
		bounds := []int64{3 * math.MaxUint32, math.MaxUint32}
		require.NoError(t, sizeReservations(bounds, []bool{false, false}))
		assert.Equal(t, []int64{3 * math.MaxUint32, math.MaxUint32}, bounds)
	})
}

func Test_reserveRanges(t *testing.T) {
	// Leave a gap between batches so batch boundaries are observable: a real
	// allocator gives no guarantee that consecutive AllocN calls are adjacent.
	type block struct{ begin, end int64 }
	newAlloc := func(calls *[]int64, blocks *[]block) func(int64) (int64, int64, error) {
		next := int64(1000)
		return func(n int64) (int64, int64, error) {
			*calls = append(*calls, n)
			begin := next
			next += n + 1_000_000
			if blocks != nil {
				*blocks = append(*blocks, block{begin, begin + n})
			}
			return begin, begin + n, nil
		}
	}
	within := func(f *internalpb.ImportFile, blocks []block) bool {
		for _, b := range blocks {
			if f.GetPkIdBegin() >= b.begin && f.GetPkIdEnd() <= b.end {
				return true
			}
		}
		return false
	}
	widths := func(files []*internalpb.ImportFile) []int64 {
		out := make([]int64, len(files))
		for i, f := range files {
			out[i] = f.GetPkIdEnd() - f.GetPkIdBegin()
		}
		return out
	}

	t.Run("one batch when the total fits", func(t *testing.T) {
		var calls []int64
		files := []*internalpb.ImportFile{{}, {}, {}}
		require.NoError(t, reserveRanges([]int64{10, 20, 30}, files, newAlloc(&calls, nil), 0))
		assert.Equal(t, []int64{60}, calls)
		assert.Equal(t, []int64{10, 20, 30}, widths(files))
		assert.Equal(t, files[0].GetPkIdEnd(), files[1].GetPkIdBegin())
		assert.Equal(t, files[1].GetPkIdEnd(), files[2].GetPkIdBegin())
	})

	t.Run("a total above the ceiling is split, and every file keeps its full width", func(t *testing.T) {
		var calls []int64
		half := maxIDsPerAllocBatch / 2
		files := []*internalpb.ImportFile{{}, {}, {}}
		// half+half fills one batch exactly; the third opens a new one.
		require.NoError(t, reserveRanges([]int64{half, half, half}, files, newAlloc(&calls, nil), 0))
		assert.Equal(t, []int64{2 * half, half}, calls)
		assert.Equal(t, []int64{half, half, half}, widths(files),
			"no reservation is shrunk to fit the ceiling")
	})

	t.Run("a range never straddles two batches", func(t *testing.T) {
		var calls []int64
		var blocks []block
		b := maxIDsPerAllocBatch / 3 * 2
		files := []*internalpb.ImportFile{{}, {}, {}}
		require.NoError(t, reserveRanges([]int64{b, b, b}, files, newAlloc(&calls, &blocks), 0))
		assert.Equal(t, []int64{b, b, b}, calls, "two of these never fit together")
		assert.Equal(t, []int64{b, b, b}, widths(files))
		for i, f := range files {
			assert.True(t, within(f, blocks), "file %d must sit inside a single batch", i)
		}
	})

	t.Run("a single file wider than one batch is a clean error", func(t *testing.T) {
		var calls []int64
		files := []*internalpb.ImportFile{{}}
		err := reserveRanges([]int64{maxIDsPerAllocBatch + 1}, files, newAlloc(&calls, nil), 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "more than one allocation batch holds")
		assert.Empty(t, calls, "nothing is allocated once the request is rejected")
	})

	t.Run("all-zero bounds allocate nothing and terminate", func(t *testing.T) {
		files := []*internalpb.ImportFile{{}, {}}
		require.NoError(t, reserveRanges([]int64{0, 0}, files,
			func(int64) (int64, int64, error) { t.Fatal("allocN must not be called"); return 0, 0, nil }, 0))
		assert.Equal(t, []int64{0, 0}, widths(files))
	})
}

// malformedNpy builds a .npy prefix whose declared header length is hdrLen but
// whose header bytes contain no '\n'. npyio's readHeader does not check r.err
// between reading those bytes and slicing on the last newline, so an unguarded
// decode panics with "slice bounds out of range [:-1]".
func malformedNpy(major byte, hdrLen uint32, body []byte) []byte {
	buf := []byte{'\x93', 'N', 'U', 'M', 'P', 'Y', major, 0}
	switch major {
	case 1:
		buf = binary.LittleEndian.AppendUint16(buf, uint16(hdrLen))
	default:
		buf = binary.LittleEndian.AppendUint32(buf, hdrLen)
	}
	return append(buf, body...)
}

func cmServing(t *testing.T, path string, data []byte) *mocks.ChunkManager {
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Size(mock.Anything, path).Return(int64(len(data)), nil).Maybe()
	cm.EXPECT().ReadAt(mock.Anything, path, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, off int64, length int64) ([]byte, error) {
			if off >= int64(len(data)) {
				return nil, io.EOF
			}
			end := off + length
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			return data[off:end], nil
		}).Maybe()
	return cm
}

// numpySchema names one Int64 field per .npy basename under test, so every path
// survives numpy.SourcePaths and reaches the header decode.
func numpySchema(names ...string) *schemapb.CollectionSchema {
	fields := make([]*schemapb.FieldSchema, 0, len(names))
	for i, name := range names {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: int64(100 + i), Name: name, DataType: schemapb.DataType_Int64,
		})
	}
	return &schemapb.CollectionSchema{Fields: fields}
}

func Test_numpyNumRows_malformedHeader(t *testing.T) {
	cases := map[string][]byte{
		// The review's repro: a 10-byte object declaring a zero-length header.
		"zero length header": malformedNpy(1, 0, nil),
		// Declared length is satisfied but carries no newline to slice on.
		"no newline in header": malformedNpy(1, 8, []byte("{'descr'")),
		// v2 takes the header length from a uint32, so the declared size is bounded
		// only by 4 GiB -- reading it before validating is an allocation DoS on its
		// own, independent of the slice panic.
		"huge declared header": malformedNpy(2, math.MaxUint32, []byte("x")),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			cm := cmServing(t, "bad.npy", data)
			assert.NotPanics(t, func() {
				_, err := numpyNumRows(context.Background(), cm, numpySchema("bad"), []string{"bad.npy"})
				assert.Error(t, err)
			})
		})
	}
}

// A path whose basename names no schema field is redundant: numpy.CreateReaders
// skips it, so sizing must skip it too. Sizing runs before the broadcast, so a
// redundant .npy that is broken (or missing from the bucket) used to fail the
// whole request at submit even though the same file set imported fine.
//
// The ChunkManager is given no expectation for the redundant path at all, so the
// test fails if sizing so much as calls Size on it -- proving the path is never
// opened, not merely that its decode error is swallowed.
func Test_numpyNumRows_skipsRedundantPaths(t *testing.T) {
	buf := new(bytes.Buffer)
	require.NoError(t, npyio.Write(buf, []int64{1, 2, 3}))

	cm := cmServing(t, "vec.npy", buf.Bytes())
	rows, err := numpyNumRows(context.Background(), cm, numpySchema("vec"),
		[]string{"vec.npy", "README.npy"})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), rows)
}

// Every path redundant means the file carries no readable column. Sizing reports
// 0 instead of an error: rejecting here would again be stricter than the reader,
// which is where "no file for field" is raised, as it was before sizing existed.
func Test_numpyNumRows_allPathsRedundant(t *testing.T) {
	cm := mocks.NewChunkManager(t)
	rows, err := numpyNumRows(context.Background(), cm, numpySchema("vec"),
		[]string{"README.npy", "LICENSE.npy"})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

// A decoder panic inside the sizing pool must fail the import rather than the
// process: conc.Submit stores the panic in the future and then re-throws onto an
// ants worker goroutine, which the caller's goroutine cannot recover. Concealing
// the panic keeps the stored error reachable through AwaitAll.
func Test_computeFileRowUpperBounds_decoderPanicBecomesError(t *testing.T) {
	mk := mockey.Mock(computeFileRowUpperBound).To(
		func(context.Context, storage.ChunkManager, *schemapb.CollectionSchema, *internalpb.ImportFile) (int64, bool, error) {
			panic("decoder blew up")
		}).Build()
	defer mk.UnPatch()

	files := []*internalpb.ImportFile{{Paths: []string{"a.npy"}}}
	bounds, exacts, err := computeFileRowUpperBounds(
		context.Background(), mocks.NewChunkManager(t), schemaVec(8, 0), files)
	require.Error(t, err, "a panicked sizing task must surface as an error, not be swallowed")
	assert.Contains(t, err.Error(), "decoder blew up")
	assert.Nil(t, bounds)
	assert.Nil(t, exacts)
}

// A hostile parquet declares a footer as large as the object itself. Arrow's only
// check is that the declared length fits inside the file, after which it allocates
// it, and sizingReaderAt buffers the ranged GET on top -- so sizing must reject the
// length rather than pay for it. Measured by allocation, since the defect is memory
// and not the (already failing) parse.
func Test_parquetNumRows_hostileFooterLength(t *testing.T) {
	const objSize = int64(200 << 20)
	declared := uint32(objSize - 8)

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Size(mock.Anything, "bad.parquet").Return(objSize, nil).Maybe()
	cm.EXPECT().ReadAt(mock.Anything, "bad.parquet", mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, off int64, length int64) ([]byte, error) {
			b := make([]byte, length)
			if off+length == objSize && length >= 8 {
				binary.LittleEndian.PutUint32(b[length-8:], declared)
				copy(b[length-4:], parquetMagic)
			}
			return b, nil
		}).Maybe()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := parquetNumRows(context.Background(), cm, "bad.parquet")
	runtime.ReadMemStats(&after)

	require.Error(t, err)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(maxParquetFooterLen),
		"sizing must reject the declared footer length instead of allocating it")
}

func Test_validateParquetFooter(t *testing.T) {
	tail := func(footerLen uint32, magic []byte) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint32(b[:4], footerLen)
		copy(b[4:], magic)
		return b
	}
	readerAt := func(tail []byte, size int64) io.ReaderAt {
		return readerAtFunc(func(p []byte, off int64) (int, error) {
			if off != size-8 {
				return 0, io.EOF
			}
			return copy(p, tail), nil
		})
	}

	t.Run("accepts a plausible footer", func(t *testing.T) {
		assert.NoError(t, validateParquetFooter(readerAt(tail(4096, parquetMagic), 1<<20), 1<<20, "a.parquet"))
	})
	t.Run("accepts an encrypted footer", func(t *testing.T) {
		assert.NoError(t, validateParquetFooter(readerAt(tail(4096, parquetMagicEncrypted), 1<<20), 1<<20, "a.parquet"))
	})
	t.Run("rejects a footer over the cap", func(t *testing.T) {
		err := validateParquetFooter(readerAt(tail(maxParquetFooterLen+1, parquetMagic), 1<<30), 1<<30, "a.parquet")
		assert.ErrorIs(t, err, merr.ErrImportFailed)
	})
	t.Run("rejects a zero-length footer", func(t *testing.T) {
		assert.Error(t, validateParquetFooter(readerAt(tail(0, parquetMagic), 1<<20), 1<<20, "a.parquet"))
	})
	t.Run("rejects a foreign magic", func(t *testing.T) {
		assert.Error(t, validateParquetFooter(readerAt(tail(4096, []byte("XXXX")), 1<<20), 1<<20, "a.parquet"))
	})
	t.Run("rejects a file too small to hold a footer", func(t *testing.T) {
		assert.Error(t, validateParquetFooter(readerAt(nil, 4), 4, "a.parquet"))
	})
}

type readerAtFunc func(p []byte, off int64) (int, error)

func (f readerAtFunc) ReadAt(p []byte, off int64) (int, error) { return f(p, off) }

// A JSON row must spell out each field name it carries, so the per-row floor for a
// text format is not the same in both formats. Without that, an all-VarChar schema
// floors at one byte per row and a legal multi-GB .json is estimated at one primary
// key per byte -- more than a single allocation batch holds.
func Test_minRowTextBytes_jsonPaysForFieldNames(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}},
			},
		},
	}

	// {"text":} -- braces plus quotes and colon around the one required name. The
	// VarChar value itself may be empty, so it still adds nothing.
	json, err := minRowTextBytes(schema, importutilv2.JSON)
	assert.NoError(t, err)
	assert.Equal(t, int64(2+len("text")+3), json)

	jsonl, err := minRowTextBytes(schema, importutilv2.JSONLines)
	assert.NoError(t, err)
	assert.Equal(t, json, jsonl)

	// CSV keeps its names in the header, so a single-column row really can be one
	// byte. This gap is not closable and is why the reserveRanges error explains
	// itself rather than pretending the count is a real row count.
	csv, err := minRowTextBytes(schema, importutilv2.CSV)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), csv)
}

// A 5 GiB all-VarChar .json is well under maxImportFileSizeInGB (16) and used to be
// rejected outright at broadcast: floored at one byte per row, its estimate crossed
// the per-file allocation ceiling.
func Test_assignPKRangesToFiles_largeVarcharJSONIsAccepted(t *testing.T) {
	const fileSize = int64(5) << 30
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}},
			},
		},
	}
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Size(mock.Anything, "big.json").Return(fileSize, nil)

	files := []*internalpb.ImportFile{{Paths: []string{"big.json"}}}
	var asked int64
	alloc := func(n int64) (int64, int64, error) { asked = n; return 1000, 1000 + n, nil }

	err := assignPKRangesToFiles(context.TODO(), cm, schema, files, alloc, 1)
	require.NoError(t, err)
	assert.LessOrEqual(t, asked, maxIDsPerAllocBatch, "the reservation must fit one allocation batch")
	assert.Positive(t, files[0].GetPkIdEnd()-files[0].GetPkIdBegin())
}

// An exact row count above one allocation batch cannot be reserved contiguously.
// Clamping it -- which is what the code did -- hands back fewer ids than the file
// has rows, and reserveRanges cannot notice because it only sees the clamped
// value. numpyNumRows returns shape[0] unclamped, so the count is reachable.
func Test_sizeReservations_rejectsExactCountOverOneBatch(t *testing.T) {
	withExpansionFactor(t, "2")
	bounds := []int64{maxIDsPerAllocBatch + 1}
	err := sizeReservations(bounds, []bool{true})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "more than one allocation batch can reserve")

	// An estimate is not an exact count and keeps its own path: reserveRanges
	// still reports it, with the wording that explains the estimate.
	bounds = []int64{maxIDsPerAllocBatch + 1}
	assert.NoError(t, sizeReservations(bounds, []bool{false}))
}

func Test_minRowTextBytes_skipsLegacyDynamicField(t *testing.T) {
	// A collection created before $meta gained Nullable/DefaultValue keeps a
	// non-nullable, no-default dynamic field in its persisted schema, and no
	// migration ever rewrote it. The JSON reader still never requires $meta
	// (json/row_parser.go:80-83 keeps it out of name2FieldID), so counting it
	// would overstate the per-row floor and under-reserve the PK range.
	legacy := &schemapb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}

	got, err := minRowTextBytes(legacy, importutilv2.JSONLines)
	assert.NoError(t, err)
	// The floor for `{"text":""}` is `"text":` (7) plus the braces (2). $meta adds
	// nothing: no name bytes, and no separator, since it is not a present field.
	assert.Equal(t, int64(9), got) // was 18 before the fix

	// A row of exactly that shape must not size below the floor.
	assert.LessOrEqual(t, got, int64(len(`{"text":""}`)))

	// The modern schema, where $meta is nullable with a default, is unchanged.
	modern := &schemapb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true,
				Nullable:     true,
				DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_BytesData{BytesData: []byte("{}")}},
			},
		},
	}
	modernGot, err := minRowTextBytes(modern, importutilv2.JSONLines)
	assert.NoError(t, err)
	assert.Equal(t, got, modernGot, "legacy and modern $meta must size identically")
}

func Test_parquetNumRows_boundsConcurrentFooterParses(t *testing.T) {
	// maxParquetFooterLen bounds the bytes read, not what Arrow's thrift decoder
	// allocates from them, so the number of decodes in flight is what caps the
	// coordinator's exposure. The gate wraps only the decode -- the ranged reads
	// before it are ordinary storage traffic -- so concurrency is measured on the
	// footer read the decoder issues, not on the 8-byte tail read that
	// validateParquetFooter does ahead of the gate.
	const objSize = int64(1 << 20)
	const declaredFooterLen = uint32(4096)
	var inFlight, peak, tailReads atomic.Int32

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Size(mock.Anything, mock.Anything).Return(objSize, nil).Maybe()
	cm.EXPECT().ReadAt(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, off int64, length int64) ([]byte, error) {
			b := make([]byte, length)
			if off+length == objSize && length == 8 {
				// The tail probe: hand back a well-formed footer descriptor so the
				// call proceeds past validateParquetFooter and into the decode.
				tailReads.Add(1)
				binary.LittleEndian.PutUint32(b[:4], declaredFooterLen)
				copy(b[4:], parquetMagic)
				return b, nil
			}
			cur := inFlight.Add(1)
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			inFlight.Add(-1)
			// Garbage where a footer should be: the decode fails, inside the gate.
			return b, nil
		}).Maybe()

	var wg sync.WaitGroup
	for i := 0; i < 4*maxConcurrentParquetFooterParses; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := parquetNumRows(context.Background(), cm, "f.parquet")
			assert.Error(t, err)
		}()
	}
	wg.Wait()

	assert.Positive(t, tailReads.Load(), "the probe must have passed footer validation")
	assert.LessOrEqual(t, peak.Load(), int32(maxConcurrentParquetFooterParses),
		"concurrent footer decodes must stay within the process-wide cap")
	assert.Greater(t, peak.Load(), int32(1), "the probe must actually have overlapped")
}

func Test_minRowTextBytes_singleColumnCSVProvesNothing(t *testing.T) {
	// CSV keeps field names in the header, so a single VarChar column charges no
	// name bytes, no separator (one field) and no braces. Nothing is provable, the
	// floor clamps to 1, and the row bound therefore tracks the file size.
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}},
			},
		},
	}
	got, err := minRowTextBytes(schema, importutilv2.CSV)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), got)
}

func Test_assignPKRangesToFiles_singleColumnCSVAboveOneBatchIsRefused(t *testing.T) {
	// Behavior change introduced by this PR, pinned deliberately: a single-column
	// all-VarChar CSV larger than maxIDsPerAllocBatch is refused at broadcast,
	// because its per-row floor proves nothing and one file's range may not
	// straddle two allocation batches. See the release note.
	//
	// The tightening suggested in review -- n*minRow + (n-1) <= size, giving
	// (size+1)/(minRow+1) -- is NOT applied here: it assumes each row really
	// occupies minRow bytes, which is false exactly when the floor was clamped.
	// A single-column CSV of empty values is one newline per row, so n rows
	// occupy n bytes and (n+1)/2 would under-count and fail a legal import.
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}},
			},
		},
	}
	alloc := func(n int64) (int64, int64, error) { return 1000, 1000 + n, nil }

	t.Run("just below one batch is accepted", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Size(mock.Anything, "ok.csv").Return(maxIDsPerAllocBatch-2, nil)
		files := []*internalpb.ImportFile{{Paths: []string{"ok.csv"}}}
		require.NoError(t, assignPKRangesToFiles(context.TODO(), cm, schema, files, alloc, 1))
		assert.Positive(t, files[0].GetPkIdEnd()-files[0].GetPkIdBegin())
	})

	t.Run("above one batch is refused", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Size(mock.Anything, "big.csv").Return(int64(5)<<30, nil)
		files := []*internalpb.ImportFile{{Paths: []string{"big.csv"}}}
		err := assignPKRangesToFiles(context.TODO(), cm, schema, files, alloc, 1)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}
