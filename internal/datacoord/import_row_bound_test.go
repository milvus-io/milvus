package datacoord

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
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
	got, err := minRowTextBytes(schemaVec(768, 1))
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
	m, err := minRowTextBytes(schemaBM25AutoID())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), m)
}

func Test_computeFileRowUpperBound(t *testing.T) {
	ctx := context.Background()

	t.Run("text json", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Size(mock.Anything, "a.json").Return(int64(768*100), nil)
		file := &internalpb.ImportFile{Paths: []string{"a.json"}}
		// minRowTextBytes(schemaVec(768,0)) == 768; 768*100 / 768 + 1 == 101.
		bound, exact, err := computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file)
		assert.NoError(t, err)
		assert.Equal(t, int64(101), bound)
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
	cm.EXPECT().Size(mock.Anything, "a.json").Return(int64(768*10), nil) // bound 10 + 1 = 11
	cm.EXPECT().Size(mock.Anything, "b.json").Return(int64(768*20), nil) // bound 20 + 1 = 21

	files := []*internalpb.ImportFile{
		{Paths: []string{"a.json"}},
		{Paths: []string{"b.json"}},
	}
	// fake allocator hands out [1000, 1000+n)
	alloc := func(n int64) (int64, int64, error) { return 1000, 1000 + n, nil }

	err := assignPKRangesToFiles(context.TODO(), cm, schema, files, alloc, 1 /*clusterID*/)
	assert.NoError(t, err)
	// each file's range width equals its own bound
	assert.Equal(t, int64(11), files[0].GetPkIdEnd()-files[0].GetPkIdBegin())
	assert.Equal(t, int64(21), files[1].GetPkIdEnd()-files[1].GetPkIdBegin())
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
	got, err := minRowTextBytes(schema)
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
		sizeReservations(bounds, []bool{true, false})
		assert.Equal(t, []int64{1000, 100}, bounds)
	})

	t.Run("a zero bound is floored to one id", func(t *testing.T) {
		withExpansionFactor(t, "10")
		// An empty range reads as "no range" on the datanode and silently falls back
		// to the local allocator, which is the divergence this mechanism prevents.
		bounds := []int64{0, 0}
		sizeReservations(bounds, []bool{true, false})
		assert.Equal(t, []int64{1, 1}, bounds)
	})

	t.Run("bounds are never shrunk to fit an allocation ceiling", func(t *testing.T) {
		withExpansionFactor(t, "1")
		// Scaling an estimate down would break the upper-bound guarantee that makes
		// it usable at all; reserveRanges splits the allocation instead.
		bounds := []int64{3 * math.MaxUint32, math.MaxUint32}
		sizeReservations(bounds, []bool{false, false})
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
				_, err := numpyNumRows(context.Background(), cm, []string{"bad.npy"})
				assert.Error(t, err)
			})
		})
	}
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
