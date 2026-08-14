package datacoord

import (
	"context"
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
	"github.com/milvus-io/milvus/internal/util/importutilv2"
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

// A decoder panic inside the sizing pool must fail the import rather than the
// process: conc.Submit stores the panic in the future and then re-throws onto an
// ants worker goroutine, which the caller's goroutine cannot recover. Concealing
// the panic keeps the stored error reachable through AwaitAll.
func Test_computeFileRowUpperBounds_decoderPanicBecomesError(t *testing.T) {
	mk := mockey.Mock(importutilv2.RowCountUpperBound).To(
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
// value. numpy.NumRows returns shape[0] unclamped, so the count is reachable.
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
