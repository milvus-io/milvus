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

// stubRowCounts makes RowCountUpperBound answer from a per-path table, so a test
// can pick the exact or the estimate path without building a real parquet/npy
// fixture. The caller unpatches.
func stubRowCounts(spec map[string]struct {
	rows  int64
	exact bool
},
) *mockey.Mocker {
	return mockey.Mock(importutilv2.RowCountUpperBound).To(
		func(_ context.Context, _ storage.ChunkManager, _ *schemapb.CollectionSchema, f *internalpb.ImportFile) (int64, bool, error) {
			s := spec[f.GetPaths()[0]]
			return s.rows, s.exact, nil
		}).Build()
}

// allocBlock is one [begin, end) handed out by a single allocN call.
type allocBlock struct{ begin, end int64 }

// recordingAlloc records what each allocN call asked for and leaves a gap between
// blocks, so a range that straddles two calls is observable: a real allocator
// gives no guarantee that consecutive AllocN results are adjacent.
func recordingAlloc(calls *[]int64, blocks *[]allocBlock) func(int64) (int64, int64, error) {
	next := int64(1000)
	return func(n int64) (int64, int64, error) {
		*calls = append(*calls, n)
		begin := next
		next += n + 1_000_000
		if blocks != nil {
			*blocks = append(*blocks, allocBlock{begin, begin + n})
		}
		return begin, begin + n, nil
	}
}

func rangeWidths(files []*internalpb.ImportFile) []int64 {
	out := make([]int64, len(files))
	for i, f := range files {
		out[i] = f.GetPkIdEnd() - f.GetPkIdBegin()
	}
	return out
}

// The reservation sizing and the batch packing are pinned here through the
// package entry point, not through the helpers that implement them, so these
// assertions stay valid across a change to those helpers' signatures.
func Test_assignPKRangesToFiles_pinsReservationSizing(t *testing.T) {
	type count = struct {
		rows  int64
		exact bool
	}
	cm := mocks.NewChunkManager(t)
	schema := schemaVec(8, 0)
	filesFor := func(paths ...string) []*internalpb.ImportFile {
		out := make([]*internalpb.ImportFile, 0, len(paths))
		for _, p := range paths {
			out = append(out, &internalpb.ImportFile{Paths: []string{p}})
		}
		return out
	}

	t.Run("the expansion factor applies to an exact count only", func(t *testing.T) {
		withExpansionFactor(t, "10")
		defer stubRowCounts(map[string]count{
			"exact.npy":     {rows: 100, exact: true},
			"estimate.json": {rows: 100, exact: false},
		}).UnPatch()

		files := filesFor("exact.npy", "estimate.json")
		var calls []int64
		require.NoError(t, assignPKRangesToFiles(context.TODO(), cm, schema, files,
			recordingAlloc(&calls, nil), 1))
		assert.Equal(t, []int64{1000, 100}, rangeWidths(files))
		assert.Equal(t, []int64{1100}, calls, "one batch holds both reservations")
	})

	t.Run("a zero-row file still reserves one id", func(t *testing.T) {
		withExpansionFactor(t, "10")
		defer stubRowCounts(map[string]count{"empty.npy": {rows: 0, exact: true}}).UnPatch()

		files := filesFor("empty.npy")
		var calls []int64
		require.NoError(t, assignPKRangesToFiles(context.TODO(), cm, schema, files,
			recordingAlloc(&calls, nil), 1))
		// An empty range reads as "no range" on the datanode and silently falls back
		// to the local allocator, which is the divergence this mechanism prevents.
		assert.Equal(t, []int64{1}, rangeWidths(files))
	})

	t.Run("a total above the ceiling is split, and no range straddles a batch", func(t *testing.T) {
		withExpansionFactor(t, "1")
		half := maxIDsPerAllocBatch / 2
		defer stubRowCounts(map[string]count{
			"a.json": {rows: half, exact: false},
			"b.json": {rows: half, exact: false},
			"c.json": {rows: half, exact: false},
		}).UnPatch()

		files := filesFor("a.json", "b.json", "c.json")
		var calls []int64
		var blocks []allocBlock
		// clusterID 0 so the recorded blocks are directly comparable: a non-zero
		// clusterID ORs its bits into every id, which shifts the ranges out of the
		// raw blocks the allocator handed back. The cluster bits themselves are
		// pinned by Test_assignPKRangesToFiles.
		require.NoError(t, assignPKRangesToFiles(context.TODO(), cm, schema, files,
			recordingAlloc(&calls, &blocks), 0))
		assert.Equal(t, []int64{2 * half, half}, calls)
		assert.Equal(t, []int64{half, half, half}, rangeWidths(files),
			"no reservation is shrunk to fit the ceiling")
		for i, f := range files {
			inOneBlock := false
			for _, b := range blocks {
				if f.GetPkIdBegin() >= b.begin && f.GetPkIdEnd() <= b.end {
					inOneBlock = true
				}
			}
			assert.True(t, inOneBlock, "file %d must sit inside a single batch", i)
		}
	})

	t.Run("an exact count above one batch is refused on the raw row count", func(t *testing.T) {
		withExpansionFactor(t, "2")
		defer stubRowCounts(map[string]count{
			"huge.npy": {rows: maxIDsPerAllocBatch + 1, exact: true},
		}).UnPatch()

		files := filesFor("huge.npy")
		err := assignPKRangesToFiles(context.TODO(), cm, schema, files,
			func(int64) (int64, int64, error) { t.Fatal("allocN must not be called"); return 0, 0, nil }, 1)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "more than one allocation batch can reserve")
	})

	t.Run("an estimate above one batch is refused with the estimate wording", func(t *testing.T) {
		withExpansionFactor(t, "2")
		defer stubRowCounts(map[string]count{
			"huge.json": {rows: maxIDsPerAllocBatch + 1, exact: false},
		}).UnPatch()

		files := filesFor("huge.json")
		err := assignPKRangesToFiles(context.TODO(), cm, schema, files,
			func(int64) (int64, int64, error) { t.Fatal("allocN must not be called"); return 0, 0, nil }, 1)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "more than one allocation batch holds")
	})
}

func withExpansionFactor(t *testing.T, factor string) {
	paramtable.Init()
	key := paramtable.Get().DataCoordCfg.ImportPreAllocIDExpansionFactor.Key
	paramtable.Get().Save(key, factor)
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

// sized builds the input sizeReservations expects: one record per file, carrying
// the row bound and whether it is exact.
func sized(rows []int64, exacts []bool) []fileSizing {
	out := make([]fileSizing, len(rows))
	for i, r := range rows {
		out[i] = fileSizing{file: &internalpb.ImportFile{}, rows: r, exact: exacts[i]}
	}
	return out
}

func reservedIDs(sizings []fileSizing) []int64 {
	out := make([]int64, len(sizings))
	for i, s := range sizings {
		out[i] = s.reservedIDs
	}
	return out
}

func Test_sizeReservations(t *testing.T) {
	t.Run("exact gets the expansion factor, estimate does not", func(t *testing.T) {
		withExpansionFactor(t, "10")
		sizings := sized([]int64{100, 100}, []bool{true, false})
		require.NoError(t, sizeReservations(sizings))
		assert.Equal(t, []int64{1000, 100}, reservedIDs(sizings))
	})

	t.Run("a zero bound is floored to one id", func(t *testing.T) {
		withExpansionFactor(t, "10")
		// An empty range reads as "no range" on the datanode and silently falls back
		// to the local allocator, which is the divergence this mechanism prevents.
		sizings := sized([]int64{0, 0}, []bool{true, false})
		require.NoError(t, sizeReservations(sizings))
		assert.Equal(t, []int64{1, 1}, reservedIDs(sizings))
	})

	t.Run("bounds are never shrunk to fit an allocation ceiling", func(t *testing.T) {
		withExpansionFactor(t, "1")
		// Scaling an estimate down would break the upper-bound guarantee that makes
		// it usable at all; reserveRanges splits the allocation instead.
		sizings := sized([]int64{3 * math.MaxUint32, math.MaxUint32}, []bool{false, false})
		require.NoError(t, sizeReservations(sizings))
		assert.Equal(t, []int64{3 * math.MaxUint32, math.MaxUint32}, reservedIDs(sizings))
	})
}

// reserved builds the input reserveRanges expects: one record per file, already
// carrying its id reservation.
func reserved(ids ...int64) []fileSizing {
	out := make([]fileSizing, len(ids))
	for i, n := range ids {
		out[i] = fileSizing{file: &internalpb.ImportFile{}, reservedIDs: n}
	}
	return out
}

func sizingFiles(sizings []fileSizing) []*internalpb.ImportFile {
	out := make([]*internalpb.ImportFile, len(sizings))
	for i, s := range sizings {
		out[i] = s.file
	}
	return out
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
		sizings := reserved(10, 20, 30)
		require.NoError(t, reserveRanges(sizings, newAlloc(&calls, nil), 0))
		files := sizingFiles(sizings)
		assert.Equal(t, []int64{60}, calls)
		assert.Equal(t, []int64{10, 20, 30}, widths(files))
		assert.Equal(t, files[0].GetPkIdEnd(), files[1].GetPkIdBegin())
		assert.Equal(t, files[1].GetPkIdEnd(), files[2].GetPkIdBegin())
	})

	t.Run("a total above the ceiling is split, and every file keeps its full width", func(t *testing.T) {
		var calls []int64
		half := maxIDsPerAllocBatch / 2
		sizings := reserved(half, half, half)
		// half+half fills one batch exactly; the third opens a new one.
		require.NoError(t, reserveRanges(sizings, newAlloc(&calls, nil), 0))
		files := sizingFiles(sizings)
		assert.Equal(t, []int64{2 * half, half}, calls)
		assert.Equal(t, []int64{half, half, half}, widths(files),
			"no reservation is shrunk to fit the ceiling")
	})

	t.Run("a range never straddles two batches", func(t *testing.T) {
		var calls []int64
		var blocks []block
		b := maxIDsPerAllocBatch / 3 * 2
		sizings := reserved(b, b, b)
		require.NoError(t, reserveRanges(sizings, newAlloc(&calls, &blocks), 0))
		files := sizingFiles(sizings)
		assert.Equal(t, []int64{b, b, b}, calls, "two of these never fit together")
		assert.Equal(t, []int64{b, b, b}, widths(files))
		for i, f := range files {
			assert.True(t, within(f, blocks), "file %d must sit inside a single batch", i)
		}
	})

	t.Run("a single file wider than one batch is a clean error", func(t *testing.T) {
		var calls []int64
		err := reserveRanges(reserved(maxIDsPerAllocBatch+1), newAlloc(&calls, nil), 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "more than one allocation batch holds")
		assert.Empty(t, calls, "nothing is allocated once the request is rejected")
	})

	t.Run("all-zero bounds allocate nothing and terminate", func(t *testing.T) {
		sizings := reserved(0, 0)
		require.NoError(t, reserveRanges(sizings,
			func(int64) (int64, int64, error) { t.Fatal("allocN must not be called"); return 0, 0, nil }, 0))
		assert.Equal(t, []int64{0, 0}, widths(sizingFiles(sizings)))
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
	sizings, err := computeFileRowUpperBounds(
		context.Background(), mocks.NewChunkManager(t), schemaVec(8, 0), files)
	require.Error(t, err, "a panicked sizing task must surface as an error, not be swallowed")
	assert.Contains(t, err.Error(), "decoder blew up")
	assert.Nil(t, sizings)
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
	err := sizeReservations(sized([]int64{maxIDsPerAllocBatch + 1}, []bool{true}))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "more than one allocation batch can reserve")

	// An estimate is not an exact count and keeps its own path: reserveRanges
	// still reports it, with the wording that explains the estimate.
	assert.NoError(t, sizeReservations(sized([]int64{maxIDsPerAllocBatch + 1}, []bool{false})))
}

func Test_assignPKRangesToFiles_singleColumnCSVAboveOneBatchIsRefused(t *testing.T) {
	// Behavior change introduced by this PR, pinned deliberately: a single-column
	// all-VarChar CSV larger than maxIDsPerAllocBatch is refused at broadcast,
	// because its per-row floor proves nothing and one file's range may not
	// straddle two allocation batches. See the release note.
	//
	// The tightening -- n*minRow + (n-1) <= size, giving (size+1)/(minRow+1) -- is
	// applied only when the floor is provable, which this schema's is not: it
	// assumes each row really occupies minRow bytes, and a single-column CSV of
	// empty values is one newline per row, so n rows occupy n bytes and (n+1)/2
	// would under-count and fail a legal import. Hence the refusal below stands.
	// See Test_assignPKRangesToFiles_twoColumnCSVAboveOneBatchIsAccepted for the
	// provable-floor case, where the same file size is accepted.
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

func Test_assignPKRangesToFiles_twoColumnCSVAboveOneBatchIsAccepted(t *testing.T) {
	// Mirror of the single-column refusal above. A second non-nullable source column
	// makes the per-row floor provable -- one field separator -- so every row costs
	// at least ",\n" and the bound charges the n-1 row separators. The same 5 GiB
	// file that a single-column schema must refuse is accepted here, because it
	// cannot hold more than ~2.7e9 rows.
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: "a", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}},
			},
			{
				FieldID: 102, Name: "b", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}},
			},
		},
	}
	alloc := func(n int64) (int64, int64, error) { return 1000, 1000 + n, nil }

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Size(mock.Anything, "two-col.csv").Return(int64(5)<<30, nil)
	files := []*internalpb.ImportFile{{Paths: []string{"two-col.csv"}}}
	require.NoError(t, assignPKRangesToFiles(context.TODO(), cm, schema, files, alloc, 1))

	reserved := files[0].GetPkIdEnd() - files[0].GetPkIdBegin()
	assert.Positive(t, reserved)
	assert.LessOrEqual(t, reserved, maxIDsPerAllocBatch)
}
