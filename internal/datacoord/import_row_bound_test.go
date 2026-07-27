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
		require.NoError(t, sizeReservations(bounds, []bool{true, false}))
		assert.Equal(t, []int64{1000, 100}, bounds)
	})

	t.Run("over budget: exact surrenders its headroom first", func(t *testing.T) {
		withExpansionFactor(t, "10")
		// 500M exact * 10 = 5G > MaxUint32, but 500M alone fits and leaves room for
		// the estimate, so only the headroom is given up.
		bounds := []int64{500_000_000, 1000}
		require.NoError(t, sizeReservations(bounds, []bool{true, false}))
		assert.Equal(t, int64(500_000_000), bounds[0], "headroom dropped, exact count kept")
		assert.Equal(t, int64(1000), bounds[1], "estimate untouched while budget remains")
	})

	t.Run("exact alone exceeds the batch limit: fail fast", func(t *testing.T) {
		withExpansionFactor(t, "10")
		bounds := []int64{math.MaxUint32 + 1}
		err := sizeReservations(bounds, []bool{true})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too large to reserve primary keys")
	})

	t.Run("estimates scale proportionally into the remaining budget", func(t *testing.T) {
		withExpansionFactor(t, "1")
		bounds := []int64{3 * math.MaxUint32, math.MaxUint32}
		require.NoError(t, sizeReservations(bounds, []bool{false, false}))
		var total int64
		for _, b := range bounds {
			total += b
		}
		assert.LessOrEqual(t, total, int64(math.MaxUint32))
		// 3:1 input ratio is preserved after scaling.
		assert.Equal(t, int64(3), bounds[0]/bounds[1])
	})

	t.Run("a reservation that would scale to zero is an error, not a clamp", func(t *testing.T) {
		withExpansionFactor(t, "1")
		// The tiny file's share of the budget rounds below one id. Handing it an
		// empty range would make the datanode treat it as "no range" and fall back
		// to its local allocator, silently diverging across clusters.
		bounds := []int64{math.MaxUint32, math.MaxUint32, 1}
		err := sizeReservations(bounds, []bool{false, false, false})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty range")
	})

	t.Run("within budget is left untouched apart from the factor", func(t *testing.T) {
		withExpansionFactor(t, "10")
		bounds := []int64{1, 2, 3}
		require.NoError(t, sizeReservations(bounds, []bool{false, false, false}))
		assert.Equal(t, []int64{1, 2, 3}, bounds)
	})
}
