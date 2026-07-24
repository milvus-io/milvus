package datacoord

import (
	"context"
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func itoa(i int) string { return strconv.Itoa(i) }

func schemaVec(dim int, extraScalars int) *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: itoa(dim)}}},
	}
	for i := 0; i < extraScalars; i++ {
		fields = append(fields, &schemapb.FieldSchema{FieldID: int64(200 + i), Name: "s" + itoa(i), DataType: schemapb.DataType_Int64})
	}
	return &schemapb.CollectionSchema{Fields: fields}
}

func Test_numpyRowByteSize(t *testing.T) {
	// float vector dim=768 => 4*768 = 3072 bytes; + 1 int64 scalar (8) = 3080.
	// autoID PK is NOT in the file, so it is excluded.
	got, err := numpyRowByteSize(schemaVec(768, 1))
	assert.NoError(t, err)
	assert.Equal(t, int64(3080), got)
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
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "512"}}},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}},
		},
	}
}

func Test_rowByteHelpers_skipFunctionOutput(t *testing.T) {
	// sparse (102) is a function output -> skipped; only the VarChar text field remains.
	n, err := numpyRowByteSize(schemaBM25AutoID())
	assert.NoError(t, err)         // must NOT error on the sparse field
	assert.Equal(t, int64(512), n) // varchar max_length only
	m, err := minRowTextBytes(schemaBM25AutoID())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, m, int64(1)) // varchar contributes 0, floored to 1
}

func Test_computeFileRowUpperBound(t *testing.T) {
	ctx := context.Background()

	t.Run("text json", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Size(mock.Anything, "a.json").Return(int64(768*100), nil)
		file := &internalpb.ImportFile{Paths: []string{"a.json"}}
		// minRowTextBytes(schemaVec(768,0)) == 768; 768*100 / 768 + 1 == 101.
		bound, err := computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file)
		assert.NoError(t, err)
		assert.Equal(t, int64(101), bound)
	})

	t.Run("numpy", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Size(mock.Anything, "a.npy").Return(int64(3072*50), nil)
		file := &internalpb.ImportFile{Paths: []string{"a.npy"}}
		// numpyRowByteSize(schemaVec(768,0)) == 3072; 3072*50 / 3072 + 1 == 51.
		bound, err := computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file)
		assert.NoError(t, err)
		assert.Equal(t, int64(51), bound)
	})

	t.Run("parquet mocked", func(t *testing.T) {
		mk := mockey.Mock(parquetNumRows).Return(int64(123), nil).Build()
		defer mk.UnPatch()
		cm := mocks.NewChunkManager(t)
		file := &internalpb.ImportFile{Paths: []string{"a.parquet"}}
		// Parquet is exact and never clamped: the range must fit the real row count.
		bound, err := computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file)
		assert.NoError(t, err)
		assert.Equal(t, int64(123), bound)
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
