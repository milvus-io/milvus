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
		bound, err := computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(101), bound)
	})

	t.Run("numpy", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Size(mock.Anything, "a.npy").Return(int64(3072*50), nil)
		file := &internalpb.ImportFile{Paths: []string{"a.npy"}}
		// numpyRowByteSize(schemaVec(768,0)) == 3072; 3072*50 / 3072 + 1 == 51.
		bound, err := computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(51), bound)
	})

	t.Run("cap applied", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Size(mock.Anything, "a.json").Return(int64(1)<<40, nil)
		file := &internalpb.ImportFile{Paths: []string{"a.json"}}
		// minRowTextBytes(schemaVec(1,0)) == 1; bound would be huge, capped to 1<<34.
		bound, err := computeFileRowUpperBound(ctx, cm, schemaVec(1, 0), file, int64(1)<<34)
		assert.NoError(t, err)
		assert.Equal(t, int64(1)<<34, bound)
	})

	t.Run("parquet mocked", func(t *testing.T) {
		mk := mockey.Mock(parquetNumRows).Return(int64(123), nil).Build()
		defer mk.UnPatch()
		cm := mocks.NewChunkManager(t)
		file := &internalpb.ImportFile{Paths: []string{"a.parquet"}}
		bound, err := computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(123), bound)

		// cap smaller than the exact count clamps it.
		bound, err = computeFileRowUpperBound(ctx, cm, schemaVec(768, 0), file, int64(100))
		assert.NoError(t, err)
		assert.Equal(t, int64(100), bound)
	})
}
