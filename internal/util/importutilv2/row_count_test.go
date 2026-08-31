package importutilv2

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
	"github.com/milvus-io/milvus/internal/util/importutilv2/numpy"
	"github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
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
	got, _ := minRowTextBytes(schemaVec(768, 1), CSV)
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
	m, clamped := minRowTextBytes(schemaBM25AutoID(), CSV)
	assert.Equal(t, int64(1), m)
	assert.True(t, clamped, "the 1 is a placeholder, not a provable byte cost")
}

func Test_RowCountUpperBound(t *testing.T) {
	ctx := context.Background()

	t.Run("text json", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Size(mock.Anything, "a.json").Return(int64(768*100), nil)
		file := &internalpb.ImportFile{Paths: []string{"a.json"}}
		// A JSON row of this schema floors at 776: 768 for the dim-768 vector plus
		// 8 for the braces and "vec": around it. The floor is provable, so the bound
		// also charges the n-1 row separators: (76800+1)/(776+1) + 1 == 99. (The
		// looser 76800/776 + 1 lands on 99 too at this size; they diverge as the
		// file grows.)
		bound, exact, err := RowCountUpperBound(ctx, cm, schemaVec(768, 0), file)
		assert.NoError(t, err)
		assert.Equal(t, int64(99), bound)
		assert.False(t, exact, "a byte-derived text bound is an estimate")
	})

	t.Run("numpy mocked", func(t *testing.T) {
		// The row count comes from the .npy header shape, so it is exact and does
		// not depend on file size or on any schema-derived per-row width.
		mk := mockey.Mock(numpy.NumRows).Return(int64(50), nil).Build()
		defer mk.UnPatch()
		cm := mocks.NewChunkManager(t)
		file := &internalpb.ImportFile{Paths: []string{"a.npy"}}
		bound, exact, err := RowCountUpperBound(ctx, cm, schemaVec(768, 0), file)
		assert.NoError(t, err)
		assert.Equal(t, int64(50), bound)
		assert.True(t, exact)
	})

	t.Run("parquet mocked", func(t *testing.T) {
		mk := mockey.Mock(parquet.NumRows).Return(int64(123), nil).Build()
		defer mk.UnPatch()
		cm := mocks.NewChunkManager(t)
		file := &internalpb.ImportFile{Paths: []string{"a.parquet"}}
		bound, exact, err := RowCountUpperBound(ctx, cm, schemaVec(768, 0), file)
		assert.NoError(t, err)
		assert.Equal(t, int64(123), bound)
		assert.True(t, exact)
	})
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
	got, _ := minRowTextBytes(schema, CSV)
	assert.Equal(t, int64(2), got) // was 4 before the fix (nullable+default counted)
}

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
	json, _ := minRowTextBytes(schema, JSON)
	assert.Equal(t, int64(2+len("text")+3), json)

	jsonl, _ := minRowTextBytes(schema, JSONLines)
	assert.Equal(t, json, jsonl)

	// CSV keeps its names in the header, so a single-column row really can be one
	// byte. This gap is not closable and is why the reserveRanges error explains
	// itself rather than pretending the count is a real row count.
	csv, _ := minRowTextBytes(schema, CSV)
	assert.Equal(t, int64(1), csv)
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

	got, _ := minRowTextBytes(legacy, JSONLines)
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
	modernGot, _ := minRowTextBytes(modern, JSONLines)
	assert.Equal(t, got, modernGot, "legacy and modern $meta must size identically")
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
	got, clamped := minRowTextBytes(schema, CSV)
	assert.Equal(t, int64(1), got)
	assert.True(t, clamped, "the 1 is a placeholder, not a provable byte cost")
}

func Test_RowCountUpperBound_multiColumnCSVChargesRowSeparators(t *testing.T) {
	// Two non-nullable VarChar source columns: values may be empty, so the value
	// floor is 0 and the whole floor is the single field separator -- provable, not
	// clamped. Every row therefore costs at least ",\n", and the bound must charge
	// the n-1 row separators the floor deliberately leaves out.
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

	minRow, clamped := minRowTextBytes(schema, CSV)
	require.Equal(t, int64(1), minRow)
	require.False(t, clamped)

	const size = int64(5) << 30 // 5 GiB, under dataNode.import.maxImportFileSizeInGB
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Size(mock.Anything, "two-col.csv").Return(size, nil)

	file := &internalpb.ImportFile{Paths: []string{"two-col.csv"}}
	bound, exact, err := RowCountUpperBound(ctx, cm, schema, file)
	require.NoError(t, err)
	assert.False(t, exact)

	// Loose form would give ~5.4e9, above the allocator's per-batch ceiling, and the
	// import would be refused outright even though the file cannot hold that many rows.
	assert.Equal(t, (size+1)/2+1, bound)
	assert.Less(t, bound, int64(math.MaxUint32))
	// Still an upper bound: n rows occupy at least 2n-1 bytes.
	assert.GreaterOrEqual(t, bound, size/2)
}
