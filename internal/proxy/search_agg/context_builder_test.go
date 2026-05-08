package search_agg

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestBuildSearchAggregationContext(t *testing.T) {
	schema := testCollectionSchema()
	spec := &commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		Size:   5,
		Metrics: map[string]*commonpb.MetricAggSpec{
			"avg_price": {Op: "avg", FieldName: "price"},
		},
		Order: []*commonpb.OrderSpec{{Key: "_count", Direction: "desc"}},
		TopHits: &commonpb.TopHitsSpec{
			Size: 2,
			Sort: []*commonpb.SortSpec{{FieldName: "_score", Direction: "desc"}},
		},
		SubAggregation: &commonpb.SearchAggregationSpec{
			Fields: []string{"category"},
			Size:   3,
			Metrics: map[string]*commonpb.MetricAggSpec{
				"sum_stock": {Op: "sum", FieldName: "stock"},
			},
			Order: []*commonpb.OrderSpec{{Key: "_key", Direction: "asc"}},
		},
	}

	ctx, err := BuildSearchAggregationContext(spec, schema, 2)
	require.NoError(t, err)
	require.Equal(t, int64(2), ctx.NQ)
	require.Len(t, ctx.Levels, 2)
	require.Equal(t, []int64{101}, ctx.Levels[0].OwnFieldIDs)
	require.Equal(t, []int64{102}, ctx.Levels[1].OwnFieldIDs)

	require.True(t, ctx.IsGroupByField(101))
	require.True(t, ctx.IsGroupByField(102))
	// Group-by fields must NOT leak into ExtraOutputFieldIDs — they come from field 17.
	require.False(t, ctx.IsGroupByField(103))
	require.False(t, ctx.IsGroupByField(104))

	require.Equal(t, []int64{103, 104}, ctx.ExtraOutputFieldIDs())
	require.Equal(t, ScoreFieldID, ctx.Levels[0].TopHits.Sort[0].FieldID)
	require.Equal(t, int64(2), ctx.DerivedGroupSize)
}

func TestBuildSearchAggregationContextCarriesTopHitsSortNullFirst(t *testing.T) {
	schema := testCollectionSchema()
	spec := &commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		Size:   3,
		TopHits: &commonpb.TopHitsSpec{
			Size: 2,
			Sort: []*commonpb.SortSpec{
				{FieldName: "stock", Direction: "asc", NullFirst: true},
				{FieldName: "_score", Direction: "desc", NullFirst: true},
			},
		},
	}

	ctx, err := BuildSearchAggregationContext(spec, schema, 1)
	require.NoError(t, err)
	require.Len(t, ctx.Levels, 1)
	require.Len(t, ctx.Levels[0].TopHits.Sort, 2)
	require.True(t, ctx.Levels[0].TopHits.Sort[0].NullFirst)
	require.True(t, ctx.Levels[0].TopHits.Sort[1].NullFirst)
}

func TestBuildSearchAggregationContextDefaultsOmittedSizeToOne(t *testing.T) {
	schema := testCollectionSchema()
	spec := &commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		SubAggregation: &commonpb.SearchAggregationSpec{
			Fields:  []string{"category"},
			TopHits: &commonpb.TopHitsSpec{},
		},
	}

	ctx, err := BuildSearchAggregationContext(spec, schema, 1)
	require.NoError(t, err)
	require.Len(t, ctx.Levels, 2)
	require.Equal(t, int64(1), ctx.Levels[0].Size)
	require.Equal(t, int64(1), ctx.Levels[1].Size)
	require.NotNil(t, ctx.Levels[1].TopHits)
	require.Equal(t, int64(1), ctx.Levels[1].TopHits.Size)
	require.Equal(t, int64(1), ctx.DerivedTopK)
	require.Equal(t, int64(1), ctx.DerivedGroupSize)
}

func TestBuildSearchAggregationContextRejectsNegativeSize(t *testing.T) {
	schema := testCollectionSchema()

	_, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		Size:   -1,
	}, schema, 1)

	require.Error(t, err)
	require.Contains(t, err.Error(), "search_aggregation size must be non-negative")
}

func TestBuildSearchAggregationContextRejectsNegativeTopHitsSize(t *testing.T) {
	schema := testCollectionSchema()

	_, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		TopHits: &commonpb.TopHitsSpec{
			Size: -1,
		},
	}, schema, 1)

	require.Error(t, err)
	require.Contains(t, err.Error(), "top_hits size must be non-negative")
}

func TestDeriveTopKAndGroupSizeDefaultsZeroSizesToOne(t *testing.T) {
	topK, groupSize := deriveTopKAndGroupSize([]LevelContext{
		{Size: 0},
		{Size: 3, TopHits: &TopHitsConfig{Size: 0}},
	})

	require.Equal(t, int64(3), topK)
	require.Equal(t, int64(1), groupSize)
}

func TestNewContextReturnsMetricCompileError(t *testing.T) {
	_, err := NewContext(1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Metrics: map[string]MetricSpec{
				"bad_metric": {Op: "bad_op", FieldID: 103, FieldType: schemapb.DataType_Int64},
			},
		}},
		nil,
		nil,
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "level 0 metric compile failed")
}

func TestDeriveTopKAndGroupSizeUsesMaxTopHitsAcrossLevels(t *testing.T) {
	cases := []struct {
		name              string
		levels            []LevelContext
		expectedTopK      int64
		expectedGroupSize int64
	}{
		{
			name: "parent larger than leaf",
			levels: []LevelContext{
				{Size: 2, TopHits: &TopHitsConfig{Size: 7}},
				{Size: 3, TopHits: &TopHitsConfig{Size: 3}},
			},
			expectedTopK:      6,
			expectedGroupSize: 7,
		},
		{
			name: "leaf larger than zero parent",
			levels: []LevelContext{
				{Size: 2, TopHits: &TopHitsConfig{Size: 0}},
				{Size: 3, TopHits: &TopHitsConfig{Size: 5}},
			},
			expectedTopK:      6,
			expectedGroupSize: 5,
		},
		{
			name: "no top hits defaults to one",
			levels: []LevelContext{
				{Size: 2},
				{Size: 3},
			},
			expectedTopK:      6,
			expectedGroupSize: 1,
		},
		{
			name: "middle level largest",
			levels: []LevelContext{
				{Size: 2, TopHits: &TopHitsConfig{Size: 4}},
				{Size: 3, TopHits: &TopHitsConfig{Size: 9}},
				{Size: 5, TopHits: &TopHitsConfig{Size: 6}},
			},
			expectedTopK:      30,
			expectedGroupSize: 9,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topK, groupSize := deriveTopKAndGroupSize(tc.levels)
			require.Equal(t, tc.expectedTopK, topK)
			require.Equal(t, tc.expectedGroupSize, groupSize)
		})
	}
}

func TestBuildSearchAggregationContextDuplicateFieldAcrossLevels(t *testing.T) {
	schema := testCollectionSchema()
	spec := &commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		SubAggregation: &commonpb.SearchAggregationSpec{
			Fields: []string{"brand"},
		},
	}

	_, err := BuildSearchAggregationContext(spec, schema, 1)
	require.Error(t, err)
}

func TestBuildSearchAggregationContextRejectsOversizedDerivedResultEntries(t *testing.T) {
	withMaxSearchAggregationResultEntries(t, "100")

	_, err := BuildSearchAggregationContext(derivedSizeTestSpec(), testCollectionSchema(), 1)

	require.Error(t, err)
	require.Contains(t, err.Error(), "number of search_aggregation result entries is too large")
}

func TestBuildSearchAggregationContextAllowsDerivedResultEntriesAtLimit(t *testing.T) {
	withMaxSearchAggregationResultEntries(t, "125")

	ctx, err := BuildSearchAggregationContext(derivedSizeTestSpec(), testCollectionSchema(), 1)

	require.NoError(t, err)
	require.Equal(t, int64(25), ctx.DerivedTopK)
	require.Equal(t, int64(5), ctx.DerivedGroupSize)
}

func TestBuildSearchAggregationContextMaxDerivedResultEntriesDisabled(t *testing.T) {
	withMaxSearchAggregationResultEntries(t, "-1")

	ctx, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		Size:   1_000_000,
		SubAggregation: &commonpb.SearchAggregationSpec{
			Fields: []string{"category"},
			Size:   1_000_000,
			TopHits: &commonpb.TopHitsSpec{
				Size: 1_000,
			},
		},
	}, testCollectionSchema(), 1)

	require.NoError(t, err)
	require.Equal(t, int64(1_000_000_000_000), ctx.DerivedTopK)
	require.Equal(t, int64(1_000), ctx.DerivedGroupSize)
}

func TestBuildSearchAggregationContextRejectsDerivedTopKOverflow(t *testing.T) {
	withMaxSearchAggregationResultEntries(t, "-1")

	_, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		Size:   math.MaxInt64,
		SubAggregation: &commonpb.SearchAggregationSpec{
			Fields: []string{"category"},
			Size:   2,
		},
	}, testCollectionSchema(), 1)

	require.Error(t, err)
	require.Contains(t, err.Error(), "derived topK overflows int64")
}

func TestBuildSearchAggregationContextRejectsResultEntriesOverflowWhenLimitEnabled(t *testing.T) {
	withMaxSearchAggregationResultEntries(t, "1000000")

	_, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		Size:   math.MaxInt64,
	}, testCollectionSchema(), 2)

	require.Error(t, err)
	require.Contains(t, err.Error(), "number of search_aggregation result entries is too large")
}

func TestBuildSearchAggregationContextQuotaUsesNonLeafTopHits(t *testing.T) {
	withMaxSearchAggregationResultEntries(t, "249")

	_, err := BuildSearchAggregationContext(nonLeafTopHitsSizeTestSpec(), testCollectionSchema(), 1)

	require.Error(t, err)
	require.Contains(t, err.Error(), "number of search_aggregation result entries is too large")
}

func TestBuildSearchAggregationContextQuotaAllowsNonLeafTopHitsAtLimit(t *testing.T) {
	withMaxSearchAggregationResultEntries(t, "250")

	ctx, err := BuildSearchAggregationContext(nonLeafTopHitsSizeTestSpec(), testCollectionSchema(), 1)

	require.NoError(t, err)
	require.Equal(t, int64(25), ctx.DerivedTopK)
	require.Equal(t, int64(10), ctx.DerivedGroupSize)
}

func withMaxSearchAggregationResultEntries(t *testing.T, value string) {
	t.Helper()
	params := paramtable.Get()
	params.Save(params.ProxyCfg.MaxSearchAggregationResultEntries.Key, value)
	t.Cleanup(func() {
		params.Reset(params.ProxyCfg.MaxSearchAggregationResultEntries.Key)
	})
}

func derivedSizeTestSpec() *commonpb.SearchAggregationSpec {
	return &commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		Size:   5,
		SubAggregation: &commonpb.SearchAggregationSpec{
			Fields: []string{"category"},
			Size:   5,
			TopHits: &commonpb.TopHitsSpec{
				Size: 5,
			},
		},
	}
}

func nonLeafTopHitsSizeTestSpec() *commonpb.SearchAggregationSpec {
	return &commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		Size:   5,
		TopHits: &commonpb.TopHitsSpec{
			Size: 10,
		},
		SubAggregation: &commonpb.SearchAggregationSpec{
			Fields: []string{"category"},
			Size:   5,
		},
	}
}

func testCollectionSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "agg_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "brand", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "category", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "price", DataType: schemapb.DataType_Double},
			{FieldID: 104, Name: "stock", DataType: schemapb.DataType_Int64},
		},
	}
}

func TestBuildSearchAggregationContextRejectsJSONField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "agg_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 108, Name: "meta", DataType: schemapb.DataType_JSON},
		},
	}
	t.Run("plain json field", func(t *testing.T) {
		_, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
			Fields: []string{"meta"}, Size: 3,
		}, schema, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not yet supported with search_aggregation")
	})
	t.Run("json path", func(t *testing.T) {
		_, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
			Fields: []string{"meta['region']"}, Size: 3,
		}, schema, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not yet supported with search_aggregation")
	})
}

func TestBuildSearchAggregationContextRejectsTopHitsSortJSONPath(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "agg_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "brand", DataType: schemapb.DataType_VarChar},
			{FieldID: 108, Name: "meta", DataType: schemapb.DataType_JSON},
		},
	}

	_, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
		Fields: []string{"brand"},
		Size:   3,
		TopHits: &commonpb.TopHitsSpec{
			Size: 2,
			Sort: []*commonpb.SortSpec{{FieldName: "meta['region']", Direction: "asc"}},
		},
	}, schema, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "top_hits.sort JSON path is not yet supported")
}

func TestBuildSearchAggregationContextRejectsFloatField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "agg_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 110, Name: "price_f", DataType: schemapb.DataType_Float},
			{FieldID: 111, Name: "price_d", DataType: schemapb.DataType_Double},
		},
	}
	t.Run("float", func(t *testing.T) {
		_, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
			Fields: []string{"price_f"}, Size: 3,
		}, schema, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "FLOAT / DOUBLE")
	})
	t.Run("double", func(t *testing.T) {
		_, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
			Fields: []string{"price_d"}, Size: 3,
		}, schema, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "FLOAT / DOUBLE")
	})
}

func TestBuildSearchAggregationContextRejectsTooDeep(t *testing.T) {
	schema := testCollectionSchema()
	// Chain len = maxAggregationLevels + 1 using "brand" field repeatedly is
	// not allowed (duplicate field ban fires first), so build with distinct
	// fields at each level.
	leaf := &commonpb.SearchAggregationSpec{Fields: []string{"stock"}, Size: 2}
	lvl4 := &commonpb.SearchAggregationSpec{Fields: []string{"price"}, Size: 2, SubAggregation: leaf}
	lvl3 := &commonpb.SearchAggregationSpec{Fields: []string{"category"}, Size: 2, SubAggregation: lvl4}
	lvl2 := &commonpb.SearchAggregationSpec{Fields: []string{"brand"}, Size: 2, SubAggregation: lvl3}
	spec := &commonpb.SearchAggregationSpec{Fields: []string{"id"}, Size: 2, SubAggregation: lvl2}

	_, err := BuildSearchAggregationContext(spec, schema, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nesting exceeds max 4 levels")
}

func TestBuildSearchAggregationContextRejectsDynamicField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "agg_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 109, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}
	_, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
		Fields: []string{"arbitrary_dyn_field"}, Size: 3,
	}, schema, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not yet supported with search_aggregation")
}

func TestBuildSearchAggregationContextValidationMatrix(t *testing.T) {
	schema := testCollectionSchema()
	cases := []struct {
		name    string
		spec    *commonpb.SearchAggregationSpec
		schema  *schemapb.CollectionSchema
		wantErr string
	}{
		{
			name:    "nil spec",
			schema:  schema,
			wantErr: "group_by spec is nil",
		},
		{
			name:    "nil schema",
			spec:    &commonpb.SearchAggregationSpec{Fields: []string{"brand"}},
			wantErr: "collection schema is nil",
		},
		{
			name:    "empty group fields",
			spec:    &commonpb.SearchAggregationSpec{},
			schema:  schema,
			wantErr: "group_by level has no fields",
		},
		{
			name:    "blank group field",
			spec:    &commonpb.SearchAggregationSpec{Fields: []string{" "}},
			schema:  schema,
			wantErr: "field is empty",
		},
		{
			name:    "unknown group field",
			spec:    &commonpb.SearchAggregationSpec{Fields: []string{"missing"}},
			schema:  schema,
			wantErr: "not found in schema",
		},
		{
			name:    "duplicate field in same level",
			spec:    &commonpb.SearchAggregationSpec{Fields: []string{"brand", "brand"}},
			schema:  schema,
			wantErr: "duplicated group_by field",
		},
		{
			name: "nil metric spec",
			spec: &commonpb.SearchAggregationSpec{
				Fields:  []string{"brand"},
				Metrics: map[string]*commonpb.MetricAggSpec{"total": nil},
			},
			schema:  schema,
			wantErr: "metric spec is nil",
		},
		{
			name: "empty metric alias",
			spec: &commonpb.SearchAggregationSpec{
				Fields:  []string{"brand"},
				Metrics: map[string]*commonpb.MetricAggSpec{" ": {Op: "count", FieldName: "*"}},
			},
			schema:  schema,
			wantErr: "metric alias cannot be empty",
		},
		{
			name: "unsupported metric op",
			spec: &commonpb.SearchAggregationSpec{
				Fields:  []string{"brand"},
				Metrics: map[string]*commonpb.MetricAggSpec{"bad": {Op: "median", FieldName: "price"}},
			},
			schema:  schema,
			wantErr: "unsupported metric op",
		},
		{
			name: "empty metric field name",
			spec: &commonpb.SearchAggregationSpec{
				Fields:  []string{"brand"},
				Metrics: map[string]*commonpb.MetricAggSpec{"bad": {Op: "sum"}},
			},
			schema:  schema,
			wantErr: "metric field_name is empty",
		},
		{
			name: "non count star metric",
			spec: &commonpb.SearchAggregationSpec{
				Fields:  []string{"brand"},
				Metrics: map[string]*commonpb.MetricAggSpec{"bad": {Op: "sum", FieldName: "*"}},
			},
			schema:  schema,
			wantErr: "only supports count op",
		},
		{
			name: "nil order item",
			spec: &commonpb.SearchAggregationSpec{
				Fields: []string{"brand"},
				Order:  []*commonpb.OrderSpec{nil},
			},
			schema:  schema,
			wantErr: "order contains nil item",
		},
		{
			name: "empty order key",
			spec: &commonpb.SearchAggregationSpec{
				Fields: []string{"brand"},
				Order:  []*commonpb.OrderSpec{{}},
			},
			schema:  schema,
			wantErr: "order key is empty",
		},
		{
			name: "unknown order metric alias",
			spec: &commonpb.SearchAggregationSpec{
				Fields: []string{"brand"},
				Order:  []*commonpb.OrderSpec{{Key: "missing"}},
			},
			schema:  schema,
			wantErr: "neither reserved key nor metric alias",
		},
		{
			name: "invalid order direction",
			spec: &commonpb.SearchAggregationSpec{
				Fields: []string{"brand"},
				Order:  []*commonpb.OrderSpec{{Key: "_count", Direction: "sideways"}},
			},
			schema:  schema,
			wantErr: "invalid order direction",
		},
		{
			name: "nil top hits sort item",
			spec: &commonpb.SearchAggregationSpec{
				Fields:  []string{"brand"},
				TopHits: &commonpb.TopHitsSpec{Sort: []*commonpb.SortSpec{nil}},
			},
			schema:  schema,
			wantErr: "top_hits.sort contains nil item",
		},
		{
			name: "empty top hits sort field",
			spec: &commonpb.SearchAggregationSpec{
				Fields:  []string{"brand"},
				TopHits: &commonpb.TopHitsSpec{Sort: []*commonpb.SortSpec{{}}},
			},
			schema:  schema,
			wantErr: "top_hits.sort field_name is empty",
		},
		{
			name: "invalid top hits sort direction",
			spec: &commonpb.SearchAggregationSpec{
				Fields:  []string{"brand"},
				TopHits: &commonpb.TopHitsSpec{Sort: []*commonpb.SortSpec{{FieldName: "stock", Direction: "up"}}},
			},
			schema:  schema,
			wantErr: "invalid top_hits.sort direction",
		},
		{
			name: "unknown top hits sort field",
			spec: &commonpb.SearchAggregationSpec{
				Fields:  []string{"brand"},
				TopHits: &commonpb.TopHitsSpec{Sort: []*commonpb.SortSpec{{FieldName: "missing"}}},
			},
			schema:  schema,
			wantErr: "invalid top_hits.sort field",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildSearchAggregationContext(tc.spec, tc.schema, 1)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}

	t.Run("count star accepted and extra outputs deduped", func(t *testing.T) {
		ctx, err := BuildSearchAggregationContext(&commonpb.SearchAggregationSpec{
			Fields: []string{"brand"},
			Metrics: map[string]*commonpb.MetricAggSpec{
				"count_all":  {Op: "count", FieldName: "*"},
				"stock_sum":  {Op: "sum", FieldName: "stock"},
				"stock_sum2": {Op: "sum", FieldName: "stock"},
			},
			TopHits: &commonpb.TopHitsSpec{
				Sort: []*commonpb.SortSpec{
					{FieldName: "stock"},
					{FieldName: "price"},
					{FieldName: "price"},
				},
			},
		}, schema, 1)
		require.NoError(t, err)
		require.Equal(t, []int64{103, 104}, ctx.ExtraOutputFieldIDs())
	})
}

func TestCheckedMulInt64Matrix(t *testing.T) {
	cases := []struct {
		name string
		a    int64
		b    int64
		want int64
		ok   bool
	}{
		{name: "negative lhs", a: -1, b: 2, ok: false},
		{name: "negative rhs", a: 2, b: -1, ok: false},
		{name: "zero lhs", a: 0, b: math.MaxInt64, want: 0, ok: true},
		{name: "zero rhs", a: math.MaxInt64, b: 0, want: 0, ok: true},
		{name: "normal", a: 12, b: 34, want: 408, ok: true},
		{name: "overflow", a: math.MaxInt64, b: 2, ok: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := checkedMulInt64(tc.a, tc.b)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestValidateSearchAggregationResultEntriesMatrix(t *testing.T) {
	cases := []struct {
		name      string
		nq        int64
		topK      int64
		groupSize int64
		max       int64
		wantErr   bool
	}{
		{name: "disabled", nq: math.MaxInt64, topK: math.MaxInt64, groupSize: math.MaxInt64, max: 0},
		{name: "equal limit", nq: 2, topK: 5, groupSize: 3, max: 30},
		{name: "over limit", nq: 2, topK: 5, groupSize: 3, max: 29, wantErr: true},
		{name: "nq topk overflow", nq: math.MaxInt64, topK: 2, groupSize: 1, max: math.MaxInt64, wantErr: true},
		{name: "entries group size overflow", nq: math.MaxInt64 / 2, topK: 2, groupSize: 2, max: math.MaxInt64, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSearchAggregationResultEntries(tc.nq, tc.topK, tc.groupSize, tc.max)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
