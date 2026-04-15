package search_agg

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func newTestAggregationContext(t *testing.T, nq int64, levels []LevelContext, userOutputFieldIDs []int64, extraOutputFieldIDs []int64) *SearchAggregationContext {
	t.Helper()
	ctx, err := NewContext(nq, levels, userOutputFieldIDs, extraOutputFieldIDs)
	require.NoError(t, err)
	return ctx
}

func TestSearchAggregationComputerComputeSingleLevel(t *testing.T) {
	t.Parallel()
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics:     map[string]MetricSpec{"sum_value": {Op: "sum", FieldID: 102, FieldType: schemapb.DataType_Int64}},
			TopHits:     &TopHitsConfig{Size: 100},
			Order:       []OrderCriterion{{Key: "_count", Dir: "desc"}, {Key: "_key", Dir: "asc"}},
		}},
		nil,
		[]int64{102},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		FieldsData: []*schemapb.FieldData{
			testLongFieldData(102, []int64{10, 20, 30, 40}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "B", "B"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0], 2)

	require.Equal(t, "A", result[0][0].Key[101])
	// Metric result type follows source type (int64) post-Phase2, not a forced float64.
	require.Equal(t, int64(30), result[0][0].Metrics["sum_value"])
	require.Equal(t, int64(2), result[0][0].Count)

	require.Equal(t, "B", result[0][1].Key[101])
	require.Equal(t, int64(70), result[0][1].Metrics["sum_value"])
	require.Equal(t, int64(2), result[0][1].Count)
}

func TestSearchAggregationComputerComputeWithTopHits(t *testing.T) {
	t.Parallel()
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			TopHits:     &TopHitsConfig{Size: 2, Sort: []SortCriterion{{FieldID: 102, Dir: "desc"}}},
			Order:       []OrderCriterion{{Key: "_key", Dir: "asc"}},
		}},
		[]int64{102},
		[]int64{102},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		FieldsData: []*schemapb.FieldData{
			testLongFieldData(102, []int64{10, 20, 30, 40}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "B", "B"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 2)

	// Bucket A: rows 0 (PK=1, score=0.9, val=10) and 1 (PK=2, score=0.8, val=20).
	// Sort-by-102 desc → Hits = [row1, row0].
	aHits := result[0][0].Hits
	require.Len(t, aHits, 2)
	require.Equal(t, int64(20), aHits[0].Fields[102])
	require.Equal(t, int64(2), aHits[0].PK)
	require.InDelta(t, float32(0.8), aHits[0].Score, 1e-6)
	require.Equal(t, int64(10), aHits[1].Fields[102])
	require.Equal(t, int64(1), aHits[1].PK)
	require.InDelta(t, float32(0.9), aHits[1].Score, 1e-6)

	// Bucket B: rows 2 (PK=3, score=0.7, val=30) and 3 (PK=4, score=0.6, val=40).
	// Sort-by-102 desc → Hits = [row3, row2].
	bHits := result[0][1].Hits
	require.Len(t, bHits, 2)
	require.Equal(t, int64(40), bHits[0].Fields[102])
	require.Equal(t, int64(4), bHits[0].PK)
	require.InDelta(t, float32(0.6), bHits[0].Score, 1e-6)
	require.Equal(t, int64(30), bHits[1].Fields[102])
	require.Equal(t, int64(3), bHits[1].PK)
	require.InDelta(t, float32(0.7), bHits[1].Score, 1e-6)
}

func TestSearchAggregationComputerTopHitsSizeBoundaries(t *testing.T) {
	t.Parallel()
	// TopHits.Size vs rows-per-bucket has three distinct regimes that
	// ComputeWithTopHits (Size==rows) does not cover:
	//   - Size > rows → limit clamps down to len(sorted)    (computer.go:197)
	//   - Size < rows → truncate to Size                     (computer.go:198-199)
	// Both regimes drive the same buildTopHits code but via different branches.

	mkData := func() *schemapb.SearchResultData {
		return &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{3},
			Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
			Scores:     []float32{0.9, 0.8, 0.7},
			FieldsData: []*schemapb.FieldData{
				testLongFieldData(102, []int64{10, 20, 30}),
			},
			GroupByFieldValues: []*schemapb.FieldData{
				testStringFieldData(101, []string{"A", "A", "A"}),
			},
		}
	}

	t.Run("clamp when Size exceeds available rows", func(t *testing.T) {
		t.Parallel()
		ctx := newTestAggregationContext(t, 1,
			[]LevelContext{{
				OwnFieldIDs: []int64{101},
				Size:        100,
				TopHits:     &TopHitsConfig{Size: 10, Sort: []SortCriterion{{FieldID: 102, Dir: "desc"}}},
			}},
			[]int64{102},
			[]int64{102},
		)
		c := NewSearchAggregationComputer(mkData(), ctx)
		result, err := c.Compute(context.Background())
		require.NoError(t, err)
		require.Len(t, result[0], 1)
		require.Len(t, result[0][0].Hits, 3, "cfg.Size=10 must clamp down to the 3 available rows")
		require.Equal(t, int64(30), result[0][0].Hits[0].Fields[102])
		require.Equal(t, int64(20), result[0][0].Hits[1].Fields[102])
		require.Equal(t, int64(10), result[0][0].Hits[2].Fields[102])
	})

	t.Run("truncate when Size below available rows", func(t *testing.T) {
		t.Parallel()
		ctx := newTestAggregationContext(t, 1,
			[]LevelContext{{
				OwnFieldIDs: []int64{101},
				Size:        100,
				TopHits:     &TopHitsConfig{Size: 1, Sort: []SortCriterion{{FieldID: 102, Dir: "desc"}}},
			}},
			[]int64{102},
			[]int64{102},
		)
		c := NewSearchAggregationComputer(mkData(), ctx)
		result, err := c.Compute(context.Background())
		require.NoError(t, err)
		require.Len(t, result[0], 1)
		require.Len(t, result[0][0].Hits, 1, "cfg.Size=1 must truncate to the top-1 row")
		require.Equal(t, int64(30), result[0][0].Hits[0].Fields[102], "top-1 under desc sort is the max")
	})

	t.Run("default Size zero to one", func(t *testing.T) {
		t.Parallel()
		ctx := newTestAggregationContext(t, 1,
			[]LevelContext{{
				OwnFieldIDs: []int64{101},
				Size:        100,
				TopHits:     &TopHitsConfig{Size: 0, Sort: []SortCriterion{{FieldID: 102, Dir: "desc"}}},
			}},
			[]int64{102},
			[]int64{102},
		)
		c := NewSearchAggregationComputer(mkData(), ctx)
		result, err := c.Compute(context.Background())
		require.NoError(t, err)
		require.Len(t, result[0], 1)
		require.Len(t, result[0][0].Hits, 1, "cfg.Size=0 must normalize to 1, not return all rows")
		require.Equal(t, int64(30), result[0][0].Hits[0].Fields[102], "top-1 under desc sort is the max")
	})
}

func TestApplyOrderAndSizeDefaultsZeroSizeToOne(t *testing.T) {
	buckets := []*AggBucketResult{
		{Count: 3},
		{Count: 2},
	}

	result, err := applyOrderAndSize(buckets, LevelContext{Size: 0})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Same(t, buckets[0], result[0])
}

func TestSearchAggregationComputerTopHitsSortNullFirst(t *testing.T) {
	t.Parallel()

	mkData := func() *schemapb.SearchResultData {
		return &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{3},
			Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
			Scores:     []float32{0.9, 0.8, 0.7},
			FieldsData: []*schemapb.FieldData{
				testNullableLongFieldData(102, []int64{0, 10, 20}, []bool{false, true, true}),
			},
			GroupByFieldValues: []*schemapb.FieldData{
				testStringFieldData(101, []string{"A", "A", "A"}),
			},
		}
	}

	testCases := []struct {
		name      string
		dir       string
		nullFirst bool
		wantPKs   []any
	}{
		{name: "asc null first", dir: "asc", nullFirst: true, wantPKs: []any{int64(1), int64(2), int64(3)}},
		{name: "asc null last", dir: "asc", nullFirst: false, wantPKs: []any{int64(2), int64(3), int64(1)}},
		{name: "desc null first", dir: "desc", nullFirst: true, wantPKs: []any{int64(1), int64(3), int64(2)}},
		{name: "desc null last", dir: "desc", nullFirst: false, wantPKs: []any{int64(3), int64(2), int64(1)}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := newTestAggregationContext(t, 1,
				[]LevelContext{{
					OwnFieldIDs: []int64{101},
					Size:        1,
					TopHits:     &TopHitsConfig{Size: 3, Sort: []SortCriterion{{FieldID: 102, Dir: tc.dir, NullFirst: tc.nullFirst}}},
				}},
				nil,
				[]int64{102},
			)

			computer := NewSearchAggregationComputer(mkData(), ctx)
			result, err := computer.Compute(context.Background())
			require.NoError(t, err)
			require.Len(t, result[0], 1)
			require.Len(t, result[0][0].Hits, 3)
			gotPKs := []any{result[0][0].Hits[0].PK, result[0][0].Hits[1].PK, result[0][0].Hits[2].PK}
			require.Equal(t, tc.wantPKs, gotPKs)
		})
	}
}

func TestSearchAggregationComputerReadsGroupByFromSeparateChannel(t *testing.T) {
	t.Parallel()
	// group-by (brand, 101) comes from group_by_field_values; metric (price, 103),
	// top_hits sort (stock, 104), and user output (title, 105) from fields_data.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics:     map[string]MetricSpec{"sum_price": {Op: "sum", FieldID: 103, FieldType: schemapb.DataType_Int64}},
			TopHits:     &TopHitsConfig{Size: 2, Sort: []SortCriterion{{FieldID: 104, Dir: "desc"}}},
			Order:       []OrderCriterion{{Key: "_key", Dir: "asc"}},
		}},
		[]int64{105},
		[]int64{103, 104},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		FieldsData: []*schemapb.FieldData{
			testLongFieldData(103, []int64{10, 30, 20, 40}),
			testLongFieldData(104, []int64{100, 200, 300, 400}),
			testStringFieldData(105, []string{"p1", "p2", "p3", "p4"}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "B", "B"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0], 2)

	require.Equal(t, "A", result[0][0].Key[101])
	require.Equal(t, int64(40), result[0][0].Metrics["sum_price"])
	require.Len(t, result[0][0].Hits, 2)
	require.Equal(t, "p2", result[0][0].Hits[0].Fields[105])
	// HitResult.Fields must contain exactly the user-requested output set (105)
	// — neither the metric source (103) nor the top_hits sort field (104) may
	// leak through, regardless of whether they sit in fields_data.
	gotFieldIDs := make([]int64, 0, len(result[0][0].Hits[0].Fields))
	for id := range result[0][0].Hits[0].Fields {
		gotFieldIDs = append(gotFieldIDs, id)
	}
	require.ElementsMatch(t, []int64{105}, gotFieldIDs)

	require.Equal(t, "B", result[0][1].Key[101])
	require.Equal(t, int64(60), result[0][1].Metrics["sum_price"])
	require.Equal(t, "p4", result[0][1].Hits[0].Fields[105])
}

func TestSearchAggregationComputerNormalizesInt32GroupKey(t *testing.T) {
	t.Parallel()
	// A shard returns an int32 group-by column. NormalizeScalar collapses the
	// raw int32 into int64 before hashing, so bucket.Key always surfaces as
	// int64 regardless of the proto-level integer width. Cross-width merging
	// across multiple SearchResultData happens upstream in searchReduceOperator,
	// not in this computer, so this test only pins the normalization contract.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics:     map[string]MetricSpec{"sum_value": {Op: "sum", FieldID: 102, FieldType: schemapb.DataType_Int64}},
			TopHits:     &TopHitsConfig{Size: 100},
		}},
		nil,
		[]int64{102},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.8},
		FieldsData: []*schemapb.FieldData{testLongFieldData(102, []int64{10, 20})},
		GroupByFieldValues: []*schemapb.FieldData{
			{
				FieldId: 101,
				Type:    schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{42, 42}}},
				}},
			},
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 1)
	require.Equal(t, int64(2), result[0][0].Count)
	require.Equal(t, int64(30), result[0][0].Metrics["sum_value"])
	// Post-normalization the group-by key must be int64, not the raw int32.
	_, isInt64 := result[0][0].Key[101].(int64)
	require.True(t, isInt64, "int32 group-by key must be normalized to int64")
}

func TestSearchAggregationComputerNaNDistinctBuckets(t *testing.T) {
	t.Parallel()
	// Two NaN group-by values must NOT merge (NaN != NaN).
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{OwnFieldIDs: []int64{101}, Size: 100, TopHits: &TopHitsConfig{Size: 100}}},
		nil,
		nil,
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.8},
		FieldsData: nil,
		GroupByFieldValues: []*schemapb.FieldData{
			{
				FieldId: 101,
				Type:    schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{
						Data: []float64{math.NaN(), math.NaN()},
					}},
				}},
			},
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 2, "two NaN rows must stay in distinct buckets")
	require.Equal(t, int64(1), result[0][0].Count, "each NaN bucket must hold exactly one row")
	require.Equal(t, int64(1), result[0][1].Count, "each NaN bucket must hold exactly one row")
	// Both bucket keys must carry the original NaN through — a silent
	// normalization into some other non-nil sentinel would still produce 2
	// buckets, so len==2 alone is not enough to pin the contract.
	k0, ok0 := result[0][0].Key[101].(float64)
	require.True(t, ok0, "NaN group key must stay float64")
	require.True(t, math.IsNaN(k0), "bucket 0 key must be NaN")
	k1, ok1 := result[0][1].Key[101].(float64)
	require.True(t, ok1, "NaN group key must stay float64")
	require.True(t, math.IsNaN(k1), "bucket 1 key must be NaN")
}

func TestSearchAggregationComputerNullGrouping(t *testing.T) {
	t.Parallel()
	// Two null group-by values must merge (null == null for grouping).
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{OwnFieldIDs: []int64{101}, Size: 100, TopHits: &TopHitsConfig{Size: 100}}},
		nil,
		nil,
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.8},
		FieldsData: nil,
		GroupByFieldValues: []*schemapb.FieldData{
			{
				FieldId:   101,
				Type:      schemapb.DataType_Int64,
				ValidData: []bool{false, false},
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{0, 0}}},
				}},
			},
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 1, "two null rows must merge into a single bucket")
	require.Equal(t, int64(2), result[0][0].Count)
	// Merged null bucket's key must be nil (extractOwnValues writes nil, not the
	// raw zero value backing the proto LongArray).
	require.Nil(t, result[0][0].Key[101], "null group-by key must surface as nil")
}

func TestSearchAggregationComputerStringMinMax(t *testing.T) {
	t.Parallel()
	// min/max on a VarChar column returns a string through the MetricValue
	// oneof rather than being forced into float64 like the pre-Phase2 code.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics: map[string]MetricSpec{
				"min_title": {Op: "min", FieldID: 102, FieldType: schemapb.DataType_VarChar},
				"max_title": {Op: "max", FieldID: 102, FieldType: schemapb.DataType_VarChar},
			},
			TopHits: &TopHitsConfig{Size: 100},
		}},
		nil,
		[]int64{102},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{3},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		Scores:     []float32{0.9, 0.8, 0.7},
		FieldsData: []*schemapb.FieldData{
			// Non-sorted insertion so "min/max = first/last row" shortcuts would fail.
			testStringFieldData(102, []string{"banana", "cherry", "apple"}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "A"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 1)
	require.Equal(t, "apple", result[0][0].Metrics["min_title"])
	require.Equal(t, "cherry", result[0][0].Metrics["max_title"])
}

func TestSearchAggregationComputerAvgMetric(t *testing.T) {
	t.Parallel()
	// avg expands into (sum, count) under the hood; finalizeMetrics turns
	// that pair back into a float64 ratio.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics:     map[string]MetricSpec{"avg_value": {Op: "avg", FieldID: 102, FieldType: schemapb.DataType_Int64}},
			TopHits:     &TopHitsConfig{Size: 100},
		}},
		nil,
		[]int64{102},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{3},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		Scores:     []float32{0.9, 0.8, 0.7},
		FieldsData: []*schemapb.FieldData{
			// 10+20+31 = 61 / 3 = 20.333… so an accidental integer-division
			// implementation would land on 20 and the test would catch it.
			testLongFieldData(102, []int64{10, 20, 31}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "A"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 1)
	require.InDelta(t, 61.0/3.0, result[0][0].Metrics["avg_value"], 1e-9)
}

func TestSearchAggregationComputerErrorsWhenGroupByMissing(t *testing.T) {
	t.Parallel()
	// Upstream reducer is expected to populate group_by_field_values; if it
	// didn't, Compute() must surface a clear error rather than silently fall
	// back to the fields_data channel.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{OwnFieldIDs: []int64{101}, Size: 100, TopHits: &TopHitsConfig{Size: 100}}},
		nil,
		nil,
	)
	data := &schemapb.SearchResultData{
		NumQueries:         1,
		Topks:              []int64{1},
		Ids:                &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		Scores:             []float32{0.9},
		FieldsData:         nil,
		GroupByFieldValues: nil,
	}

	computer := NewSearchAggregationComputer(data, ctx)
	_, err := computer.Compute(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "group-by field 101 missing from group_by_field_values")
}

func TestSearchAggregationComputerNQMultiple(t *testing.T) {
	t.Parallel()
	// Two independent queries packed into one SearchResultData. computeForQi
	// must slice rows by the per-qi Topks offset (computer.go:79-81) so bucket
	// sets do not bleed across nq.
	ctx := newTestAggregationContext(t, 2,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics:     map[string]MetricSpec{"sum_value": {Op: "sum", FieldID: 102, FieldType: schemapb.DataType_Int64}},
			TopHits:     &TopHitsConfig{Size: 100},
			Order:       []OrderCriterion{{Key: "_key", Dir: "asc"}},
		}},
		nil,
		[]int64{102},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 2,
		Topks:      []int64{2, 3},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		FieldsData: []*schemapb.FieldData{
			testLongFieldData(102, []int64{10, 20, 30, 40, 50}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			// qi=0: [A,A]   qi=1: [B,B,C]
			testStringFieldData(101, []string{"A", "A", "B", "B", "C"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 2)

	// qi=0: single bucket A, count=2, sum=10+20=30
	require.Len(t, result[0], 1)
	require.Equal(t, "A", result[0][0].Key[101])
	require.Equal(t, int64(2), result[0][0].Count)
	require.Equal(t, int64(30), result[0][0].Metrics["sum_value"])

	// qi=1: two buckets B(count=2,sum=70) and C(count=1,sum=50), _key asc
	require.Len(t, result[1], 2)
	require.Equal(t, "B", result[1][0].Key[101])
	require.Equal(t, int64(2), result[1][0].Count)
	require.Equal(t, int64(70), result[1][0].Metrics["sum_value"])
	require.Equal(t, "C", result[1][1].Key[101])
	require.Equal(t, int64(1), result[1][1].Count)
	require.Equal(t, int64(50), result[1][1].Metrics["sum_value"])
}

func TestSearchAggregationComputerMultiLevelNested(t *testing.T) {
	t.Parallel()
	// Two levels: brand (101) → color (102). Exercises the recursive
	// computeLevel branch (computer.go:153-159) that builds SubGroups.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{
			{
				OwnFieldIDs: []int64{101},
				Size:        100,
				Order:       []OrderCriterion{{Key: "_key", Dir: "asc"}},
			},
			{
				OwnFieldIDs: []int64{102},
				Size:        100,
				Metrics:     map[string]MetricSpec{"sum_price": {Op: "sum", FieldID: 103, FieldType: schemapb.DataType_Int64}},
				TopHits:     &TopHitsConfig{Size: 10},
				Order:       []OrderCriterion{{Key: "_key", Dir: "asc"}},
			},
		},
		nil,
		[]int64{103},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		FieldsData: []*schemapb.FieldData{
			testLongFieldData(103, []int64{10, 20, 30, 40}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "A", "B"}),          // brand
			testStringFieldData(102, []string{"red", "red", "blue", "red"}), // color
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0], 2)

	// Bucket A → sub-buckets blue(1 row sum=30), red(2 rows sum=30), _key asc.
	bucketA := result[0][0]
	require.Equal(t, "A", bucketA.Key[101])
	require.Equal(t, int64(3), bucketA.Count)
	require.Len(t, bucketA.SubAggBuckets, 2)

	aBlue := bucketA.SubAggBuckets[0]
	require.Equal(t, "blue", aBlue.Key[102])
	require.Equal(t, int64(1), aBlue.Count)
	require.Equal(t, int64(30), aBlue.Metrics["sum_price"])
	// A/blue came from row 2 (PK=3, score=0.7). The leaf-level TopHits must
	// emit that row — a bug assigning hits to the wrong sub-bucket would land
	// a PK from A/red here.
	require.Len(t, aBlue.Hits, 1)
	require.Equal(t, int64(3), aBlue.Hits[0].PK)
	require.InDelta(t, float32(0.7), aBlue.Hits[0].Score, 1e-6)
	require.Empty(t, aBlue.Hits[0].Fields, "no user output fields were requested")

	aRed := bucketA.SubAggBuckets[1]
	require.Equal(t, "red", aRed.Key[102])
	require.Equal(t, int64(2), aRed.Count)
	require.Equal(t, int64(30), aRed.Metrics["sum_price"])
	// A/red has rows 0 (PK=1, score=0.9) and 1 (PK=2, score=0.8). No Sort
	// configured on leaf TopHits, so the default score/PK fallback applies.
	require.Len(t, aRed.Hits, 2)
	require.Equal(t, int64(1), aRed.Hits[0].PK)
	require.InDelta(t, float32(0.9), aRed.Hits[0].Score, 1e-6)
	require.Equal(t, int64(2), aRed.Hits[1].PK)
	require.InDelta(t, float32(0.8), aRed.Hits[1].Score, 1e-6)

	// Bucket B → single sub-bucket red (row 3, PK=4, score=0.6).
	bucketB := result[0][1]
	require.Equal(t, "B", bucketB.Key[101])
	require.Len(t, bucketB.SubAggBuckets, 1)
	bRed := bucketB.SubAggBuckets[0]
	require.Equal(t, "red", bRed.Key[102])
	require.Equal(t, int64(40), bRed.Metrics["sum_price"])
	require.Len(t, bRed.Hits, 1)
	require.Equal(t, int64(4), bRed.Hits[0].PK)
	require.InDelta(t, float32(0.6), bRed.Hits[0].Score, 1e-6)
}

func TestSearchAggregationComputerCompositeKey(t *testing.T) {
	t.Parallel()
	// Single level, two-field composite key (brand, color). Verifies the
	// hash-chain collision path and compareBucketKeys lexicographic sort.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101, 102},
			Size:        100,
			Order:       []OrderCriterion{{Key: "_key", Dir: "asc"}},
		}},
		nil,
		nil,
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "A", "B"}),
			testStringFieldData(102, []string{"red", "red", "blue", "red"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 3, "three distinct (brand,color) composites expected")

	// _key asc across (101, 102): (A,blue) < (A,red) < (B,red).
	require.Equal(t, "A", result[0][0].Key[101])
	require.Equal(t, "blue", result[0][0].Key[102])
	require.Equal(t, int64(1), result[0][0].Count)

	require.Equal(t, "A", result[0][1].Key[101])
	require.Equal(t, "red", result[0][1].Key[102])
	require.Equal(t, int64(2), result[0][1].Count)

	require.Equal(t, "B", result[0][2].Key[101])
	require.Equal(t, "red", result[0][2].Key[102])
	require.Equal(t, int64(1), result[0][2].Count)
}

func TestSearchAggregationComputerSizeTruncation(t *testing.T) {
	t.Parallel()
	// Size=2 must truncate the ordered bucket list (order.go:31-33). Critically,
	// the input row order is inverted relative to the expected sort order
	// (C inserted first, then B, then A) so a bug that truncated BEFORE
	// sorting would keep [C, B] and drop A — catching the swap.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        2,
			Order:       []OrderCriterion{{Key: "_count", Dir: "desc"}, {Key: "_key", Dir: "asc"}},
		}},
		nil,
		nil,
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{6},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
		GroupByFieldValues: []*schemapb.FieldData{
			// Insertion order: C (row 0), B (rows 1,2), A (rows 3,4,5).
			// Final _count desc order: A(3), B(2), C(1). Truncate to 2: [A, B].
			testStringFieldData(101, []string{"C", "B", "B", "A", "A", "A"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 2, "Size=2 must truncate C out after sorting")
	require.Equal(t, "A", result[0][0].Key[101], "sort-then-slice must keep A even though it was inserted last")
	require.Equal(t, int64(3), result[0][0].Count)
	require.Equal(t, "B", result[0][1].Key[101])
	require.Equal(t, int64(2), result[0][1].Count)
}

func TestSearchAggregationComputerCountAll(t *testing.T) {
	t.Parallel()
	// count(*) uses CountAllFieldID (0) + DataType_None, with a synthetic
	// always-1 input (computer.go:338-340). No fields_data entry needed.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics: map[string]MetricSpec{
				"n_docs": {Op: "count", FieldID: CountAllFieldID, FieldType: schemapb.DataType_None},
			},
			Order: []OrderCriterion{{Key: "_key", Dir: "asc"}},
		}},
		nil,
		nil,
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "A", "B"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 2)
	require.Equal(t, int64(3), result[0][0].Count)
	require.Equal(t, int64(1), result[0][1].Count)
	// Pin the static type, not just numeric equivalence: internal/agg's count
	// aggregator returns int64, and downstream consumers rely on that shape.
	n0, ok0 := result[0][0].Metrics["n_docs"].(int64)
	require.True(t, ok0, "count(*) metric must be int64, got %T", result[0][0].Metrics["n_docs"])
	require.Equal(t, int64(3), n0)
	n1, ok1 := result[0][1].Metrics["n_docs"].(int64)
	require.True(t, ok1, "count(*) metric must be int64, got %T", result[0][1].Metrics["n_docs"])
	require.Equal(t, int64(1), n1)
}

func TestSearchAggregationComputerScoreMetric(t *testing.T) {
	t.Parallel()
	// A metric whose source is the _score column (ScoreFieldID=-1). Exercises
	// the dedicated read branch in readValueByFieldID (computer.go:390-395).
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics: map[string]MetricSpec{
				"max_score": {Op: "max", FieldID: ScoreFieldID, FieldType: schemapb.DataType_Float},
			},
			Order: []OrderCriterion{{Key: "_key", Dir: "asc"}},
		}},
		nil,
		nil,
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.3, 0.7, 0.2},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "B", "B"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 2)
	require.InDelta(t, float32(0.9), result[0][0].Metrics["max_score"], 1e-6)
	require.InDelta(t, float32(0.7), result[0][1].Metrics["max_score"], 1e-6)
}

func TestSearchAggregationComputerOrderByMetricAlias(t *testing.T) {
	t.Parallel()
	// Ordering by a metric alias (not _count / _key). compareBucketByCriterion
	// must route to the metrics map (order.go:61-67).
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics:     map[string]MetricSpec{"sum_value": {Op: "sum", FieldID: 102, FieldType: schemapb.DataType_Int64}},
			Order:       []OrderCriterion{{Key: "sum_value", Dir: "desc"}},
		}},
		nil,
		[]int64{102},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		FieldsData: []*schemapb.FieldData{
			testLongFieldData(102, []int64{1, 1, 50, 60}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			// A sum=2, B sum=110: B must come first under desc ordering.
			testStringFieldData(101, []string{"A", "A", "B", "B"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 2)
	require.Equal(t, "B", result[0][0].Key[101])
	require.Equal(t, int64(110), result[0][0].Metrics["sum_value"])
	require.Equal(t, "A", result[0][1].Key[101])
	require.Equal(t, int64(2), result[0][1].Metrics["sum_value"])
}

func TestSearchAggregationComputerNumericMinMax(t *testing.T) {
	t.Parallel()
	// min/max on Int64 must route through the numeric comparator (not the
	// string path). Two buckets with non-overlapping value ranges verify
	// per-bucket metric state isolation — a bug sharing the accumulator across
	// buckets would collapse both buckets to the global min/max.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics: map[string]MetricSpec{
				"min_v": {Op: "min", FieldID: 102, FieldType: schemapb.DataType_Int64},
				"max_v": {Op: "max", FieldID: 102, FieldType: schemapb.DataType_Int64},
			},
			Order: []OrderCriterion{{Key: "_key", Dir: "asc"}},
		}},
		nil,
		[]int64{102},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{6},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
		FieldsData: []*schemapb.FieldData{
			// A rows non-sorted (30,10,40,20) so first/last shortcuts fail.
			// B rows span a disjoint range (5..100) so a shared accumulator
			// bug would surface as A's min dropping to 5 or B's max rising to 40.
			testLongFieldData(102, []int64{30, 10, 40, 20, 5, 100}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "A", "A", "B", "B"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 2)
	require.Equal(t, "A", result[0][0].Key[101])
	require.Equal(t, int64(10), result[0][0].Metrics["min_v"])
	require.Equal(t, int64(40), result[0][0].Metrics["max_v"])
	require.Equal(t, "B", result[0][1].Key[101])
	require.Equal(t, int64(5), result[0][1].Metrics["min_v"])
	require.Equal(t, int64(100), result[0][1].Metrics["max_v"])
}

func TestSearchAggregationComputerNullMetricSkipped(t *testing.T) {
	t.Parallel()
	// Rows whose metric source is null must be skipped by updateMetrics
	// (computer.go:349-352) but still counted in bucket.count. Use two buckets
	// with different null patterns so a bug that shared metric state across
	// buckets (or double-counted null rows globally) would diverge from both
	// per-bucket expectations.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics:     map[string]MetricSpec{"sum_v": {Op: "sum", FieldID: 102, FieldType: schemapb.DataType_Int64}},
			Order:       []OrderCriterion{{Key: "_key", Dir: "asc"}},
		}},
		nil,
		[]int64{102},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{6},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
		FieldsData: []*schemapb.FieldData{
			// A: rows 0,1,2 with values 10/20/30, null mask  t,f,t → sum=10+30=40
			// B: rows 3,4,5 with values 40/50/60, null mask  f,t,t → sum=50+60=110
			testNullableLongFieldData(102, []int64{10, 20, 30, 40, 50, 60}, []bool{true, false, true, false, true, true}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "A", "B", "B", "B"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 2)
	require.Equal(t, "A", result[0][0].Key[101])
	require.Equal(t, int64(3), result[0][0].Count, "null metric row still contributes to bucket count")
	require.Equal(t, int64(40), result[0][0].Metrics["sum_v"], "A: null row 1 must be skipped from sum")
	require.Equal(t, "B", result[0][1].Key[101])
	require.Equal(t, int64(3), result[0][1].Count)
	require.Equal(t, int64(110), result[0][1].Metrics["sum_v"], "B: null row 3 must be skipped from sum")
}

func TestSearchAggregationComputerAvgAllNull(t *testing.T) {
	t.Parallel()
	// When every row's metric source is null, avg's sum + count slots stay
	// null and finalizeMetrics (computer.go:458-461) yields a nil metric.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics:     map[string]MetricSpec{"avg_v": {Op: "avg", FieldID: 102, FieldType: schemapb.DataType_Int64}},
		}},
		nil,
		[]int64{102},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.8},
		FieldsData: []*schemapb.FieldData{
			testNullableLongFieldData(102, []int64{0, 0}, []bool{false, false}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A"}),
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 1)
	require.Equal(t, int64(2), result[0][0].Count)
	require.Nil(t, result[0][0].Metrics["avg_v"], "all-null avg must surface as nil")
}

func TestSearchAggregationComputerTopHitsTieBreaker(t *testing.T) {
	t.Parallel()
	// compareRowsForTopHits (computer.go:212-267) falls through a chain when
	// the primary sort key ties: score → PK → ResultIdx → RowIdx. Exercise
	// each reachable step in its own sub-case so a regression at any link in
	// the chain surfaces a distinct failure rather than hiding behind an
	// earlier winner.

	mkCtx := func() *SearchAggregationContext {
		return newTestAggregationContext(t, 1,
			[]LevelContext{{
				OwnFieldIDs: []int64{101},
				Size:        100,
				TopHits:     &TopHitsConfig{Size: 3, Sort: []SortCriterion{{FieldID: 102, Dir: "desc"}}},
			}},
			[]int64{102},
			[]int64{102},
		)
	}

	t.Run("score breaks sort-key tie", func(t *testing.T) {
		t.Parallel()
		// All rows share sort value 100. Scores differ → score desc wins.
		data := &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{3},
			Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10, 20, 30}}}},
			Scores:     []float32{0.5, 0.3, 0.7}, // row2 > row0 > row1
			FieldsData: []*schemapb.FieldData{
				testLongFieldData(102, []int64{100, 100, 100}),
			},
			GroupByFieldValues: []*schemapb.FieldData{
				testStringFieldData(101, []string{"A", "A", "A"}),
			},
		}
		c := NewSearchAggregationComputer(data, mkCtx())
		result, err := c.Compute(context.Background())
		require.NoError(t, err)
		require.Len(t, result[0], 1)
		hits := result[0][0].Hits
		require.Len(t, hits, 3)
		require.InDelta(t, float32(0.7), hits[0].Score, 1e-6, "highest score wins when sort-key ties")
		require.InDelta(t, float32(0.5), hits[1].Score, 1e-6)
		require.InDelta(t, float32(0.3), hits[2].Score, 1e-6)
	})

	t.Run("PK breaks score tie", func(t *testing.T) {
		t.Parallel()
		// All rows share sort value AND score. Only PK differs, so ComparePK
		// (int64: a<b) must fire — ascending PK wins. This exercises the
		// branch the previous test hid behind the score comparator.
		data := &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{3},
			// PKs deliberately non-ascending-in-row-order to prove the sort
			// drives ordering, not row iteration.
			Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{30, 10, 20}}}},
			Scores: []float32{0.5, 0.5, 0.5},
			FieldsData: []*schemapb.FieldData{
				testLongFieldData(102, []int64{100, 100, 100}),
			},
			GroupByFieldValues: []*schemapb.FieldData{
				testStringFieldData(101, []string{"A", "A", "A"}),
			},
		}
		c := NewSearchAggregationComputer(data, mkCtx())
		result, err := c.Compute(context.Background())
		require.NoError(t, err)
		require.Len(t, result[0], 1)
		hits := result[0][0].Hits
		require.Len(t, hits, 3)
		require.Equal(t, int64(10), hits[0].PK, "smallest PK first per ComparePK(a<b)")
		require.Equal(t, int64(20), hits[1].PK)
		require.Equal(t, int64(30), hits[2].PK)
	})
}

func TestSearchAggregationComputerErrorPaths(t *testing.T) {
	t.Parallel()
	// Covers the Compute-entry and readValueByFieldID guard rails that the
	// existing GroupByMissing test does not exercise.

	goodData := func() *schemapb.SearchResultData {
		return &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{1},
			Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			Scores:     []float32{0.9},
			GroupByFieldValues: []*schemapb.FieldData{
				testStringFieldData(101, []string{"A"}),
			},
		}
	}

	t.Run("nil ctx", func(t *testing.T) {
		t.Parallel()
		c := NewSearchAggregationComputer(goodData(), nil)
		_, err := c.Compute(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "context is nil")
	})

	t.Run("empty levels", func(t *testing.T) {
		t.Parallel()
		ctx := newTestAggregationContext(t, 1, nil, nil, nil)
		c := NewSearchAggregationComputer(goodData(), ctx)
		_, err := c.Compute(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "no levels")
	})

	t.Run("nq mismatch with topks", func(t *testing.T) {
		t.Parallel()
		// ctx claims nq=2 but data.Topks has length 1 → invalid qi=1 path.
		ctx := newTestAggregationContext(t, 2,
			[]LevelContext{{OwnFieldIDs: []int64{101}, Size: 100}},
			nil,
			nil,
		)
		c := NewSearchAggregationComputer(goodData(), ctx)
		_, err := c.Compute(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid qi")
	})

	t.Run("non-group-by field missing from fields_data", func(t *testing.T) {
		t.Parallel()
		ctx := newTestAggregationContext(t, 1,
			[]LevelContext{{
				OwnFieldIDs: []int64{101},
				Size:        100,
				// Declare a metric whose source field 999 is NOT provided in
				// fields_data; readValueByFieldID must report the missing field.
				Metrics: map[string]MetricSpec{
					"sum_v": {Op: "sum", FieldID: 999, FieldType: schemapb.DataType_Int64},
				},
			}},
			nil,
			[]int64{999},
		)
		c := NewSearchAggregationComputer(goodData(), ctx)
		_, err := c.Compute(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "field 999 missing from fields_data")
	})
}

func TestCompareValuesErrorPaths(t *testing.T) {
	t.Parallel()
	// Direct unit test of the package-private compareValues contract. The two
	// error branches (computer.go:520, :546) feed every sort comparator in
	// this package — if either one silently returns 0 instead of an error,
	// top_hits sort and bucket ordering produce undefined output under bad
	// inputs. Exercise them in isolation because crafting a mismatch through
	// the Compute() surface requires data that violates FieldData type
	// invariants, which is not something production callers can synthesize.

	t.Run("large int64 values keep integer precision", func(t *testing.T) {
		t.Parallel()
		cmp, err := compareValues(int64(1<<53), int64(1<<53+1))
		require.NoError(t, err)
		require.Equal(t, -1, cmp)
	})

	t.Run("large uint64 values keep integer precision", func(t *testing.T) {
		t.Parallel()
		cmp, err := compareValues(uint64(1<<63), uint64(1<<63+1))
		require.NoError(t, err)
		require.Equal(t, -1, cmp)
	})

	t.Run("mixed numeric types surface type mismatch", func(t *testing.T) {
		t.Parallel()
		_, err := compareValues(int64(1), float64(1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "type mismatch")
		require.Contains(t, err.Error(), "int64")
		require.Contains(t, err.Error(), "float64")
	})

	t.Run("numeric vs string surfaces type mismatch", func(t *testing.T) {
		t.Parallel()
		_, err := compareValues(int64(5), "hello")
		require.Error(t, err)
		require.Contains(t, err.Error(), "type mismatch")
		require.Contains(t, err.Error(), "int64")
		require.Contains(t, err.Error(), "string")
	})

	t.Run("both sides unsupported surfaces unsupported error", func(t *testing.T) {
		t.Parallel()
		_, err := compareValues(struct{}{}, struct{}{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported comparable types")
	})
}

func TestSearchAggregationComputerTopHitsDefaultSortUsesScoreAndPK(t *testing.T) {
	t.Parallel()

	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			TopHits:     &TopHitsConfig{Size: 3},
		}},
		nil,
		nil,
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{5},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{30, 10, 40, 20, 50}}}},
		Scores:     []float32{0.2, 0.9, 0.8, 0.9, 0.1},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "A", "A", "A"}),
		},
	}

	result, err := NewSearchAggregationComputer(data, ctx).Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 1)
	require.Len(t, result[0][0].Hits, 3)
	require.Equal(t, []any{int64(10), int64(20), int64(40)}, []any{
		result[0][0].Hits[0].PK,
		result[0][0].Hits[1].PK,
		result[0][0].Hits[2].PK,
	})
}

func TestSearchAggregationComputerTopHitsMultiSort(t *testing.T) {
	t.Parallel()

	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			TopHits: &TopHitsConfig{Size: 3, Sort: []SortCriterion{
				{FieldID: 102, Dir: "asc"},
				{FieldID: 103, Dir: "desc"},
			}},
		}},
		nil,
		[]int64{102, 103},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.1, 0.2, 0.3, 0.4},
		FieldsData: []*schemapb.FieldData{
			testLongFieldData(102, []int64{10, 10, 5, 10}),
			testLongFieldData(103, []int64{1, 3, 9, 2}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "A", "A"}),
		},
	}

	result, err := NewSearchAggregationComputer(data, ctx).Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 1)
	require.Len(t, result[0][0].Hits, 3)
	require.Equal(t, []any{int64(3), int64(2), int64(4)}, []any{
		result[0][0].Hits[0].PK,
		result[0][0].Hits[1].PK,
		result[0][0].Hits[2].PK,
	})
}

func TestSearchAggregationComputerParentAndChildTopHits(t *testing.T) {
	t.Parallel()

	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{
			{
				OwnFieldIDs: []int64{101},
				Size:        100,
				TopHits:     &TopHitsConfig{Size: 2, Sort: []SortCriterion{{FieldID: 103, Dir: "desc"}}},
				Order:       []OrderCriterion{{Key: "_key", Dir: "asc"}},
			},
			{
				OwnFieldIDs: []int64{102},
				Size:        100,
				TopHits:     &TopHitsConfig{Size: 1, Sort: []SortCriterion{{FieldID: 104, Dir: "asc"}}},
				Order:       []OrderCriterion{{Key: "_key", Dir: "asc"}},
			},
		},
		nil,
		[]int64{103, 104},
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{5},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		FieldsData: []*schemapb.FieldData{
			testLongFieldData(103, []int64{50, 10, 30, 40, 20}),
			testLongFieldData(104, []int64{5, 1, 3, 4, 2}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testStringFieldData(101, []string{"A", "A", "A", "B", "B"}),
			testStringFieldData(102, []string{"red", "blue", "red", "red", "blue"}),
		},
	}

	result, err := NewSearchAggregationComputer(data, ctx).Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 2)

	brandA := result[0][0]
	require.Equal(t, "A", brandA.Key[101])
	require.Equal(t, []any{int64(1), int64(3)}, []any{brandA.Hits[0].PK, brandA.Hits[1].PK})
	require.Len(t, brandA.SubAggBuckets, 2)
	require.Equal(t, "blue", brandA.SubAggBuckets[0].Key[102])
	require.Equal(t, []any{int64(2)}, []any{brandA.SubAggBuckets[0].Hits[0].PK})
	require.Equal(t, "red", brandA.SubAggBuckets[1].Key[102])
	require.Equal(t, []any{int64(3)}, []any{brandA.SubAggBuckets[1].Hits[0].PK})

	brandB := result[0][1]
	require.Equal(t, "B", brandB.Key[101])
	require.Equal(t, []any{int64(4), int64(5)}, []any{brandB.Hits[0].PK, brandB.Hits[1].PK})
	require.Len(t, brandB.SubAggBuckets, 2)
	require.Equal(t, "blue", brandB.SubAggBuckets[0].Key[102])
	require.Equal(t, []any{int64(5)}, []any{brandB.SubAggBuckets[0].Hits[0].PK})
	require.Equal(t, "red", brandB.SubAggBuckets[1].Key[102])
	require.Equal(t, []any{int64(4)}, []any{brandB.SubAggBuckets[1].Hits[0].PK})
}

func TestSearchAggregationComputerInterleavedNullGroupBy(t *testing.T) {
	t.Parallel()
	// ValidData interleaved across a group-by column (not all-null like
	// TestSearchAggregationComputerNullGrouping). Nulls must merge only with
	// other nulls regardless of row position; "A" rows straddling a null row
	// must not be split across buckets by the interleaving.
	ctx := newTestAggregationContext(t, 1,
		[]LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Order:       []OrderCriterion{{Key: "_count", Dir: "desc"}, {Key: "_key", Dir: "asc"}},
		}},
		nil,
		nil,
	)

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{5},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		GroupByFieldValues: []*schemapb.FieldData{
			// Row sequence:  A | null | B | null | A
			// ValidData:     t |  f   | t |  f   | t
			{
				FieldId:   101,
				Type:      schemapb.DataType_VarChar,
				ValidData: []bool{true, false, true, false, true},
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{
						Data: []string{"A", "", "B", "", "A"},
					}},
				}},
			},
		},
	}

	computer := NewSearchAggregationComputer(data, ctx)
	result, err := computer.Compute(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0], 3, "3 distinct buckets: null, A, B — nulls must not merge with A")

	// Order: _count desc, _key asc. null(2) and A(2) tie on count; compareValues
	// sorts nil before any non-nil value, so null bucket comes first. B(1) last.
	require.Nil(t, result[0][0].Key[101], "null rows must form a single bucket with nil Key")
	require.Equal(t, int64(2), result[0][0].Count)

	require.Equal(t, "A", result[0][1].Key[101])
	require.Equal(t, int64(2), result[0][1].Count, "two A rows separated by null rows must still share one bucket")

	require.Equal(t, "B", result[0][2].Key[101])
	require.Equal(t, int64(1), result[0][2].Count)
}

func testStringFieldData(fieldID int64, values []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: fieldID,
		Type:    schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}},
		}},
	}
}

func testLongFieldData(fieldID int64, values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: fieldID,
		Type:    schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
		}},
	}
}

func testNullableLongFieldData(fieldID int64, values []int64, validData []bool) *schemapb.FieldData {
	fd := testLongFieldData(fieldID, values)
	fd.ValidData = validData
	return fd
}

func testInt32FieldData(fieldID int64, values []int32) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: fieldID,
		Type:    schemapb.DataType_Int32,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: values}},
		}},
	}
}
