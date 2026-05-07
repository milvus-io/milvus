package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
)

type SearchReduceUtilTestSuite struct {
	suite.Suite
}

func genTestDataSearchResultsData() []*schemapb.SearchResultData {
	var searchResultData1 *schemapb.SearchResultData
	var searchResultData2 *schemapb.SearchResultData

	{
		groupFieldValue := []string{"aaa", "bbb", "ccc", "bbb", "bbb", "ccc", "aaa", "ccc", "aaa"}
		searchResultData1 = &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"7", "5", "4", "2", "3", "6", "1", "9", "8"},
					},
				},
			},
			Topks:             []int64{9},
			Scores:            []float32{0.6, 0.53, 0.52, 0.43, 0.41, 0.33, 0.30, 0.27, 0.22},
			GroupByFieldValue: getFieldData("string", int64(101), schemapb.DataType_VarChar, groupFieldValue, 1),
		}
	}

	{
		groupFieldValue := []string{"www", "aaa", "ccc", "www", "www", "ccc", "aaa", "ccc", "aaa"}
		searchResultData2 = &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{
						Data: []string{"17", "15", "14", "12", "13", "16", "11", "19", "18"},
					},
				},
			},
			Topks:             []int64{9},
			Scores:            []float32{0.7, 0.43, 0.32, 0.32, 0.31, 0.31, 0.30, 0.30, 0.30},
			GroupByFieldValue: getFieldData("string", int64(101), schemapb.DataType_VarChar, groupFieldValue, 1),
		}
	}
	return []*schemapb.SearchResultData{searchResultData1, searchResultData2}
}

func (struts *SearchReduceUtilTestSuite) TestReduceSearchResult() {
	data := genTestDataSearchResultsData()

	{
		results, err := reduceSearchResultDataNoGroupBy(context.Background(), []*schemapb.SearchResultData{data[0]}, 0, 0, "L2", schemapb.DataType_Int64, 0)
		struts.NoError(err)
		struts.Equal([]string{"7", "5", "4", "2", "3", "6", "1", "9", "8"}, results.Results.GetIds().GetStrId().Data)
	}
}

func (struts *SearchReduceUtilTestSuite) TestReduceSearchResultWithGroupByPreservesAllSearchCount() {
	shardA := &schemapb.SearchResultData{
		NumQueries:     1,
		TopK:           2,
		Topks:          []int64{2},
		Ids:            &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:         []float32{0.9, 0.8},
		AllSearchCount: 11,
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "B"}),
		},
	}
	shardB := &schemapb.SearchResultData{
		NumQueries:     1,
		TopK:           2,
		Topks:          []int64{2},
		Ids:            &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3, 4}}}},
		Scores:         []float32{0.7, 0.6},
		AllSearchCount: 13,
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"C", "D"}),
		},
	}

	results, err := reduceSearchResultDataWithGroupBy(context.Background(), []*schemapb.SearchResultData{shardA, shardB},
		1, 2, metric.IP, schemapb.DataType_Int64, 0, 1, []int64{101}, false)

	struts.Require().NoError(err)
	struts.Equal(int64(24), results.GetResults().GetAllSearchCount())
}

func (struts *SearchReduceUtilTestSuite) TestReduceSearchResultWithEmptyGroupData() {
	nq := int64(1)
	topk := int64(1)
	emptyData := &schemapb.SearchResultData{
		NumQueries:       nq,
		TopK:             topk,
		FieldsData:       make([]*schemapb.FieldData, 0),
		Scores:           make([]float32, 0),
		Ids:              &schemapb.IDs{},
		Topks:            make([]int64, 0),
		OutputFields:     make([]string, 0),
		AllSearchCount:   0,
		Distances:        make([]float32, 0),
		Recalls:          make([]float32, 0),
		PrimaryFieldName: "",
	}
	results, err := reduceSearchResultDataWithGroupBy(context.Background(), []*schemapb.SearchResultData{emptyData},
		nq, topk, "L2", schemapb.DataType_Int64, 0, 1, []int64{101}, false)
	struts.Require().NoError(err)
	struts.Equal([]int64{0}, results.GetResults().GetTopks())
	struts.Empty(results.Results.GetGroupByFieldValues(),
		"reducer must not populate the plural channel when no group-by column was present in any shard")
}

// TestReduceWithEmptyFieldsData tests reduce functions when FieldsData is empty (requery scenario)
func (struts *SearchReduceUtilTestSuite) TestReduceWithEmptyFieldsData() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(5)
	offset := int64(0)

	// Create search results with empty FieldsData (simulating requery scenario)
	searchResultData1 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5},
				},
			},
		},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6, 0.5},
		Topks:      []int64{5},
		NumQueries: nq,
		TopK:       topK,
		FieldsData: []*schemapb.FieldData{}, // Empty FieldsData for requery
	}

	searchResultData2 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{6, 7, 8, 9, 10},
				},
			},
		},
		Scores:     []float32{0.85, 0.75, 0.65, 0.55, 0.45},
		Topks:      []int64{5},
		NumQueries: nq,
		TopK:       topK,
		FieldsData: []*schemapb.FieldData{}, // Empty FieldsData for requery
	}

	// Test reduceSearchResultDataNoGroupBy with empty FieldsData
	{
		results, err := reduceSearchResultDataNoGroupBy(ctx, []*schemapb.SearchResultData{searchResultData1, searchResultData2}, nq, topK, "L2", schemapb.DataType_Int64, offset)
		struts.NoError(err)
		struts.NotNil(results)
		// Should have merged results without panic
		struts.Equal(int64(5), results.Results.Topks[0])
		// FieldsData should be empty since all inputs were empty
		struts.Equal(0, len(results.Results.FieldsData))
	}

	// Test reduceSearchResultDataWithSingleGroupBy with empty FieldsData
	{
		// Add GroupByFieldValue to support group by
		searchResultData1.GroupByFieldValue = &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: "group",
			FieldId:   101,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"a", "b", "c", "a", "b"},
						},
					},
				},
			},
		}
		searchResultData2.GroupByFieldValue = &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: "group",
			FieldId:   101,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"c", "a", "b", "c", "a"},
						},
					},
				},
			},
		}

		results, err := reduceSearchResultDataWithGroupBy(ctx, []*schemapb.SearchResultData{searchResultData1, searchResultData2}, nq, topK, "L2", schemapb.DataType_Int64, offset, int64(2), []int64{101}, false)
		struts.NoError(err)
		struts.NotNil(results)
		// FieldsData should be empty since all inputs were empty
		struts.Equal(0, len(results.Results.FieldsData))
	}

	// Test reduceAdvanceGroupBy with empty FieldsData
	{
		results, err := reduceAdvanceGroupBy(ctx, []*schemapb.SearchResultData{searchResultData1, searchResultData2}, nq, topK, schemapb.DataType_Int64, "L2", []int64{101})
		struts.NoError(err)
		struts.NotNil(results)
		// FieldsData should be empty since all inputs were empty
		struts.Equal(0, len(results.Results.FieldsData))
	}
}

// TestReduceWithPartialEmptyFieldsData tests when first result has empty FieldsData but second has data
func (struts *SearchReduceUtilTestSuite) TestReduceWithPartialEmptyFieldsData() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(3)
	offset := int64(0)

	// First result with empty FieldsData
	searchResultData1 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3},
				},
			},
		},
		Scores:     []float32{0.9, 0.8, 0.7},
		Topks:      []int64{3},
		NumQueries: nq,
		TopK:       topK,
		FieldsData: []*schemapb.FieldData{}, // Empty
	}

	// Second result with non-empty FieldsData
	searchResultData2 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{4, 5, 6},
				},
			},
		},
		Scores:     []float32{0.85, 0.75, 0.65},
		Topks:      []int64{3},
		NumQueries: nq,
		TopK:       topK,
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "field1",
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{40, 50, 60},
							},
						},
					},
				},
			},
		},
	}

	// Test: Should use the non-empty FieldsData from second result
	results, err := reduceSearchResultDataNoGroupBy(ctx, []*schemapb.SearchResultData{searchResultData1, searchResultData2}, nq, topK, "L2", schemapb.DataType_Int64, offset)
	struts.NoError(err)
	struts.NotNil(results)
	// Should have initialized FieldsData from second result
	struts.Greater(len(results.Results.FieldsData), 0)
}

func (s *SearchReduceUtilTestSuite) TestReduceMaxSimMetricsComparison() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(4)
	offset := int64(0)

	// For positively related metrics (higher is better):
	// Segment 1: scores 10, 8
	// Segment 2: scores 9, 7
	// Expected merge: 10, 9, 8, 7 (IDs: 1, 3, 2, 4)
	positiveSegment1 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		},
		Scores:     []float32{10, 8},
		Topks:      []int64{2},
		NumQueries: nq,
		TopK:       topK,
		FieldsData: []*schemapb.FieldData{},
	}

	positiveSegment2 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{3, 4}},
			},
		},
		Scores:     []float32{9, 7},
		Topks:      []int64{2},
		NumQueries: nq,
		TopK:       topK,
		FieldsData: []*schemapb.FieldData{},
	}

	// Test positively related MAX_SIM metrics
	positiveMetrics := []string{"MAX_SIM", "MAX_SIM_IP", "MAX_SIM_COSINE"}
	for _, metricType := range positiveMetrics {
		results, err := reduceSearchResultDataNoGroupBy(ctx,
			[]*schemapb.SearchResultData{positiveSegment1, positiveSegment2},
			nq, topK, metricType, schemapb.DataType_Int64, offset)

		s.NoError(err, "metric: %s", metricType)
		s.Equal([]int64{1, 3, 2, 4}, results.Results.GetIds().GetIntId().GetData(),
			"metric %s: IDs should be merged correctly", metricType)
		s.Equal([]float32{10, 9, 8, 7}, results.Results.GetScores(),
			"metric %s: Scores should be in descending order", metricType)
	}

	// For negatively related metrics (lower is better):
	// Input scores are negated internally, so we provide negative scores
	// Segment 1: scores -1, -3 (representing distances 1, 3)
	// Segment 2: scores -2, -4 (representing distances 2, 4)
	// Internal merge uses negated values, then negates back
	// Expected final scores: 1, 2, 3, 4 (IDs: 1, 3, 2, 4)
	negativeSegment1 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		},
		Scores:     []float32{-1, -3},
		Topks:      []int64{2},
		NumQueries: nq,
		TopK:       topK,
		FieldsData: []*schemapb.FieldData{},
	}

	negativeSegment2 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{3, 4}},
			},
		},
		Scores:     []float32{-2, -4},
		Topks:      []int64{2},
		NumQueries: nq,
		TopK:       topK,
		FieldsData: []*schemapb.FieldData{},
	}

	// Test negatively related MAX_SIM metrics
	negativeMetrics := []string{"MAX_SIM_L2", "MAX_SIM_HAMMING", "MAX_SIM_JACCARD"}
	for _, metricType := range negativeMetrics {
		results, err := reduceSearchResultDataNoGroupBy(ctx,
			[]*schemapb.SearchResultData{negativeSegment1, negativeSegment2},
			nq, topK, metricType, schemapb.DataType_Int64, offset)

		s.NoError(err, "metric: %s", metricType)
		s.Equal([]int64{1, 3, 2, 4}, results.Results.GetIds().GetIntId().GetData(),
			"metric %s: IDs should be merged correctly", metricType)
		// Scores are negated back for negatively related metrics
		s.Equal([]float32{1, 2, 3, 4}, results.Results.GetScores(),
			"metric %s: Scores should be distances in ascending order", metricType)
	}
}

func (s *SearchReduceUtilTestSuite) TestDecodeSearchResultsSupportsResultDataAndBlob() {
	blobData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       1,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}}},
		Scores:     []float32{0.8},
		Topks:      []int64{1},
	}
	blob, err := proto.Marshal(blobData)
	s.NoError(err)

	resultData := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       1,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{20}}}},
		Scores:     []float32{0.9},
		Topks:      []int64{1},
	}

	decoded, err := decodeSearchResults(context.Background(), []*internalpb.SearchResults{
		{ResultData: resultData},
		{SlicedBlob: blob},
		{},
	})
	s.NoError(err)
	s.Len(decoded, 2)
	s.Same(resultData, decoded[0])
	s.True(proto.Equal(blobData, decoded[1]))
}

// TestReduceAdvanceGroupBy_SingleShardScoreNegation verifies that the
// single-shard early-return path in reduceAdvanceGroupBy applies the same
// metric-direction-aware score negation as the multi-shard path.
//
// segcore (segment_c.cpp:302-307) negates distances for "smaller is better"
// metrics so its internal representation is "larger is better". The proxy
// is responsible for negating again at the reduce stage so downstream code
// sees the metric's natural direction (smaller is better for L2/HAMMING/
// JACCARD; larger is better for COSINE/IP/BM25).
//
// Historically the single-shard early return passed segcore's data through
// unchanged, while the multi-shard path applied negation at the end of the
// function. This created a shard-count-dependent behavior: 1-shard
// collections returned -L2, multi-shard collections returned +L2. Chain
// rerank then applied its normalization (1 − 2·atan(d)/π) to the negated
// values, producing a monotonically inverted score and silently flipping
// the result ordering — only manifesting when the metric is distance-class
// AND group_by is enabled AND the collection has a single shard.
func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_SingleShardScoreNegation() {
	cases := []struct {
		name           string
		metric         string
		inputScores    []float32 // as returned by segcore (negated for distance metrics)
		expectedScores []float32 // after proxy reduce, in metric's natural direction
	}{
		{
			name:           "L2 distances are re-negated to natural smaller=better direction",
			metric:         "L2",
			inputScores:    []float32{-0.1, -0.5, -1.0},
			expectedScores: []float32{0.1, 0.5, 1.0},
		},
		{
			name:           "HAMMING distances are re-negated",
			metric:         "HAMMING",
			inputScores:    []float32{-2, -5, -8},
			expectedScores: []float32{2, 5, 8},
		},
		{
			name:           "JACCARD distances are re-negated",
			metric:         "JACCARD",
			inputScores:    []float32{-0.2, -0.4, -0.6},
			expectedScores: []float32{0.2, 0.4, 0.6},
		},
		{
			name:           "COSINE scores pass through unchanged (positively related)",
			metric:         "COSINE",
			inputScores:    []float32{0.9, 0.7, 0.5},
			expectedScores: []float32{0.9, 0.7, 0.5},
		},
		{
			name:           "IP scores pass through unchanged (positively related)",
			metric:         "IP",
			inputScores:    []float32{2.0, 1.5, 1.0},
			expectedScores: []float32{2.0, 1.5, 1.0},
		},
	}

	for _, tc := range cases {
		struts.Run(tc.name, func() {
			n := int64(len(tc.inputScores))
			data := &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       n,
				Topks:      []int64{n},
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
					},
				},
				// fresh copy so we don't mutate test fixture across runs
				Scores: append([]float32(nil), tc.inputScores...),
				GroupByFieldValue: &schemapb.FieldData{
					Type:      schemapb.DataType_VarChar,
					FieldName: "category",
					FieldId:   101,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"A", "B", "A"},
								},
							},
						},
					},
				},
			}

			result, err := reduceAdvanceGroupBy(context.Background(),
				[]*schemapb.SearchResultData{data}, 1, n, schemapb.DataType_Int64, tc.metric, []int64{101})
			struts.NoError(err)
			struts.NotNil(result)
			struts.Equal(tc.expectedScores, result.Results.GetScores(),
				"%s: single-shard early return must match multi-shard score-direction handling", tc.name)
		})
	}
}

// TestReduceAdvanceGroupBy_SingleShardMatchesMultiShard pins the invariant
// that single-shard and multi-shard paths produce identical score directions
// for the same input. Without the negation fix the single-shard path
// returned -L2 while the multi-shard path returned +L2 — same metric, same
// data, different output, depending only on shard count.
func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_SingleShardMatchesMultiShard() {
	makeShard := func(ids []int64, scores []float32, groups []string) *schemapb.SearchResultData {
		return &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       int64(len(ids)),
			Topks:      []int64{int64(len(ids))},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: append([]int64(nil), ids...)},
				},
			},
			Scores: append([]float32(nil), scores...),
			GroupByFieldValue: &schemapb.FieldData{
				Type:      schemapb.DataType_VarChar,
				FieldName: "category",
				FieldId:   101,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: append([]string(nil), groups...),
							},
						},
					},
				},
			},
		}
	}

	// segcore-negated L2 distances; same data fed to both paths
	ids := []int64{1, 2, 3}
	scores := []float32{-0.1, -0.5, -1.0}
	groups := []string{"A", "B", "A"}

	singleShardResult, err := reduceAdvanceGroupBy(context.Background(),
		[]*schemapb.SearchResultData{makeShard(ids, scores, groups)},
		1, 3, schemapb.DataType_Int64, "L2", []int64{101})
	struts.Require().NoError(err)

	// Force the multi-shard path by passing two identical shards.
	// Both paths should produce scores in the same direction (positive L2).
	multiShardResult, err := reduceAdvanceGroupBy(context.Background(),
		[]*schemapb.SearchResultData{
			makeShard(ids, scores, groups),
			makeShard([]int64{4, 5, 6}, []float32{-0.2, -0.4, -0.8}, []string{"A", "B", "A"}),
		},
		1, 3, schemapb.DataType_Int64, "L2", []int64{101})
	struts.Require().NoError(err)

	// Both paths must produce strictly positive scores (i.e., negation
	// applied). Without the fix, singleShardResult would have negative
	// scores, breaking this invariant.
	for _, s := range singleShardResult.Results.GetScores() {
		struts.GreaterOrEqual(s, float32(0), "single-shard L2 score must be in natural direction (>=0); got %v", s)
	}
	for _, s := range multiShardResult.Results.GetScores() {
		struts.GreaterOrEqual(s, float32(0), "multi-shard L2 score must be in natural direction (>=0); got %v", s)
	}
}

// TestReduceSearchResult_MultiGroupBy_OffsetSemantics pins the offset contract
// for regular multi-groupBy reduce: ResultInfo.Offset skips the first `offset`
// distinct composite keys (score-desc walk order) before acceptance begins; a
// later row belonging to an already-skipped group is also dropped so
// group-level pagination is preserved end-to-end.
// SearchAggregation is not covered here because offset with SearchAggregation
// is rejected at request validation (see initSearchAggregation).
func (struts *SearchReduceUtilTestSuite) TestReduceSearchResult_MultiGroupBy_OffsetSemantics() {
	// Score-desc walk order of distinct composite keys across the two shards:
	//   (A,X) @0.9  (shardA pk=1)      ← distinct #1
	//   (A,X) @0.85 (shardB pk=11)     already seen
	//   (A,Y) @0.7  (shardA pk=2)      ← distinct #2
	//   (B,Y) @0.65 (shardB pk=12)     ← distinct #3
	//   (B,X) @0.5  (shardA pk=3)      ← distinct #4
	//   (C,X) @0.45 (shardB pk=13)     ← distinct #5
	buildShards := func() []*schemapb.SearchResultData {
		shardA := &schemapb.SearchResultData{
			NumQueries: 1, TopK: 3,
			Topks:  []int64{3},
			Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
			Scores: []float32{0.9, 0.7, 0.5},
			GroupByFieldValues: []*schemapb.FieldData{
				multiGroupByTestStringField(101, []string{"A", "A", "B"}),
				multiGroupByTestStringField(102, []string{"X", "Y", "X"}),
			},
		}
		shardB := &schemapb.SearchResultData{
			NumQueries: 1, TopK: 3,
			Topks:  []int64{3},
			Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{11, 12, 13}}}},
			Scores: []float32{0.85, 0.65, 0.45},
			GroupByFieldValues: []*schemapb.FieldData{
				multiGroupByTestStringField(101, []string{"A", "B", "C"}),
				multiGroupByTestStringField(102, []string{"X", "Y", "X"}),
			},
		}
		return []*schemapb.SearchResultData{shardA, shardB}
	}

	runWithOffset := func(offset int64) *schemapb.SearchResultData {
		info := reduce.NewReduceSearchResultInfo(1, 3).
			WithMetricType(metric.IP).
			WithPkType(schemapb.DataType_Int64).
			WithOffset(offset).
			WithGroupSize(2).
			WithGroupByFieldIds([]int64{101, 102})
		ret, err := reduceSearchResult(context.Background(), buildShards(), info)
		struts.Require().NoError(err)
		struts.Require().NotNil(ret)
		return ret.GetResults()
	}

	base := runWithOffset(0)
	withOffset := runWithOffset(1)
	struts.False(proto.Equal(base, withOffset),
		"multi-groupBy with offset=1 must diverge from offset=0 (offset applied)")
	// offset=1 skips the first distinct group (A,X). Rows belonging to (A,X)
	// — shardA pk=1 and shardB pk=11 — must not appear in the output.
	ids := withOffset.GetIds().GetIntId().GetData()
	struts.NotContains(ids, int64(1), "pk=1 belongs to skipped group (A,X)")
	struts.NotContains(ids, int64(11), "pk=11 belongs to skipped group (A,X)")
}

// TestReduceSearchResult_SingleGroupBy_FieldIdStampedByReducer verifies that
// the unified reducer stamps the requested FieldId on its plural-channel
// output for a single-field (N=1) request, overwriting any id the shard's
// source GroupByFieldValue happened to carry. After the Step 3.3 unification
// the reducer always emits to the plural channel regardless of field count,
// so the singular channel must stay empty.
func (struts *SearchReduceUtilTestSuite) TestReduceSearchResult_MultiGroupByEmptyResultsReturnsSuccess() {
	shardA := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{0},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}},
		Scores:     []float32{},
	}
	shardB := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{0},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}},
		Scores:     []float32{},
	}
	info := reduce.NewReduceSearchResultInfo(1, 2).
		WithMetricType(metric.IP).
		WithPkType(schemapb.DataType_Int64).
		WithGroupByFieldIds([]int64{101, 102}).
		WithGroupSize(1)

	ret, err := reduceSearchResult(context.Background(), []*schemapb.SearchResultData{shardA, shardB}, info)

	struts.Require().NoError(err)
	res := ret.GetResults()
	struts.Equal([]int64{0}, res.GetTopks())
	struts.Empty(res.GetScores())
	struts.Empty(res.GetIds().GetIntId().GetData())
	struts.Empty(res.GetGroupByFieldValues())
}

func (struts *SearchReduceUtilTestSuite) TestReduceSearchResult_SingleGroupBy_FieldIdStampedByReducer() {
	const requestedFieldId int64 = 101
	const sourceFieldId int64 = 999 // intentionally != requestedFieldId

	shard := &schemapb.SearchResultData{
		NumQueries: 1, TopK: 2,
		Topks:  []int64{3},
		Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		Scores: []float32{0.9, 0.8, 0.7},
		GroupByFieldValue: &schemapb.FieldData{
			FieldId: sourceFieldId,
			Type:    schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{
					Data: []string{"x", "y", "z"},
				}},
			}},
		},
	}

	info := reduce.NewReduceSearchResultInfo(1, 2).
		WithMetricType(metric.IP).
		WithPkType(schemapb.DataType_Int64).
		WithGroupByFieldIdsFromProto(requestedFieldId, nil).
		WithGroupSize(1)
	ret, err := reduceSearchResult(context.Background(),
		[]*schemapb.SearchResultData{shard}, info)

	struts.Require().NoError(err)
	struts.Require().NotNil(ret)
	// Plural channel carries the group-by column for N=1 after Step 3.3.
	gbvs := ret.GetResults().GetGroupByFieldValues()
	struts.Require().Len(gbvs, 1, "unified reducer must emit exactly one plural-channel column for N=1")
	struts.Equal(requestedFieldId, gbvs[0].GetFieldId(),
		"FieldId must be stamped with the value from WithGroupByFieldIdsFromProto(%d, nil); got %d",
		requestedFieldId, gbvs[0].GetFieldId())
	// Singular channel must stay empty — unified reducer emits plural only.
	struts.Nil(ret.GetResults().GetGroupByFieldValue(),
		"unified reducer must not populate the legacy singular channel")
}

// TestReduceSearchResult_MultiGroupBy_TwoNq_TwoShards_Semantics verifies the
// multi-groupBy path end-to-end with a realistic nq=2 / shards=2 layout.
// A single snapshot locks three independent invariants of the multi path:
//
//  1. Composite-key equality. Groups are keyed by the FULL (brand, category)
//     tuple. Rows sharing only the first field (brand=A but category=X vs Y)
//     must land in distinct groups. findMultiGroupEntry's values-equality
//     chain is the mechanism — any regression that keys by hash only, or by
//     a single field, breaks this test.
//
//  2. GroupSize cap. Once a group has groupSize (=2) accepted rows, further
//     rows for the same group are dropped regardless of score (see pk=2 in
//     nq=0 being skipped while its score 0.8 is higher than later accepted
//     rows).
//
//  3. TopK cap. After topK (=3) distinct groups have been accepted within a
//     given nq, rows belonging to new groups are rejected (pk=13 in nq=0,
//     pk=7 in nq=1).
//
// Per-nq isolation is validated implicitly: nq=1 starts fresh (bucket state
// reset), so groups (A,X)/(A,Y)/(B,X) already "seen" in nq=0 do NOT carry
// over; nq=1 independently fills its own topK budget.
func (struts *SearchReduceUtilTestSuite) TestReduceSearchResult_MultiGroupBy_TwoNq_TwoShards_Semantics() {
	// shardA flat (Topks=[4,3] splits into nq=0 / nq=1):
	//   nq=0: (pk=1,0.9,A,X) (pk=2,0.8,A,X) (pk=3,0.7,A,Y) (pk=4,0.6,B,X)
	//   nq=1: (pk=5,0.95,B,X) (pk=6,0.75,B,Y) (pk=7,0.55,A,X)
	shardA := &schemapb.SearchResultData{
		NumQueries: 2, TopK: 3,
		Topks:  []int64{4, 3},
		Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6, 7}}}},
		Scores: []float32{0.9, 0.8, 0.7, 0.6, 0.95, 0.75, 0.55},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "A", "A", "B", "B", "B", "A"}),
			multiGroupByTestStringField(102, []string{"X", "X", "Y", "X", "X", "Y", "X"}),
		},
	}
	// shardB flat (Topks=[3,2]):
	//   nq=0: (pk=11,0.85,A,X) (pk=12,0.65,A,Y) (pk=13,0.55,C,X)
	//   nq=1: (pk=14,0.90,B,X) (pk=15,0.60,A,Y)
	shardB := &schemapb.SearchResultData{
		NumQueries: 2, TopK: 3,
		Topks:  []int64{3, 2},
		Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{11, 12, 13, 14, 15}}}},
		Scores: []float32{0.85, 0.65, 0.55, 0.90, 0.60},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "A", "C", "B", "A"}),
			multiGroupByTestStringField(102, []string{"X", "Y", "X", "X", "Y"}),
		},
	}

	info := reduce.NewReduceSearchResultInfo(2, 3).
		WithMetricType(metric.IP).
		WithPkType(schemapb.DataType_Int64).
		WithGroupSize(2).
		WithGroupByFieldIds([]int64{101, 102})
	ret, err := reduceSearchResult(context.Background(),
		[]*schemapb.SearchResultData{shardA, shardB}, info)
	struts.Require().NoError(err)
	struts.Require().NotNil(ret)

	// Expected walk traces (score desc within each nq):
	//   nq=0 accepts 5 rows:  (A,X)@0.9, (A,X)@0.85, (A,Y)@0.7, (A,Y)@0.65, (B,X)@0.6
	//        rejects (A,X)@0.8     [(A,X) already full @groupSize=2]
	//        rejects (C,X)@0.55    [topK=3 distinct groups reached]
	//   nq=1 accepts 4 rows:  (B,X)@0.95, (B,X)@0.90, (B,Y)@0.75, (A,Y)@0.60
	//        rejects (A,X)@0.55    [topK=3 distinct groups reached]
	res := ret.GetResults()
	struts.Equal([]int64{5, 4}, res.GetTopks(),
		"per-nq accept counts: nq=0 should accept 5, nq=1 should accept 4")
	struts.Equal([]int64{1, 11, 3, 12, 4, 5, 14, 6, 15},
		res.GetIds().GetIntId().GetData(),
		"rows must appear in strict per-nq score-desc order")
	struts.Equal([]float32{0.9, 0.85, 0.7, 0.65, 0.6, 0.95, 0.90, 0.75, 0.60},
		res.GetScores(),
		"scores must match the accepted rows in emission order")

	// GroupByFieldValues: two columns, row-aligned with Ids/Scores above.
	struts.Require().Len(res.GetGroupByFieldValues(), 2,
		"multi-groupBy must populate one FieldData per group-by field id")
	brand := res.GetGroupByFieldValues()[0]
	cat := res.GetGroupByFieldValues()[1]
	struts.Equal(int64(101), brand.GetFieldId(), "column 0 must be stamped with fieldID 101")
	struts.Equal(int64(102), cat.GetFieldId(), "column 1 must be stamped with fieldID 102")
	struts.Equal([]string{"A", "A", "A", "A", "B", "B", "B", "B", "A"},
		brand.GetScalars().GetStringData().GetData(),
		"brand column must be row-aligned with Ids")
	struts.Equal([]string{"X", "X", "Y", "Y", "X", "X", "X", "Y", "Y"},
		cat.GetScalars().GetStringData().GetData(),
		"category column must be row-aligned with Ids")
	// Sanity: the singular channel must remain empty for multi-groupBy.
	struts.Nil(res.GetGroupByFieldValue(),
		"multi-groupBy output must not populate the single-field channel")
}

func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_ReturnsSetupIdListError() {
	shard := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       1,
		Topks:      []int64{1},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		Scores:     []float32{0.9},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A"}),
		},
	}

	ret, err := reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{shard, shard}, 1, 1, schemapb.DataType_None, metric.IP, []int64{101})

	struts.Error(err)
	struts.NotNil(ret)
}

func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_UsesFirstShardWithGroupByFieldAsTemplate() {
	emptyShard := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{0},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}},
		Scores:     []float32{},
	}
	dataShard := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.8},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "B"}),
		},
	}

	ret, err := reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{emptyShard, dataShard}, 1, 2, schemapb.DataType_Int64, metric.IP, []int64{101})

	struts.Require().NoError(err)
	struts.Require().NotNil(ret)
	res := ret.GetResults()
	struts.Equal([]int64{2}, res.GetTopks())
	struts.Equal([]int64{1, 2}, res.GetIds().GetIntId().GetData())
	struts.Require().Len(res.GetGroupByFieldValues(), 1)
	struts.Equal([]string{"A", "B"}, res.GetGroupByFieldValues()[0].GetScalars().GetStringData().GetData())
}

func (struts *SearchReduceUtilTestSuite) TestReduceSearchResult_MultiGroupByMissingColumnReturnsError() {
	shardA := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.8},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "B"}),
			multiGroupByTestStringField(102, []string{"X", "Y"}),
		},
	}
	shardB := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{1},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3}}}},
		Scores:     []float32{0.7},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"C"}),
		},
	}

	info := reduce.NewReduceSearchResultInfo(1, 2).
		WithMetricType(metric.IP).
		WithPkType(schemapb.DataType_Int64).
		WithGroupSize(1).
		WithGroupByFieldIds([]int64{101, 102})
	ret, err := reduceSearchResult(context.Background(), []*schemapb.SearchResultData{shardA, shardB}, info)

	struts.Require().Error(err)
	struts.NotNil(ret)
	struts.Contains(err.Error(), "group-by field 102 missing")
}

func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_PluralOnlyGroupByValues() {
	shardA := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.8},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "B"}),
		},
	}
	shardB := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3, 4}}}},
		Scores:     []float32{0.7, 0.6},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"C", "D"}),
		},
	}

	info := reduce.NewReduceSearchResultInfo(1, 2).
		WithMetricType(metric.IP).
		WithPkType(schemapb.DataType_Int64).
		WithGroupByFieldIds([]int64{101}).
		WithAdvance(true)
	ret, err := reduceSearchResult(context.Background(), []*schemapb.SearchResultData{shardA, shardB}, info)

	struts.Require().NoError(err)
	struts.Require().NotNil(ret)
	res := ret.GetResults()
	struts.Equal([]int64{4}, res.GetTopks())
	struts.Equal([]int64{1, 2, 3, 4}, res.GetIds().GetIntId().GetData())
	struts.Require().Len(res.GetGroupByFieldValues(), 1)
	struts.Equal(int64(101), res.GetGroupByFieldValues()[0].GetFieldId())
	struts.Equal([]string{"A", "B", "C", "D"}, res.GetGroupByFieldValues()[0].GetScalars().GetStringData().GetData())
	struts.Nil(res.GetGroupByFieldValue())
}

func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_MultiFieldSingleShardPluralOutput() {
	shard := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       3,
		Topks:      []int64{3},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		Scores:     []float32{0.9, 0.8, 0.7},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "B", "C"}),
			multiGroupByTestStringField(102, []string{"X", "Y", "Z"}),
		},
	}

	ret, err := reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{shard}, 1, 3, schemapb.DataType_Int64, metric.IP, []int64{101, 102})

	struts.Require().NoError(err)
	res := ret.GetResults()
	struts.Equal([]int64{1, 2, 3}, res.GetIds().GetIntId().GetData())
	struts.Require().Len(res.GetGroupByFieldValues(), 2)
	struts.Equal(int64(101), res.GetGroupByFieldValues()[0].GetFieldId())
	struts.Equal(int64(102), res.GetGroupByFieldValues()[1].GetFieldId())
	struts.Equal([]string{"A", "B", "C"}, res.GetGroupByFieldValues()[0].GetScalars().GetStringData().GetData())
	struts.Equal([]string{"X", "Y", "Z"}, res.GetGroupByFieldValues()[1].GetScalars().GetStringData().GetData())
	struts.Nil(res.GetGroupByFieldValue())
}

func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_MultiFieldMultiShardPreservesAppendOrder() {
	shardA := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       2,
		Topks:      []int64{2, 1},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		Scores:     []float32{0.9, 0.8, 0.7},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "B", "C"}),
			multiGroupByTestStringField(102, []string{"X", "Y", "Z"}),
		},
	}
	shardB := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       2,
		Topks:      []int64{1, 2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{11, 12, 13}}}},
		Scores:     []float32{0.95, 0.75, 0.65},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"D", "E", "F"}),
			multiGroupByTestStringField(102, []string{"U", "V", "W"}),
		},
	}

	ret, err := reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{shardA, shardB}, 2, 2, schemapb.DataType_Int64, metric.IP, []int64{101, 102})

	struts.Require().NoError(err)
	res := ret.GetResults()
	struts.Equal([]int64{3, 3}, res.GetTopks())
	struts.Equal([]int64{1, 2, 11, 3, 12, 13}, res.GetIds().GetIntId().GetData())
	struts.Equal([]float32{0.9, 0.8, 0.95, 0.7, 0.75, 0.65}, res.GetScores())
	struts.Require().Len(res.GetGroupByFieldValues(), 2)
	struts.Equal([]string{"A", "B", "D", "C", "E", "F"}, res.GetGroupByFieldValues()[0].GetScalars().GetStringData().GetData())
	struts.Equal([]string{"X", "Y", "U", "Z", "V", "W"}, res.GetGroupByFieldValues()[1].GetScalars().GetStringData().GetData())
	struts.Nil(res.GetGroupByFieldValue())
}

func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_MultiFieldMissingColumnReturnsError() {
	shardA := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.8},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "B"}),
			multiGroupByTestStringField(102, []string{"X", "Y"}),
		},
	}
	shardB := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{1},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3}}}},
		Scores:     []float32{0.7},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"C"}),
		},
	}

	ret, err := reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{shardA, shardB}, 1, 2, schemapb.DataType_Int64, metric.IP, []int64{101, 102})

	struts.Require().Error(err)
	struts.NotNil(ret)
	struts.Contains(err.Error(), "group-by field 102 missing")
}

func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_SingleFieldLegacyGroupByValue() {
	shard := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.8},
		GroupByFieldValue: &schemapb.FieldData{
			Type:    schemapb.DataType_VarChar,
			FieldId: 101,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"A", "B"}}},
			}},
		},
	}

	ret, err := reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{shard}, 1, 2, schemapb.DataType_Int64, metric.IP, []int64{101})

	struts.Require().NoError(err)
	res := ret.GetResults()
	struts.Require().Len(res.GetGroupByFieldValues(), 1)
	struts.Equal([]string{"A", "B"}, res.GetGroupByFieldValues()[0].GetScalars().GetStringData().GetData())
	struts.Nil(res.GetGroupByFieldValue())
}

func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_MultiFieldNullableRowAlignment() {
	shard := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       3,
		Topks:      []int64{3},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		Scores:     []float32{0.9, 0.8, 0.7},
		GroupByFieldValues: []*schemapb.FieldData{
			{
				FieldId:   101,
				FieldName: "brand",
				Type:      schemapb.DataType_VarChar,
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"A", "C"}}},
				}},
			},
			multiGroupByTestStringField(102, []string{"X", "Y", "Z"}),
		},
	}

	ret, err := reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{shard}, 1, 3, schemapb.DataType_Int64, metric.IP, []int64{101, 102})

	struts.Require().NoError(err)
	res := ret.GetResults()
	struts.Require().Len(res.GetGroupByFieldValues(), 2)
	nullableField := res.GetGroupByFieldValues()[0]
	struts.Equal("brand", nullableField.GetFieldName())
	struts.Equal([]bool{true, false, true}, nullableField.GetValidData())
	struts.Equal([]string{"A", "", "C"}, nullableField.GetScalars().GetStringData().GetData())
	struts.Equal([]string{"X", "Y", "Z"}, res.GetGroupByFieldValues()[1].GetScalars().GetStringData().GetData())
}

func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_MultiShardEmptyResults() {
	shardA := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{0},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}},
		Scores:     []float32{},
	}
	shardB := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{0},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}},
		Scores:     []float32{},
	}

	ret, err := reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{shardA, shardB}, 1, 2, schemapb.DataType_Int64, metric.IP, []int64{101, 102})

	struts.Require().NoError(err)
	res := ret.GetResults()
	struts.Equal([]int64{0}, res.GetTopks())
	struts.Empty(res.GetIds().GetIntId().GetData())
	struts.Empty(res.GetGroupByFieldValues())
}

func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupBy_MultiShardNullableRowAlignment() {
	shardA := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.8},
		GroupByFieldValues: []*schemapb.FieldData{
			{
				FieldId:   101,
				FieldName: "brand",
				Type:      schemapb.DataType_VarChar,
				ValidData: []bool{true, false},
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"A"}}},
				}},
			},
			multiGroupByTestStringField(102, []string{"X", "Y"}),
		},
	}
	shardB := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3, 4}}}},
		Scores:     []float32{0.7, 0.6},
		GroupByFieldValues: []*schemapb.FieldData{
			{
				FieldId:   101,
				FieldName: "brand",
				Type:      schemapb.DataType_VarChar,
				ValidData: []bool{false, true},
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"D"}}},
				}},
			},
			multiGroupByTestStringField(102, []string{"U", "V"}),
		},
	}

	ret, err := reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{shardA, shardB}, 1, 2, schemapb.DataType_Int64, metric.IP, []int64{101, 102})

	struts.Require().NoError(err)
	res := ret.GetResults()
	struts.Equal([]int64{1, 2, 3, 4}, res.GetIds().GetIntId().GetData())
	struts.Require().Len(res.GetGroupByFieldValues(), 2)
	nullableField := res.GetGroupByFieldValues()[0]
	struts.Equal("brand", nullableField.GetFieldName())
	struts.Equal([]bool{true, false, false, true}, nullableField.GetValidData())
	struts.Equal([]string{"A", "", "", "D"}, nullableField.GetScalars().GetStringData().GetData())
	struts.Equal([]string{"X", "Y", "U", "V"}, res.GetGroupByFieldValues()[1].GetScalars().GetStringData().GetData())
}

func TestSearchReduceUtilTestSuite(t *testing.T) {
	suite.Run(t, new(SearchReduceUtilTestSuite))
}
