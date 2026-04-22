package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
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

func (struts *SearchReduceUtilTestSuite) TestReduceSearchResultWithEmtpyGroupData() {
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
		nq, topk, "L2", schemapb.DataType_Int64, 0, 1)
	struts.Error(err)
	struts.ErrorContains(err, "failed to construct group by field data builder")
	struts.Nil(results.Results.GetGroupByFieldValue())
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

	// Test reduceSearchResultDataWithGroupBy with empty FieldsData
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

		results, err := reduceSearchResultDataWithGroupBy(ctx, []*schemapb.SearchResultData{searchResultData1, searchResultData2}, nq, topK, "L2", schemapb.DataType_Int64, offset, int64(2))
		struts.NoError(err)
		struts.NotNil(results)
		// FieldsData should be empty since all inputs were empty
		struts.Equal(0, len(results.Results.FieldsData))
	}

	// Test reduceAdvanceGroupBy with empty FieldsData
	{
		results, err := reduceAdvanceGroupBy(ctx, []*schemapb.SearchResultData{searchResultData1, searchResultData2}, nq, topK, schemapb.DataType_Int64, "L2")
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
				[]*schemapb.SearchResultData{data}, 1, n, schemapb.DataType_Int64, tc.metric)
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
		1, 3, schemapb.DataType_Int64, "L2")
	struts.Require().NoError(err)

	// Force the multi-shard path by passing two identical shards.
	// Both paths should produce scores in the same direction (positive L2).
	multiShardResult, err := reduceAdvanceGroupBy(context.Background(),
		[]*schemapb.SearchResultData{
			makeShard(ids, scores, groups),
			makeShard([]int64{4, 5, 6}, []float32{-0.2, -0.4, -0.8}, []string{"A", "B", "A"}),
		},
		1, 3, schemapb.DataType_Int64, "L2")
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

// TestReduceCosineScoreClamping is a regression test for
// https://github.com/milvus-io/milvus/issues/49059.
//
// Knowhere/Segcore can return cosine similarity values slightly outside [-1, 1]
// (e.g. 1.0000001192092896) due to floating-point rounding during the dot
// product computation. All reduce paths must clamp these values to the
// mathematically valid range before returning results to the client.
func (struts *SearchReduceUtilTestSuite) TestReduceCosineScoreClamping() {
	// fp32 precision overflow: 1 + epsilon (same as reported in the issue)
	overflowScore := float32(1.0000001192092896)
	underflowScore := float32(-1.0000001192092896)

	makeData := func(scores []float32) *schemapb.SearchResultData {
		n := int64(len(scores))
		ids := make([]int64, n)
		for i := range ids {
			ids[i] = int64(i + 1)
		}
		return &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       n,
			Topks:      []int64{n},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: ids},
				},
			},
			Scores: append([]float32(nil), scores...),
		}
	}

	struts.Run("reduceSearchResultDataNoGroupBy clamps COSINE overflow", func() {
		data := makeData([]float32{overflowScore, 0.5, underflowScore})
		result, err := reduceSearchResultDataNoGroupBy(
			context.Background(),
			[]*schemapb.SearchResultData{data, makeData([]float32{0.3})},
			1, 3, "COSINE", schemapb.DataType_Int64, 0,
		)
		struts.Require().NoError(err)
		scores := result.Results.GetScores()
		for _, s := range scores {
			struts.LessOrEqual(s, float32(1.0), "COSINE score must be <= 1.0, got %v", s)
			struts.GreaterOrEqual(s, float32(-1.0), "COSINE score must be >= -1.0, got %v", s)
		}
	})

	struts.Run("reduceAdvanceGroupBy clamps COSINE overflow (single shard)", func() {
		groups := []string{"A", "B", "C"}
		data := &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{overflowScore, 0.5, underflowScore},
			GroupByFieldValue: &schemapb.FieldData{
				Type:      schemapb.DataType_VarChar,
				FieldName: "category",
				FieldId:   101,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: groups},
						},
					},
				},
			},
		}
		result, err := reduceAdvanceGroupBy(context.Background(),
			[]*schemapb.SearchResultData{data}, 1, 3, schemapb.DataType_Int64, "COSINE")
		struts.Require().NoError(err)
		for _, s := range result.Results.GetScores() {
			struts.LessOrEqual(s, float32(1.0), "COSINE score must be <= 1.0, got %v", s)
			struts.GreaterOrEqual(s, float32(-1.0), "COSINE score must be >= -1.0, got %v", s)
		}
	})

	struts.Run("non-COSINE metrics are not clamped (IP can exceed 1.0)", func() {
		data := makeData([]float32{5.0, 100.0, -3.0})
		result, err := reduceSearchResultDataNoGroupBy(
			context.Background(),
			[]*schemapb.SearchResultData{data},
			1, 3, "IP", schemapb.DataType_Int64, 0,
		)
		struts.Require().NoError(err)
		scores := result.Results.GetScores()
		// IP scores outside [-1, 1] must be preserved exactly.
		struts.Equal(float32(100.0), scores[0])
		struts.Equal(float32(5.0), scores[1])
		struts.Equal(float32(-3.0), scores[2])
	})
}

func TestSearchReduceUtilTestSuite(t *testing.T) {
	suite.Run(t, new(SearchReduceUtilTestSuite))
}
