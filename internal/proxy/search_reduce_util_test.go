package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
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

// ====================================================================================
// Order By Tests
// ====================================================================================

// genTestDataWithOrderByValues generates test data with order_by field values
func genTestDataWithOrderByValues() []*schemapb.SearchResultData {
	// Segment 1: IDs 1-5 with prices [100, 50, 200, 75, 150]
	searchResultData1 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5},
				},
			},
		},
		Scores:     []float32{0.9, 0.85, 0.8, 0.75, 0.7},
		Topks:      []int64{5},
		NumQueries: 1,
		TopK:       5,
		FieldsData: []*schemapb.FieldData{},
		OrderByFieldValue: []*schemapb.FieldData{{
			Type:      schemapb.DataType_Int64,
			FieldName: "price",
			FieldId:   102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{100, 50, 200, 75, 150},
						},
					},
				},
			},
		}},
	}

	// Segment 2: IDs 6-10 with prices [80, 120, 30, 180, 90]
	searchResultData2 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{6, 7, 8, 9, 10},
				},
			},
		},
		Scores:     []float32{0.88, 0.82, 0.78, 0.72, 0.68},
		Topks:      []int64{5},
		NumQueries: 1,
		TopK:       5,
		FieldsData: []*schemapb.FieldData{},
		OrderByFieldValue: []*schemapb.FieldData{{
			Type:      schemapb.DataType_Int64,
			FieldName: "price",
			FieldId:   102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{80, 120, 30, 180, 90},
						},
					},
				},
			},
		}},
	}

	return []*schemapb.SearchResultData{searchResultData1, searchResultData2}
}

// TestReduceSearchResultDataWithOrderBy tests basic order_by reduce functionality
func (s *SearchReduceUtilTestSuite) TestReduceSearchResultDataWithOrderBy() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(5)
	offset := int64(0)

	data := genTestDataWithOrderByValues()

	// Test ascending order by price
	orderByFields := []*planpb.OrderByField{
		{
			FieldId:   102,
			Ascending: true,
		},
	}

	results, err := reduceSearchResultDataWithOrderBy(ctx, data, nq, topK, "L2", schemapb.DataType_Int64, offset, orderByFields)
	s.NoError(err)
	s.NotNil(results)

	// Expected order by price ASC: 30(ID8), 50(ID2), 75(ID4), 80(ID6), 90(ID10)
	expectedIDs := []int64{8, 2, 4, 6, 10}
	s.Equal(expectedIDs, results.Results.GetIds().GetIntId().GetData())

	// Verify OrderByFieldValue is set in result
	s.NotNil(results.Results.OrderByFieldValue)
}

// TestReduceSearchResultDataWithOrderByDescending tests descending order
func (s *SearchReduceUtilTestSuite) TestReduceSearchResultDataWithOrderByDescending() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(5)
	offset := int64(0)

	data := genTestDataWithOrderByValues()

	// Test descending order by price
	orderByFields := []*planpb.OrderByField{
		{
			FieldId:   102,
			Ascending: false,
		},
	}

	results, err := reduceSearchResultDataWithOrderBy(ctx, data, nq, topK, "L2", schemapb.DataType_Int64, offset, orderByFields)
	s.NoError(err)
	s.NotNil(results)

	// Expected order by price DESC: 200(ID3), 180(ID9), 150(ID5), 120(ID7), 100(ID1)
	expectedIDs := []int64{3, 9, 5, 7, 1}
	s.Equal(expectedIDs, results.Results.GetIds().GetIntId().GetData())
}

// TestReduceSearchResultDataWithOrderByNoOrderByFields tests when no order_by fields (fallback to score)
func (s *SearchReduceUtilTestSuite) TestReduceSearchResultDataWithOrderByNoOrderByFields() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(5)
	offset := int64(0)

	data := genTestDataWithOrderByValues()

	// Empty order_by fields - should sort by score
	orderByFields := []*planpb.OrderByField{}

	results, err := reduceSearchResultDataWithOrderBy(ctx, data, nq, topK, "L2", schemapb.DataType_Int64, offset, orderByFields)
	s.NoError(err)
	s.NotNil(results)

	// Should be sorted by score (descending): 0.9(ID1), 0.88(ID6), 0.85(ID2), 0.82(ID7), 0.8(ID3)
	expectedIDs := []int64{1, 6, 2, 7, 3}
	s.Equal(expectedIDs, results.Results.GetIds().GetIntId().GetData())
}

// TestReduceSearchResultDataWithOrderByWithOffset tests order_by with offset
func (s *SearchReduceUtilTestSuite) TestReduceSearchResultDataWithOrderByWithOffset() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(5)
	offset := int64(2) // Skip first 2 results

	data := genTestDataWithOrderByValues()

	orderByFields := []*planpb.OrderByField{
		{
			FieldId:   102,
			Ascending: true,
		},
	}

	results, err := reduceSearchResultDataWithOrderBy(ctx, data, nq, topK, "L2", schemapb.DataType_Int64, offset, orderByFields)
	s.NoError(err)
	s.NotNil(results)

	// After offset=2, expected: 75(ID4), 80(ID6), 90(ID10) (only 3 results since topK-offset=3)
	expectedIDs := []int64{4, 6, 10}
	s.Equal(expectedIDs, results.Results.GetIds().GetIntId().GetData())
}

// genTestDataWithGroupByAndOrderBy generates test data with both group_by and order_by values
func genTestDataWithGroupByAndOrderBy() []*schemapb.SearchResultData {
	// Segment 1: Items with category groups and prices
	// Categories: [A, B, A, B, C] Prices: [100, 50, 80, 120, 60]
	searchResultData1 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5},
				},
			},
		},
		Scores:     []float32{0.9, 0.85, 0.8, 0.75, 0.7},
		Topks:      []int64{5},
		NumQueries: 1,
		TopK:       5,
		FieldsData: []*schemapb.FieldData{},
		GroupByFieldValue: &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: "category",
			FieldId:   101,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"A", "B", "A", "B", "C"},
						},
					},
				},
			},
		},
		OrderByFieldValue: []*schemapb.FieldData{{
			Type:      schemapb.DataType_Int64,
			FieldName: "price",
			FieldId:   102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{100, 50, 80, 120, 60},
						},
					},
				},
			},
		}},
	}

	// Segment 2: More items
	// Categories: [C, A, B, C, A] Prices: [40, 90, 70, 110, 55]
	searchResultData2 := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{6, 7, 8, 9, 10},
				},
			},
		},
		Scores:     []float32{0.88, 0.82, 0.78, 0.72, 0.68},
		Topks:      []int64{5},
		NumQueries: 1,
		TopK:       5,
		FieldsData: []*schemapb.FieldData{},
		GroupByFieldValue: &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: "category",
			FieldId:   101,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"C", "A", "B", "C", "A"},
						},
					},
				},
			},
		},
		OrderByFieldValue: []*schemapb.FieldData{{
			Type:      schemapb.DataType_Int64,
			FieldName: "price",
			FieldId:   102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{40, 90, 70, 110, 55},
						},
					},
				},
			},
		}},
		},
	}

	return []*schemapb.SearchResultData{searchResultData1, searchResultData2}
}

// TestReduceSearchResultDataWithGroupOrderBy tests combined group_by + order_by
func (s *SearchReduceUtilTestSuite) TestReduceSearchResultDataWithGroupOrderBy() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(3) // 3 groups
	offset := int64(0)
	groupSize := int64(2) // 2 items per group

	data := genTestDataWithGroupByAndOrderBy()

	// Order groups by price ASC (using first item's price in each group)
	orderByFields := []*planpb.OrderByField{
		{
			FieldId:   102,
			Ascending: true,
		},
	}

	results, err := reduceSearchResultDataWithGroupOrderBy(ctx, data, nq, topK, "L2", schemapb.DataType_Int64, offset, groupSize, orderByFields)
	s.NoError(err)
	s.NotNil(results)

	// Groups by first item's price:
	// Group B: first item ID2 has price 50 (lowest)
	// Group C: first item ID5 has price 60
	// Group A: first item ID1 has price 100
	// Expected group order: B, C, A

	// Verify GroupByFieldValue is set
	s.NotNil(results.Results.GroupByFieldValue)

	// Verify OrderByFieldValue is set
	s.NotNil(results.Results.OrderByFieldValue)
}

// TestReduceSearchResultDataWithGroupOrderByDescending tests group_by + order_by DESC
func (s *SearchReduceUtilTestSuite) TestReduceSearchResultDataWithGroupOrderByDescending() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(3)
	offset := int64(0)
	groupSize := int64(1)

	data := genTestDataWithGroupByAndOrderBy()

	// Order groups by price DESC
	orderByFields := []*planpb.OrderByField{
		{
			FieldId:   102,
			Ascending: false,
		},
	}

	results, err := reduceSearchResultDataWithGroupOrderBy(ctx, data, nq, topK, "L2", schemapb.DataType_Int64, offset, groupSize, orderByFields)
	s.NoError(err)
	s.NotNil(results)

	// Groups by first item's price DESC:
	// Group A: first item ID1 has price 100 (highest)
	// Group C: first item ID5 has price 60
	// Group B: first item ID2 has price 50
	// Expected group order: A, C, B

	s.NotNil(results.Results.GroupByFieldValue)
	s.NotNil(results.Results.OrderByFieldValue)
}

// TestCompareOrderByValuesProxy tests the comparison function for order_by values
func (s *SearchReduceUtilTestSuite) TestCompareOrderByValuesProxy() {
	// Test int64 comparison
	{
		lhsVals := []any{int64(100)}
		rhsVals := []any{int64(200)}
		orderByFields := []*planpb.OrderByField{{FieldId: 1, Ascending: true}}

		result := compareOrderByValuesProxy(lhsVals, rhsVals, orderByFields)
		s.Equal(-1, result, "100 < 200 with ASC should return -1")

		result = compareOrderByValuesProxy(rhsVals, lhsVals, orderByFields)
		s.Equal(1, result, "200 > 100 with ASC should return 1")

		result = compareOrderByValuesProxy(lhsVals, lhsVals, orderByFields)
		s.Equal(0, result, "100 == 100 should return 0")
	}

	// Test descending order
	{
		lhsVals := []any{int64(100)}
		rhsVals := []any{int64(200)}
		orderByFields := []*planpb.OrderByField{{FieldId: 1, Ascending: false}}

		result := compareOrderByValuesProxy(lhsVals, rhsVals, orderByFields)
		s.Equal(1, result, "100 < 200 with DESC should return 1 (reversed)")

		result = compareOrderByValuesProxy(rhsVals, lhsVals, orderByFields)
		s.Equal(-1, result, "200 > 100 with DESC should return -1 (reversed)")
	}

	// Test string comparison
	{
		lhsVals := []any{"apple"}
		rhsVals := []any{"banana"}
		orderByFields := []*planpb.OrderByField{{FieldId: 1, Ascending: true}}

		result := compareOrderByValuesProxy(lhsVals, rhsVals, orderByFields)
		s.Equal(-1, result, "apple < banana should return -1")
	}

	// Test nil handling
	{
		lhsVals := []any{nil}
		rhsVals := []any{int64(100)}
		orderByFields := []*planpb.OrderByField{{FieldId: 1, Ascending: true}}

		result := compareOrderByValuesProxy(lhsVals, rhsVals, orderByFields)
		s.Equal(-1, result, "nil < non-nil with ASC should return -1")

		result = compareOrderByValuesProxy(rhsVals, lhsVals, orderByFields)
		s.Equal(1, result, "non-nil > nil with ASC should return 1")
	}

	// Test float comparison
	{
		lhsVals := []any{float64(1.5)}
		rhsVals := []any{float64(2.5)}
		orderByFields := []*planpb.OrderByField{{FieldId: 1, Ascending: true}}

		result := compareOrderByValuesProxy(lhsVals, rhsVals, orderByFields)
		s.Equal(-1, result, "1.5 < 2.5 should return -1")
	}
}

// TestBuildOrderByIterators tests the iterator construction for order_by fields
func (s *SearchReduceUtilTestSuite) TestBuildOrderByIterators() {
	data := genTestDataWithOrderByValues()

	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true},
	}

	iterators := buildOrderByIterators(data, orderByFields)

	// Should have iterators for both search results
	s.Equal(2, len(iterators))

	// Each should have one iterator (for one order_by field)
	s.Equal(1, len(iterators[0]))
	s.Equal(1, len(iterators[1]))

	// Test first iterator returns correct values
	// Segment 1 prices: [100, 50, 200, 75, 150]
	s.NotNil(iterators[0][0])
	val0 := iterators[0][0](0)
	s.Equal(int64(100), val0)
	val1 := iterators[0][0](1)
	s.Equal(int64(50), val1)
}

// TestReduceSearchResultDataWithOrderBySingleShard tests single shard optimization
func (s *SearchReduceUtilTestSuite) TestReduceSearchResultDataWithOrderBySingleShard() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(5)
	offset := int64(0)

	// Only one shard
	data := genTestDataWithOrderByValues()[:1]

	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true},
	}

	results, err := reduceSearchResultDataWithOrderBy(ctx, data, nq, topK, "L2", schemapb.DataType_Int64, offset, orderByFields)
	s.NoError(err)
	s.NotNil(results)

	// Single shard should still be sorted by order_by field
	// Prices: [100, 50, 200, 75, 150] -> sorted ASC: [50, 75, 100, 150, 200]
	// IDs:    [1,   2,  3,   4,  5]   -> sorted:     [2,  4,  1,   5,   3]
	expectedIDs := []int64{2, 4, 1, 5, 3}
	s.Equal(expectedIDs, results.Results.GetIds().GetIntId().GetData())
}

// TestReduceSearchResultDataWithOrderByEmptyData tests handling of empty data
func (s *SearchReduceUtilTestSuite) TestReduceSearchResultDataWithOrderByEmptyData() {
	ctx := context.Background()
	nq := int64(1)
	topK := int64(5)
	offset := int64(0)

	emptyData := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{}},
			},
		},
		Scores:     []float32{},
		Topks:      []int64{0},
		NumQueries: nq,
		TopK:       topK,
		FieldsData: []*schemapb.FieldData{},
	}

	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true},
	}

	results, err := reduceSearchResultDataWithOrderBy(ctx, []*schemapb.SearchResultData{emptyData}, nq, topK, "L2", schemapb.DataType_Int64, offset, orderByFields)
	s.NoError(err)
	s.NotNil(results)
	s.Equal(0, len(results.Results.GetIds().GetIntId().GetData()))
}

func TestSearchReduceUtilTestSuite(t *testing.T) {
	suite.Run(t, new(SearchReduceUtilTestSuite))
}
