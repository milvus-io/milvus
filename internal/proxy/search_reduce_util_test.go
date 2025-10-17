package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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
		NumQueries: nq,
		TopK:       topk,
		FieldsData: []*schemapb.FieldData{},
		Scores:     make([]float32, topk),
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1},
				},
			},
		},
		Topks:            []int64{1},
		OutputFields:     []string{"id"},
		AllSearchCount:   0,
		Distances:        []float32{},
		Recalls:          []float32{},
		PrimaryFieldName: "",
	}
	results, err := reduceSearchResultDataWithGroupBy(context.Background(), []*schemapb.SearchResultData{emptyData},
		nq, topk, "L2", schemapb.DataType_Int64, 0, 1)
	struts.Error(err)
	struts.ErrorContains(err, "failed to construct group by field data builder")
	struts.Nil(results.Results.GetGroupByFieldValue())
}

// Test filtering empty results in reduceAdvanceGroupBy
func (struts *SearchReduceUtilTestSuite) TestReduceAdvanceGroupByWithEmptyResults() {
	nq := int64(1)
	topk := int64(10)
	pkType := schemapb.DataType_Int64
	metricType := "L2"

	// Create empty result with no IDs
	emptyResult := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       0,
		FieldsData: []*schemapb.FieldData{},
		Scores:     []float32{},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{},
				},
			},
		},
		Topks: []int64{0},
	}

	// Test with only empty results
	result, err := reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{emptyResult, emptyResult}, nq, topk, pkType, metricType)
	struts.NoError(err)
	struts.NotNil(result)
	struts.Equal(nq, result.Results.NumQueries)
	struts.Equal(nq, int64(len(result.Results.Topks)))

	// Create valid result
	validResult := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       2,
		FieldsData: []*schemapb.FieldData{
			getFieldData("id", int64(100), schemapb.DataType_Int64, []int64{1, 2}, 1),
		},
		Scores: []float32{0.9, 0.8},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2},
				},
			},
		},
		Topks:             []int64{2},
		GroupByFieldValue: getFieldData("group", int64(101), schemapb.DataType_VarChar, []string{"a", "b"}, 1),
	}

	// Test with mixed valid and empty results
	result, err = reduceAdvanceGroupBy(context.Background(), []*schemapb.SearchResultData{emptyResult, validResult, emptyResult}, nq, topk, pkType, metricType)
	struts.NoError(err)
	struts.NotNil(result)
	struts.Equal(validResult, result.Results)
}

// Test filtering empty results in reduceSearchResultDataWithGroupBy
func (struts *SearchReduceUtilTestSuite) TestReduceSearchResultDataWithGroupByEmptyResults() {
	nq := int64(1)
	topk := int64(10)
	offset := int64(0)
	groupSize := int64(1)
	pkType := schemapb.DataType_Int64
	metricType := "L2"

	// Create empty result
	emptyResult := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       0,
		FieldsData: []*schemapb.FieldData{},
		Scores:     []float32{},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{},
				},
			},
		},
		Topks: []int64{0},
	}

	// Test with all empty results - should return empty result without error
	result, err := reduceSearchResultDataWithGroupBy(context.Background(), []*schemapb.SearchResultData{emptyResult}, nq, topk, metricType, pkType, offset, groupSize)
	struts.NoError(err)
	struts.NotNil(result)
	struts.Equal(nq, result.Results.NumQueries)
}

// Test filtering empty results in reduceSearchResultDataNoGroupBy
func (struts *SearchReduceUtilTestSuite) TestReduceSearchResultDataNoGroupByEmptyResults() {
	nq := int64(1)
	topk := int64(10)
	offset := int64(0)
	pkType := schemapb.DataType_Int64
	metricType := "L2"

	// Create empty result with no data
	emptyResult1 := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       0,
		FieldsData: []*schemapb.FieldData{},
		Scores:     []float32{},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{},
				},
			},
		},
		Topks: []int64{0},
	}

	// Create empty result with nil fields
	emptyResult2 := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       0,
		FieldsData: nil,
		Scores:     []float32{},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{},
				},
			},
		},
		Topks: []int64{0},
	}

	// Test with all empty results
	result, err := reduceSearchResultDataNoGroupBy(context.Background(), []*schemapb.SearchResultData{emptyResult1, emptyResult2}, nq, topk, metricType, pkType, offset)
	struts.NoError(err)
	struts.NotNil(result)
	struts.Equal(nq, result.Results.NumQueries)

	// Create valid result
	validResult := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topk,
		FieldsData: []*schemapb.FieldData{
			getFieldData("id", int64(100), schemapb.DataType_Int64, []int64{1, 2}, 1),
		},
		Scores: []float32{0.9, 0.8},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2},
				},
			},
		},
		Topks: []int64{topk},
	}

	// Test with mixed valid and empty results
	result, err = reduceSearchResultDataNoGroupBy(context.Background(), []*schemapb.SearchResultData{emptyResult1, validResult, emptyResult2}, nq, topk, metricType, pkType, offset)
	struts.NoError(err)
	struts.NotNil(result)
	struts.NotNil(result.Results)
	struts.Equal(int64(2), int64(len(result.Results.Scores)))
}

func TestSearchReduceUtilTestSuite(t *testing.T) {
	suite.Run(t, new(SearchReduceUtilTestSuite))
}
