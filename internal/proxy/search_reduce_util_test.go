package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
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

func (struts *SearchReduceUtilTestSuite) TestRankByGroup() {
	data := genTestDataSearchResultsData()
	searchResults := []*milvuspb.SearchResults{
		{Results: data[0]},
		{Results: data[1]},
	}

	nq := int64(1)
	limit := int64(3)
	offset := int64(0)
	roundDecimal := int64(1)
	groupSize := int64(3)
	groupByFieldId := int64(101)
	rankParams := &rankParams{limit: limit, offset: offset, roundDecimal: roundDecimal}

	{
		// test for sum group scorer
		scorerType := "sum"
		groupScorer, _ := GetGroupScorer(scorerType)
		rankedRes, err := rankSearchResultData(context.Background(), nq, rankParams, schemapb.DataType_VarChar, searchResults, groupByFieldId, groupSize, groupScorer)
		struts.NoError(err)
		struts.Equal([]string{"5", "2", "3", "17", "12", "13", "7", "15", "1"}, rankedRes.GetResults().GetIds().GetStrId().Data)
		struts.Equal([]float32{0.5, 0.4, 0.4, 0.7, 0.3, 0.3, 0.6, 0.4, 0.3}, rankedRes.GetResults().GetScores())
		struts.Equal([]string{"bbb", "bbb", "bbb", "www", "www", "www", "aaa", "aaa", "aaa"}, rankedRes.GetResults().GetGroupByFieldValue().GetScalars().GetStringData().Data)
	}

	{
		// test for max group scorer
		scorerType := "max"
		groupScorer, _ := GetGroupScorer(scorerType)
		rankedRes, err := rankSearchResultData(context.Background(), nq, rankParams, schemapb.DataType_VarChar, searchResults, groupByFieldId, groupSize, groupScorer)
		struts.NoError(err)
		struts.Equal([]string{"17", "12", "13", "7", "15", "1", "5", "2", "3"}, rankedRes.GetResults().GetIds().GetStrId().Data)
		struts.Equal([]float32{0.7, 0.3, 0.3, 0.6, 0.4, 0.3, 0.5, 0.4, 0.4}, rankedRes.GetResults().GetScores())
		struts.Equal([]string{"www", "www", "www", "aaa", "aaa", "aaa", "bbb", "bbb", "bbb"}, rankedRes.GetResults().GetGroupByFieldValue().GetScalars().GetStringData().Data)
	}

	{
		// test for avg group scorer
		scorerType := "avg"
		groupScorer, _ := GetGroupScorer(scorerType)
		rankedRes, err := rankSearchResultData(context.Background(), nq, rankParams, schemapb.DataType_VarChar, searchResults, groupByFieldId, groupSize, groupScorer)
		struts.NoError(err)
		struts.Equal([]string{"5", "2", "3", "17", "12", "13", "7", "15", "1"}, rankedRes.GetResults().GetIds().GetStrId().Data)
		struts.Equal([]float32{0.5, 0.4, 0.4, 0.7, 0.3, 0.3, 0.6, 0.4, 0.3}, rankedRes.GetResults().GetScores())
		struts.Equal([]string{"bbb", "bbb", "bbb", "www", "www", "www", "aaa", "aaa", "aaa"}, rankedRes.GetResults().GetGroupByFieldValue().GetScalars().GetStringData().Data)
	}

	{
		// test for offset for ranking group
		scorerType := "avg"
		groupScorer, _ := GetGroupScorer(scorerType)
		rankParams.offset = 2
		rankedRes, err := rankSearchResultData(context.Background(), nq, rankParams, schemapb.DataType_VarChar, searchResults, groupByFieldId, groupSize, groupScorer)
		struts.NoError(err)
		struts.Equal([]string{"7", "15", "1", "4", "6", "14"}, rankedRes.GetResults().GetIds().GetStrId().Data)
		struts.Equal([]float32{0.6, 0.4, 0.3, 0.5, 0.3, 0.3}, rankedRes.GetResults().GetScores())
		struts.Equal([]string{"aaa", "aaa", "aaa", "ccc", "ccc", "ccc"}, rankedRes.GetResults().GetGroupByFieldValue().GetScalars().GetStringData().Data)
	}

	{
		// test for offset exceeding the count of final groups
		scorerType := "avg"
		groupScorer, _ := GetGroupScorer(scorerType)
		rankParams.offset = 4
		rankedRes, err := rankSearchResultData(context.Background(), nq, rankParams, schemapb.DataType_VarChar, searchResults, groupByFieldId, groupSize, groupScorer)
		struts.NoError(err)
		struts.Equal([]string{}, rankedRes.GetResults().GetIds().GetStrId().Data)
		struts.Equal([]float32{}, rankedRes.GetResults().GetScores())
	}

	{
		// test for invalid group scorer
		scorerType := "xxx"
		groupScorer, err := GetGroupScorer(scorerType)
		struts.Error(err)
		struts.Nil(groupScorer)
	}
}

func (struts *SearchReduceUtilTestSuite) TestReduceSearchResult() {
	data := genTestDataSearchResultsData()

	{
		results, err := reduceSearchResultDataNoGroupBy(context.Background(), []*schemapb.SearchResultData{data[0]}, 0, 0, "L2", schemapb.DataType_Int64, 0)
		struts.NoError(err)
		struts.Equal([]string{"7", "5", "4", "2", "3", "6", "1", "9", "8"}, results.Results.GetIds().GetStrId().Data)
	}
}

func TestSearchReduceUtilTestSuite(t *testing.T) {
	suite.Run(t, new(SearchReduceUtilTestSuite))
}
