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

func TestSearchReduceUtilTestSuite(t *testing.T) {
	suite.Run(t, new(SearchReduceUtilTestSuite))
}
