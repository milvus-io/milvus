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

func TestSearchReduceUtilTestSuite(t *testing.T) {
	suite.Run(t, new(SearchReduceUtilTestSuite))
}
