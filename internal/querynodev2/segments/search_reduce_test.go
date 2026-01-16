package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type SearchReduceSuite struct {
	suite.Suite
}

func (suite *SearchReduceSuite) TestResult_ReduceSearchResultData() {
	const (
		nq   = 1
		topk = 4
	)
	suite.Run("case1", func() {
		ids := []int64{1, 2, 3, 4}
		scores := []float32{-1.0, -2.0, -3.0, -4.0}
		topks := []int64{int64(len(ids))}
		data1 := mock_segcore.GenSearchResultData(nq, topk, ids, scores, topks)
		data2 := mock_segcore.GenSearchResultData(nq, topk, ids, scores, topks)
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.Equal(ids, res.Ids.GetIntId().Data)
		suite.Equal(scores, res.Scores)
	})
	suite.Run("case2", func() {
		ids1 := []int64{1, 2, 3, 4}
		scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{5, 1, 3, 4}
		scores2 := []float32{-1.0, -1.0, -3.0, -4.0}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.ElementsMatch([]int64{1, 5, 2, 3}, res.Ids.GetIntId().Data)
	})
}

func (suite *SearchReduceSuite) TestResult_SearchGroupByResult() {
	const (
		nq   = 1
		topk = 4
	)
	suite.Run("reduce_group_by_int", func() {
		ids1 := []int64{1, 2, 3, 4}
		scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{5, 1, 3, 4}
		scores2 := []float32{-1.0, -1.0, -3.0, -4.0}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
		data1.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_Int8,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 4, 5},
						},
					},
				},
			},
		}
		data2.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_Int8,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 4, 5},
						},
					},
				},
			},
		}
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByField(101)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.ElementsMatch([]int64{1, 2, 3, 4}, res.Ids.GetIntId().Data)
		suite.ElementsMatch([]float32{-1.0, -2.0, -3.0, -4.0}, res.Scores)
		suite.ElementsMatch([]int32{2, 3, 4, 5}, res.GroupByFieldValue.GetScalars().GetIntData().Data)
	})
	suite.Run("reduce_group_by_bool", func() {
		ids1 := []int64{1, 2}
		scores1 := []float32{-1.0, -2.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{3, 4}
		scores2 := []float32{-1.0, -1.0}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
		data1.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_Bool,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: []bool{true, false},
						},
					},
				},
			},
		}
		data2.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_Bool,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: []bool{true, false},
						},
					},
				},
			},
		}
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByField(101)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.ElementsMatch([]int64{1, 4}, res.Ids.GetIntId().Data)
		suite.ElementsMatch([]float32{-1.0, -1.0}, res.Scores)
		suite.ElementsMatch([]bool{true, false}, res.GroupByFieldValue.GetScalars().GetBoolData().Data)
	})
	suite.Run("reduce_group_by_string", func() {
		ids1 := []int64{1, 2, 3, 4}
		scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{5, 1, 3, 4}
		scores2 := []float32{-1.0, -1.0, -3.0, -4.0}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
		data1.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"1", "2", "3", "4"},
						},
					},
				},
			},
		}
		data2.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"1", "2", "3", "4"},
						},
					},
				},
			},
		}
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByField(101)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.ElementsMatch([]int64{1, 2, 3, 4}, res.Ids.GetIntId().Data)
		suite.ElementsMatch([]float32{-1.0, -2.0, -3.0, -4.0}, res.Scores)
		suite.ElementsMatch([]string{"1", "2", "3", "4"}, res.GroupByFieldValue.GetScalars().GetStringData().Data)
	})
	suite.Run("reduce_group_by_string_with_group_size", func() {
		ids1 := []int64{1, 2, 3, 4}
		scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{4, 5, 6, 7}
		scores2 := []float32{-1.0, -1.0, -3.0, -4.0}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
		data1.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"1", "2", "3", "4"},
						},
					},
				},
			},
		}
		data2.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"1", "2", "3", "4"},
						},
					},
				},
			},
		}
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(3).WithGroupByField(101)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.ElementsMatch([]int64{1, 4, 2, 5, 3, 6, 7}, res.Ids.GetIntId().Data)
		suite.ElementsMatch([]float32{-1.0, -1.0, -1.0, -2.0, -3.0, -3.0, -4.0}, res.Scores)
		suite.ElementsMatch([]string{"1", "1", "2", "2", "3", "3", "4"}, res.GroupByFieldValue.GetScalars().GetStringData().Data)
	})

	suite.Run("reduce_group_by_empty_input", func() {
		dataArray := make([]*schemapb.SearchResultData, 0)
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(3).WithGroupByField(101)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.Nil(res.GetIds().GetIdField())
		suite.Equal(0, len(res.GetTopks()))
		suite.Equal(0, len(res.GetScores()))
		suite.Equal(int64(nq), res.GetNumQueries())
		suite.Equal(int64(topk), res.GetTopK())
		suite.Equal(0, len(res.GetFieldsData()))
	})
}

func (suite *SearchReduceSuite) TestResult_SearchGroupByStrictGroupSize() {
	const (
		nq   = 1
		topk = 4
	)

	suite.Run("strict_group_size_filters_incomplete_groups", func() {
		// Group "1" has 2 elements (ids 1, 4), Group "2" has 2 elements (ids 2, 5)
		// Group "3" has 1 element (id 3), Group "4" has 1 element (id 6)
		// With groupSize=2 and strictGroupSize=true, only groups "1" and "2" should be returned
		ids1 := []int64{1, 2, 3}
		scores1 := []float32{-1.0, -2.0, -3.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{4, 5, 6}
		scores2 := []float32{-1.5, -2.5, -3.5}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
		data1.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"1", "2", "3"},
						},
					},
				},
			},
		}
		data2.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"1", "2", "4"},
						},
					},
				},
			},
		}
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)

		// With strictGroupSize=true, only complete groups (with groupSize elements) should be returned
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(2).WithGroupByField(101).WithStrictGroupSize(true)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		// Only groups "1" and "2" have 2 elements each, groups "3" and "4" have only 1 element
		// So we expect 4 results (2 groups * 2 elements)
		suite.Equal(4, len(res.Ids.GetIntId().Data))
		suite.ElementsMatch([]int64{1, 4, 2, 5}, res.Ids.GetIntId().Data)
		suite.ElementsMatch([]string{"1", "1", "2", "2"}, res.GroupByFieldValue.GetScalars().GetStringData().Data)
	})

	suite.Run("strict_group_size_false_includes_incomplete_groups", func() {
		// Same data as above, but with strictGroupSize=false
		ids1 := []int64{1, 2, 3}
		scores1 := []float32{-1.0, -2.0, -3.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{4, 5, 6}
		scores2 := []float32{-1.5, -2.5, -3.5}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
		data1.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"1", "2", "3"},
						},
					},
				},
			},
		}
		data2.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"1", "2", "4"},
						},
					},
				},
			},
		}
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)

		// With strictGroupSize=false (default), all groups should be returned including incomplete ones
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(2).WithGroupByField(101).WithStrictGroupSize(false)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		// All 4 groups should be returned: "1" (2 elements), "2" (2 elements), "3" (1 element), "4" (1 element)
		// Total: 6 results
		suite.Equal(6, len(res.Ids.GetIntId().Data))
		suite.ElementsMatch([]int64{1, 4, 2, 5, 3, 6}, res.Ids.GetIntId().Data)
	})

	suite.Run("strict_group_size_all_groups_complete", func() {
		// All groups have exactly groupSize elements
		ids1 := []int64{1, 2}
		scores1 := []float32{-1.0, -2.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{3, 4}
		scores2 := []float32{-1.5, -2.5}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
		data1.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"A", "B"},
						},
					},
				},
			},
		}
		data2.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"A", "B"},
						},
					},
				},
			},
		}
		dataArray := make([]*schemapb.SearchResultData, 0)
		dataArray = append(dataArray, data1)
		dataArray = append(dataArray, data2)

		// With strictGroupSize=true, all groups have 2 elements, so all should be returned
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(2).WithGroupByField(101).WithStrictGroupSize(true)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		// Both groups "A" and "B" have 2 elements each
		suite.Equal(4, len(res.Ids.GetIntId().Data))
		suite.ElementsMatch([]int64{1, 3, 2, 4}, res.Ids.GetIntId().Data)
	})
}

func TestSearchReduce(t *testing.T) {
	paramtable.Init()
	suite.Run(t, new(SearchReduceSuite))
}
