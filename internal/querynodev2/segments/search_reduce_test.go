package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func (suite *SearchReduceSuite) TestElementIndices_BackfillNilForEmptyResult() {
	// Reproduces the bug in issue #48602: when one segment returns 0 hits for an
	// element_filter search, C++ reduce creates an empty LongArray for ElementIndices
	// which proto3 serializes as absent (nil). The other segment returns hits with
	// ElementIndices set. The reduce should back-fill nil with empty LongArray.
	const (
		nq   = 1
		topk = 4
	)

	suite.Run("common_reduce", func() {
		// data1: has hits with ElementIndices
		data1 := mock_segcore.GenSearchResultData(nq, topk,
			[]int64{1, 2, 3, 4},
			[]float32{-1.0, -2.0, -3.0, -4.0},
			[]int64{4},
		)
		data1.ElementIndices = &schemapb.LongArray{Data: []int64{0, 1, 2, 3}}

		// data2: 0 hits, ElementIndices nil (proto3 lost the empty LongArray)
		data2 := mock_segcore.GenSearchResultData(nq, topk,
			[]int64{},
			[]float32{},
			[]int64{0},
		)
		data2.Ids = nil
		data2.ElementIndices = nil // simulates proto3 deserialization of empty LongArray

		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1)
		searchReduce := &SearchCommonReduce{}
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{data1, data2}, reduceInfo)
		suite.NoError(err)
		suite.NotNil(res.ElementIndices)
		suite.Equal([]int64{0, 1, 2, 3}, res.ElementIndices.Data)
		suite.Equal([]int64{1, 2, 3, 4}, res.Ids.GetIntId().Data)
	})

	suite.Run("common_reduce_reversed_order", func() {
		// Empty result first, then result with hits — order should not matter
		data1 := mock_segcore.GenSearchResultData(nq, topk,
			[]int64{},
			[]float32{},
			[]int64{0},
		)
		data1.Ids = nil
		data1.ElementIndices = nil

		data2 := mock_segcore.GenSearchResultData(nq, topk,
			[]int64{1, 2},
			[]float32{-1.0, -2.0},
			[]int64{2},
		)
		data2.ElementIndices = &schemapb.LongArray{Data: []int64{5, 6}}

		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1)
		searchReduce := &SearchCommonReduce{}
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{data1, data2}, reduceInfo)
		suite.NoError(err)
		suite.NotNil(res.ElementIndices)
		suite.Equal([]int64{5, 6}, res.ElementIndices.Data)
	})

	suite.Run("group_by_reduce", func() {
		data1 := mock_segcore.GenSearchResultData(nq, topk,
			[]int64{1, 2},
			[]float32{-1.0, -2.0},
			[]int64{2},
		)
		data1.ElementIndices = &schemapb.LongArray{Data: []int64{10, 11}}
		data1.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{100, 200}}},
				},
			},
		}

		data2 := mock_segcore.GenSearchResultData(nq, topk,
			[]int64{},
			[]float32{},
			[]int64{0},
		)
		data2.Ids = nil
		data2.ElementIndices = nil
		data2.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{}}},
				},
			},
		}

		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByField(101)
		searchReduce := &SearchGroupByReduce{}
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{data1, data2}, reduceInfo)
		suite.NoError(err)
		suite.NotNil(res.ElementIndices)
		suite.Equal([]int64{10, 11}, res.ElementIndices.Data)
	})
}

func (suite *SearchReduceSuite) TestElementLevelDedupUsesPKAndElementIndex() {
	const (
		nq   = 1
		topk = 4
	)

	makeData := func() *schemapb.SearchResultData {
		data := mock_segcore.GenSearchResultData(nq, topk,
			[]int64{5, 5, 5, 6},
			[]float32{0.99, 0.98, 0.97, 0.96},
			[]int64{4},
		)
		data.ElementIndices = &schemapb.LongArray{Data: []int64{0, 1, 1, 0}}
		return data
	}

	suite.Run("common_reduce", func() {
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1)
		searchReduce := &SearchCommonReduce{}
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{makeData()}, reduceInfo)
		suite.NoError(err)

		suite.Equal([]int64{5, 5, 6}, res.Ids.GetIntId().Data)
		suite.Equal([]float32{0.99, 0.98, 0.96}, res.Scores)
		suite.Equal([]int64{0, 1, 0}, res.ElementIndices.GetData())
		suite.Equal([]int64{3}, res.Topks)
	})

	suite.Run("group_by_reduce", func() {
		data := makeData()
		data.GroupByFieldValue = &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{5, 5, 5, 6}},
					},
				},
			},
		}

		reduceInfo := reduce.NewReduceSearchResultInfo(nq, 2).WithGroupSize(2).WithGroupByField(101)
		searchReduce := &SearchGroupByReduce{}
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{data}, reduceInfo)
		suite.NoError(err)

		suite.Equal([]int64{5, 5, 6}, res.Ids.GetIntId().Data)
		suite.Equal([]float32{0.99, 0.98, 0.96}, res.Scores)
		suite.Equal([]int64{0, 1, 0}, res.ElementIndices.GetData())
		suite.Equal([]int64{3}, res.Topks)
	})
}

func (suite *SearchReduceSuite) TestElementIndices_NoElementLevel() {
	// When no result has ElementIndices, ret.ElementIndices should remain nil.
	const (
		nq   = 1
		topk = 4
	)
	data1 := mock_segcore.GenSearchResultData(nq, topk,
		[]int64{1, 2}, []float32{-1.0, -2.0}, []int64{2})
	data2 := mock_segcore.GenSearchResultData(nq, topk,
		[]int64{3, 4}, []float32{-3.0, -4.0}, []int64{2})

	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1)
	searchReduce := &SearchCommonReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{data1, data2}, reduceInfo)
	suite.NoError(err)
	suite.Nil(res.ElementIndices)
}

func (suite *SearchReduceSuite) TestElementIndices_InconsistentWithData() {
	// If a result has IDs (actual hits) but missing ElementIndices while others have it,
	// this is a real inconsistency and should return error.
	const (
		nq   = 1
		topk = 4
	)
	data1 := mock_segcore.GenSearchResultData(nq, topk,
		[]int64{1, 2}, []float32{-1.0, -2.0}, []int64{2})
	data1.ElementIndices = &schemapb.LongArray{Data: []int64{0, 1}}

	data2 := mock_segcore.GenSearchResultData(nq, topk,
		[]int64{3, 4}, []float32{-3.0, -4.0}, []int64{2})
	data2.ElementIndices = nil // has data but no ElementIndices — inconsistent

	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1)
	searchReduce := &SearchCommonReduce{}
	_, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{data1, data2}, reduceInfo)
	suite.Error(err)
	suite.Contains(err.Error(), "inconsistent element-level flag")
}

func TestSearchReduce(t *testing.T) {
	paramtable.Init()
	suite.Run(t, new(SearchReduceSuite))
}
