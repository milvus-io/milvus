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
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByFieldIdsFromProto(101, nil)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.ElementsMatch([]int64{1, 2, 3, 4}, res.Ids.GetIntId().Data)
		suite.ElementsMatch([]float32{-1.0, -2.0, -3.0, -4.0}, res.Scores)
		suite.Require().Len(res.GroupByFieldValues, 1)
		suite.ElementsMatch([]int32{2, 3, 4, 5}, res.GroupByFieldValues[0].GetScalars().GetIntData().Data)
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
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByFieldIdsFromProto(101, nil)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.ElementsMatch([]int64{1, 4}, res.Ids.GetIntId().Data)
		suite.ElementsMatch([]float32{-1.0, -1.0}, res.Scores)
		suite.Require().Len(res.GroupByFieldValues, 1)
		suite.ElementsMatch([]bool{true, false}, res.GroupByFieldValues[0].GetScalars().GetBoolData().Data)
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
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByFieldIdsFromProto(101, nil)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.ElementsMatch([]int64{1, 2, 3, 4}, res.Ids.GetIntId().Data)
		suite.ElementsMatch([]float32{-1.0, -2.0, -3.0, -4.0}, res.Scores)
		suite.Require().Len(res.GroupByFieldValues, 1)
		suite.ElementsMatch([]string{"1", "2", "3", "4"}, res.GroupByFieldValues[0].GetScalars().GetStringData().Data)
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
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(3).WithGroupByFieldIdsFromProto(101, nil)
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.ElementsMatch([]int64{1, 4, 2, 5, 3, 6, 7}, res.Ids.GetIntId().Data)
		suite.ElementsMatch([]float32{-1.0, -1.0, -1.0, -2.0, -3.0, -3.0, -4.0}, res.Scores)
		suite.Require().Len(res.GroupByFieldValues, 1)
		suite.ElementsMatch([]string{"1", "1", "2", "2", "3", "3", "4"}, res.GroupByFieldValues[0].GetScalars().GetStringData().Data)
	})

	suite.Run("reduce_agg_single_field", func() {
		ids1 := []int64{1, 2, 3}
		scores1 := []float32{-1.0, -2.0, -3.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{4, 5, 6}
		scores2 := []float32{-1.5, -2.5, -3.5}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, 3, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, 3, ids2, scores2, topks2)
		data1.GroupByFieldValues = []*schemapb.FieldData{
			{
				FieldId: 101,
				Type:    schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}}},
					},
				},
			},
		}
		data2.GroupByFieldValues = []*schemapb.FieldData{
			{
				FieldId: 101,
				Type:    schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 40, 50}}},
					},
				},
			},
		}
		dataArray := []*schemapb.SearchResultData{data1, data2}
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, 3).WithGroupSize(1).WithGroupByFieldIds([]int64{101})
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.Equal([]int64{1, 2, 5}, res.Ids.GetIntId().GetData())
		suite.Equal([]float32{-1.0, -2.0, -2.5}, res.Scores)
		suite.Nil(res.GroupByFieldValue)
		suite.Len(res.GroupByFieldValues, 1)
		suite.Equal(int64(101), res.GroupByFieldValues[0].GetFieldId())
		suite.Equal([]int64{10, 20, 40}, res.GroupByFieldValues[0].GetScalars().GetLongData().GetData())
	})

	suite.Run("reduce_agg_multi_field_no_collision", func() {
		ids1 := []int64{1, 2}
		scores1 := []float32{-1.0, -2.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{3, 4}
		scores2 := []float32{-1.5, -2.5}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, 2, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, 2, ids2, scores2, topks2)
		data1.GroupByFieldValues = []*schemapb.FieldData{
			{
				FieldId: 101,
				Type:    schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 12}}},
					},
				},
			},
			{
				FieldId: 102,
				Type:    schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"23", "3"}}},
					},
				},
			},
		}
		data2.GroupByFieldValues = []*schemapb.FieldData{
			{
				FieldId: 101,
				Type:    schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 12}}},
					},
				},
			},
			{
				FieldId: 102,
				Type:    schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"23", "3"}}},
					},
				},
			},
		}
		dataArray := []*schemapb.SearchResultData{data1, data2}
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, 2).WithGroupSize(1).WithGroupByFieldIds([]int64{101, 102})
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)
		suite.Nil(err)
		suite.Equal([]int64{1, 2}, res.Ids.GetIntId().GetData())
		suite.Equal([]float32{-1.0, -2.0}, res.Scores)
		suite.Nil(res.GroupByFieldValue)
		suite.Len(res.GroupByFieldValues, 2)
		byID := map[int64]*schemapb.FieldData{}
		for _, fd := range res.GroupByFieldValues {
			byID[fd.GetFieldId()] = fd
		}
		suite.Equal([]int64{1, 12}, byID[101].GetScalars().GetLongData().GetData())
		suite.Equal([]string{"23", "3"}, byID[102].GetScalars().GetStringData().GetData())
	})

	suite.Run("reduce_agg_multi_missing_column_returns_error", func() {
		ids1 := []int64{1, 2}
		scores1 := []float32{-1.0, -2.0}
		topks1 := []int64{int64(len(ids1))}
		ids2 := []int64{3}
		scores2 := []float32{-1.5}
		topks2 := []int64{int64(len(ids2))}
		data1 := mock_segcore.GenSearchResultData(nq, 2, ids1, scores1, topks1)
		data2 := mock_segcore.GenSearchResultData(nq, 2, ids2, scores2, topks2)
		data1.GroupByFieldValues = []*schemapb.FieldData{
			{
				FieldId: 101,
				Type:    schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}},
				}},
			},
			{
				FieldId: 102,
				Type:    schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"A", "B"}}},
				}},
			},
		}
		data2.GroupByFieldValues = []*schemapb.FieldData{
			{
				FieldId: 101,
				Type:    schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{3}}},
				}},
			},
		}
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, 2).WithGroupSize(1).WithGroupByFieldIds([]int64{101, 102})
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{data1, data2}, reduceInfo)
		suite.Error(err)
		suite.Nil(res)
		suite.Contains(err.Error(), "group-by field 102 missing")
	})

	suite.Run("reduce_agg_multi_null_merges", func() {
		// Two rows with null composite keys must merge into the same bucket
		// (null == null for grouping). groupSize=1 keeps only the highest-scored.
		ids := []int64{1, 2}
		scores := []float32{-1.0, -2.0}
		topks := []int64{int64(len(ids))}
		data := mock_segcore.GenSearchResultData(nq, 2, ids, scores, topks)
		data.GroupByFieldValues = []*schemapb.FieldData{
			{
				FieldId:   101,
				Type:      schemapb.DataType_Int64,
				ValidData: []bool{false, false},
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{0, 0}}},
				}},
			},
		}
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, 5).WithGroupSize(1).WithGroupByFieldIds([]int64{101})
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{data}, reduceInfo)
		suite.Nil(err)
		suite.Equal([]int64{1}, res.Ids.GetIntId().GetData(), "null-keyed rows merge; groupSize=1 keeps only best-score")
	})

	suite.Run("reduce_agg_multi_type_normalization", func() {
		// int32 column values surface through the iterator as int32; after
		// reduce.NormalizeScalar both rows hash identically, so two rows
		// carrying the same int32=42 value land in one bucket.
		ids := []int64{1, 2}
		scores := []float32{-1.0, -2.0}
		topks := []int64{int64(len(ids))}
		data := mock_segcore.GenSearchResultData(nq, 2, ids, scores, topks)
		data.GroupByFieldValues = []*schemapb.FieldData{
			{
				FieldId: 101,
				Type:    schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{42, 42}}},
				}},
			},
		}
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, 5).WithGroupSize(2).WithGroupByFieldIds([]int64{101})
		searchReduce := InitSearchReducer(reduceInfo)
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{data}, reduceInfo)
		suite.Nil(err)
		suite.Equal([]int64{1, 2}, res.Ids.GetIntId().GetData(), "same int32 value merges via normalization; both rows kept (groupSize=2)")
	})

	suite.Run("reduce_group_by_empty_input", func() {
		dataArray := make([]*schemapb.SearchResultData, 0)
		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(3).WithGroupByFieldIdsFromProto(101, nil)
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

		reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByFieldIdsFromProto(101, nil)
		searchReduce := &SearchGroupByReduce{}
		res, err := searchReduce.ReduceSearchResultData(context.TODO(), []*schemapb.SearchResultData{data1, data2}, reduceInfo)
		suite.NoError(err)
		suite.NotNil(res.ElementIndices)
		suite.Equal([]int64{10, 11}, res.ElementIndices.Data)
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
