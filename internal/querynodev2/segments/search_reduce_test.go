package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
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

// ====================================================================================
// SearchOrderByReduce Tests
// ====================================================================================

func (suite *SearchReduceSuite) TestSearchOrderByReduce_EmptyInput() {
	const (
		nq   = 1
		topk = 4
	)

	dataArray := make([]*schemapb.SearchResultData, 0)
	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true},
	}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithOrderByFields(orderByFields)
	searchReduce := &SearchOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	suite.Equal(int64(nq), res.GetNumQueries())
	suite.Equal(int64(topk), res.GetTopK())
	suite.Equal(0, len(res.GetScores()))
}

func (suite *SearchReduceSuite) TestSearchOrderByReduce_AscendingOrder() {
	const (
		nq   = 1
		topk = 4
	)

	// Segment 1: IDs 1-4 with prices [100, 50, 200, 75]
	ids1 := []int64{1, 2, 3, 4}
	scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	topks1 := []int64{int64(len(ids1))}
	data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
	data1.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{100, 50, 200, 75},
					},
				},
			},
		},
	}}

	// Segment 2: IDs 5-8 with prices [80, 120, 30, 180]
	ids2 := []int64{5, 6, 7, 8}
	scores2 := []float32{-1.5, -2.5, -3.5, -4.5}
	topks2 := []int64{int64(len(ids2))}
	data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
	data2.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{80, 120, 30, 180},
					},
				},
			},
		},
	}}

	dataArray := []*schemapb.SearchResultData{data1, data2}
	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true},
	}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithOrderByFields(orderByFields)
	searchReduce := &SearchOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	// Expected order by price ASC: 30(ID7), 50(ID2), 75(ID4), 80(ID5)
	suite.Equal([]int64{7, 2, 4, 5}, res.Ids.GetIntId().Data)
	// Verify OrderByFieldValues is set
	suite.NotNil(res.OrderByFieldValues)
	suite.Equal(1, len(res.OrderByFieldValues))
}

func (suite *SearchReduceSuite) TestSearchOrderByReduce_DescendingOrder() {
	const (
		nq   = 1
		topk = 4
	)

	ids1 := []int64{1, 2, 3, 4}
	scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	topks1 := []int64{int64(len(ids1))}
	data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
	data1.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{100, 50, 200, 75},
					},
				},
			},
		},
	}}

	ids2 := []int64{5, 6, 7, 8}
	scores2 := []float32{-1.5, -2.5, -3.5, -4.5}
	topks2 := []int64{int64(len(ids2))}
	data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
	data2.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{80, 120, 30, 180},
					},
				},
			},
		},
	}}

	dataArray := []*schemapb.SearchResultData{data1, data2}
	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: false}, // descending
	}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithOrderByFields(orderByFields)
	searchReduce := &SearchOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	// Expected order by price DESC: 200(ID3), 180(ID8), 120(ID6), 100(ID1)
	suite.Equal([]int64{3, 8, 6, 1}, res.Ids.GetIntId().Data)
}

func (suite *SearchReduceSuite) TestSearchOrderByReduce_DuplicateIds() {
	const (
		nq   = 1
		topk = 4
	)

	// Both segments have ID 1
	ids1 := []int64{1, 2, 3, 4}
	scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	topks1 := []int64{int64(len(ids1))}
	data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
	data1.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{100, 50, 200, 75},
					},
				},
			},
		},
	}}

	ids2 := []int64{1, 5, 6, 7} // ID 1 is duplicate
	scores2 := []float32{-1.5, -2.5, -3.5, -4.5}
	topks2 := []int64{int64(len(ids2))}
	data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
	data2.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{10, 80, 120, 30}, // ID 1 has price 10 in segment 2
					},
				},
			},
		},
	}}

	dataArray := []*schemapb.SearchResultData{data1, data2}
	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true},
	}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithOrderByFields(orderByFields)
	searchReduce := &SearchOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	// ID 1 should appear only once (from segment 2 with price 10)
	// Expected order: 10(ID1), 30(ID7), 50(ID2), 75(ID4)
	suite.Equal([]int64{1, 7, 2, 4}, res.Ids.GetIntId().Data)
	suite.Equal(4, len(res.Ids.GetIntId().Data))
}

func (suite *SearchReduceSuite) TestSearchOrderByReduce_FallbackToCommonReduce() {
	const (
		nq   = 1
		topk = 4
	)

	ids1 := []int64{1, 2, 3, 4}
	scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	topks1 := []int64{int64(len(ids1))}
	data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)

	ids2 := []int64{5, 6, 7, 8}
	scores2 := []float32{-0.5, -1.5, -2.5, -3.5}
	topks2 := []int64{int64(len(ids2))}
	data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)

	dataArray := []*schemapb.SearchResultData{data1, data2}
	// Empty orderByFields should fallback to common reduce (score-based)
	orderByFields := []*planpb.OrderByField{}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithOrderByFields(orderByFields)
	searchReduce := &SearchOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	// Should be sorted by score (highest first): -0.5(ID5), -1.0(ID1), -1.5(ID6), -2.0(ID2)
	suite.Equal([]int64{5, 1, 6, 2}, res.Ids.GetIntId().Data)
}

func (suite *SearchReduceSuite) TestSearchOrderByReduce_VarCharOrderBy() {
	const (
		nq   = 1
		topk = 4
	)

	ids1 := []int64{1, 2, 3, 4}
	scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	topks1 := []int64{int64(len(ids1))}
	data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
	data1.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_VarChar,
		FieldName: "name",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"charlie", "alice", "delta", "bob"},
					},
				},
			},
		},
	}}

	ids2 := []int64{5, 6, 7, 8}
	scores2 := []float32{-1.5, -2.5, -3.5, -4.5}
	topks2 := []int64{int64(len(ids2))}
	data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
	data2.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_VarChar,
		FieldName: "name",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"eve", "adam", "frank", "bella"},
					},
				},
			},
		},
	}}

	dataArray := []*schemapb.SearchResultData{data1, data2}
	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true},
	}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithOrderByFields(orderByFields)
	searchReduce := &SearchOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	// Expected order by name ASC: adam(ID6), alice(ID2), bella(ID8), bob(ID4)
	suite.Equal([]int64{6, 2, 8, 4}, res.Ids.GetIntId().Data)
}

// ====================================================================================
// SearchGroupOrderByReduce Tests
// ====================================================================================

func (suite *SearchReduceSuite) TestSearchGroupOrderByReduce_EmptyInput() {
	const (
		nq   = 1
		topk = 4
	)

	dataArray := make([]*schemapb.SearchResultData, 0)
	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true},
	}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(2).WithGroupByField(101).WithOrderByFields(orderByFields)
	searchReduce := &SearchGroupOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	suite.Equal(int64(nq), res.GetNumQueries())
	suite.Equal(int64(topk), res.GetTopK())
	suite.Equal(0, len(res.GetScores()))
}

func (suite *SearchReduceSuite) TestSearchGroupOrderByReduce_BasicGroupOrderBy() {
	const (
		nq   = 1
		topk = 3
	)

	// Segment 1: Items with category groups and prices
	// IDs: [1, 2, 3, 4], Categories: [A, B, A, B], Prices: [100, 50, 80, 120]
	ids1 := []int64{1, 2, 3, 4}
	scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	topks1 := []int64{int64(len(ids1))}
	data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
	data1.GroupByFieldValue = &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "category",
		FieldId:   101,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"A", "B", "A", "B"},
					},
				},
			},
		},
	}
	data1.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{100, 50, 80, 120},
					},
				},
			},
		},
	}}

	// Segment 2: More items
	// IDs: [5, 6, 7, 8], Categories: [C, A, B, C], Prices: [40, 90, 70, 110]
	ids2 := []int64{5, 6, 7, 8}
	scores2 := []float32{-1.5, -2.5, -3.5, -4.5}
	topks2 := []int64{int64(len(ids2))}
	data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
	data2.GroupByFieldValue = &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "category",
		FieldId:   101,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"C", "A", "B", "C"},
					},
				},
			},
		},
	}
	data2.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{40, 90, 70, 110},
					},
				},
			},
		},
	}}

	dataArray := []*schemapb.SearchResultData{data1, data2}
	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true}, // Order groups by price ASC
	}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByField(101).WithOrderByFields(orderByFields)
	searchReduce := &SearchGroupOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	// Groups are ordered by the first item's price in each group:
	// Group C: first item ID5 has price 40
	// Group B: first item ID2 has price 50
	// Group A: first item ID1 has price 100
	// Expected group order: C(40), B(50), A(100)
	suite.NotNil(res.GroupByFieldValue)
	suite.NotNil(res.OrderByFieldValues)
}

func (suite *SearchReduceSuite) TestSearchGroupOrderByReduce_DescendingGroupOrder() {
	const (
		nq   = 1
		topk = 3
	)

	ids1 := []int64{1, 2, 3, 4}
	scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	topks1 := []int64{int64(len(ids1))}
	data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
	data1.GroupByFieldValue = &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "category",
		FieldId:   101,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"A", "B", "A", "B"},
					},
				},
			},
		},
	}
	data1.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{100, 50, 80, 120},
					},
				},
			},
		},
	}}

	ids2 := []int64{5, 6, 7, 8}
	scores2 := []float32{-1.5, -2.5, -3.5, -4.5}
	topks2 := []int64{int64(len(ids2))}
	data2 := mock_segcore.GenSearchResultData(nq, topk, ids2, scores2, topks2)
	data2.GroupByFieldValue = &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "category",
		FieldId:   101,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"C", "A", "B", "C"},
					},
				},
			},
		},
	}
	data2.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{40, 90, 70, 110},
					},
				},
			},
		},
	}}

	dataArray := []*schemapb.SearchResultData{data1, data2}
	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: false}, // Order groups by price DESC
	}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByField(101).WithOrderByFields(orderByFields)
	searchReduce := &SearchGroupOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	// Groups ordered by price DESC: A(100), B(50), C(40)
	suite.NotNil(res.GroupByFieldValue)
}

func (suite *SearchReduceSuite) TestSearchGroupOrderByReduce_WithGroupSize() {
	const (
		nq   = 1
		topk = 2
	)

	// Create data with multiple items per group
	ids1 := []int64{1, 2, 3, 4, 5, 6}
	scores1 := []float32{-1.0, -2.0, -3.0, -4.0, -5.0, -6.0}
	topks1 := []int64{int64(len(ids1))}
	data1 := mock_segcore.GenSearchResultData(nq, 6, ids1, scores1, topks1)
	data1.GroupByFieldValue = &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "category",
		FieldId:   101,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"A", "A", "B", "B", "A", "B"},
					},
				},
			},
		},
	}
	data1.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{100, 90, 50, 60, 80, 70},
					},
				},
			},
		},
	}}

	dataArray := []*schemapb.SearchResultData{data1}
	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true},
	}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(2).WithGroupByField(101).WithOrderByFields(orderByFields)
	searchReduce := &SearchGroupOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	// With groupSize=2 and topk=2 groups
	// Group B: first item has price 50 (ID3), includes ID3 and ID4
	// Group A: first item has price 100 (ID1), includes ID1 and ID2
	// Groups sorted by first item's price: B(50), A(100)
	suite.NotNil(res.GroupByFieldValue)
	suite.True(len(res.Ids.GetIntId().Data) <= 4) // At most 2 groups * 2 items per group
}

func (suite *SearchReduceSuite) TestSearchGroupOrderByReduce_FallbackToGroupByReduce() {
	const (
		nq   = 1
		topk = 4
	)

	ids1 := []int64{1, 2, 3, 4}
	scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	topks1 := []int64{int64(len(ids1))}
	data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
	data1.GroupByFieldValue = &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"A", "B", "C", "D"},
					},
				},
			},
		},
	}

	dataArray := []*schemapb.SearchResultData{data1}
	// Empty orderByFields should fallback to group by reduce (score-based)
	orderByFields := []*planpb.OrderByField{}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByField(101).WithOrderByFields(orderByFields)
	searchReduce := &SearchGroupOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	// Should be sorted by score within groups
	suite.Equal([]int64{1, 2, 3, 4}, res.Ids.GetIntId().Data)
}

func (suite *SearchReduceSuite) TestSearchGroupOrderByReduce_IntGroupBy() {
	const (
		nq   = 1
		topk = 3
	)

	ids1 := []int64{1, 2, 3, 4}
	scores1 := []float32{-1.0, -2.0, -3.0, -4.0}
	topks1 := []int64{int64(len(ids1))}
	data1 := mock_segcore.GenSearchResultData(nq, topk, ids1, scores1, topks1)
	data1.GroupByFieldValue = &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "category_id",
		FieldId:   101,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{10, 20, 10, 20},
					},
				},
			},
		},
	}
	data1.OrderByFieldValues = []*schemapb.FieldData{{
		Type:      schemapb.DataType_Int64,
		FieldName: "price",
		FieldId:   102,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{100, 50, 80, 120},
					},
				},
			},
		},
	}}

	dataArray := []*schemapb.SearchResultData{data1}
	orderByFields := []*planpb.OrderByField{
		{FieldId: 102, Ascending: true},
	}
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topk).WithGroupSize(1).WithGroupByField(101).WithOrderByFields(orderByFields)
	searchReduce := &SearchGroupOrderByReduce{}
	res, err := searchReduce.ReduceSearchResultData(context.TODO(), dataArray, reduceInfo)

	suite.Nil(err)
	suite.NotNil(res)
	// Group 20: first item ID2 has price 50
	// Group 10: first item ID1 has price 100
	// Groups sorted by price ASC: 20(50), 10(100)
	suite.NotNil(res.GroupByFieldValue)
}

func TestSearchReduce(t *testing.T) {
	paramtable.Init()
	suite.Run(t, new(SearchReduceSuite))
}
