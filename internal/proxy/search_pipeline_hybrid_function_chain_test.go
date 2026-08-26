// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"math"

	"github.com/bytedance/mockey"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
)

func (s *SearchPipelineSuite) TestHybridSearchPipeFunctionChainOwnsFinalCount() {
	strategies := []string{
		string(chain.MergeStrategyRRF),
		string(chain.MergeStrategyWeighted),
		string(chain.MergeStrategyMax),
		string(chain.MergeStrategySum),
		string(chain.MergeStrategyAvg),
	}
	for _, strategy := range strategies {
		s.Run(strategy, func() {
			task := getHybridSearchTask("test_collection", [][]string{
				{"1"},
				{"2"},
			}, nil)
			task.Nq = 1
			for _, subReq := range task.SubReqs {
				subReq.Nq = 1
				subReq.Topk = 3
			}
			task.rankParams = &rankParams{
				limit:        1,
				offset:       1,
				roundDecimal: 1,
			}

			mergeParams := map[string]*schemapb.FunctionParamValue{
				chain.MergeParamStrategy: chainStringParam(strategy),
			}
			if strategy == string(chain.MergeStrategyWeighted) {
				mergeParams[chain.MergeParamWeights] = chainArrayParam(
					chainDoubleParam(0.5),
					chainDoubleParam(0.5),
				)
			}
			chainPB := l2FunctionChain(
				&schemapb.FunctionChainOp{Op: types.OpTypeMerge, Params: mergeParams},
				l2LimitFunctionChain(2).GetOps()[0],
			)
			repr, err := chain.ProtoChainToRepr(chainPB)
			s.Require().NoError(err)
			task.rerankMeta = &functionChainRerankMeta{repr: repr, chainPB: chainPB}

			pipeline, err := newPipeline(hybridSearchPipe, task)
			s.Require().NoError(err)
			s.Require().NoError(pipeline.AddNodes(task, endNode))

			f1 := genTestSearchResultData(1, 3, schemapb.DataType_Int64, "intField", 101, true)
			f2 := genTestSearchResultData(1, 3, schemapb.DataType_Int64, "intField", 101, true)
			results, _, err := pipeline.Run(
				context.Background(),
				s.span,
				[]*internalpb.SearchResults{f1, f2},
				segcore.StorageCost{},
			)
			s.Require().NoError(err)

			result := results.GetResults()
			s.Equal([]int64{2}, result.GetTopks())
			s.Len(result.GetIds().GetIntId().GetData(), 2)
			s.Len(result.GetScores(), 2)
		})
	}
}

func (s *SearchPipelineSuite) TestHybridFunctionChainWithoutLimitEmitsAllMergedCandidates() {
	originalReduceFactory := opFactory[hybridSearchReduceOp]
	defer func() { opFactory[hybridSearchReduceOp] = originalReduceFactory }()

	makeResult := func(ids []int64, scores []float32) *milvuspb.SearchResults {
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       int64(len(ids)),
				Topks:      []int64{int64(len(ids))},
				Ids:        testSearchResultIDs(ids...),
				Scores:     scores,
			},
		}
	}
	reduced := []*milvuspb.SearchResults{
		makeResult([]int64{1, 2}, []float32{0.9, 0.7}),
		makeResult([]int64{3, 4}, []float32{0.8, 0.6}),
	}
	opFactory[hybridSearchReduceOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(context.Context, trace.Span, ...any) ([]any, error) {
			return []any{reduced, []string{metric.IP, metric.IP}}, nil
		}), nil
	}

	chainPB := l2FunctionChain(&schemapb.FunctionChainOp{
		Op: types.OpTypeMerge,
		Params: map[string]*schemapb.FunctionParamValue{
			chain.MergeParamStrategy: chainStringParam(string(chain.MergeStrategyMax)),
		},
	})
	repr, err := chain.ProtoChainToRepr(chainPB)
	s.Require().NoError(err)

	task := getHybridSearchTask("test_collection", [][]string{{"1"}, {"2"}}, nil)
	task.Nq = 1
	// Legacy response controls must not add an implicit tail to a public chain.
	task.rankParams = &rankParams{limit: 1, offset: 2, roundDecimal: 0}
	task.rerankMeta = &functionChainRerankMeta{chainPB: chainPB, repr: repr}
	for _, subReq := range task.SubReqs {
		subReq.Nq = 1
		subReq.Topk = 2
	}

	pipeline, err := newPipeline(hybridSearchPipe, task)
	s.Require().NoError(err)
	s.Require().NoError(pipeline.AddNodes(task, endNode))
	results, _, err := pipeline.Run(context.Background(), s.span, nil, segcore.StorageCost{})
	s.Require().NoError(err)

	result := results.GetResults()
	s.Equal([]int64{4}, result.GetTopks())
	s.ElementsMatch([]int64{1, 2, 3, 4}, result.GetIds().GetIntId().GetData())
	s.Len(result.GetScores(), 4)
}

func (s *SearchPipelineSuite) TestHybridFunctionChainRequeryInputDoesNotLeak() {
	task := getHybridSearchTask("test_collection", [][]string{
		{"1", "2"},
		{"3", "4"},
	}, []string{"outputField"})
	task.needRequery = true
	task.schema.Fields = append(task.schema.Fields,
		&schemapb.FieldSchema{FieldID: 102, Name: "outputField", DataType: schemapb.DataType_Int64})

	chainPB := l2FunctionChain(
		&schemapb.FunctionChainOp{
			Op: types.OpTypeMerge,
			Params: map[string]*schemapb.FunctionParamValue{
				chain.MergeParamStrategy: chainStringParam(string(chain.MergeStrategyMax)),
			},
		},
		mapOp(types.ScoreFieldName, "num_combine", columnArg(types.ScoreFieldName), columnArg("intField")),
		l2LimitFunctionChain(2).GetOps()[0],
	)
	repr, err := chain.ProtoChainToRepr(chainPB)
	s.Require().NoError(err)
	task.rerankMeta = &functionChainRerankMeta{
		inputFieldNames: []string{"intField"},
		inputFieldIDs:   []int64{101},
		chainPB:         chainPB,
		repr:            repr,
	}

	intField := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	intField.FieldId = 101
	outputField := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "outputField", 20)
	outputField.FieldId = 102
	pkField := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	pkField.FieldId = 100
	requeryMock := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{intField, outputField, pkField},
	}, segcore.StorageCost{}, nil).Build()
	defer requeryMock.UnPatch()

	pipeline, err := newSearchPipeline(task)
	s.Require().NoError(err)
	s.Equal(hybridSearchWithRequeryAndRerankByFieldDataPipe.name, pipeline.name)

	f1 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	f2 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	results, _, err := pipeline.Run(
		context.Background(),
		s.span,
		[]*internalpb.SearchResults{f1, f2},
		segcore.StorageCost{},
	)
	s.Require().NoError(err)

	result := results.GetResults()
	s.Equal([]int64{2, 2}, result.GetTopks())
	s.Len(result.GetIds().GetIntId().GetData(), 4)
	s.Len(result.GetScores(), 4)
	s.Require().Len(result.GetFieldsData(), 1)
	s.Equal(int64(102), result.GetFieldsData()[0].GetFieldId())
	s.Equal("outputField", result.GetFieldsData()[0].GetFieldName())
	s.Len(result.GetFieldsData()[0].GetScalars().GetLongData().GetData(), 4)
}

func (s *SearchPipelineSuite) TestHybridFunctionChainElementLevelPreservesNativeKeys() {
	chainPB := l2FunctionChain(
		&schemapb.FunctionChainOp{
			Op: types.OpTypeMerge,
			Params: map[string]*schemapb.FunctionParamValue{
				chain.MergeParamStrategy: chainStringParam(string(chain.MergeStrategyMax)),
			},
		},
		l2LimitFunctionChain(3).GetOps()[0],
	)
	repr, err := chain.ProtoChainToRepr(chainPB)
	s.Require().NoError(err)

	originalReduceFactory := opFactory[hybridSearchReduceOp]
	defer func() { opFactory[hybridSearchReduceOp] = originalReduceFactory }()

	tests := []struct {
		name   string
		pkType schemapb.DataType
		ids1   *schemapb.IDs
		ids2   *schemapb.IDs
	}{
		{
			name:   "int64 primary key",
			pkType: schemapb.DataType_Int64,
			ids1:   testSearchResultIDs(10, 10, 20),
			ids2:   testSearchResultIDs(10, 20, 20),
		},
		{
			name:   "string primary key",
			pkType: schemapb.DataType_VarChar,
			ids1:   testSearchResultStringIDs("row-a", "row-a", "row-b"),
			ids2:   testSearchResultStringIDs("row-a", "row-b", "row-b"),
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			makeResult := func(ids *schemapb.IDs, elementIndices []int64, scores []float32, values []int64) *milvuspb.SearchResults {
				return &milvuspb.SearchResults{
					Status: merr.Success(),
					Results: &schemapb.SearchResultData{
						NumQueries:     1,
						TopK:           3,
						Topks:          []int64{3},
						Ids:            ids,
						Scores:         scores,
						ElementIndices: &schemapb.LongArray{Data: elementIndices},
						FieldsData: []*schemapb.FieldData{
							testutils.GenerateScalarFieldDataWithValue(schemapb.DataType_Int64, "intField", 101, values),
						},
					},
				}
			}
			reduced := []*milvuspb.SearchResults{
				makeResult(test.ids1, []int64{0, 1, 0}, []float32{0.90, 0.80, 0.70}, []int64{100, 110, 200}),
				makeResult(test.ids2, []int64{1, 0, 2}, []float32{0.95, 0.75, 0.60}, []int64{110, 200, 220}),
			}

			opFactory[hybridSearchReduceOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
				return searchPipelineTestOperator(func(context.Context, trace.Span, ...any) ([]any, error) {
					return []any{reduced, []string{"IP", "IP"}}, nil
				}), nil
			}

			task := getHybridSearchTask("test_collection", [][]string{{"1"}, {"2"}}, []string{"intField"})
			task.hybridElementLevel = true
			task.Nq = 1
			task.rerankMeta = &functionChainRerankMeta{chainPB: chainPB, repr: repr}
			task.schema.CollectionSchema.Fields[0].DataType = test.pkType
			task.schema.PkField.DataType = test.pkType
			for _, subReq := range task.SubReqs {
				subReq.Nq = 1
				subReq.Topk = 3
			}

			pipeline, err := newPipeline(hybridSearchPipe, task)
			s.Require().NoError(err)
			s.Require().NoError(pipeline.AddNodes(task, endNode))
			results, _, err := pipeline.Run(context.Background(), s.span, nil, segcore.StorageCost{})
			s.Require().NoError(err)

			result := results.GetResults()
			s.Equal([]int64{3}, result.GetTopks())
			s.Equal([]int64{1, 0, 0}, result.GetElementIndices().GetData())
			s.Equal([]int64{110, 100, 200}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
			if test.pkType == schemapb.DataType_Int64 {
				s.Equal([]int64{10, 10, 20}, result.GetIds().GetIntId().GetData())
				s.Nil(result.GetIds().GetStrId())
			} else {
				s.Equal([]string{"row-a", "row-a", "row-b"}, result.GetIds().GetStrId().GetData())
				s.Nil(result.GetIds().GetIntId())
			}
		})
	}
}

func (s *SearchPipelineSuite) TestHybridFunctionChainUsesRuntimeMetricsInSubRequestOrder() {
	originalReduceFactory := opFactory[hybridSearchReduceOp]
	defer func() { opFactory[hybridSearchReduceOp] = originalReduceFactory }()

	makeResult := func(ids []int64, scores []float32) *milvuspb.SearchResults {
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       int64(len(ids)),
				Topks:      []int64{int64(len(ids))},
				Ids:        testSearchResultIDs(ids...),
				Scores:     scores,
			},
		}
	}
	reduced := []*milvuspb.SearchResults{
		makeResult([]int64{1, 2}, []float32{10, 0}),
		makeResult([]int64{3, 4}, []float32{0, 10}),
	}
	opFactory[hybridSearchReduceOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(context.Context, trace.Span, ...any) ([]any, error) {
			return []any{reduced, []string{metric.IP, metric.L2}}, nil
		}), nil
	}

	tests := []struct {
		name        string
		normalize   bool
		expectedIDs []int64
	}{
		{name: "normalize", normalize: true, expectedIDs: []int64{3, 1, 2, 4}},
		{name: "direction conversion", normalize: false, expectedIDs: []int64{1, 3, 4, 2}},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			chainPB := l2FunctionChain(
				&schemapb.FunctionChainOp{
					Op: types.OpTypeMerge,
					Params: map[string]*schemapb.FunctionParamValue{
						chain.MergeParamStrategy: chainStringParam(string(chain.MergeStrategyWeighted)),
						chain.MergeParamWeights: chainArrayParam(
							chainDoubleParam(0.5),
							chainDoubleParam(0.5),
						),
						chain.MergeParamNormScore: {
							Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: test.normalize},
						},
					},
				},
				l2LimitFunctionChain(4).GetOps()[0],
			)
			repr, err := chain.ProtoChainToRepr(chainPB)
			s.Require().NoError(err)

			task := getHybridSearchTask("test_collection", [][]string{{"1"}, {"2"}}, nil)
			task.Nq = 1
			task.rerankMeta = &functionChainRerankMeta{chainPB: chainPB, repr: repr}
			for _, subReq := range task.SubReqs {
				subReq.Nq = 1
				subReq.Topk = 2
			}

			pipeline, err := newPipeline(hybridSearchPipe, task)
			s.Require().NoError(err)
			s.Require().NoError(pipeline.AddNodes(task, endNode))
			results, _, err := pipeline.Run(context.Background(), s.span, nil, segcore.StorageCost{})
			s.Require().NoError(err)

			result := results.GetResults()
			s.Equal(test.expectedIDs, result.GetIds().GetIntId().GetData())
			s.IsDecreasing(result.GetScores())
		})
	}
}

func (s *SearchPipelineSuite) TestHybridFunctionChainOwnsSortOffsetAndRounding() {
	originalReduceFactory := opFactory[hybridSearchReduceOp]
	defer func() { opFactory[hybridSearchReduceOp] = originalReduceFactory }()

	reduced := []*milvuspb.SearchResults{
		{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       2,
				Topks:      []int64{2},
				Ids:        testSearchResultIDs(1, 2),
				Scores:     []float32{0.949, 0.841},
			},
		},
		{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       2,
				Topks:      []int64{2},
				Ids:        testSearchResultIDs(3, 4),
				Scores:     []float32{0.734, 0.626},
			},
		},
	}
	opFactory[hybridSearchReduceOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(context.Context, trace.Span, ...any) ([]any, error) {
			return []any{reduced, []string{metric.IP, metric.IP}}, nil
		}), nil
	}

	chainPB := l2FunctionChain(
		&schemapb.FunctionChainOp{
			Op: types.OpTypeMerge,
			Params: map[string]*schemapb.FunctionParamValue{
				chain.MergeParamStrategy: chainStringParam(string(chain.MergeStrategyMax)),
			},
		},
		&schemapb.FunctionChainOp{
			Op:     types.OpTypeSort,
			Inputs: []string{types.ScoreFieldName, types.IDFieldName},
			Params: map[string]*schemapb.FunctionParamValue{
				"desc": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: false}},
			},
		},
		&schemapb.FunctionChainOp{
			Op: types.OpTypeLimit,
			Params: map[string]*schemapb.FunctionParamValue{
				"limit":  chainIntParam(2),
				"offset": chainIntParam(1),
			},
		},
		&schemapb.FunctionChainOp{
			Op:      types.OpTypeMap,
			Outputs: []string{types.ScoreFieldName},
			Expr: &schemapb.FunctionChainExpr{
				Name: "round_decimal",
				Args: []*schemapb.FunctionChainExprArg{columnArg(types.ScoreFieldName)},
				Params: map[string]*schemapb.FunctionParamValue{
					"decimal": chainIntParam(1),
				},
			},
		},
	)
	repr, err := chain.ProtoChainToRepr(chainPB)
	s.Require().NoError(err)

	task := getHybridSearchTask("test_collection", [][]string{{"1"}, {"2"}}, nil)
	task.Nq = 1
	// These legacy response controls deliberately conflict with the public chain.
	task.rankParams = &rankParams{limit: 1, offset: 2, roundDecimal: 0}
	task.rerankMeta = &functionChainRerankMeta{chainPB: chainPB, repr: repr}
	for _, subReq := range task.SubReqs {
		subReq.Nq = 1
		subReq.Topk = 2
	}

	pipeline, err := newPipeline(hybridSearchPipe, task)
	s.Require().NoError(err)
	s.Require().NoError(pipeline.AddNodes(task, endNode))
	results, _, err := pipeline.Run(context.Background(), s.span, nil, segcore.StorageCost{})
	s.Require().NoError(err)

	result := results.GetResults()
	s.Equal([]int64{2}, result.GetTopks())
	s.Equal([]int64{3, 2}, result.GetIds().GetIntId().GetData())
	s.InDeltaSlice([]float32{0.7, 0.8}, result.GetScores(), 0.00001)
}

func (s *SearchPipelineSuite) TestHybridFunctionChainGroupByDoesNotOverrideFinalOrder() {
	originalReduceFactory := opFactory[hybridSearchReduceOp]
	defer func() { opFactory[hybridSearchReduceOp] = originalReduceFactory }()

	makeResult := func(ids []int64, scores []float32, values []int64) *milvuspb.SearchResults {
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       2,
				Topks:      []int64{2},
				Ids:        testSearchResultIDs(ids...),
				Scores:     scores,
				FieldsData: []*schemapb.FieldData{
					testutils.GenerateScalarFieldDataWithValue(schemapb.DataType_Int64, "intField", 101, values),
				},
				GroupByFieldValues: []*schemapb.FieldData{
					testutils.GenerateScalarFieldDataWithValue(schemapb.DataType_VarChar, "category", 102, []string{"A", "A"}),
				},
			},
		}
	}
	reduced := []*milvuspb.SearchResults{
		makeResult([]int64{1, 2}, []float32{0.9, 0.7}, []int64{100, 200}),
		makeResult([]int64{3, 4}, []float32{0.8, 0.6}, []int64{300, 400}),
	}
	opFactory[hybridSearchReduceOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(context.Context, trace.Span, ...any) ([]any, error) {
			return []any{reduced, []string{metric.IP, metric.IP}}, nil
		}), nil
	}

	chainPB := l2FunctionChain(
		&schemapb.FunctionChainOp{
			Op: types.OpTypeMerge,
			Params: map[string]*schemapb.FunctionParamValue{
				chain.MergeParamStrategy: chainStringParam(string(chain.MergeStrategyMax)),
			},
		},
		&schemapb.FunctionChainOp{
			Op:     types.OpTypeSort,
			Inputs: []string{types.ScoreFieldName, types.IDFieldName},
			Params: map[string]*schemapb.FunctionParamValue{
				"desc": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: false}},
			},
		},
		l2LimitFunctionChain(4).GetOps()[0],
	)
	repr, err := chain.ProtoChainToRepr(chainPB)
	s.Require().NoError(err)

	task := getHybridSearchTask("test_collection", [][]string{{"1"}, {"2"}}, []string{"intField"})
	task.Nq = 1
	task.rankParams = &rankParams{
		limit:             1,
		groupByFieldIds:   []int64{102},
		groupByFieldNames: []string{"category"},
		groupSize:         1,
	}
	task.schema.Fields = append(task.schema.Fields,
		&schemapb.FieldSchema{FieldID: 102, Name: "category", DataType: schemapb.DataType_VarChar})
	task.rerankMeta = &functionChainRerankMeta{chainPB: chainPB, repr: repr}
	for _, subReq := range task.SubReqs {
		subReq.Nq = 1
		subReq.Topk = 2
		subReq.GroupByFieldId = 102
		subReq.GroupSize = 1
	}

	pipeline, err := newPipeline(hybridSearchPipe, task)
	s.Require().NoError(err)
	s.Require().NoError(pipeline.AddNodes(task, endNode))
	results, _, err := pipeline.Run(context.Background(), s.span, nil, segcore.StorageCost{})
	s.Require().NoError(err)

	result := results.GetResults()
	s.Equal([]int64{4}, result.GetTopks())
	s.Equal([]int64{4, 2, 3, 1}, result.GetIds().GetIntId().GetData())
	s.Equal([]int64{400, 200, 300, 100}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	s.Require().Len(result.GetGroupByFieldValues(), 1)
	s.Equal([]string{"A", "A", "A", "A"}, result.GetGroupByFieldValues()[0].GetScalars().GetStringData().GetData())
}

func (s *SearchPipelineSuite) TestHybridFunctionChainRuntimeInvariantErrors() {
	chainPB := l2FunctionChain(&schemapb.FunctionChainOp{
		Op: types.OpTypeMerge,
		Params: map[string]*schemapb.FunctionParamValue{
			chain.MergeParamStrategy: chainStringParam(string(chain.MergeStrategyMax)),
			chain.MergeParamNormScore: {
				Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true},
			},
		},
	})
	repr, err := chain.ProtoChainToRepr(chainPB)
	s.Require().NoError(err)
	op := &rerankOperator{
		nq:           1,
		topK:         2,
		roundDecimal: -1,
		rerankMeta:   &functionChainRerankMeta{chainPB: chainPB, repr: repr},
	}

	makeResult := func(ids *schemapb.IDs, scores []float32, elementIndices *schemapb.LongArray) *milvuspb.SearchResults {
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries:     1,
				TopK:           1,
				Topks:          []int64{1},
				Ids:            ids,
				Scores:         scores,
				ElementIndices: elementIndices,
			},
		}
	}
	valid1 := makeResult(testSearchResultIDs(1), []float32{0.9}, nil)
	valid2 := makeResult(testSearchResultIDs(2), []float32{0.8}, nil)

	s.Run("runtime metric count mismatch is system error", func() {
		_, err := op.run(context.Background(), s.span,
			[]*milvuspb.SearchResults{valid1, valid2}, []string{metric.IP})
		s.ErrorIs(err, merr.ErrServiceInternal)
		s.ErrorContains(err, "input count 2 != expected count 1")
	})

	s.Run("element index outside Int32 range is system error", func() {
		outOfRange := makeResult(
			testSearchResultIDs(1),
			[]float32{0.9},
			&schemapb.LongArray{Data: []int64{int64(math.MaxInt32) + 1}},
		)
		_, err := op.run(context.Background(), s.span,
			[]*milvuspb.SearchResults{outOfRange, valid2}, []string{metric.IP, metric.IP})
		s.ErrorIs(err, merr.ErrServiceInternal)
		s.ErrorContains(err, "out of Int32 range")
	})

	s.Run("missing id inside chain input is function failure", func() {
		missingID := makeResult(nil, []float32{0.9}, nil)
		_, err := op.run(context.Background(), s.span,
			[]*milvuspb.SearchResults{missingID, valid2}, []string{metric.IP, metric.IP})
		s.ErrorIs(err, merr.ErrFunctionFailed)
		s.ErrorContains(err, "missing $id column")
	})

	s.Run("missing score inside chain input is function failure", func() {
		missingScore := makeResult(testSearchResultIDs(1), nil, nil)
		_, err := op.run(context.Background(), s.span,
			[]*milvuspb.SearchResults{missingScore, valid2}, []string{metric.IP, metric.IP})
		s.ErrorIs(err, merr.ErrFunctionFailed)
		s.ErrorContains(err, "missing $score column")
	})
}

func (s *SearchPipelineSuite) TestHybridAssembleMissingCandidatePreservesInconsistentRequery() {
	reduced := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			Ids:        testSearchResultIDs(1),
			Scores:     []float32{0.9},
			FieldsData: []*schemapb.FieldData{
				testutils.GenerateScalarFieldDataWithValue(schemapb.DataType_Int64, "intField", 101, []int64{100}),
			},
		},
	}
	ranked := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			Ids:        testSearchResultIDs(2),
			Scores:     []float32{0.8},
		},
	}

	op := &hybridAssembleOperator{collectionID: 12345}
	_, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{reduced}, ranked)
	s.ErrorIs(err, merr.ErrInconsistentRequery)
	s.ErrorContains(err, "hybrid assemble: missing id 2")
}

func (s *SearchPipelineSuite) TestHybridFunctionChainEmptyResultValidatesDownstreamBuild() {
	originalReduceFactory := opFactory[hybridSearchReduceOp]
	defer func() { opFactory[hybridSearchReduceOp] = originalReduceFactory }()

	emptyResult := func() *milvuspb.SearchResults {
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       0,
				Topks:      []int64{0},
				Ids:        &schemapb.IDs{},
				Scores:     []float32{},
			},
		}
	}
	opFactory[hybridSearchReduceOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(context.Context, trace.Span, ...any) ([]any, error) {
			return []any{[]*milvuspb.SearchResults{emptyResult(), emptyResult()}, []string{metric.IP, metric.IP}}, nil
		}), nil
	}

	chainPB := l2FunctionChain(
		&schemapb.FunctionChainOp{
			Op: types.OpTypeMerge,
			Params: map[string]*schemapb.FunctionParamValue{
				chain.MergeParamStrategy: chainStringParam(string(chain.MergeStrategyMax)),
			},
		},
		&schemapb.FunctionChainOp{
			Op:      types.OpTypeMap,
			Outputs: []string{types.ScoreFieldName},
			Expr: &schemapb.FunctionChainExpr{
				Name:   "unknown_downstream_function",
				Args:   []*schemapb.FunctionChainExprArg{columnArg(types.ScoreFieldName)},
				Params: map[string]*schemapb.FunctionParamValue{},
			},
		},
	)
	repr, err := chain.ProtoChainToRepr(chainPB)
	s.Require().NoError(err)

	task := getHybridSearchTask("test_collection", [][]string{{"1"}, {"2"}}, nil)
	task.Nq = 1
	task.rerankMeta = &functionChainRerankMeta{chainPB: chainPB, repr: repr}
	for _, subReq := range task.SubReqs {
		subReq.Nq = 1
	}

	pipeline, err := newPipeline(hybridSearchPipe, task)
	s.Require().NoError(err)
	s.Require().NoError(pipeline.AddNodes(task, endNode))
	results, _, err := pipeline.Run(context.Background(), s.span, nil, segcore.StorageCost{})
	s.Require().Error(err)
	s.Nil(results)
	s.ErrorIs(err, merr.ErrParameterInvalid)
	s.ErrorContains(err, "unknown function: unknown_downstream_function")
}
