// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/search_agg"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/highlight"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func TestSearchPipeline(t *testing.T) {
	suite.Run(t, new(SearchPipelineSuite))
}

type SearchPipelineSuite struct {
	suite.Suite
	span trace.Span
}

type searchPipelineTestOperator func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error)

func (op searchPipelineTestOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	return op(ctx, span, inputs...)
}

func testSearchResultIDs(ids ...int64) *schemapb.IDs {
	return &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: ids},
		},
	}
}

func (s *SearchPipelineSuite) SetupTest() {
	_, sp := otel.Tracer("test").Start(context.Background(), "Proxy-Search-PostExecute")
	s.span = sp
}

func (s *SearchPipelineSuite) TearDownTest() {
	s.span.End()
}

func (s *SearchPipelineSuite) TestBuildChainFromFunctionChainRerankMeta() {
	repr, err := chain.ProtoChainToRepr(l2LimitFunctionChain(10))
	s.Require().NoError(err)

	fc, err := buildChainFromMeta(&functionChainRerankMeta{repr: repr}, nil, nil, nil, memory.NewGoAllocator())
	s.Require().NoError(err)
	s.NotNil(fc)
}

func (s *SearchPipelineSuite) TestSerializeBucketKeyPreservesRequestedOrder() {
	bucket := &search_agg.AggBucketResult{
		Key: map[int64]interface{}{
			200: "brand-a",
			100: "category-x",
		},
	}
	fieldIDToName := map[int64]string{
		200: "brand",
		100: "category",
	}

	serialized := serializeAggBucket(bucket, fieldIDToName, []search_agg.LevelContext{{OwnFieldIDs: []int64{200, 100}}}, 0)

	s.Require().Len(serialized.GetKey(), 2)
	s.Equal(int64(200), serialized.GetKey()[0].GetFieldId())
	s.Equal("brand", serialized.GetKey()[0].GetFieldName())
	s.Equal("brand-a", serialized.GetKey()[0].GetStringVal())
	s.Equal(int64(100), serialized.GetKey()[1].GetFieldId())
	s.Equal("category", serialized.GetKey()[1].GetFieldName())
	s.Equal("category-x", serialized.GetKey()[1].GetStringVal())
}

func (s *SearchPipelineSuite) TestSerializeBucketKeyLeavesNullValueUnset() {
	bucket := &search_agg.AggBucketResult{
		Key: map[int64]interface{}{
			100: nil,
		},
	}
	fieldIDToName := map[int64]string{100: "category"}

	serialized := serializeAggBucket(bucket, fieldIDToName, []search_agg.LevelContext{{OwnFieldIDs: []int64{100}}}, 0)

	s.Require().Len(serialized.GetKey(), 1)
	s.Equal(int64(100), serialized.GetKey()[0].GetFieldId())
	s.Equal("category", serialized.GetKey()[0].GetFieldName())
	s.Nil(serialized.GetKey()[0].GetValue())
}

func (s *SearchPipelineSuite) TestSerializeAggHitFieldsLeavesNullValueUnset() {
	fields := serializeAggHitFields(map[int64]interface{}{100: nil}, map[int64]string{100: "nullable_field"})

	s.Require().Len(fields, 1)
	s.Equal(int64(100), fields[0].GetFieldId())
	s.Equal("nullable_field", fields[0].GetFieldName())
	s.Nil(fields[0].GetValue())
}

func (s *SearchPipelineSuite) TestSearchReduceOp() {
	nq := int64(2)
	topk := int64(10)
	pk := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
		AutoID:       true,
	}
	data := genTestSearchResultData(nq, topk, schemapb.DataType_Int64, "intField", 102, false)
	op := searchReduceOperator{
		context.Background(),
		pk,
		nq,
		topk,
		0,
		1,
		[]int64{1},
		[]*planpb.QueryInfo{{}},
		nil,
		false,
	}
	_, err := op.run(context.Background(), s.span, []*internalpb.SearchResults{data})
	s.NoError(err)
}

func (s *SearchPipelineSuite) TestHybridSearchReduceOp() {
	nq := int64(2)
	topk := int64(10)
	pk := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
		AutoID:       true,
	}
	data1 := genTestSearchResultData(nq, topk, schemapb.DataType_Int64, "intField", 102, true)
	data1.SubResults[0].ReqIndex = 0
	data2 := genTestSearchResultData(nq, topk, schemapb.DataType_Int64, "intField", 102, true)
	data2.SubResults[0].ReqIndex = 1

	subReqs := []*internalpb.SubSearchRequest{
		{
			Nq:     2,
			Topk:   10,
			Offset: 0,
		},
		{
			Nq:     2,
			Topk:   10,
			Offset: 0,
		},
	}

	op := hybridSearchReduceOperator{
		context.Background(),
		subReqs,
		pk,
		1,
		[]int64{1},
		[]*planpb.QueryInfo{{}, {}},
		nil,
	}
	_, err := op.run(context.Background(), s.span, []*internalpb.SearchResults{data1, data2})
	s.NoError(err)
}

func (s *SearchPipelineSuite) TestRerankOp() {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
			{FieldID: 103, Name: "ts", DataType: schemapb.DataType_Int64},
		},
	}
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{"ts"},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "decay"},
			{Key: "origin", Value: "4"},
			{Key: "scale", Value: "4"},
			{Key: "offset", Value: "4"},
			{Key: "decay", Value: "0.5"},
			{Key: "function", Value: "gauss"},
		},
	}
	nq := int64(2)
	topk := int64(10)
	offset := int64(0)

	reduceOp := searchReduceOperator{
		context.Background(),
		schema.Fields[0],
		nq,
		topk,
		offset,
		1,
		[]int64{1},
		[]*planpb.QueryInfo{{}},
		nil,
		false,
	}

	data := genTestSearchResultData(nq, topk, schemapb.DataType_Int64, "ts", 103, false)
	reduced, err := reduceOp.run(context.Background(), s.span, []*internalpb.SearchResults{data})
	s.NoError(err)

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}
	op := rerankOperator{
		nq:           nq,
		topK:         topk,
		offset:       offset,
		roundDecimal: -1,
		collSchema:   schema,
		rerankMeta:   newRerankMeta(schema, funcScoreSchema),
	}

	_, err = op.run(context.Background(), s.span, reduced[0], []string{"IP"})
	s.NoError(err)
}

func (s *SearchPipelineSuite) TestRerankOpWithFunctionChainMeta() {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Int64},
		},
	}
	nq := int64(2)
	topk := int64(10)
	limit := int64(3)

	reduceOp := searchReduceOperator{
		context.Background(),
		schema.Fields[0],
		nq,
		topk,
		0,
		1,
		[]int64{1},
		[]*planpb.QueryInfo{{}},
		nil,
		false,
	}

	data := genTestSearchResultData(nq, topk, schemapb.DataType_Int64, "ts", 101, false)
	reduced, err := reduceOp.run(context.Background(), s.span, []*internalpb.SearchResults{data})
	s.Require().NoError(err)

	repr, err := chain.ProtoChainToRepr(l2LimitFunctionChain(limit))
	s.Require().NoError(err)

	op := rerankOperator{
		nq:           nq,
		topK:         topk,
		roundDecimal: -1,
		collSchema:   schema,
		rerankMeta:   &functionChainRerankMeta{repr: repr},
	}

	outputs, err := op.run(context.Background(), s.span, reduced[0], []string{"IP"})
	s.Require().NoError(err)
	s.Require().Len(outputs, 1)

	result := outputs[0].(*milvuspb.SearchResults).GetResults()
	s.Equal([]int64{limit, limit}, result.GetTopks())
	s.Equal(limit, result.GetTopK())
	s.Len(result.GetScores(), int(nq*limit))
	s.Len(result.GetIds().GetIntId().GetData(), int(nq*limit))
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_CollapsesElementLevelResultsByRowID() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       4,
			Topks:      []int64{4},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 1, 2, 3}},
				},
			},
			Scores:         []float32{0.72, 0.85, 0.60, 0.95},
			Distances:      []float32{7.2, 8.5, 6.0, 9.5},
			ElementIndices: &schemapb.LongArray{Data: []int64{0, 3, 1, 0}},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "value",
					FieldId:   101,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{10, 30, 20, 90}},
							},
						},
					},
				},
			},
			AllSearchCount: 4,
		},
	}

	op := &elementBestCollapseOperator{}
	out, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{input}, []string{"IP"})
	s.Require().NoError(err)

	results := out[0].([]*milvuspb.SearchResults)
	s.Require().Len(results, 1)
	result := results[0].GetResults()

	s.Nil(result.GetElementIndices())
	s.Equal(int64(1), result.GetNumQueries())
	s.Equal(int64(3), result.GetTopK())
	s.Equal([]int64{3}, result.GetTopks())
	s.Equal([]int64{3, 1, 2}, result.GetIds().GetIntId().GetData())
	s.Equal([]float32{0.95, 0.85, 0.60}, result.GetScores())
	s.Equal([]float32{9.5, 8.5, 6.0}, result.GetDistances())
	s.Equal([]int64{90, 30, 20}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	s.Equal(int64(4), result.GetAllSearchCount())
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_CollapsesEachQueryChunkIndependently() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 2,
			TopK:       3,
			Topks:      []int64{3, 3},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 1, 2, 1, 2, 2}},
				},
			},
			Scores:         []float32{0.30, 0.90, 0.70, 0.80, 0.20, 0.60},
			ElementIndices: &schemapb.LongArray{Data: []int64{0, 2, 0, 1, 0, 3}},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "value",
					FieldId:   101,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{10, 11, 20, 30, 40, 41}},
							},
						},
					},
				},
			},
		},
	}

	op := &elementBestCollapseOperator{}
	out, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{input}, []string{"IP"})
	s.Require().NoError(err)

	results := out[0].([]*milvuspb.SearchResults)
	result := results[0].GetResults()

	s.Nil(result.GetElementIndices())
	s.Equal(int64(2), result.GetNumQueries())
	s.Equal(int64(2), result.GetTopK())
	s.Equal([]int64{2, 2}, result.GetTopks())
	s.Equal([]int64{1, 2, 1, 2}, result.GetIds().GetIntId().GetData())
	s.Equal([]float32{0.90, 0.70, 0.80, 0.60}, result.GetScores())
	s.Equal([]int64{11, 20, 30, 41}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_CollapsesStringPrimaryKeys() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{Data: []string{"row-a", "row-a", "row-b"}},
				},
			},
			Scores:         []float32{0.10, 0.60, 0.40},
			ElementIndices: &schemapb.LongArray{Data: []int64{0, 2, 1}},
		},
	}

	op := &elementBestCollapseOperator{}
	out, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{input}, []string{"IP"})
	s.Require().NoError(err)

	results := out[0].([]*milvuspb.SearchResults)
	result := results[0].GetResults()

	s.Nil(result.GetElementIndices())
	s.Equal([]string{"row-a", "row-b"}, result.GetIds().GetStrId().GetData())
	s.Equal([]float32{0.60, 0.40}, result.GetScores())
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_PassesRowLevelResultsThrough() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{10, 20}},
				},
			},
			Scores: []float32{0.90, 0.80},
		},
	}

	op := &elementBestCollapseOperator{}
	out, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{input}, []string{""})
	s.Require().NoError(err)

	results := out[0].([]*milvuspb.SearchResults)
	s.Require().Len(results, 1)
	s.Same(input, results[0])
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_AllowsEmptyElementLevelResultWithoutMetric() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries:     1,
			TopK:           0,
			Topks:          []int64{0},
			ElementIndices: &schemapb.LongArray{},
			AllSearchCount: 10,
		},
	}

	tests := []struct {
		name   string
		config elementCollapseConfig
	}{
		{name: "default max"},
		{name: "topk sum", config: elementCollapseConfig{Strategy: elementCollapseTopKSum, TopK: 2}},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			op := &elementBestCollapseOperator{}
			if test.config.Strategy != "" {
				op.configs = []elementCollapseConfig{test.config}
			}
			out, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{input}, []string{""})
			s.Require().NoError(err)

			results := out[0].([]*milvuspb.SearchResults)
			result := results[0].GetResults()

			s.Nil(result.GetElementIndices())
			s.Equal(int64(1), result.GetNumQueries())
			s.Equal(int64(0), result.GetTopK())
			s.Equal([]int64{0}, result.GetTopks())
			s.Empty(result.GetScores())
			s.Equal(int64(10), result.GetAllSearchCount())
		})
	}
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_DeduplicatesEqualScoreElementsByRowID() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 1, 2}},
				},
			},
			Scores:         []float32{0.50, 0.50, 0.40},
			ElementIndices: &schemapb.LongArray{Data: []int64{0, 1, 0}},
		},
	}

	op := &elementBestCollapseOperator{}
	out, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{input}, []string{"IP"})
	s.Require().NoError(err)

	results := out[0].([]*milvuspb.SearchResults)
	result := results[0].GetResults()

	s.Nil(result.GetElementIndices())
	s.Equal([]int64{1, 2}, result.GetIds().GetIntId().GetData())
	s.Equal([]float32{0.50, 0.40}, result.GetScores())
}

func (s *SearchPipelineSuite) TestHybridSearchPipe_RerankReceivesCollapsedRowLevelResults() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 1, 2}},
				},
			},
			Scores:         []float32{0.40, 0.60, 0.90},
			ElementIndices: &schemapb.LongArray{Data: []int64{0, 1, 0}},
		},
	}

	originalReduceFactory := opFactory[hybridSearchReduceOp]
	originalRerankFactory := opFactory[rerankOp]
	originalAssembleFactory := opFactory[hybridAssembleOp]
	defer func() {
		opFactory[hybridSearchReduceOp] = originalReduceFactory
		opFactory[rerankOp] = originalRerankFactory
		opFactory[hybridAssembleOp] = originalAssembleFactory
	}()

	opFactory[hybridSearchReduceOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
			return []any{[]*milvuspb.SearchResults{input}, []string{"IP"}}, nil
		}), nil
	}

	rerankCalled := false
	opFactory[rerankOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
			rerankCalled = true

			collapsed, ok := inputs[0].([]*milvuspb.SearchResults)
			s.Require().True(ok)
			metrics, ok := inputs[1].([]string)
			s.Require().True(ok)
			s.Equal([]string{"IP"}, metrics)
			s.Require().Len(collapsed, 1)

			data := collapsed[0].GetResults()
			s.Nil(data.GetElementIndices())
			s.Equal([]int64{2, 1}, data.GetIds().GetIntId().GetData())
			s.Equal([]float32{0.90, 0.60}, data.GetScores())

			return []any{&milvuspb.SearchResults{
				Status: merr.Success(),
				Results: &schemapb.SearchResultData{
					NumQueries: data.GetNumQueries(),
					TopK:       data.GetTopK(),
					Topks:      append([]int64(nil), data.GetTopks()...),
					Ids:        data.GetIds(),
					Scores:     append([]float32(nil), data.GetScores()...),
				},
			}}, nil
		}), nil
	}

	opFactory[hybridAssembleOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
			collapsed, ok := inputs[0].([]*milvuspb.SearchResults)
			s.Require().True(ok)
			s.Require().Len(collapsed, 1)
			s.Nil(collapsed[0].GetResults().GetElementIndices())

			rankResult, ok := inputs[1].(*milvuspb.SearchResults)
			s.Require().True(ok)
			return []any{rankResult}, nil
		}), nil
	}

	pipeline, err := newPipeline(hybridSearchPipe, &searchTask{})
	s.Require().NoError(err)
	err = pipeline.AddNodes(&searchTask{}, &nodeDef{
		name:    "finish",
		inputs:  []string{"result"},
		outputs: []string{pipelineOutput},
		opName:  lambdaOp,
		params: map[string]any{
			lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
				return []any{inputs[0]}, nil
			},
		},
	})
	s.Require().NoError(err)

	result, _, err := pipeline.Run(context.Background(), s.span, nil, segcore.StorageCost{})
	s.Require().NoError(err)
	s.True(rerankCalled)
	s.Equal([]int64{2, 1}, result.GetResults().GetIds().GetIntId().GetData())
	s.Equal([]float32{0.90, 0.60}, result.GetResults().GetScores())
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_UsesMetricDirection() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 1, 2}},
				},
			},
			Scores:         []float32{0.8, 0.2, 0.5},
			ElementIndices: &schemapb.LongArray{Data: []int64{0, 3, 1}},
		},
	}

	op := &elementBestCollapseOperator{}
	out, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{input}, []string{"L2"})
	s.Require().NoError(err)

	results := out[0].([]*milvuspb.SearchResults)
	result := results[0].GetResults()

	s.Nil(result.GetElementIndices())
	s.Equal([]int64{1, 2}, result.GetIds().GetIntId().GetData())
	s.Equal([]float32{0.2, 0.5}, result.GetScores())
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_UsesConfiguredCollapseStrategies() {
	makeInput := func() *milvuspb.SearchResults {
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       6,
				Topks:      []int64{6},
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{Data: []int64{1, 1, 1, 2, 2, 3}},
					},
				},
				Scores:         []float32{0.9, 0.6, 0.3, 0.5, 0.1, 0.55},
				Distances:      []float32{0.9, 0.6, 0.3, 0.5, 0.1, 0.55},
				ElementIndices: &schemapb.LongArray{Data: []int64{0, 1, 2, 0, 1, 0}},
			},
		}
	}

	tests := []struct {
		name           string
		config         elementCollapseConfig
		expectedIDs    []int64
		expectedScores []float32
		expectedDists  []float32
	}{
		{
			name:           "sum",
			config:         elementCollapseConfig{Strategy: elementCollapseSum},
			expectedIDs:    []int64{1, 2, 3},
			expectedScores: []float32{1.8, 0.6, 0.55},
			expectedDists:  []float32{0.9, 0.5, 0.55},
		},
		{
			name:           "avg",
			config:         elementCollapseConfig{Strategy: elementCollapseAvg},
			expectedIDs:    []int64{1, 3, 2},
			expectedScores: []float32{0.6, 0.55, 0.3},
			expectedDists:  []float32{0.9, 0.55, 0.5},
		},
		{
			name:           "topk_sum",
			config:         elementCollapseConfig{Strategy: elementCollapseTopKSum, TopK: 2},
			expectedIDs:    []int64{1, 2, 3},
			expectedScores: []float32{1.5, 0.6, 0.55},
			expectedDists:  []float32{0.9, 0.5, 0.55},
		},
		{
			name:           "topk_avg",
			config:         elementCollapseConfig{Strategy: elementCollapseTopKAvg, TopK: 2},
			expectedIDs:    []int64{1, 3, 2},
			expectedScores: []float32{0.75, 0.55, 0.3},
			expectedDists:  []float32{0.9, 0.55, 0.5},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			op := &elementBestCollapseOperator{configs: []elementCollapseConfig{test.config}}
			out, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{makeInput()}, []string{"IP"})
			s.Require().NoError(err)

			result := out[0].([]*milvuspb.SearchResults)[0].GetResults()
			s.Nil(result.GetElementIndices())
			s.Equal(test.expectedIDs, result.GetIds().GetIntId().GetData())
			s.InDeltaSlice(test.expectedScores, result.GetScores(), 0.00001)
			s.InDeltaSlice(test.expectedDists, result.GetDistances(), 0.00001)
		})
	}
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_RejectsSumCollapseForNegativeMetrics() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 1}},
				},
			},
			Scores:         []float32{0.8, 0.2},
			ElementIndices: &schemapb.LongArray{Data: []int64{0, 1}},
		},
	}
	op := &elementBestCollapseOperator{configs: []elementCollapseConfig{{Strategy: elementCollapseTopKSum, TopK: 2}}}

	_, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{input}, []string{"L2"})

	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrParameterInvalid)
	s.Contains(err.Error(), "only supported for positively related metrics")
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_RejectsSumCollapseForNegativeMetricsWithEmptyResult() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries:     1,
			TopK:           0,
			Topks:          []int64{0},
			ElementIndices: &schemapb.LongArray{},
		},
	}
	op := &elementBestCollapseOperator{configs: []elementCollapseConfig{{Strategy: elementCollapseTopKSum, TopK: 2}}}

	_, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{input}, []string{"L2"})

	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrParameterInvalid)
	s.Contains(err.Error(), "only supported for positively related metrics")
}

func (s *SearchPipelineSuite) TestElementLevelHybridPrepareAndRestoreKeys() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{10, 10, 20}},
				},
			},
			Scores:         []float32{0.8, 0.9, 0.7},
			ElementIndices: &schemapb.LongArray{Data: []int64{0, 2, 1}},
			GroupByFieldValues: []*schemapb.FieldData{{
				FieldId:   100,
				FieldName: "pk",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 10, 20}}},
				}},
			}},
		},
	}

	prepared, err := prepareElementLevelHybridResult(input)
	s.Require().NoError(err)
	preparedIDs := prepared.GetResults().GetIds().GetStrId().GetData()
	s.Require().Len(preparedIDs, 3)
	s.Require().Len(prepared.GetResults().GetGroupByFieldValues(), 1)
	s.Equal([]int64{10, 10, 20}, prepared.GetResults().GetGroupByFieldValues()[0].GetScalars().GetLongData().GetData())

	rankResult := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{Data: []string{preparedIDs[1], preparedIDs[2]}},
				},
			},
			Scores: []float32{0.99, 0.88},
			GroupByFieldValues: []*schemapb.FieldData{{
				FieldId:   100,
				FieldName: "pk",
				Type:      schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 20}}},
				}},
			}},
		},
	}

	restored, err := restoreElementLevelHybridRankResult(rankResult)
	s.Require().NoError(err)
	s.Equal([]int64{10, 20}, restored.GetResults().GetIds().GetIntId().GetData())
	s.Equal([]int64{2, 1}, restored.GetResults().GetElementIndices().GetData())
	s.Equal([]float32{0.99, 0.88}, restored.GetResults().GetScores())
	s.Require().Len(restored.GetResults().GetGroupByFieldValues(), 1)
	s.Equal([]int64{10, 20}, restored.GetResults().GetGroupByFieldValues()[0].GetScalars().GetLongData().GetData())

	_, err = prepareElementLevelHybridResult(&milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{10}},
				},
			},
			Scores: []float32{0.8},
		},
	})
	s.Require().Error(err)
	s.Contains(err.Error(), "missing element_indices")
}

func (s *SearchPipelineSuite) TestElementBestCollapseOp_RejectsEmptyMetricForElementLevelResult() {
	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1}},
				},
			},
			Scores:         []float32{0.8},
			ElementIndices: &schemapb.LongArray{Data: []int64{0}},
		},
	}

	op := &elementBestCollapseOperator{}
	_, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{input}, []string{""})
	s.Error(err)
	s.Contains(err.Error(), "missing metric type")
}

// TestHybridAssembleOp_MixedFieldsDataLayoutErrors verifies that hybrid
// assemble fails loud when sub-results have inconsistent FieldsData layouts —
// specifically when a sub-result contributes reranked IDs but has an empty
// FieldsData while another sub-result has a populated one.
//
// In current proxy flow this state is unreachable: task_search.go sets
// identical plan.OutputFieldIds for every sub-request and the hybridSearchPipe
// is only chosen when needRequery=false, where plan.OutputFieldIds always
// contains at least the PK. Any deviation means an upstream invariant has
// been broken — silently corrupting the result (the previous code did
// `continue`, leaving fewer FieldsData rows than reranked IDs and causing PK
// ↔ field misalignment downstream) is strictly worse than returning an error.
//
// Scenario:
//   - sub[0]: PKs [10, 20], FieldsData = [{long_field: [100, 200]}]
//   - sub[1]: PKs [30, 40], FieldsData = []                     (empty)
//   - rerank: PKs [10, 30, 20, 40] (4 ids, picked across both sub-results)
//
// Expected: assemble must return an error mentioning the inconsistent
// sub-result, not silently produce 4 IDs paired with only 2 field rows.
func (s *SearchPipelineSuite) TestHybridAssembleOp_MixedFieldsDataLayoutErrors() {
	// sub[0]: 2 IDs with a long field populated.
	sub0 := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{10, 20}},
				},
			},
			Scores: []float32{0.9, 0.8},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "value",
					FieldId:   101,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{100, 200}},
							},
						},
					},
				},
			},
		},
	}

	// sub[1]: 2 IDs but FieldsData empty — invariant violation that proxy
	// upstream code is supposed to prevent. We assert assemble catches it
	// rather than silently dropping rows.
	sub1 := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{30, 40}},
				},
			},
			Scores:     []float32{0.7, 0.6},
			FieldsData: nil,
		},
	}

	// rank result: rerank picked 4 IDs across both sub-results.
	rankResult := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       4,
			Topks:      []int64{4},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{10, 30, 20, 40}},
				},
			},
			Scores: []float32{0.95, 0.85, 0.75, 0.65},
		},
	}

	op := &hybridAssembleOperator{collectionID: 12345}
	_, err := op.run(context.Background(), s.span,
		[]*milvuspb.SearchResults{sub0, sub1}, rankResult)

	s.Require().Error(err,
		"mixed FieldsData layout must be reported as an error, not silently dropped")
	// Diagnostic must point at the offending sub-result so an operator can
	// trace the upstream invariant violation.
	s.Contains(err.Error(), "FieldsData",
		"error message must mention FieldsData inconsistency")
}

func (s *SearchPipelineSuite) TestHybridAssembleOpNullableVectorCompactData() {
	sub0 := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_FloatVector,
					FieldName: "nullable_vec",
					FieldId:   101,
					ValidData: []bool{false, true, true},
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: 2,
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{Data: []float32{20, 20, 30, 30}},
							},
						},
					},
				},
			},
		},
	}
	sub1 := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{40, 50}},
				},
			},
			Scores: []float32{0.6, 0.5},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_FloatVector,
					FieldName: "nullable_vec",
					FieldId:   101,
					ValidData: []bool{true, false},
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: 2,
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{Data: []float32{40, 40}},
							},
						},
					},
				},
			},
		},
	}
	rankResult := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       5,
			Topks:      []int64{5},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{30, 10, 50, 40, 20}},
				},
			},
			Scores: []float32{0.95, 0.85, 0.75, 0.65, 0.55},
		},
	}

	op := &hybridAssembleOperator{collectionID: 12345}
	out, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{sub0, sub1}, rankResult)

	s.NoError(err)
	result := out[0].(*milvuspb.SearchResults).GetResults()
	s.Require().Len(result.GetFieldsData(), 1)
	field := result.GetFieldsData()[0]
	s.Equal([]bool{true, false, false, true, true}, field.GetValidData())
	s.Equal([]float32{30, 30, 40, 40, 20, 20}, field.GetVectors().GetFloatVector().GetData())
}

func (s *SearchPipelineSuite) TestComputeFieldIdxsByOriginalOrderUsesAscendingRowsAndPreservesOutputOrder() {
	rowIdxs := []int64{5, 1, 4, 2}
	calls := make([]int64, 0, len(rowIdxs))

	fieldIdxs := computeFieldIdxsByOriginalOrder(rowIdxs, func(rowIdx int64) []int64 {
		calls = append(calls, rowIdx)
		return []int64{rowIdx + 100}
	})

	s.Equal([]int64{1, 2, 4, 5}, calls)
	s.Equal([][]int64{{105}, {101}, {104}, {102}}, fieldIdxs)
}

func (s *SearchPipelineSuite) TestComputeFieldIdxsByOriginalOrderCopiesSharedComputeBuffer() {
	rowIdxs := []int64{5, 1, 4, 2}
	shared := []int64{0, 0}

	fieldIdxs := computeFieldIdxsByOriginalOrder(rowIdxs, func(rowIdx int64) []int64 {
		shared[0] = rowIdx + 100
		shared[1] = rowIdx + 200
		return shared
	})

	s.Equal([][]int64{{105, 205}, {101, 201}, {104, 204}, {102, 202}}, fieldIdxs)
}

func (s *SearchPipelineSuite) TestHybridAssembleOp_ElementLevelHybridUsesElementKey() {
	reduced := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{Data: []string{
						makeHybridElementKey(int64(10), 0),
						makeHybridElementKey(int64(10), 2),
						makeHybridElementKey(int64(20), 1),
					}},
				},
			},
			Scores: []float32{0.8, 0.9, 0.7},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "value",
					FieldId:   101,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{100, 200, 300}},
							},
						},
					},
				},
			},
		},
	}
	rankResult := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{10, 20}},
				},
			},
			Scores:         []float32{0.99, 0.88},
			ElementIndices: &schemapb.LongArray{Data: []int64{2, 1}},
		},
	}

	op := &hybridAssembleOperator{collectionID: 12345, elementLevelHybrid: true}
	out, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{reduced}, rankResult)
	s.Require().NoError(err)

	result := out[0].(*milvuspb.SearchResults).GetResults()
	s.Equal([]int64{10, 20}, result.GetIds().GetIntId().GetData())
	s.Equal([]int64{2, 1}, result.GetElementIndices().GetData())
	s.Equal([]float32{0.99, 0.88}, result.GetScores())
	s.Equal([]int64{200, 300}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
}

func (s *SearchPipelineSuite) TestRequeryOp() {
	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f1.FieldId = 101

	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1},
	}, segcore.StorageCost{}, nil).Build()
	defer mocker.UnPatch()

	op := requeryOperator{
		traceCtx:         context.Background(),
		outputFieldNames: []string{"int64"},
	}
	ids := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{1, 2},
			},
		},
	}
	_, err := op.run(context.Background(), s.span, ids, segcore.StorageCost{})
	s.NoError(err)
}

func (s *SearchPipelineSuite) TestOrganizeOp() {
	op := organizeOperator{
		traceCtx:           context.Background(),
		primaryFieldSchema: &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		collectionID:       1,
	}
	fields := []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "pk",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
						},
					},
				},
			},
		}, {
			Type:      schemapb.DataType_Int64,
			FieldName: "int64",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
						},
					},
				},
			},
		},
	}

	ids := []*schemapb.IDs{
		{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 4, 5, 9, 10},
				},
			},
		},
		{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{5, 6, 7, 8, 9, 10},
				},
			},
		},
	}
	ret, err := op.run(context.Background(), s.span, fields, ids)
	s.NoError(err)
	fmt.Println(ret)
}

func (s *SearchPipelineSuite) TestHighlightOp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &Proxy{}
	proxy.tsoAllocator = &timestampAllocator{
		tso: newMockTimestampAllocatorInterface(),
	}
	sched, err := newTaskScheduler(ctx, proxy.tsoAllocator)
	s.Require().NoError(err)

	err = sched.Start()
	s.Require().NoError(err)
	defer sched.Close()
	proxy.sched = sched

	collName := "test_coll_highlight"
	fieldName2Types := map[string]schemapb.DataType{
		testVarCharField: schemapb.DataType_VarChar,
	}
	schema := constructCollectionSchemaByDataType(collName, fieldName2Types, testVarCharField, false)

	req := &milvuspb.SearchRequest{
		CollectionName: collName,
		DbName:         "default",
	}

	highlightTasks := map[int64]*highlightTask{
		100: {
			HighlightTask: &querypb.HighlightTask{
				Texts:     []string{"target text"},
				FieldName: testVarCharField,
				FieldId:   100,
			},
			preTags:  [][]byte{[]byte(DefaultPreTag)},
			postTags: [][]byte{[]byte(DefaultPostTag)},
		},
	}

	mockLb := shardclient.NewMockLBPolicy(s.T())
	searchTask := &searchTask{
		node: proxy,
		highlighter: &LexicalHighlighter{
			tasks: highlightTasks,
		},
		lb:             mockLb,
		schema:         mustNewSchemaInfo(schema),
		request:        req,
		collectionName: collName,
		SearchRequest: &internalpb.SearchRequest{
			CollectionID: 0,
		},
	}

	op, err := opFactory[highlightOp](searchTask, map[string]any{})
	s.Require().NoError(err)

	// mockery
	mockLb.EXPECT().ExecuteOneChannel(mock.Anything, mock.Anything).Run(func(ctx context.Context, workload shardclient.CollectionWorkLoad) {
		qn := mocks.NewMockQueryNodeClient(s.T())
		qn.EXPECT().GetHighlight(mock.Anything, mock.Anything).Return(
			&querypb.GetHighlightResponse{
				Status:  merr.Success(),
				Results: []*querypb.HighlightResult{{}},
			}, nil)
		workload.Exec(ctx, 0, qn, "test_chan")
	}).Return(nil)

	_, err = op.run(ctx, s.span, &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			TopK:  3,
			Topks: []int64{1},
			Ids:   testSearchResultIDs(1),
			FieldsData: []*schemapb.FieldData{{
				FieldName: testVarCharField,
				FieldId:   100,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"match text"},
							},
						},
					},
				},
			}},
		},
	})
	s.NoError(err)
}

func (s *SearchPipelineSuite) TestLexicalHighlightOpNullableStringKeepsEmptyHighlightData() {
	cases := []struct {
		name          string
		rowNum        int
		stringData    []string
		validData     []bool
		expectedTexts []string
	}{
		{
			name:          "compact nullable string",
			rowNum:        2,
			stringData:    []string{"match text"},
			validData:     []bool{true, false},
			expectedTexts: []string{"target text", "match text", ""},
		},
		{
			name:          "all null string",
			rowNum:        1,
			stringData:    []string{},
			validData:     []bool{false},
			expectedTexts: []string{"target text", ""},
		},
		{
			name:          "empty string",
			rowNum:        1,
			stringData:    []string{""},
			validData:     []bool{true},
			expectedTexts: []string{"target text", ""},
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			proxy := &Proxy{}
			proxy.tsoAllocator = &timestampAllocator{
				tso: newMockTimestampAllocatorInterface(),
			}
			sched, err := newTaskScheduler(ctx, proxy.tsoAllocator)
			s.Require().NoError(err)

			err = sched.Start()
			s.Require().NoError(err)
			defer sched.Close()
			proxy.sched = sched

			collName := "test_coll_highlight_nullable"
			fieldName2Types := map[string]schemapb.DataType{
				testVarCharField: schemapb.DataType_VarChar,
			}
			schema := constructCollectionSchemaByDataType(collName, fieldName2Types, testVarCharField, false)

			highlightTasks := map[int64]*highlightTask{
				100: {
					HighlightTask: &querypb.HighlightTask{
						Texts:         []string{"target text"},
						FieldName:     testVarCharField,
						FieldId:       100,
						SearchTextNum: 1,
					},
					preTags:  [][]byte{[]byte(DefaultPreTag)},
					postTags: [][]byte{[]byte(DefaultPostTag)},
				},
			}

			mockLb := shardclient.NewMockLBPolicy(s.T())
			searchTask := &searchTask{
				node: proxy,
				highlighter: &LexicalHighlighter{
					tasks: highlightTasks,
				},
				lb:             mockLb,
				schema:         mustNewSchemaInfo(schema),
				request:        &milvuspb.SearchRequest{CollectionName: collName, DbName: "default"},
				collectionName: collName,
				SearchRequest:  &internalpb.SearchRequest{CollectionID: 0},
			}

			op, err := opFactory[highlightOp](searchTask, map[string]any{})
			s.Require().NoError(err)

			queryNodeResults := make([]*querypb.HighlightResult, tc.rowNum)
			for i := range queryNodeResults {
				queryNodeResults[i] = &querypb.HighlightResult{}
			}

			mockLb.EXPECT().ExecuteOneChannel(mock.Anything, mock.Anything).Run(func(ctx context.Context, workload shardclient.CollectionWorkLoad) {
				qn := mocks.NewMockQueryNodeClient(s.T())
				qn.EXPECT().GetHighlight(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *querypb.GetHighlightRequest, opts ...grpc.CallOption) {
					s.Require().Len(req.GetTasks(), 1)
					task := req.GetTasks()[0]
					s.Equal(int64(tc.rowNum), task.GetCorpusTextNum())
					s.Equal(tc.expectedTexts, task.GetTexts())
				}).Return(
					&querypb.GetHighlightResponse{
						Status:  merr.Success(),
						Results: queryNodeResults,
					}, nil)
				workload.Exec(ctx, 0, qn, "test_chan")
			}).Return(nil)

			ids := make([]int64, tc.rowNum)
			scores := make([]float32, tc.rowNum)
			for i := range tc.rowNum {
				ids[i] = int64(i + 1)
				scores[i] = 1.0 - float32(i)*0.1
			}

			results, err := op.run(ctx, s.span, &milvuspb.SearchResults{
				Results: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       int64(tc.rowNum),
					Topks:      []int64{int64(tc.rowNum)},
					Ids:        testSearchResultIDs(ids...),
					Scores:     scores,
					FieldsData: []*schemapb.FieldData{
						{
							FieldId:   100,
							FieldName: testVarCharField,
							Type:      schemapb.DataType_VarChar,
							ValidData: tc.validData,
							Field: &schemapb.FieldData_Scalars{
								Scalars: &schemapb.ScalarField{
									Data: &schemapb.ScalarField_StringData{
										StringData: &schemapb.StringArray{Data: tc.stringData},
									},
								},
							},
						},
					},
				},
			})
			s.NoError(err)
			s.Require().Len(results, 1)

			result := results[0].(*milvuspb.SearchResults)
			highlightResults := result.GetResults().GetHighlightResults()
			s.Require().Len(highlightResults, 1)
			s.Equal(testVarCharField, highlightResults[0].GetFieldName())
			s.Require().Len(highlightResults[0].GetDatas(), tc.rowNum)
			for _, data := range highlightResults[0].GetDatas() {
				s.NotNil(data)
				s.Empty(data.GetFragments())
			}
		})
	}
}

func (s *SearchPipelineSuite) TestLexicalHighlightOpZeroHitWithNonEmptyFieldsData() {
	op := &lexicalHighlightOperator{
		tasks: []*highlightTask{
			{
				HighlightTask: &querypb.HighlightTask{
					Texts:     []string{"target text"},
					FieldName: testVarCharField,
					FieldId:   100,
				},
				preTags:  [][]byte{[]byte(DefaultPreTag)},
				postTags: [][]byte{[]byte(DefaultPostTag)},
			},
		},
	}

	results, err := op.run(context.Background(), s.span, &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       5,
			Topks:      []int64{0},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: "unrelated_field",
					Type:      schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{}},
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	s.Require().Len(results, 1)

	result := results[0].(*milvuspb.SearchResults)
	s.Empty(result.GetResults().GetHighlightResults())
}

func (s *SearchPipelineSuite) TestLexicalHighlightOpNonZeroHitWithEmptyFieldsData() {
	op := &lexicalHighlightOperator{}
	_, err := op.run(context.Background(), s.span, &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			Ids:        testSearchResultIDs(1),
		},
	})
	s.Error(err)
	s.Contains(err.Error(), "field data is empty for non-empty search result")
}

func (s *SearchPipelineSuite) TestSemanticHighlightOp() {
	ctx := context.Background()

	// Mock SemanticHighlight methods
	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			return [][]string{
					{"<em>highlighted</em> text 1"},
					{"<em>highlighted</em> text 2"},
					{"<em>highlighted</em> text 3"},
				}, [][]float32{
					{0.9},
					{0.8},
					{0.7},
				}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).To(func(h *highlight.SemanticHighlight) []int64 {
		return []int64{101}
	}).Build()
	defer mockFieldIDs.UnPatch()

	mockGetFieldName := mockey.Mock((*highlight.SemanticHighlight).GetFieldName).To(func(h *highlight.SemanticHighlight, id int64) string {
		return testVarCharField
	}).Build()
	defer mockGetFieldName.UnPatch()

	// Create operator
	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create search results with text data
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids:        testSearchResultIDs(1, 2, 3),
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: testVarCharField,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"text 1", "text 2", "text 3"},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator
	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.NotNil(results)
	s.Len(results, 1)

	// Verify results
	result := results[0].(*milvuspb.SearchResults)
	s.NotNil(result.Results.HighlightResults)
	s.Len(result.Results.HighlightResults, 1)

	highlightResult := result.Results.HighlightResults[0]
	s.Equal(testVarCharField, highlightResult.FieldName)
	s.Len(highlightResult.Datas, 3)
	s.Equal([]string{"<em>highlighted</em> text 1"}, highlightResult.Datas[0].Fragments)
	s.Equal([]string{"<em>highlighted</em> text 2"}, highlightResult.Datas[1].Fragments)
	s.Equal([]string{"<em>highlighted</em> text 3"}, highlightResult.Datas[2].Fragments)
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpNullableStringAlignsRows() {
	ctx := context.Background()

	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			s.Equal([]int64{3}, topks)
			s.Equal([]string{"text 1", "", "text 3"}, texts)
			return [][]string{
					{"highlighted text 1"},
					{},
					{"highlighted text 3"},
				}, [][]float32{
					{0.9},
					{},
					{0.7},
				}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{101}).Build()
	defer mockFieldIDs.UnPatch()

	mockGetFieldName := mockey.Mock((*highlight.SemanticHighlight).GetFieldName).Return(testVarCharField).Build()
	defer mockGetFieldName.UnPatch()

	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids:        testSearchResultIDs(1, 2, 3),
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: testVarCharField,
					Type:      schemapb.DataType_VarChar,
					ValidData: []bool{true, false, true},
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"text 1", "text 3"},
								},
							},
						},
					},
				},
			},
		},
	}

	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.Require().Len(results, 1)

	result := results[0].(*milvuspb.SearchResults)
	s.Require().Len(result.GetResults().GetHighlightResults(), 1)
	highlightResult := result.GetResults().GetHighlightResults()[0]
	// The fix under test is the row alignment feeding Process: the NULL row
	// materializes as "" at its own index, asserted on `texts` inside the mock
	// above. Here we only verify the per-row results survive alignment 1:1 —
	// the row-1 payload is whatever the mock returned, not a NULL-semantics
	// guarantee, so we assert positional pass-through across all three rows.
	s.Require().Len(highlightResult.GetDatas(), 3)
	s.Equal([]string{"highlighted text 1"}, highlightResult.GetDatas()[0].GetFragments())
	s.Equal([]string{}, highlightResult.GetDatas()[1].GetFragments())
	s.Equal([]string{"highlighted text 3"}, highlightResult.GetDatas()[2].GetFragments())
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpMissingField() {
	ctx := context.Background()

	// Mock FieldIDs to return field 999 (not in results)
	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{999}).Build()
	defer mockFieldIDs.UnPatch()

	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create search results without the expected field
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			Ids:        testSearchResultIDs(1),
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: testVarCharField,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"text 1"},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator and expect error
	_, err := op.run(ctx, s.span, searchResults)
	s.Error(err)
	s.Contains(err.Error(), "text field not in output field")
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpMultipleFields() {
	ctx := context.Background()

	// Use a counter to return different results for different calls
	callCount := 0
	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			callCount++
			return [][]string{
					{fmt.Sprintf("<em>highlighted</em> text field%d-1", callCount)},
					{fmt.Sprintf("<em>highlighted</em> text field%d-2", callCount)},
				}, [][]float32{
					{0.9},
					{0.8},
				}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{101, 102}).Build()
	defer mockFieldIDs.UnPatch()

	mockGetFieldName := mockey.Mock((*highlight.SemanticHighlight).GetFieldName).To(func(h *highlight.SemanticHighlight, id int64) string {
		if id == 101 {
			return "field1"
		}
		return "field2"
	}).Build()
	defer mockGetFieldName.UnPatch()

	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create search results with multiple text fields
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			Ids:        testSearchResultIDs(1, 2),
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: "field1",
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"text 1", "text 2"},
								},
							},
						},
					},
				},
				{
					FieldId:   102,
					FieldName: "field2",
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"another text 1", "another text 2"},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator
	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.NotNil(results)

	// Verify results
	result := results[0].(*milvuspb.SearchResults)
	s.NotNil(result.Results.HighlightResults)
	s.Len(result.Results.HighlightResults, 2)

	// Verify first field
	s.Equal("field1", result.Results.HighlightResults[0].FieldName)
	s.Len(result.Results.HighlightResults[0].Datas, 2)

	// Verify second field
	s.Equal("field2", result.Results.HighlightResults[1].FieldName)
	s.Len(result.Results.HighlightResults[1].Datas, 2)
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpEmptyResults() {
	ctx := context.Background()

	// Mock Process to return empty results
	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			return [][]string{}, [][]float32{}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{101}).Build()
	defer mockFieldIDs.UnPatch()

	mockGetFieldName := mockey.Mock((*highlight.SemanticHighlight).GetFieldName).To(func(h *highlight.SemanticHighlight, id int64) string {
		return testVarCharField
	}).Build()
	defer mockGetFieldName.UnPatch()

	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create empty search results
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       0,
			Topks:      []int64{0},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: testVarCharField,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator
	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.NotNil(results)

	// Verify results
	result := results[0].(*milvuspb.SearchResults)
	s.Empty(result.Results.HighlightResults)
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpZeroHitWithNonEmptyFieldsData() {
	ctx := context.Background()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{999}).Build()
	defer mockFieldIDs.UnPatch()

	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       5,
			Topks:      []int64{0},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: testVarCharField,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{}},
							},
						},
					},
				},
			},
		},
	}

	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.Require().Len(results, 1)

	result := results[0].(*milvuspb.SearchResults)
	s.Empty(result.GetResults().GetHighlightResults())
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpNonZeroHitWithEmptyFieldsData() {
	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	_, err := op.run(context.Background(), s.span, &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			Ids:        testSearchResultIDs(1),
		},
	})
	s.Error(err)
	s.Contains(err.Error(), "field data is empty for non-empty search result")
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpDynamicField() {
	ctx := context.Background()

	// Mock SemanticHighlight methods for schema fields
	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			return [][]string{
					{"<em>dynamic</em> content 1"},
					{"<em>dynamic</em> content 2"},
				}, [][]float32{
					{0.95},
					{0.85},
				}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{}).Build()
	defer mockFieldIDs.UnPatch()

	mockHasDynamicFields := mockey.Mock((*highlight.SemanticHighlight).HasDynamicFields).Return(true).Build()
	defer mockHasDynamicFields.UnPatch()

	mockDynamicFieldNames := mockey.Mock((*highlight.SemanticHighlight).DynamicFieldNames).Return([]string{"dyn_content"}).Build()
	defer mockDynamicFieldNames.UnPatch()

	mockDynamicFieldID := mockey.Mock((*highlight.SemanticHighlight).DynamicFieldID).Return(int64(102)).Build()
	defer mockDynamicFieldID.UnPatch()

	// Create operator
	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create search results with $meta JSON field data
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       2,
			Topks:      []int64{2},
			Ids:        testSearchResultIDs(1, 2),
			Scores:     []float32{0.9, 0.8},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   102,
					FieldName: "$meta",
					Type:      schemapb.DataType_JSON,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{
									Data: [][]byte{
										[]byte(`{"dyn_content": "dynamic content 1"}`),
										[]byte(`{"dyn_content": "dynamic content 2"}`),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator
	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.NotNil(results)

	// Verify results
	result := results[0].(*milvuspb.SearchResults)
	s.NotNil(result.Results.HighlightResults)
	s.Len(result.Results.HighlightResults, 1)

	highlightResult := result.Results.HighlightResults[0]
	s.Equal("dyn_content", highlightResult.FieldName)
	s.Len(highlightResult.Datas, 2)
	s.Equal([]string{"<em>dynamic</em> content 1"}, highlightResult.Datas[0].Fragments)
	s.Equal([]string{"<em>dynamic</em> content 2"}, highlightResult.Datas[1].Fragments)
}

func (s *SearchPipelineSuite) TestSemanticHighlightOpMixedFields() {
	ctx := context.Background()

	// Track calls to distinguish schema field vs dynamic field processing
	callCount := 0
	mockProcess := mockey.Mock((*highlight.SemanticHighlight).Process).To(
		func(h *highlight.SemanticHighlight, ctx context.Context, topks []int64, texts []string) ([][]string, [][]float32, error) {
			callCount++
			if callCount == 1 {
				// Schema field
				return [][]string{{"<em>schema</em> text"}}, [][]float32{{0.95}}, nil
			}
			// Dynamic field
			return [][]string{{"<em>dynamic</em> text"}}, [][]float32{{0.90}}, nil
		}).Build()
	defer mockProcess.UnPatch()

	mockFieldIDs := mockey.Mock((*highlight.SemanticHighlight).FieldIDs).Return([]int64{101}).Build()
	defer mockFieldIDs.UnPatch()

	mockGetFieldName := mockey.Mock((*highlight.SemanticHighlight).GetFieldName).Return(testVarCharField).Build()
	defer mockGetFieldName.UnPatch()

	mockHasDynamicFields := mockey.Mock((*highlight.SemanticHighlight).HasDynamicFields).Return(true).Build()
	defer mockHasDynamicFields.UnPatch()

	mockDynamicFieldNames := mockey.Mock((*highlight.SemanticHighlight).DynamicFieldNames).Return([]string{"dyn_content"}).Build()
	defer mockDynamicFieldNames.UnPatch()

	mockDynamicFieldID := mockey.Mock((*highlight.SemanticHighlight).DynamicFieldID).Return(int64(102)).Build()
	defer mockDynamicFieldID.UnPatch()

	// Create operator
	op := &semanticHighlightOperator{
		highlight: &highlight.SemanticHighlight{},
	}

	// Create search results with both schema field and $meta JSON field
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       1,
			Topks:      []int64{1},
			Ids:        testSearchResultIDs(1),
			Scores:     []float32{0.9},
			FieldsData: []*schemapb.FieldData{
				{
					FieldId:   101,
					FieldName: testVarCharField,
					Type:      schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{
									Data: []string{"schema text"},
								},
							},
						},
					},
				},
				{
					FieldId:   102,
					FieldName: "$meta",
					Type:      schemapb.DataType_JSON,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{
									Data: [][]byte{
										[]byte(`{"dyn_content": "dynamic text"}`),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Run the operator
	results, err := op.run(ctx, s.span, searchResults)
	s.NoError(err)
	s.NotNil(results)

	// Verify results
	result := results[0].(*milvuspb.SearchResults)
	s.NotNil(result.Results.HighlightResults)
	s.Len(result.Results.HighlightResults, 2)

	// Schema field result
	s.Equal(testVarCharField, result.Results.HighlightResults[0].FieldName)
	s.Equal([]string{"<em>schema</em> text"}, result.Results.HighlightResults[0].Datas[0].Fragments)

	// Dynamic field result
	s.Equal("dyn_content", result.Results.HighlightResults[1].FieldName)
	s.Equal([]string{"<em>dynamic</em> text"}, result.Results.HighlightResults[1].Datas[0].Fragments)
}

func (s *SearchPipelineSuite) TestExtractMultipleDynamicFieldTexts() {
	// Test normal extraction with multiple fields
	jsonData := [][]byte{
		[]byte(`{"field1": "value1", "field2": "value2"}`),
		[]byte(`{"field1": "value3", "field2": "value4"}`),
	}
	result, err := extractMultipleDynamicFieldTexts(jsonData, []string{"field1", "field2"})
	s.NoError(err)
	s.Equal([]string{"value1", "value3"}, result["field1"])
	s.Equal([]string{"value2", "value4"}, result["field2"])

	// Test single field extraction
	result2, err := extractMultipleDynamicFieldTexts(jsonData, []string{"field1"})
	s.NoError(err)
	s.Equal([]string{"value1", "value3"}, result2["field1"])

	// Test missing field (graceful degradation)
	jsonData2 := [][]byte{
		[]byte(`{"field1": "value1"}`),
		[]byte(`{"other": "value"}`),
	}
	result3, err := extractMultipleDynamicFieldTexts(jsonData2, []string{"field1"})
	s.NoError(err)
	s.Equal([]string{"value1", ""}, result3["field1"])

	// Test empty JSON
	jsonData3 := [][]byte{
		[]byte(`{"field1": "value1"}`),
		[]byte(``),
	}
	result4, err := extractMultipleDynamicFieldTexts(jsonData3, []string{"field1"})
	s.NoError(err)
	s.Equal([]string{"value1", ""}, result4["field1"])

	// Test non-string value
	jsonData4 := [][]byte{
		[]byte(`{"field1": 123}`),
	}
	_, err = extractMultipleDynamicFieldTexts(jsonData4, []string{"field1"})
	s.Error(err)
	s.Contains(err.Error(), "is not a string type")

	// Test invalid JSON
	jsonData5 := [][]byte{
		[]byte(`{invalid json}`),
	}
	_, err = extractMultipleDynamicFieldTexts(jsonData5, []string{"field1"})
	s.Error(err)
	s.Contains(err.Error(), "failed to unmarshal")

	// Test multiple fields with partial data
	jsonData6 := [][]byte{
		[]byte(`{"field1": "a", "field2": "b"}`),
		[]byte(`{"field1": "c"}`), // field2 missing
		[]byte(`{"field2": "d"}`), // field1 missing
	}
	result5, err := extractMultipleDynamicFieldTexts(jsonData6, []string{"field1", "field2"})
	s.NoError(err)
	s.Equal([]string{"a", "c", ""}, result5["field1"])
	s.Equal([]string{"b", "", "d"}, result5["field2"])
}

func (s *SearchPipelineSuite) TestSearchPipeline() {
	collectionName := "test"
	task := &searchTask{
		ctx:            context.Background(),
		collectionName: collectionName,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
			MetricType:   "L2",
			Topk:         10,
			Nq:           2,
			PartitionIDs: []int64{1},
			CollectionID: 1,
			DbID:         1,
		},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
				},
			},
			pkField: &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		queryInfos:             []*planpb.QueryInfo{{}},
		translatedOutputFields: []string{"intField"},
	}

	pipeline, err := newPipeline(searchPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, endNode)

	sr := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, false)
	results, storageCost, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{sr}, segcore.StorageCost{ScannedRemoteBytes: 100, ScannedTotalBytes: 250})
	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(100), storageCost.ScannedRemoteBytes)
	s.Equal(int64(250), storageCost.ScannedTotalBytes)
	s.Equal(int64(2*10), results.GetResults().AllSearchCount)
	fmt.Println(results)
}

func (s *SearchPipelineSuite) TestSearchPipelineWithRequery() {
	collectionName := "test_collection"
	task := &searchTask{
		ctx:            context.Background(),
		collectionName: collectionName,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
			MetricType:   "L2",
			Topk:         10,
			Nq:           2,
			PartitionIDs: []int64{1},
			CollectionID: 1,
			DbID:         1,
		},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
				},
			},
			pkField: &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		queryInfos:             []*planpb.QueryInfo{{}},
		translatedOutputFields: []string{"intField"},
		node:                   nil,
		request:                &milvuspb.SearchRequest{Namespace: nil},
	}

	// Mock requery operation
	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	f1.FieldId = 101
	f2 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f2.FieldId = 100
	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1, f2},
	}, segcore.StorageCost{ScannedRemoteBytes: 100, ScannedTotalBytes: 200}, nil).Build()
	defer mocker.UnPatch()

	pipeline, err := newPipeline(searchWithRequeryPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, endNode)

	results, storageCost, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{
		genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, false),
	}, segcore.StorageCost{ScannedRemoteBytes: 100, ScannedTotalBytes: 200})
	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(200), storageCost.ScannedRemoteBytes)
	s.Equal(int64(400), storageCost.ScannedTotalBytes)
	s.Equal(int64(2*10), results.GetResults().AllSearchCount)
}

func (s *SearchPipelineSuite) TestSearchWithRerankPipe() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{"intField"},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "decay"},
			{Key: "origin", Value: "4"},
			{Key: "scale", Value: "4"},
			{Key: "offset", Value: "4"},
			{Key: "decay", Value: "0.5"},
			{Key: "function", Value: "gauss"},
		},
	}
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
		},
	}
	funcScoreSchema := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{functionSchema}}

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest: &internalpb.SearchRequest{
			MetricType:   "L2",
			Topk:         10,
			Nq:           2,
			PartitionIDs: []int64{1},
			CollectionID: 1,
			DbID:         1,
		},
		schema: &schemaInfo{
			CollectionSchema: schema,
			pkField:          &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		queryInfos:             []*planpb.QueryInfo{{}},
		translatedOutputFields: []string{"intField"},
		node:                   nil,
		rerankMeta:             newRerankMeta(schema, funcScoreSchema),
	}

	pipeline, err := newPipeline(searchWithRerankPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, endNode)

	searchResults := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, false)
	results, _, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{searchResults}, segcore.StorageCost{})

	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(2*10), results.GetResults().AllSearchCount)
}

func (s *SearchPipelineSuite) TestSearchWithRerankRequeryPipe() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{"intField"},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "decay"},
			{Key: "origin", Value: "4"},
			{Key: "scale", Value: "4"},
			{Key: "offset", Value: "4"},
			{Key: "decay", Value: "0.5"},
			{Key: "function", Value: "gauss"},
		},
	}
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
		},
	}
	funcScoreSchema := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{functionSchema}}

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
			MetricType:   "L2",
			Topk:         10,
			Nq:           2,
			PartitionIDs: []int64{1},
			CollectionID: 1,
			DbID:         1,
		},
		schema: &schemaInfo{
			CollectionSchema: schema,
			pkField:          &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		queryInfos:             []*planpb.QueryInfo{{}},
		translatedOutputFields: []string{"intField"},
		node:                   nil,
		rerankMeta:             newRerankMeta(schema, funcScoreSchema),
		request:                &milvuspb.SearchRequest{Namespace: nil},
	}
	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	f1.FieldId = 101
	f2 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f2.FieldId = 100
	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1, f2},
	}, segcore.StorageCost{}, nil).Build()
	defer mocker.UnPatch()

	pipeline, err := newPipeline(searchWithRerankRequeryPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, endNode)

	searchResults := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, false)
	results, storageCost, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{searchResults}, segcore.StorageCost{})

	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(0), storageCost.ScannedRemoteBytes)
	s.Equal(int64(0), storageCost.ScannedTotalBytes)
	s.Equal(int64(2*10), results.GetResults().AllSearchCount)
}

func (s *SearchPipelineSuite) TestHybridSearchPipe() {
	task := getHybridSearchTask("test_collection", [][]string{
		{"1", "2"},
		{"3", "4"},
	},
		[]string{},
	)

	pipeline, err := newPipeline(hybridSearchPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, endNode)

	f1 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	f2 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	results, storageCost, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{f1, f2}, segcore.StorageCost{ScannedRemoteBytes: 900, ScannedTotalBytes: 2000})

	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.Equal(int64(900), storageCost.ScannedRemoteBytes)
	s.Equal(int64(2000), storageCost.ScannedTotalBytes)
	s.Equal(int64(2*2*10), results.GetResults().AllSearchCount)
}

func (s *SearchPipelineSuite) TestFilterFieldOperatorWithStructArrayFields() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "floatField", DataType: schemapb.DataType_Float},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				Name: "structArray",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 104, Name: "structArrayField", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
					{FieldID: 105, Name: "structVectorField", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector},
				},
			},
		},
	}

	task := &searchTask{
		schema: &schemaInfo{
			CollectionSchema: schema,
		},
		translatedOutputFields: []string{"intField", "floatField", "structArrayField", "structVectorField"},
	}

	op, err := newEndOperator(task, nil)
	s.NoError(err)

	// Create mock search results with fields including struct array fields
	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			FieldsData: []*schemapb.FieldData{
				{FieldId: 101}, // intField
				{FieldId: 102}, // floatField
				{FieldId: 104}, // structArrayField
				{FieldId: 105}, // structVectorField
			},
		},
	}

	results, err := op.run(context.Background(), s.span, searchResults, []*milvuspb.SearchResults{{Results: &schemapb.SearchResultData{AllSearchCount: 0}}})
	s.NoError(err)
	s.NotNil(results)

	resultData := results[0].(*milvuspb.SearchResults)
	s.NotNil(resultData.Results.FieldsData)
	s.Len(resultData.Results.FieldsData, 4)

	// Verify all fields including struct array fields got their names and types set
	for _, field := range resultData.Results.FieldsData {
		switch field.FieldId {
		case 101:
			s.Equal("intField", field.FieldName)
			s.Equal(schemapb.DataType_Int64, field.Type)
			s.False(field.IsDynamic)
		case 102:
			s.Equal("floatField", field.FieldName)
			s.Equal(schemapb.DataType_Float, field.Type)
			s.False(field.IsDynamic)
		case 104:
			// Struct array field should be handled by GetAllFieldSchemas
			s.Equal("structArrayField", field.FieldName)
			s.Equal(schemapb.DataType_Array, field.Type)
			s.False(field.IsDynamic)
		case 105:
			// Struct array vector field should be handled by GetAllFieldSchemas
			s.Equal("structVectorField", field.FieldName)
			s.Equal(schemapb.DataType_ArrayOfVector, field.Type)
			s.False(field.IsDynamic)
		}
	}
}

func (s *SearchPipelineSuite) TestEndOperatorRoundsScores() {
	task := &searchTask{
		queryInfos: []*planpb.QueryInfo{{RoundDecimal: 0}},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{},
		},
	}

	op, err := newEndOperator(task, nil)
	s.NoError(err)

	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Scores: []float32{0.49, 0.36},
		},
	}

	results, err := op.run(context.Background(), s.span, searchResults, []*milvuspb.SearchResults{{Results: &schemapb.SearchResultData{AllSearchCount: 0}}})
	s.NoError(err)

	resultData := results[0].(*milvuspb.SearchResults).GetResults()
	s.Equal([]float32{0, 0}, resultData.GetScores())
}

func (s *SearchPipelineSuite) TestRoundAggHitScores() {
	// Aggregation searches bypass endOperator, so hit scores are rounded at the
	// aggregate operator's terminal step. Covers nested sub-aggregation buckets.
	buckets := []*search_agg.AggBucketResult{
		{
			Hits: []*search_agg.HitResult{{Score: 0.49}, {Score: 0.36}},
			SubAggBuckets: []*search_agg.AggBucketResult{
				{Hits: []*search_agg.HitResult{{Score: 0.51}, nil}},
			},
		},
		nil,
	}

	roundAggHitScores(buckets, 0)

	s.Equal(float32(0), buckets[0].Hits[0].Score)
	s.Equal(float32(0), buckets[0].Hits[1].Score)
	s.Equal(float32(1), buckets[0].SubAggBuckets[0].Hits[0].Score)
}

func (s *SearchPipelineSuite) TestRoundAggHitScoresDisabled() {
	buckets := []*search_agg.AggBucketResult{
		{Hits: []*search_agg.HitResult{{Score: 0.49}, {Score: 0.36}}},
	}

	roundAggHitScores(buckets, -1)

	s.Equal(float32(0.49), buckets[0].Hits[0].Score)
	s.Equal(float32(0.36), buckets[0].Hits[1].Score)
}

func (s *SearchPipelineSuite) TestEndOperatorKeepsScoresWhenRoundDecimalDisabled() {
	task := &searchTask{
		queryInfos: []*planpb.QueryInfo{{RoundDecimal: -1}},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{},
		},
	}

	op, err := newEndOperator(task, nil)
	s.NoError(err)

	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Scores: []float32{0.49, 0.36},
		},
	}

	results, err := op.run(context.Background(), s.span, searchResults, []*milvuspb.SearchResults{{Results: &schemapb.SearchResultData{AllSearchCount: 0}}})
	s.NoError(err)

	resultData := results[0].(*milvuspb.SearchResults).GetResults()
	s.Equal([]float32{float32(0.49), float32(0.36)}, resultData.GetScores())
}

func (s *SearchPipelineSuite) TestEndOperatorKeepsAdvancedRerankScores() {
	task := &searchTask{
		SearchRequest: &internalpb.SearchRequest{IsAdvanced: true},
		rankParams:    &rankParams{roundDecimal: 0},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{},
		},
	}

	op, err := newEndOperator(task, nil)
	s.NoError(err)

	searchResults := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Scores: []float32{0.99, 0.88},
		},
	}

	results, err := op.run(context.Background(), s.span, searchResults, []*milvuspb.SearchResults{{Results: &schemapb.SearchResultData{AllSearchCount: 0}}})
	s.NoError(err)

	resultData := results[0].(*milvuspb.SearchResults).GetResults()
	s.Equal([]float32{float32(0.99), float32(0.88)}, resultData.GetScores())
}

func (s *SearchPipelineSuite) TestHybridSearchWithRequeryAndRerankByDataPipe() {
	task := getHybridSearchTask("test_collection", [][]string{
		{"1", "2"},
		{"3", "4"},
	},
		[]string{"intField"},
	)

	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	f1.FieldId = 101
	f2 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f2.FieldId = 100
	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1, f2},
	}, segcore.StorageCost{}, nil).Build()
	defer mocker.UnPatch()

	pipeline, err := newPipeline(hybridSearchWithRequeryAndRerankByFieldDataPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, endNode)
	d1 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	d2 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	results, _, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{d1, d2}, segcore.StorageCost{})

	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(2*2*10), results.GetResults().AllSearchCount)
}

func (s *SearchPipelineSuite) TestHybridSearchWithRequeryAndRerankByDataPipe_ElementLevelRequeryUsesPKs() {
	task := getHybridSearchTask("test_collection", [][]string{
		{"1"},
		{"2"},
	}, []string{"intField"})
	task.hybridElementLevel = true

	input := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       3,
			Topks:      []int64{3},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{10, 10, 20}},
				},
			},
			Scores:         []float32{0.8, 0.9, 0.7},
			ElementIndices: &schemapb.LongArray{Data: []int64{0, 2, 1}},
			AllSearchCount: 3,
		},
	}
	pkField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "int64",
		FieldId:   100,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20}},
				},
			},
		},
	}
	intField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "intField",
		FieldId:   101,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{100, 200}},
				},
			},
		},
	}

	originalReduceFactory := opFactory[hybridSearchReduceOp]
	originalRequeryFactory := opFactory[requeryOp]
	originalRerankFactory := opFactory[rerankOp]
	defer func() {
		opFactory[hybridSearchReduceOp] = originalReduceFactory
		opFactory[requeryOp] = originalRequeryFactory
		opFactory[rerankOp] = originalRerankFactory
	}()

	opFactory[hybridSearchReduceOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
			return []any{[]*milvuspb.SearchResults{input}, []string{"IP"}}, nil
		}), nil
	}

	requeryCalled := false
	opFactory[requeryOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
			requeryCalled = true

			ids, ok := inputs[0].(*schemapb.IDs)
			s.Require().True(ok)
			s.ElementsMatch([]int64{10, 20}, ids.GetIntId().GetData())
			s.Nil(ids.GetStrId())

			storageCost := inputs[1].(segcore.StorageCost)
			return []any{[]*schemapb.FieldData{intField, pkField}, storageCost}, nil
		}), nil
	}

	rerankCalled := false
	opFactory[rerankOp] = func(_ *searchTask, _ map[string]any) (operator, error) {
		return searchPipelineTestOperator(func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
			rerankCalled = true

			rankData, ok := inputs[0].([]*milvuspb.SearchResults)
			s.Require().True(ok)
			s.Require().Len(rankData, 1)
			data := rankData[0].GetResults()
			s.Equal([]string{
				makeHybridElementKey(int64(10), 0),
				makeHybridElementKey(int64(10), 2),
				makeHybridElementKey(int64(20), 1),
			}, data.GetIds().GetStrId().GetData())
			s.Equal([]int64{100, 100, 200}, data.GetFieldsData()[0].GetScalars().GetLongData().GetData())

			return []any{&milvuspb.SearchResults{
				Status: merr.Success(),
				Results: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       2,
					Topks:      []int64{2},
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_StrId{
							StrId: &schemapb.StringArray{Data: []string{
								makeHybridElementKey(int64(10), 2),
								makeHybridElementKey(int64(20), 1),
							}},
						},
					},
					Scores: []float32{0.99, 0.88},
				},
			}}, nil
		}), nil
	}

	pipeline, err := newPipeline(hybridSearchWithRequeryAndRerankByFieldDataPipe, task)
	s.Require().NoError(err)
	s.Require().NoError(pipeline.AddNodes(task, endNode))

	results, _, err := pipeline.Run(context.Background(), s.span, nil, segcore.StorageCost{})
	s.Require().NoError(err)
	s.True(requeryCalled)
	s.True(rerankCalled)

	result := results.GetResults()
	s.Equal([]int64{10, 20}, result.GetIds().GetIntId().GetData())
	s.Equal([]int64{2, 1}, result.GetElementIndices().GetData())
	s.Equal([]float32{0.99, 0.88}, result.GetScores())
	s.Equal([]int64{100, 200}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
}

func (s *SearchPipelineSuite) TestHybridSearchWithRequeryPipe() {
	task := getHybridSearchTask("test_collection", [][]string{
		{"1", "2"},
		{"3", "4"},
	},
		[]string{"intField"},
	)

	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	f1.FieldId = 101
	f2 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f2.FieldId = 100
	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1, f2},
	}, segcore.StorageCost{}, nil).Build()
	defer mocker.UnPatch()

	pipeline, err := newPipeline(hybridSearchWithRequeryPipe, task)
	s.NoError(err)
	pipeline.AddNodes(task, endNode)

	d1 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	d2 := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, true)
	results, _, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{d1, d2}, segcore.StorageCost{})

	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
	s.Equal(int64(10), results.Results.Topks[0])
	s.Equal(int64(10), results.Results.Topks[1])
	s.NotNil(results.Results.Ids)
	s.NotNil(results.Results.Ids.GetIntId())
	s.Len(results.Results.Ids.GetIntId().Data, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.Scores)
	s.Len(results.Results.Scores, 20) // 2 queries * 10 topk
	s.NotNil(results.Results.FieldsData)
	s.Len(results.Results.FieldsData, 1) // One output field
	s.Equal("intField", results.Results.FieldsData[0].FieldName)
	s.Equal(int64(101), results.Results.FieldsData[0].FieldId)
	s.Equal(int64(2*2*10), results.GetResults().AllSearchCount)
}

func getHybridSearchTask(collName string, data [][]string, outputFields []string) *searchTask {
	subReqs := []*milvuspb.SubSearchRequest{}
	for _, item := range data {
		subReq := &milvuspb.SubSearchRequest{
			SearchParams: []*commonpb.KeyValuePair{
				{Key: TopKKey, Value: "10"},
			},
			Nq: int64(len(item)),
		}
		subReqs = append(subReqs, subReq)
	}
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{},
		OutputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "rrf"},
		},
	}

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
		},
	}
	funcScoreSchema := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{functionSchema}}
	task := &searchTask{
		ctx:            context.Background(),
		collectionName: collName,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
			Topk:       10,
			Nq:         2,
			IsAdvanced: true,
			SubReqs: []*internalpb.SubSearchRequest{
				{
					Topk: 10,
					Nq:   2,
				},
				{
					Topk: 10,
					Nq:   2,
				},
			},
		},
		request: &milvuspb.SearchRequest{
			CollectionName: collName,
			SubReqs:        subReqs,
			SearchParams: []*commonpb.KeyValuePair{
				{Key: LimitKey, Value: "10"},
			},
			FunctionScore: funcScoreSchema,
			OutputFields:  outputFields,
		},
		schema: &schemaInfo{
			CollectionSchema: schema,
			pkField:          &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		mixCoord: nil,
		tr:       timerecord.NewTimeRecorder("test-search"),
		rankParams: &rankParams{
			limit:        10,
			offset:       0,
			roundDecimal: 0,
		},
		queryInfos:             []*planpb.QueryInfo{{}, {}},
		rerankMeta:             newRerankMeta(schema, funcScoreSchema),
		translatedOutputFields: outputFields,
	}
	return task
}

func (s *SearchPipelineSuite) TestMergeIDsFunc() {
	{
		ids1 := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 5},
				},
			},
		}

		ids2 := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 4, 5, 6},
				},
			},
		}
		rets := []*milvuspb.SearchResults{
			{
				Results: &schemapb.SearchResultData{
					Ids: ids1,
				},
			},
			{
				Results: &schemapb.SearchResultData{
					Ids: ids2,
				},
			},
		}
		allIDs, err := mergeIDsFunc(context.Background(), s.span, rets)
		s.NoError(err)
		sortedIds := allIDs[0].(*schemapb.IDs).GetIntId().GetData()
		slices.Sort(sortedIds)
		s.Equal(sortedIds, []int64{1, 2, 3, 4, 5, 6})
	}
	{
		ids1 := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: []string{"a", "b", "e"},
				},
			},
		}

		ids2 := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: []string{"a", "b", "c", "d"},
				},
			},
		}
		rets := []*milvuspb.SearchResults{
			{
				Results: &schemapb.SearchResultData{
					Ids: ids1,
				},
			},
		}
		rets = append(rets, &milvuspb.SearchResults{
			Results: &schemapb.SearchResultData{
				Ids: ids2,
			},
		})
		allIDs, err := mergeIDsFunc(context.Background(), s.span, rets)
		s.NoError(err)
		sortedIds := allIDs[0].(*schemapb.IDs).GetStrId().GetData()
		slices.Sort(sortedIds)
		s.Equal(sortedIds, []string{"a", "b", "c", "d", "e"})
	}
}

// Test parseOrderByFields function
func (s *SearchPipelineSuite) TestParseOrderByFields() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", DataType: schemapb.DataType_Int64},
			{Name: "score", DataType: schemapb.DataType_Float},
			{Name: "name", DataType: schemapb.DataType_VarChar},
			{Name: "active", DataType: schemapb.DataType_Bool},
			{Name: "vector", DataType: schemapb.DataType_FloatVector},
		},
	}

	// Test empty order_by_fields
	params := []*commonpb.KeyValuePair{}
	result, err := parseOrderByFields(params, schema)
	s.NoError(err)
	s.Nil(result)

	// Test single field ascending (default)
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)
	s.True(result[0].Ascending)
	s.False(result[0].NullsFirst)

	// Test single field with explicit asc
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:asc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)
	s.True(result[0].Ascending)
	s.False(result[0].NullsFirst)

	// Test single field descending
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:desc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)
	s.False(result[0].Ascending)
	s.True(result[0].NullsFirst)

	// Test explicit null ordering
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:asc:nulls_first"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.True(result[0].Ascending)
	s.True(result[0].NullsFirst)

	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:desc:nulls_last"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.False(result[0].Ascending)
	s.False(result[0].NullsFirst)

	// Test multiple fields
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:desc,name:asc,id"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 3)
	s.Equal("score", result[0].FieldName)
	s.False(result[0].Ascending)
	s.True(result[0].NullsFirst)
	s.Equal("name", result[1].FieldName)
	s.True(result[1].Ascending)
	s.False(result[1].NullsFirst)
	s.Equal("id", result[2].FieldName)
	s.True(result[2].Ascending)
	s.False(result[2].NullsFirst)

	// Test invalid direction
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:invalid"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "invalid order direction")

	// Test invalid null ordering
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:asc:nulls_middle"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "invalid null ordering 'nulls_middle', expected 'nulls_first' or 'nulls_last'")

	// Test non-existent field
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "nonexistent"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "does not exist")

	// Test unsortable type (vector)
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "vector"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "unsortable type")

	// Test empty field name
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: ":asc"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "empty field name")
}

// Test parseOrderByFields with various input formats and edge cases
func (s *SearchPipelineSuite) TestParseOrderByFieldsEdgeCases() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", DataType: schemapb.DataType_Int64},
			{Name: "score", DataType: schemapb.DataType_Float},
			{Name: "name", DataType: schemapb.DataType_VarChar},
		},
	}

	// Test whitespace handling - spaces around field name
	params := []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "  score  "}}
	result, err := parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)

	// Test whitespace handling - spaces around colon
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score : desc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)
	s.False(result[0].Ascending)

	// Test whitespace handling - spaces around comma
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:desc , name:asc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 2)
	s.Equal("score", result[0].FieldName)
	s.Equal("name", result[1].FieldName)

	// Test case insensitivity - uppercase ASC
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:ASC"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.True(result[0].Ascending)

	// Test case insensitivity - uppercase DESC
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:DESC"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.False(result[0].Ascending)

	// Test case insensitivity - mixed case
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:Desc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.False(result[0].Ascending)

	// Test "ascending" keyword
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:ascending"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.True(result[0].Ascending)

	// Test "descending" keyword
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:descending"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.False(result[0].Ascending)

	// Test empty string value
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: ""}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Nil(result)

	// Test trailing comma - should skip empty part
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:asc,"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)

	// Test leading comma - should skip empty part
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: ",score:asc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("score", result[0].FieldName)

	// Test multiple commas - should skip empty parts
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:asc,,name:desc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 2)

	// Test only commas - should return nil
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: ",,,"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Nil(result)

	// Test key not present - should return nil
	params = []*commonpb.KeyValuePair{{Key: "other_key", Value: "score:asc"}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Nil(result)

	// Test all supported sortable types
	schemaAllTypes := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "f_bool", DataType: schemapb.DataType_Bool},
			{Name: "f_int8", DataType: schemapb.DataType_Int8},
			{Name: "f_int16", DataType: schemapb.DataType_Int16},
			{Name: "f_int32", DataType: schemapb.DataType_Int32},
			{Name: "f_int64", DataType: schemapb.DataType_Int64},
			{Name: "f_float", DataType: schemapb.DataType_Float},
			{Name: "f_double", DataType: schemapb.DataType_Double},
			{Name: "f_string", DataType: schemapb.DataType_String},
			{Name: "f_varchar", DataType: schemapb.DataType_VarChar},
		},
	}

	// All these should be valid
	for _, fieldName := range []string{"f_bool", "f_int8", "f_int16", "f_int32", "f_int64", "f_float", "f_double", "f_string", "f_varchar"} {
		params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: fieldName}}
		result, err = parseOrderByFields(params, schemaAllTypes)
		s.NoError(err, "field %s should be sortable", fieldName)
		s.Len(result, 1)
		s.Equal(fieldName, result[0].FieldName)
	}

	// Test unsortable types
	// Note: JSON without path syntax is not directly sortable; use path syntax like field["key"] instead
	schemaUnsortable := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "f_float_vector", DataType: schemapb.DataType_FloatVector},
			{Name: "f_binary_vector", DataType: schemapb.DataType_BinaryVector},
			{Name: "f_array", DataType: schemapb.DataType_Array},
			{Name: "f_json", DataType: schemapb.DataType_JSON},
		},
	}

	for _, fieldName := range []string{"f_float_vector", "f_binary_vector", "f_array", "f_json"} {
		params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: fieldName}}
		_, err = parseOrderByFields(params, schemaUnsortable)
		s.Error(err, "field %s should not be sortable", fieldName)
		s.Contains(err.Error(), "unsortable type")
	}
}

// Test isSortableFieldType function
func (s *SearchPipelineSuite) TestIsSortableFieldType() {
	// Sortable types
	s.True(isSortableFieldType(schemapb.DataType_Bool))
	s.True(isSortableFieldType(schemapb.DataType_Int8))
	s.True(isSortableFieldType(schemapb.DataType_Int16))
	s.True(isSortableFieldType(schemapb.DataType_Int32))
	s.True(isSortableFieldType(schemapb.DataType_Int64))
	s.True(isSortableFieldType(schemapb.DataType_Float))
	s.True(isSortableFieldType(schemapb.DataType_Double))
	s.True(isSortableFieldType(schemapb.DataType_String))
	s.True(isSortableFieldType(schemapb.DataType_VarChar))

	// Non-sortable types
	// Note: JSON type requires path syntax (e.g., field["key"]) for sorting
	s.False(isSortableFieldType(schemapb.DataType_JSON))
	s.False(isSortableFieldType(schemapb.DataType_FloatVector))
	s.False(isSortableFieldType(schemapb.DataType_BinaryVector))
	s.False(isSortableFieldType(schemapb.DataType_Array))
}

// Test compareFieldDataAt function
func (s *SearchPipelineSuite) TestCompareFieldDataAt() {
	// Helper to call compareFieldDataAt and assert no error
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j, true)
		s.NoError(err)
		return cmp
	}

	// Test Int64 comparison
	int64Field := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "int64_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 15}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(int64Field, 0, 1)) // 10 < 20
	s.Equal(1, mustCompare(int64Field, 1, 0))  // 20 > 10
	s.Equal(0, mustCompare(int64Field, 0, 0))  // 10 == 10

	// Test Float comparison
	floatField := &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: "float_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: []float32{1.5, 2.5, 1.5}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(floatField, 0, 1)) // 1.5 < 2.5
	s.Equal(1, mustCompare(floatField, 1, 0))  // 2.5 > 1.5
	s.Equal(0, mustCompare(floatField, 0, 2))  // 1.5 == 1.5

	// Test String comparison
	stringField := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "string_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"apple", "banana", "apple"}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(stringField, 0, 1)) // "apple" < "banana"
	s.Equal(1, mustCompare(stringField, 1, 0))  // "banana" > "apple"
	s.Equal(0, mustCompare(stringField, 0, 2))  // "apple" == "apple"

	// Test Bool comparison (false < true)
	boolField := &schemapb.FieldData{
		Type:      schemapb.DataType_Bool,
		FieldName: "bool_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: []bool{false, true, false}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(boolField, 0, 1)) // false < true
	s.Equal(1, mustCompare(boolField, 1, 0))  // true > false
	s.Equal(0, mustCompare(boolField, 0, 2))  // false == false
}

// Test compareFieldDataAt with null handling
func (s *SearchPipelineSuite) TestCompareFieldDataAtWithNulls() {
	// Helper to call compareFieldDataAt and assert no error
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j, true)
		s.NoError(err)
		return cmp
	}

	// Test with ValidData (nullable field)
	nullableField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "nullable_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
		},
		ValidData: []bool{true, false, true}, // index 1 is null
	}

	// NULLS FIRST
	s.Equal(-1, mustCompare(nullableField, 1, 0)) // null < 10
	s.Equal(1, mustCompare(nullableField, 0, 1))  // 10 > null

	cmp, err := compareFieldDataAt(nullableField, 1, 0, false)
	s.NoError(err)
	s.Equal(1, cmp) // NULLS LAST: null > 10
	cmp, err = compareFieldDataAt(nullableField, 0, 1, false)
	s.NoError(err)
	s.Equal(-1, cmp) // NULLS LAST: 10 < null

	// null vs null: equal
	nullableField2 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "nullable_field2",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
		},
		ValidData: []bool{false, false, true}, // index 0 and 1 are null
	}
	s.Equal(0, mustCompare(nullableField2, 0, 1)) // null == null

	// non-null vs non-null: normal comparison
	s.Equal(-1, mustCompare(nullableField, 0, 2)) // 10 < 30
}

// Test isSameGroupByValue function
func (s *SearchPipelineSuite) TestIsSameGroupByValue() {
	// Test Int64
	int64Field := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{1, 1, 2, 2, 3}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(int64Field, 0, 1))  // 1 == 1
	s.False(isSameGroupByValue(int64Field, 1, 2)) // 1 != 2
	s.True(isSameGroupByValue(int64Field, 2, 3))  // 2 == 2

	// Test String
	stringField := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"a", "a", "b"}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(stringField, 0, 1))  // "a" == "a"
	s.False(isSameGroupByValue(stringField, 1, 2)) // "a" != "b"

	// Test Bool
	boolField := &schemapb.FieldData{
		Type: schemapb.DataType_Bool,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: []bool{true, true, false}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(boolField, 0, 1))  // true == true
	s.False(isSameGroupByValue(boolField, 1, 2)) // true != false
}

func (s *SearchPipelineSuite) TestNewSearchReduceOperatorUsesPipelineOffsetParam() {
	task := &searchTask{
		ctx: context.Background(),
		SearchRequest: &internalpb.SearchRequest{
			Nq:           1,
			Topk:         3,
			Offset:       2,
			CollectionID: 100,
			PartitionIDs: []int64{10},
		},
		schema: mustNewSchemaInfo(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 101, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			},
		}),
		queryInfos: []*planpb.QueryInfo{{}},
	}

	op, err := newSearchReduceOperator(task, map[string]any{reduceOffsetParamKey: int64(0)})

	s.NoError(err)
	s.Equal(int64(0), op.(*searchReduceOperator).offset)
}

func (s *SearchPipelineSuite) TestNewOrderByOperatorUsesPluralGroupByFieldIDs() {
	task := &searchTask{
		orderByFields: []OrderByField{{FieldName: "price", Ascending: true}},
		queryInfos: []*planpb.QueryInfo{{
			GroupByFieldId:  -1,
			GroupByFieldIds: []int64{101, 102},
			GroupSize:       2,
		}},
	}

	op, err := newOrderByOperator(task, nil)
	s.NoError(err)
	orderByOp := op.(*orderByOperator)
	s.Equal(int64(101), orderByOp.groupByFieldId)
	s.Equal(int64(2), orderByOp.groupSize)
}

// Test orderByOperator sorting
func (s *SearchPipelineSuite) TestOrderByOperator() {
	// Create test search result
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4}, // nq=1, 4 results
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 40, 20}},
							},
						},
					},
				},
			},
		},
	}

	// Test ascending sort by price
	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort by price: 10, 20, 30, 40
	// Original order: 30(id=1), 10(id=2), 40(id=3), 20(id=4)
	// Sorted order: 10(id=2), 20(id=4), 30(id=1), 40(id=3)
	expectedIds := []int64{2, 4, 1, 3}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedPrices := []int64{10, 20, 30, 40}
	actualPrices := sortedResult.Results.FieldsData[0].GetScalars().GetLongData().Data
	s.Equal(expectedPrices, actualPrices)
}

func (s *SearchPipelineSuite) TestOrderByOperatorAppliesOffsetAfterOrderBy() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores:         []float32{0.9, 0.8, 0.7, 0.6},
			Distances:      []float32{9, 8, 7, 6},
			Recalls:        []float32{90, 80, 70, 60},
			ElementIndices: &schemapb.LongArray{Data: []int64{10, 20, 30, 40}},
			TopK:           4,
			Topks:          []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 20, 40}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: -1,
		limit:          2,
		offset:         1,
	}
	outputs, err := op.run(context.Background(), s.span, result)

	s.NoError(err)
	sliced := outputs[0].(*milvuspb.SearchResults).GetResults()
	s.Equal([]int64{3, 1}, sliced.GetIds().GetIntId().GetData())
	s.Equal([]float32{0.7, 0.9}, sliced.GetScores())
	s.Equal([]float32{7, 9}, sliced.GetDistances())
	s.Equal([]float32{70, 90}, sliced.GetRecalls())
	s.Equal([]int64{30, 10}, sliced.GetElementIndices().GetData())
	s.Equal([]int64{20, 30}, sliced.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	s.Equal([]int64{2}, sliced.GetTopks())
	s.Equal(int64(2), sliced.GetTopK())
}

func (s *SearchPipelineSuite) TestOrderByOperatorSlicesStringIDsAndNonNullableVectorAfterOrderBy() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{Data: []string{"a", "b", "c", "d"}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			TopK:   4,
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 20, 40}},
							},
						},
					},
				},
				{
					Type:      schemapb.DataType_FloatVector,
					FieldName: "vec",
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: 2,
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6, 7, 8}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: -1,
		limit:          2,
		offset:         1,
	}
	outputs, err := op.run(context.Background(), s.span, result)

	s.NoError(err)
	sliced := outputs[0].(*milvuspb.SearchResults).GetResults()
	s.Equal([]string{"c", "a"}, sliced.GetIds().GetStrId().GetData())
	s.Equal([]float32{0.7, 0.9}, sliced.GetScores())
	s.Equal([]int64{20, 30}, sliced.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	s.Equal([]float32{5, 6, 1, 2}, sliced.GetFieldsData()[1].GetVectors().GetFloatVector().GetData())
	s.Equal([]int64{2}, sliced.GetTopks())
	s.Equal(int64(2), sliced.GetTopK())
}

func (s *SearchPipelineSuite) TestOrderByOperatorAppliesGroupOffsetAfterOrderBy() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6}},
				},
			},
			Scores: []float32{0.9, 0.85, 0.8, 0.75, 0.7, 0.65},
			TopK:   6,
			Topks:  []int64{6},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 25, 10, 20, 15, 18}},
							},
						},
					},
				},
			},
			GroupByFieldValues: []*schemapb.FieldData{
				{
					Type: schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{"A", "A", "B", "C", "C", "C"}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: 100,
		groupSize:      3,
		limit:          1,
		offset:         1,
	}
	outputs, err := op.run(context.Background(), s.span, result)

	s.NoError(err)
	sliced := outputs[0].(*milvuspb.SearchResults).GetResults()
	s.Equal([]int64{4, 5, 6}, sliced.GetIds().GetIntId().GetData())
	s.Equal([]float32{0.75, 0.7, 0.65}, sliced.GetScores())
	s.Equal([]int64{20, 15, 18}, sliced.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	s.Equal([]string{"C", "C", "C"}, sliced.GetGroupByFieldValues()[0].GetScalars().GetStringData().GetData())
	s.Nil(sliced.GetGroupByFieldValue())
	s.Equal([]int64{3}, sliced.GetTopks())
	s.Equal(int64(3), sliced.GetTopK())
}

func (s *SearchPipelineSuite) TestOrderByOperatorReordersNullableVectorCompactOutput() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
			Topks:  []int64{3},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 20}},
							},
						},
					},
				},
				{
					Type:      schemapb.DataType_FloatVector,
					FieldName: "nullable_float_vec",
					ValidData: []bool{true, false, true},
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: 2,
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 5, 6}},
							},
						},
					},
				},
				{
					Type:      schemapb.DataType_SparseFloatVector,
					FieldName: "nullable_sparse_vec",
					ValidData: []bool{true, false, true},
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: 3,
							Data: &schemapb.VectorField_SparseFloatVector{
								SparseFloatVector: &schemapb.SparseFloatArray{
									Dim:      3,
									Contents: [][]byte{{0x01}, {0x03}},
								},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	s.Equal([]int64{2, 3, 1}, sortedResult.Results.Ids.GetIntId().Data)
	s.Equal([]int64{10, 20, 30}, sortedResult.Results.FieldsData[0].GetScalars().GetLongData().Data)
	s.Equal([]bool{false, true, true}, sortedResult.Results.FieldsData[1].GetValidData())
	s.Equal([]float32{5, 6, 1, 2}, sortedResult.Results.FieldsData[1].GetVectors().GetFloatVector().GetData())
	s.Equal([]bool{false, true, true}, sortedResult.Results.FieldsData[2].GetValidData())
	s.Equal([][]byte{{0x03}, {0x01}}, sortedResult.Results.FieldsData[2].GetVectors().GetSparseFloatVector().GetContents())
}

// Test orderByOperator with nq>1 (multiple queries)
// Each query's results should be sorted independently
func (s *SearchPipelineSuite) TestOrderByOperatorMultipleQueries() {
	// Create test search result with nq=2, topk=3 each
	// Query 1: ids [1,2,3] with prices [30,10,20] -> sorted: [2,3,1] with prices [10,20,30]
	// Query 2: ids [4,5,6] with prices [60,40,50] -> sorted: [5,6,4] with prices [40,50,60]
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
			Topks:  []int64{3, 3}, // nq=2, each query has 3 results
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 20, 60, 40, 50}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Each query should be sorted independently
	// Query 1: [10,20,30] -> ids [2,3,1]
	// Query 2: [40,50,60] -> ids [5,6,4]
	expectedIds := []int64{2, 3, 1, 5, 6, 4}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedPrices := []int64{10, 20, 30, 40, 50, 60}
	actualPrices := sortedResult.Results.FieldsData[0].GetScalars().GetLongData().Data
	s.Equal(expectedPrices, actualPrices)

	// Verify Topks unchanged
	s.Equal([]int64{3, 3}, sortedResult.Results.Topks)
}

// Test orderByOperator with nq>1 and group_by
// Each query's groups should be sorted independently
func (s *SearchPipelineSuite) TestOrderByOperatorMultipleQueriesWithGroupBy() {
	// nq=2, each query has 2 groups with 2 results each
	// Query 1: group A [ids 1,2], group B [ids 3,4] with prices [20,25,10,15]
	//   -> After sort by first row price: group B (10) comes before group A (20)
	//   -> Result: [3,4,1,2] with prices [10,15,20,25]
	// Query 2: group C [ids 5,6], group D [ids 7,8] with prices [50,55,30,35]
	//   -> After sort by first row price: group D (30) comes before group C (50)
	//   -> Result: [7,8,5,6] with prices [30,35,50,55]
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6, 7, 8}},
				},
			},
			Scores: []float32{0.9, 0.85, 0.8, 0.75, 0.7, 0.65, 0.6, 0.55},
			Topks:  []int64{4, 4}, // nq=2, each query has 4 results (2 groups x 2)
			GroupByFieldValue: &schemapb.FieldData{
				Type:      schemapb.DataType_Int64,
				FieldName: "category",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							// Query 1: groups A(100), A(100), B(200), B(200)
							// Query 2: groups C(300), C(300), D(400), D(400)
							LongData: &schemapb.LongArray{Data: []int64{100, 100, 200, 200, 300, 300, 400, 400}},
						},
					},
				},
			},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{20, 25, 10, 15, 50, 55, 30, 35}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: 1, // Enable group_by
		groupSize:      2,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Query 1: group B (price 10) before group A (price 20) -> [3,4,1,2]
	// Query 2: group D (price 30) before group C (price 50) -> [7,8,5,6]
	expectedIds := []int64{3, 4, 1, 2, 7, 8, 5, 6}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedPrices := []int64{10, 15, 20, 25, 30, 35, 50, 55}
	actualPrices := sortedResult.Results.FieldsData[0].GetScalars().GetLongData().Data
	s.Equal(expectedPrices, actualPrices)
}

// Per liliu-z review (search_pipeline.go:1855): the per-query offset/limit loop was only
// exercised with nq=1. Assert offset/limit apply PER QUERY, not globally over the flattened
// result, so a regression that slices the offset window globally returns misaligned rows for nq>1.
func (s *SearchPipelineSuite) TestOrderByOperatorAppliesOffsetPerQueryForMultipleQueries() {
	// nq=2, topk=4 each.
	// q1 ids[1,2,3,4] price[40,10,30,20] -> sort asc [2,4,3,1] -> offset1/limit2 -> [4,3]
	// q2 ids[5,6,7,8] price[80,50,70,60] -> sort asc [6,8,7,5] -> offset1/limit2 -> [8,7]
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6, 7, 8}}}},
			Scores: []float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2},
			TopK:   4,
			Topks:  []int64{4, 4},
			FieldsData: []*schemapb.FieldData{{
				Type:      schemapb.DataType_Int64,
				FieldName: "price",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{40, 10, 30, 20, 80, 50, 70, 60}}},
				}},
			}},
		},
	}

	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: -1,
		limit:          2,
		offset:         1,
	}
	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sliced := outputs[0].(*milvuspb.SearchResults).GetResults()

	// per-query pages: q1 -> [4,3], q2 -> [8,7]; a global slice would drop q2's page entirely.
	s.Equal([]int64{4, 3, 8, 7}, sliced.GetIds().GetIntId().GetData())
	s.Equal([]int64{20, 30, 60, 70}, sliced.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	s.Equal([]float32{0.6, 0.7, 0.2, 0.3}, sliced.GetScores())
	s.Equal([]int64{2, 2}, sliced.GetTopks()) // each query keeps its own page size
}

// Same per-query invariant as above, combined with group-by: the group offset/limit must
// paginate groups PER QUERY, not across the flattened multi-query result.
func (s *SearchPipelineSuite) TestOrderByOperatorAppliesGroupOffsetPerQueryForMultipleQueries() {
	// nq=2, topk=4 each, 2 groups (size 2) per query.
	// q1 A(ids1,2 p20,25) B(ids3,4 p10,15) -> groups asc by first price [B,A] -> off1/lim1 group -> A=[1,2]
	// q2 C(ids5,6 p50,55) D(ids7,8 p30,35) -> groups asc by first price [D,C] -> off1/lim1 group -> C=[5,6]
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6, 7, 8}}}},
			Scores: []float32{0.9, 0.85, 0.8, 0.75, 0.7, 0.65, 0.6, 0.55},
			TopK:   4,
			Topks:  []int64{4, 4},
			GroupByFieldValues: []*schemapb.FieldData{{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{100, 100, 200, 200, 300, 300, 400, 400}}},
				}},
			}},
			FieldsData: []*schemapb.FieldData{{
				Type:      schemapb.DataType_Int64,
				FieldName: "price",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{20, 25, 10, 15, 50, 55, 30, 35}}},
				}},
			}},
		},
	}

	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: 1,
		groupSize:      2,
		limit:          1,
		offset:         1,
	}
	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sliced := outputs[0].(*milvuspb.SearchResults).GetResults()

	s.Equal([]int64{1, 2, 5, 6}, sliced.GetIds().GetIntId().GetData())
	s.Equal([]int64{20, 25, 50, 55}, sliced.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	s.Equal([]int64{2, 2}, sliced.GetTopks())
}

// paginateSortedRows: offset at/beyond the row count clamps to an empty page.
func (s *SearchPipelineSuite) TestPaginateSortedRowsOffsetBeyondLength() {
	indices := []int{0, 1, 2}
	s.Empty(paginateSortedRows(indices, 5, 2))                 // offset > len -> empty
	s.Empty(paginateSortedRows(indices, 3, 0))                 // offset == len -> empty
	s.Equal([]int{1, 2}, paginateSortedRows(indices, 1, 10))   // limit beyond end -> offset..end
	s.Equal([]int{0, 1, 2}, paginateSortedRows(indices, 0, 0)) // no limit -> all
}

// getOrderByGroupByFieldValue: nil input, plural-preferred, legacy fallback.
func (s *SearchPipelineSuite) TestGetOrderByGroupByFieldValueNilAndSelection() {
	s.Nil(getOrderByGroupByFieldValue(nil))

	plural := &schemapb.SearchResultData{
		GroupByFieldValues: []*schemapb.FieldData{{FieldName: "g"}},
	}
	s.Equal("g", getOrderByGroupByFieldValue(plural).GetFieldName())

	legacy := &schemapb.SearchResultData{
		GroupByFieldValue: &schemapb.FieldData{FieldName: "l"},
	}
	s.Equal("l", getOrderByGroupByFieldValue(legacy).GetFieldName())
}

// sortQueryResults returns early on empty indices (op.run guards with topk>0, so cover directly).
func (s *SearchPipelineSuite) TestSortQueryResultsEmptyIndices() {
	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: -1,
	}
	out, err := op.sortQueryResults(&milvuspb.SearchResults{Results: &schemapb.SearchResultData{}}, []int{})
	s.NoError(err)
	s.Empty(out)
}

// sortGroupsByOrderByFields returns early on empty indices.
func (s *SearchPipelineSuite) TestSortGroupsByOrderByFieldsEmptyIndices() {
	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: 0,
		groupSize:      1,
	}
	out, err := op.sortGroupsByOrderByFields(&milvuspb.SearchResults{Results: &schemapb.SearchResultData{}}, []int{})
	s.NoError(err)
	s.Empty(out)
}

// reorderFieldData: vector payload length not divisible by per-vector width -> error (defensive guard).
func (s *SearchPipelineSuite) TestReorderFieldDataDimNotDivisibleErrors() {
	indices := []int{0}
	cases := []struct {
		name  string
		field *schemapb.FieldData
	}{
		{"float_vector", &schemapb.FieldData{
			Type: schemapb.DataType_FloatVector, FieldName: "fv",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3}}},
			}},
		}},
		{"binary_vector", &schemapb.FieldData{
			Type: schemapb.DataType_BinaryVector, FieldName: "bv",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  16,
				Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0x01, 0x02, 0x03}},
			}},
		}},
		{"float16_vector", &schemapb.FieldData{
			Type: schemapb.DataType_Float16Vector, FieldName: "f16",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06}},
			}},
		}},
		{"bfloat16_vector", &schemapb.FieldData{
			Type: schemapb.DataType_BFloat16Vector, FieldName: "bf16",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06}},
			}},
		}},
		{"int8_vector", &schemapb.FieldData{
			Type: schemapb.DataType_Int8Vector, FieldName: "i8",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  4,
				Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06}},
			}},
		}},
	}
	for _, c := range cases {
		s.Error(reorderFieldData(c.field, indices), c.name)
	}
}

// reorderFieldData: index out of range for the vector payload -> validateReorderIndex error (defensive guard).
func (s *SearchPipelineSuite) TestReorderFieldDataIndexOutOfBoundsErrors() {
	oob := []int{5} // every field below holds 2 vectors; index 5 is out of range
	cases := []struct {
		name  string
		field *schemapb.FieldData
	}{
		{"float_vector", &schemapb.FieldData{
			Type: schemapb.DataType_FloatVector, FieldName: "fv",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}},
			}},
		}},
		{"binary_vector", &schemapb.FieldData{
			Type: schemapb.DataType_BinaryVector, FieldName: "bv",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  16,
				Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0x01, 0x02, 0x03, 0x04}},
			}},
		}},
		{"float16_vector", &schemapb.FieldData{
			Type: schemapb.DataType_Float16Vector, FieldName: "f16",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}},
			}},
		}},
		{"bfloat16_vector", &schemapb.FieldData{
			Type: schemapb.DataType_BFloat16Vector, FieldName: "bf16",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}},
			}},
		}},
		{"sparse_vector", &schemapb.FieldData{
			Type: schemapb.DataType_SparseFloatVector, FieldName: "sv",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: 100,
				Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
					Contents: [][]byte{{0x01, 0x02}, {0x03, 0x04}},
				}},
			}},
		}},
		{"int8_vector", &schemapb.FieldData{
			Type: schemapb.DataType_Int8Vector, FieldName: "i8",
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  4,
				Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}},
			}},
		}},
	}
	for _, c := range cases {
		s.Error(reorderFieldData(c.field, oob), c.name)
	}
}

// reorderResults: out-of-range index for each result column -> internal error (defensive guard).
func (s *SearchPipelineSuite) TestReorderResultsIndexOutOfBounds() {
	op := &orderByOperator{}
	oob := []int{5}
	longIDs := func() *schemapb.IDs {
		return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}}}
	}

	// int IDs
	s.Error(op.reorderResults(&milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
	}}, oob), "int_ids")

	// string IDs
	s.Error(op.reorderResults(&milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a"}}}},
	}}, oob), "str_ids")

	// distances (IDs long enough so the distances check is the one that trips)
	s.Error(op.reorderResults(&milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		Ids: longIDs(), Distances: []float32{0.1},
	}}, oob), "distances")

	// recalls
	s.Error(op.reorderResults(&milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		Ids: longIDs(), Recalls: []float32{0.1},
	}}, oob), "recalls")

	// element indices
	s.Error(op.reorderResults(&milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		Ids: longIDs(), ElementIndices: &schemapb.LongArray{Data: []int64{10}},
	}}, oob), "element_indices")
}

// op.run propagates a sort error: the order_by field has fewer values than rows, so
// compareFieldDataAt hits an out-of-bounds index during the per-query sort.
func (s *SearchPipelineSuite) TestOrderByRunReturnsSortError() {
	result := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		Scores: []float32{0.9, 0.8, 0.7},
		Topks:  []int64{3},
		FieldsData: []*schemapb.FieldData{{
			Type: schemapb.DataType_Int64, FieldName: "price",
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10}}}, // 1 value for 3 rows
			}},
		}},
	}}
	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: -1,
	}
	_, err := op.run(context.Background(), s.span, result)
	s.Error(err)
}

// op.run propagates a reorder error: the sort succeeds but a malformed vector field fails reorderResults.
func (s *SearchPipelineSuite) TestOrderByRunReturnsReorderError() {
	result := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores: []float32{0.9, 0.8},
		Topks:  []int64{2},
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64, FieldName: "price",
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{20, 10}}},
				}},
			},
			{
				Type: schemapb.DataType_FloatVector, FieldName: "vec",
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3}}}, // 3 not divisible by dim 2
				}},
			},
		},
	}}
	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: -1,
	}
	_, err := op.run(context.Background(), s.span, result)
	s.Error(err)
}

// sortGroupsByOrderByFields propagates a sort error during group ordering (group value present).
func (s *SearchPipelineSuite) TestSortGroupsByOrderByFieldsReturnsSortError() {
	result := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		GroupByFieldValues: []*schemapb.FieldData{{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{100, 100, 200, 200}}},
			}},
		}},
		FieldsData: []*schemapb.FieldData{{
			Type: schemapb.DataType_Int64, FieldName: "price",
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10}}}, // 1 value for 4 rows
			}},
		}},
	}}
	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: 1,
		groupSize:      2,
	}
	_, err := op.sortGroupsByOrderByFields(result, []int{0, 1, 2, 3})
	s.Error(err)
}

// sortGroupsByOrderByFields with no group value falls back to row sort and propagates its error.
func (s *SearchPipelineSuite) TestSortGroupsByOrderByFieldsNilGroupValueReturnsSortError() {
	result := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		FieldsData: []*schemapb.FieldData{{
			Type: schemapb.DataType_Int64, FieldName: "price",
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10}}},
			}},
		}},
	}}
	op := &orderByOperator{
		orderByFields:  []OrderByField{{FieldName: "price", Ascending: true}},
		groupByFieldId: 1,
		groupSize:      2,
	}
	_, err := op.sortGroupsByOrderByFields(result, []int{0, 1, 2})
	s.Error(err)
}

// Test orderByOperator with descending sort
func (s *SearchPipelineSuite) TestOrderByOperatorDescending() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 40, 20}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: false},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After descending sort by price: 40, 30, 20, 10
	expectedIds := []int64{3, 1, 4, 2}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

func (s *SearchPipelineSuite) TestOrderByOperatorNullableScalarNullOrdering() {
	makeResult := func() *milvuspb.SearchResults {
		return &milvuspb.SearchResults{
			Results: &schemapb.SearchResultData{
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
					},
				},
				Scores: []float32{0.9, 0.8, 0.7, 0.6},
				Topks:  []int64{4},
				FieldsData: []*schemapb.FieldData{
					{
						Type:      schemapb.DataType_Int64,
						FieldName: "price",
						ValidData: []bool{true, false, true, false},
						Field: &schemapb.FieldData_Scalars{
							Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_LongData{
									LongData: &schemapb.LongArray{Data: []int64{30, 10, 40, 20}},
								},
							},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name          string
		orderBy       OrderByField
		expectedIDs   []int64
		expectedValid []bool
	}{
		{
			name:          "asc_default_nulls_last",
			orderBy:       OrderByField{FieldName: "price", Ascending: true, NullsFirst: false},
			expectedIDs:   []int64{1, 3, 2, 4},
			expectedValid: []bool{true, true, false, false},
		},
		{
			name:          "desc_default_nulls_first",
			orderBy:       OrderByField{FieldName: "price", Ascending: false, NullsFirst: true},
			expectedIDs:   []int64{2, 4, 3, 1},
			expectedValid: []bool{false, false, true, true},
		},
		{
			name:          "asc_explicit_nulls_first",
			orderBy:       OrderByField{FieldName: "price", Ascending: true, NullsFirst: true},
			expectedIDs:   []int64{2, 4, 1, 3},
			expectedValid: []bool{false, false, true, true},
		},
		{
			name:          "desc_explicit_nulls_last",
			orderBy:       OrderByField{FieldName: "price", Ascending: false, NullsFirst: false},
			expectedIDs:   []int64{3, 1, 2, 4},
			expectedValid: []bool{true, true, false, false},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			op := &orderByOperator{
				orderByFields:  []OrderByField{tt.orderBy},
				groupByFieldId: -1,
			}

			outputs, err := op.run(context.Background(), s.span, makeResult())
			s.NoError(err)
			sortedResult := outputs[0].(*milvuspb.SearchResults)

			s.Equal(tt.expectedIDs, sortedResult.Results.Ids.GetIntId().Data)
			s.Equal(tt.expectedValid, sortedResult.Results.FieldsData[0].GetValidData())
		})
	}
}

// Test orderByOperator validates missing fields
func (s *SearchPipelineSuite) TestOrderByOperatorMissingField() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2}},
				},
			},
			Scores:     []float32{0.9, 0.8},
			FieldsData: []*schemapb.FieldData{},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "nonexistent", Ascending: true},
		},
		groupByFieldId: -1,
	}

	_, err := op.run(context.Background(), s.span, result)
	s.Error(err)
	s.Contains(err.Error(), "not found")
}

// Test orderByOperator with empty results
func (s *SearchPipelineSuite) TestOrderByOperatorEmptyResults() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids:        nil,
			Scores:     []float32{},
			FieldsData: []*schemapb.FieldData{},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	s.NotNil(outputs)
}

// Test orderByOperator with no order_by fields (passthrough)
func (s *SearchPipelineSuite) TestOrderByOperatorNoOrderBy() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
		},
	}

	op := &orderByOperator{
		orderByFields:  []OrderByField{},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Should be unchanged
	s.Equal([]int64{1, 2, 3}, sortedResult.Results.Ids.GetIntId().Data)
}

// Test orderByOperator with group_by - critical test for sortGroupsByOrderByFields
func (s *SearchPipelineSuite) TestOrderByOperatorWithGroupBy() {
	// Create result with GroupByFieldValue where groups have varying sizes
	// Group 1: ids [1,2] with group value "A", prices [30, 25]
	// Group 2: ids [3] with group value "B", price [10]
	// Group 3: ids [4,5,6] with group value "C", prices [20, 15, 18]
	// After order_by price:asc (by first row in each group), groups should be ordered: B(10), C(20), A(30)
	// Final order: [3, 4, 5, 6, 1, 2]
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5, 6}},
				},
			},
			Scores: []float32{0.9, 0.85, 0.8, 0.75, 0.7, 0.65},
			Topks:  []int64{6},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 25, 10, 20, 15, 18}},
							},
						},
					},
				},
			},
			// Group-by column via the plural channel — orderBy reads plural only
			// after the Step 3.3c.3 redo; task-output boundary handles legacy
			// wire downgrade.
			// "A", "A", "B", "C", "C", "C"
			GroupByFieldValues: []*schemapb.FieldData{{
				Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"A", "A", "B", "C", "C", "C"}},
						},
					},
				},
			}},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: 100, // Any value >= 0 indicates group_by mode
		groupSize:      3,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After sorting by first row's price in each group:
	// Group B (price=10) -> Group C (price=20) -> Group A (price=30)
	// Final order: [3, 4, 5, 6, 1, 2]
	expectedIds := []int64{3, 4, 5, 6, 1, 2}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	// Verify group by values are also reordered correctly (plural channel).
	expectedGroupValues := []string{"B", "C", "C", "C", "A", "A"}
	s.Require().Len(sortedResult.Results.GetGroupByFieldValues(), 1)
	actualGroupValues := sortedResult.Results.GetGroupByFieldValues()[0].GetScalars().GetStringData().Data
	s.Equal(expectedGroupValues, actualGroupValues)
}

// Test orderByOperator with multiple sort fields (tie-breaking)
func (s *SearchPipelineSuite) TestOrderByOperatorMultiField() {
	// Test tie-breaking: sort by category:asc, then by price:desc
	// Same category should be ordered by price descending
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6, 0.5},
			Topks:  []int64{5},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_VarChar,
					FieldName: "category",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{"B", "A", "B", "A", "A"}},
							},
						},
					},
				},
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{100, 200, 150, 50, 300}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "category", Ascending: true}, // First sort by category asc
			{FieldName: "price", Ascending: false},   // Then by price desc
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Category A: ids 2(200), 4(50), 5(300) -> sorted by price desc: 5(300), 2(200), 4(50)
	// Category B: ids 1(100), 3(150) -> sorted by price desc: 3(150), 1(100)
	// Final order: A first (5, 2, 4), then B (3, 1)
	expectedIds := []int64{5, 2, 4, 3, 1}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	// Verify categories are in order
	expectedCategories := []string{"A", "A", "A", "B", "B"}
	actualCategories := sortedResult.Results.FieldsData[0].GetScalars().GetStringData().Data
	s.Equal(expectedCategories, actualCategories)

	// Verify prices within each category are descending
	expectedPrices := []int64{300, 200, 50, 150, 100}
	actualPrices := sortedResult.Results.FieldsData[1].GetScalars().GetLongData().Data
	s.Equal(expectedPrices, actualPrices)
}

// Test orderByOperator with string IDs
func (s *SearchPipelineSuite) TestOrderByOperatorStringIds() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{Data: []string{"id_a", "id_b", "id_c", "id_d"}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "priority",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{3, 1, 4, 2}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "priority", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After sorting by priority asc: 1, 2, 3, 4
	// Original: id_a(3), id_b(1), id_c(4), id_d(2)
	// Sorted: id_b(1), id_d(2), id_a(3), id_c(4)
	expectedIds := []string{"id_b", "id_d", "id_a", "id_c"}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetStrId().Data)

	// Verify scores are also reordered
	expectedScores := []float32{0.8, 0.6, 0.9, 0.7}
	s.Equal(expectedScores, sortedResult.Results.Scores)
}

// Test JSON field comparison (byte-level ordering)
func (s *SearchPipelineSuite) TestCompareFieldDataAtJSON() {
	jsonField := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "metadata",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{Data: [][]byte{
						[]byte(`{"a": 2}`),
						[]byte(`{"a": 10}`),
						[]byte(`{"b": 1}`),
					}},
				},
			},
		},
	}

	// JSON comparison is byte-level, so "2" > "1" (comparing '2' vs '1' in "10")
	// This tests the documented behavior that JSON sorting is lexicographic
	cmp, err := compareFieldDataAt(jsonField, 0, 1, true)
	s.NoError(err)
	s.Greater(cmp, 0) // {"a": 2} > {"a": 10} in byte comparison because '2' > '1'

	// Different keys
	cmp, err = compareFieldDataAt(jsonField, 0, 2, true)
	s.NoError(err)
	s.Less(cmp, 0) // {"a": ...} < {"b": ...} because 'a' < 'b'
}

// Test vector field reordering (FloatVector)
func (s *SearchPipelineSuite) TestReorderFieldDataFloatVector() {
	dim := 4
	// 3 vectors, each with dim=4
	vectorField := &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: "embedding",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: []float32{
							1.0, 1.1, 1.2, 1.3, // vector 0
							2.0, 2.1, 2.2, 2.3, // vector 1
							3.0, 3.1, 3.2, 3.3, // vector 2
						},
					},
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(vectorField, indices)
	s.NoError(err)

	expectedData := []float32{
		3.0, 3.1, 3.2, 3.3, // was vector 2
		1.0, 1.1, 1.2, 1.3, // was vector 0
		2.0, 2.1, 2.2, 2.3, // was vector 1
	}
	actualData := vectorField.GetVectors().GetFloatVector().GetData()
	s.Equal(expectedData, actualData)
}

// Test Double comparison in compareFieldDataAt
func (s *SearchPipelineSuite) TestCompareFieldDataAtDouble() {
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j, true)
		s.NoError(err)
		return cmp
	}

	doubleField := &schemapb.FieldData{
		Type:      schemapb.DataType_Double,
		FieldName: "score",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: []float64{1.5, 2.5, 1.5}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(doubleField, 0, 1)) // 1.5 < 2.5
	s.Equal(1, mustCompare(doubleField, 1, 0))  // 2.5 > 1.5
	s.Equal(0, mustCompare(doubleField, 0, 2))  // 1.5 == 1.5
}

// Test Int32 comparison in compareFieldDataAt
func (s *SearchPipelineSuite) TestCompareFieldDataAtInt32() {
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j, true)
		s.NoError(err)
		return cmp
	}

	int32Field := &schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: "count",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{Data: []int32{10, 20, 10}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(int32Field, 0, 1)) // 10 < 20
	s.Equal(1, mustCompare(int32Field, 1, 0))  // 20 > 10
	s.Equal(0, mustCompare(int32Field, 0, 2))  // 10 == 10
}

// Test extractJSONValue function
func (s *SearchPipelineSuite) TestExtractJSONValue() {
	jsonData := []byte(`{"price": 99.99, "name": "product", "active": true, "count": 42}`)

	// Extract number
	result := extractJSONValue(jsonData, "/price")
	s.True(result.Exists())
	s.Equal(99.99, result.Float())

	// Extract string
	result = extractJSONValue(jsonData, "/name")
	s.True(result.Exists())
	s.Equal("product", result.String())

	// Extract boolean
	result = extractJSONValue(jsonData, "/active")
	s.True(result.Exists())
	s.True(result.Bool())

	// Extract integer
	result = extractJSONValue(jsonData, "/count")
	s.True(result.Exists())
	s.Equal(int64(42), result.Int())

	// Non-existent path
	result = extractJSONValue(jsonData, "/nonexistent")
	s.False(result.Exists())

	// Empty path returns empty result
	result = extractJSONValue(jsonData, "")
	s.False(result.Exists())

	// Nested path
	nestedJSON := []byte(`{"user": {"profile": {"age": 30}}}`)
	result = extractJSONValue(nestedJSON, "/user/profile/age")
	s.True(result.Exists())
	s.Equal(int64(30), result.Int())

	// Key with literal slash (escaped as ~1 in JSON Pointer)
	jsonWithSlash := []byte(`{"key/with/slash": 100}`)
	result = extractJSONValue(jsonWithSlash, "/key~1with~1slash")
	s.True(result.Exists())
	s.Equal(int64(100), result.Int())

	// Key with literal tilde (escaped as ~0 in JSON Pointer)
	jsonWithTilde := []byte(`{"key~tilde": 200}`)
	result = extractJSONValue(jsonWithTilde, "/key~0tilde")
	s.True(result.Exists())
	s.Equal(int64(200), result.Int())

	// Key with dot (should be escaped for gjson)
	jsonWithDot := []byte(`{"key.with.dot": 300}`)
	result = extractJSONValue(jsonWithDot, "/key.with.dot")
	s.True(result.Exists())
	s.Equal(int64(300), result.Int())
}

// Test compareJSONValues function
func (s *SearchPipelineSuite) TestCompareJSONValues() {
	// Number comparison
	a := extractJSONValue([]byte(`{"v": 10}`), "/v")
	b := extractJSONValue([]byte(`{"v": 20}`), "/v")
	s.Equal(-1, compareJSONValues(a, b, true)) // 10 < 20
	s.Equal(1, compareJSONValues(b, a, true))  // 20 > 10
	s.Equal(0, compareJSONValues(a, a, true))  // 10 == 10

	// String comparison
	a = extractJSONValue([]byte(`{"v": "apple"}`), "/v")
	b = extractJSONValue([]byte(`{"v": "banana"}`), "/v")
	s.Equal(-1, compareJSONValues(a, b, true)) // "apple" < "banana"
	s.Equal(1, compareJSONValues(b, a, true))  // "banana" > "apple"

	// Boolean comparison (false < true)
	a = extractJSONValue([]byte(`{"v": false}`), "/v")
	b = extractJSONValue([]byte(`{"v": true}`), "/v")
	s.Equal(-1, compareJSONValues(a, b, true)) // false < true
	s.Equal(1, compareJSONValues(b, a, true))  // true > false

	// Non-existent values
	a = extractJSONValue([]byte(`{}`), "/v")
	b = extractJSONValue([]byte(`{"v": 10}`), "/v")
	s.Equal(-1, compareJSONValues(a, b, true))  // NULLS FIRST: null < 10
	s.Equal(1, compareJSONValues(b, a, true))   // NULLS FIRST: 10 > null
	s.Equal(1, compareJSONValues(a, b, false))  // NULLS LAST: null > 10
	s.Equal(-1, compareJSONValues(b, a, false)) // NULLS LAST: 10 < null

	// Both non-existent
	a = extractJSONValue([]byte(`{}`), "/v")
	b = extractJSONValue([]byte(`{}`), "/v")
	s.Equal(0, compareJSONValues(a, b, true)) // null == null

	// Explicit JSON null value (type gjson.Null)
	a = extractJSONValue([]byte(`{"v": null}`), "/v")
	b = extractJSONValue([]byte(`{"v": 10}`), "/v")
	s.Equal(-1, compareJSONValues(a, b, true))  // NULLS FIRST: null < 10
	s.Equal(1, compareJSONValues(b, a, true))   // NULLS FIRST: 10 > null
	s.Equal(1, compareJSONValues(a, b, false))  // NULLS LAST: null > 10
	s.Equal(-1, compareJSONValues(b, a, false)) // NULLS LAST: 10 < null

	// Both explicit null
	a = extractJSONValue([]byte(`{"v": null}`), "/v")
	b = extractJSONValue([]byte(`{"v": null}`), "/v")
	s.Equal(0, compareJSONValues(a, b, true)) // null == null

	// Explicit null vs non-existent (both treated as null)
	a = extractJSONValue([]byte(`{"v": null}`), "/v")
	b = extractJSONValue([]byte(`{}`), "/v")
	s.Equal(0, compareJSONValues(a, b, true)) // null == null
}

// Test orderByOperator with JSON subfield path
func (s *SearchPipelineSuite) TestOrderByOperatorWithJSONPath() {
	// Create result with JSON field containing nested data
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_JSON,
					FieldName: "metadata",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{Data: [][]byte{
									[]byte(`{"price": 30, "category": "A"}`),
									[]byte(`{"price": 10, "category": "B"}`),
									[]byte(`{"price": 40, "category": "A"}`),
									[]byte(`{"price": 20, "category": "B"}`),
								}},
							},
						},
					},
				},
			},
		},
	}

	// Sort by metadata["price"] ascending
	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "metadata", FieldID: 100, JSONPath: "/price", Ascending: true, NullsFirst: false},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort by price: 10, 20, 30, 40
	// Original order: 30(id=1), 10(id=2), 40(id=3), 20(id=4)
	// Sorted order: 10(id=2), 20(id=4), 30(id=1), 40(id=3)
	expectedIds := []int64{2, 4, 1, 3}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test orderByOperator with JSON path - descending order
func (s *SearchPipelineSuite) TestOrderByOperatorWithJSONPathDescending() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
			Topks:  []int64{3},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_JSON,
					FieldName: "data",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{Data: [][]byte{
									[]byte(`{"score": 100}`),
									[]byte(`{"score": 300}`),
									[]byte(`{"score": 200}`),
								}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "data", FieldID: 100, JSONPath: "/score", Ascending: false, NullsFirst: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After descending sort: 300, 200, 100
	// Original: 100(id=1), 300(id=2), 200(id=3)
	// Sorted: 300(id=2), 200(id=3), 100(id=1)
	expectedIds := []int64{2, 3, 1}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

func (s *SearchPipelineSuite) TestOrderByOperatorWithMissingJSONPath() {
	makeResult := func() *milvuspb.SearchResults {
		return &milvuspb.SearchResults{
			Results: &schemapb.SearchResultData{
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
					},
				},
				Scores: []float32{0.9, 0.8, 0.7, 0.6, 0.5},
				Topks:  []int64{5},
				FieldsData: []*schemapb.FieldData{
					{
						Type:      schemapb.DataType_JSON,
						FieldName: "metadata",
						Field: &schemapb.FieldData_Scalars{
							Scalars: &schemapb.ScalarField{
								Data: &schemapb.ScalarField_JsonData{
									JsonData: &schemapb.JSONArray{Data: [][]byte{
										[]byte(`{"price": 30}`),
										[]byte(`{}`),
										[]byte(`{"price": 10}`),
										[]byte(`{"other": 99}`),
										[]byte(`{"price": null}`),
									}},
								},
							},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name        string
		orderBy     OrderByField
		expectedIDs []int64
	}{
		{
			name:        "asc_default_nulls_last",
			orderBy:     OrderByField{FieldName: "metadata", FieldID: 100, JSONPath: "/price", Ascending: true, NullsFirst: false},
			expectedIDs: []int64{3, 1, 2, 4, 5},
		},
		{
			name:        "desc_default_nulls_first",
			orderBy:     OrderByField{FieldName: "metadata", FieldID: 100, JSONPath: "/price", Ascending: false, NullsFirst: true},
			expectedIDs: []int64{2, 4, 5, 1, 3},
		},
		{
			name:        "desc_explicit_nulls_last",
			orderBy:     OrderByField{FieldName: "metadata", FieldID: 100, JSONPath: "/price", Ascending: false, NullsFirst: false},
			expectedIDs: []int64{1, 3, 2, 4, 5},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			op := &orderByOperator{
				orderByFields:  []OrderByField{tt.orderBy},
				groupByFieldId: -1,
			}

			outputs, err := op.run(context.Background(), s.span, makeResult())
			s.NoError(err)
			sortedResult := outputs[0].(*milvuspb.SearchResults)

			s.Equal(tt.expectedIDs, sortedResult.Results.Ids.GetIntId().Data)
		})
	}
}

func (s *SearchPipelineSuite) TestOrderByOperatorDynamicJSONNullOrdering() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_JSON,
					FieldName: "$meta",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_JsonData{
								JsonData: &schemapb.JSONArray{Data: [][]byte{
									[]byte(`{"dyn_price": 30}`),
									[]byte(`{}`),
									[]byte(`{"dyn_price": 10}`),
									[]byte(`{"dyn_price": null}`),
								}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "$meta", FieldID: 100, JSONPath: "/dyn_price", Ascending: false, NullsFirst: true, IsDynamicField: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	s.Equal([]int64{2, 4, 1, 3}, sortedResult.Results.Ids.GetIntId().Data)
}

func (s *SearchPipelineSuite) TestParseOrderByFieldsWithJSONPath() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "metadata", DataType: schemapb.DataType_JSON},
			{FieldID: 102, Name: "score", DataType: schemapb.DataType_Float},
		},
	}

	// Test JSON path with ascending
	params := []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `metadata["price"]:asc`}}
	result, err := parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("metadata", result[0].FieldName)
	s.Equal(int64(101), result[0].FieldID)
	s.Equal("/price", result[0].JSONPath)
	s.True(result[0].Ascending)
	s.False(result[0].NullsFirst)
	s.Equal("metadata", result[0].OutputFieldName) // Regular JSON: request whole field
	s.False(result[0].IsDynamicField)              // Not a dynamic field

	// Test JSON path with descending
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `metadata["rating"]:desc:nulls_last`}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("metadata", result[0].FieldName)
	s.Equal("/rating", result[0].JSONPath)
	s.False(result[0].Ascending)
	s.False(result[0].NullsFirst)
	s.Equal("metadata", result[0].OutputFieldName) // Regular JSON: request whole field
	s.False(result[0].IsDynamicField)

	// Test mixed: regular field + JSON path
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `score:desc,metadata["price"]:asc`}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 2)
	// First field: regular
	s.Equal("score", result[0].FieldName)
	s.Equal("", result[0].JSONPath)
	s.False(result[0].Ascending)
	s.True(result[0].NullsFirst)
	// Second field: JSON path
	s.Equal("metadata", result[1].FieldName)
	s.Equal("/price", result[1].JSONPath)
	s.True(result[1].Ascending)
	s.False(result[1].NullsFirst)

	// Test nested JSON path
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `metadata["user"]["age"]:asc`}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("metadata", result[0].FieldName)
	s.Equal("/user/age", result[0].JSONPath)
}

// Test parseOrderByFields with dynamic fields
func (s *SearchPipelineSuite) TestParseOrderByFieldsWithDynamicField() {
	schema := &schemapb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}

	// Test dynamic field key without brackets (age -> $meta["age"])
	params := []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "age:asc"}}
	result, err := parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("$meta", result[0].FieldName)
	s.Equal(int64(102), result[0].FieldID)
	s.Equal("/age", result[0].JSONPath)
	s.True(result[0].Ascending)
	s.False(result[0].NullsFirst)
	s.Equal("age", result[0].OutputFieldName) // Dynamic field: use original key for requery
	s.True(result[0].IsDynamicField)          // Is a dynamic field - QueryNode extracts subfield

	// Test dynamic field with explicit path
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `$meta["category"]:desc:nulls_last`}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("$meta", result[0].FieldName)
	s.Equal("/category", result[0].JSONPath)
	s.False(result[0].Ascending)
	s.False(result[0].NullsFirst)
	s.Equal(`$meta["category"]`, result[0].OutputFieldName) // Explicit path for requery
	s.True(result[0].IsDynamicField)

	// Test unknown field with brackets treated as dynamic (e.g., dyn_meta["price"])
	// outputFieldName should be baseName ("dyn_meta"), NOT fieldSpec ("dyn_meta[\"price\"]"),
	// because the output field parser would reject multi-level dynamic paths like $meta["dyn_meta"]["price"].
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `dyn_meta["price"]:asc`}}
	result, err = parseOrderByFields(params, schema)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal("$meta", result[0].FieldName)
	s.Equal("/dyn_meta/price", result[0].JSONPath)
	s.True(result[0].Ascending)
	s.False(result[0].NullsFirst)
	s.Equal("dyn_meta", result[0].OutputFieldName) // Base name only; full path would cause multi-level rejection
	s.True(result[0].IsDynamicField)
}

// Test splitOrderByFieldOptions helper
func (s *SearchPipelineSuite) TestSplitOrderByFieldOptions() {
	// Simple field
	field, dir, nullOrdering, err := splitOrderByFieldOptions("name:asc")
	s.NoError(err)
	s.Equal("name", field)
	s.Equal("asc", dir)
	s.Equal("", nullOrdering)

	// Field without direction
	field, dir, nullOrdering, err = splitOrderByFieldOptions("name")
	s.NoError(err)
	s.Equal("name", field)
	s.Equal("", dir)
	s.Equal("", nullOrdering)

	// JSON path with direction
	field, dir, nullOrdering, err = splitOrderByFieldOptions(`metadata["price"]:desc`)
	s.NoError(err)
	s.Equal(`metadata["price"]`, field)
	s.Equal("desc", dir)
	s.Equal("", nullOrdering)

	// JSON path without direction
	field, dir, nullOrdering, err = splitOrderByFieldOptions(`metadata["price"]`)
	s.NoError(err)
	s.Equal(`metadata["price"]`, field)
	s.Equal("", dir)
	s.Equal("", nullOrdering)

	// Nested JSON path with direction
	field, dir, nullOrdering, err = splitOrderByFieldOptions(`data["user"]["age"]:asc`)
	s.NoError(err)
	s.Equal(`data["user"]["age"]`, field)
	s.Equal("asc", dir)
	s.Equal("", nullOrdering)

	// JSON path with colon in value and explicit null ordering
	field, dir, nullOrdering, err = splitOrderByFieldOptions(`metadata["key:with:colons"]:asc:nulls_last`)
	s.NoError(err)
	s.Equal(`metadata["key:with:colons"]`, field)
	s.Equal("asc", dir)
	s.Equal("nulls_last", nullOrdering)

	_, _, _, err = splitOrderByFieldOptions(`metadata["price"]:asc:nulls_last:extra`)
	s.Error(err)
	s.Contains(err.Error(), "too many order_by field options")
}

// Test jsonPointerToGjsonPath conversion
func (s *SearchPipelineSuite) TestJSONPointerToGjsonPath() {
	// Simple path
	s.Equal("price", jsonPointerToGjsonPath("/price"))

	// Nested path
	s.Equal("user.profile.age", jsonPointerToGjsonPath("/user/profile/age"))

	// Key with literal slash (escaped as ~1)
	s.Equal("key/with/slash", jsonPointerToGjsonPath("/key~1with~1slash"))

	// Key with literal tilde (escaped as ~0)
	s.Equal("key~tilde", jsonPointerToGjsonPath("/key~0tilde"))

	// Key with dot (escaped for gjson)
	s.Equal(`key\.with\.dot`, jsonPointerToGjsonPath("/key.with.dot"))

	// Combined: nested path with special characters
	s.Equal(`user.profile\.v2.score`, jsonPointerToGjsonPath("/user/profile.v2/score"))

	// Empty path
	s.Equal("", jsonPointerToGjsonPath(""))
	s.Equal("", jsonPointerToGjsonPath("/"))

	// Path without leading slash
	s.Equal("price", jsonPointerToGjsonPath("price"))
}

// Test parseOrderByFields error cases for better coverage
func (s *SearchPipelineSuite) TestParseOrderByFieldsErrors() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, Name: "score", DataType: schemapb.DataType_Float},
			{FieldID: 103, Name: "metadata", DataType: schemapb.DataType_JSON},
		},
	}

	// Test invalid direction
	params := []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "score:invalid"}}
	_, err := parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "invalid order direction")

	// Test non-JSON field with brackets
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `score["key"]:asc`}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "not a JSON type")

	// Test unknown field with brackets and no dynamic field
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: `unknown["key"]:asc`}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "not found")

	// Test unknown field without brackets and no dynamic field
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "unknown_field:asc"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "does not exist")

	// Test unsortable field type (vector)
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "vector:asc"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "unsortable type")

	// Test JSON field without path - not allowed, must use path syntax like metadata["key"]
	params = []*commonpb.KeyValuePair{{Key: OrderByFieldsKey, Value: "metadata:asc"}}
	_, err = parseOrderByFields(params, schema)
	s.Error(err)
	s.Contains(err.Error(), "unsortable type")
	s.Contains(err.Error(), "JSON")
}

// Test compareFieldDataAt for different data types
func (s *SearchPipelineSuite) TestCompareFieldDataAtAllTypes() {
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j, true)
		s.NoError(err)
		return cmp
	}

	// Test Int8/Int16/Int32 comparison
	intField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: "int_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{Data: []int32{30, 10, 20}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(intField, 1, 2)) // 10 < 20
	s.Equal(1, mustCompare(intField, 0, 1))  // 30 > 10
	s.Equal(0, mustCompare(intField, 0, 0))  // 30 == 30

	// Test Float comparison
	floatField := &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: "float_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: []float32{3.5, 1.5, 2.5}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(floatField, 1, 2)) // 1.5 < 2.5
	s.Equal(1, mustCompare(floatField, 0, 1))  // 3.5 > 1.5
	s.Equal(0, mustCompare(floatField, 0, 0))  // 3.5 == 3.5

	// Test Double comparison
	doubleField := &schemapb.FieldData{
		Type:      schemapb.DataType_Double,
		FieldName: "double_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: []float64{3.5, 1.5, 2.5}},
				},
			},
		},
	}
	s.Equal(-1, mustCompare(doubleField, 1, 2)) // 1.5 < 2.5
	s.Equal(1, mustCompare(doubleField, 0, 1))  // 3.5 > 1.5
	s.Equal(0, mustCompare(doubleField, 0, 0))  // 3.5 == 3.5

	// Test Bool comparison
	boolField := &schemapb.FieldData{
		Type:      schemapb.DataType_Bool,
		FieldName: "bool_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: []bool{true, false, true}},
				},
			},
		},
	}
	s.Equal(1, mustCompare(boolField, 0, 1))  // true > false
	s.Equal(-1, mustCompare(boolField, 1, 0)) // false < true
	s.Equal(0, mustCompare(boolField, 0, 2))  // true == true

	// Test out of bounds returns error
	_, err := compareFieldDataAt(intField, 10, 20, true)
	s.Error(err)
	_, err = compareFieldDataAt(floatField, 10, 20, true)
	s.Error(err)
	_, err = compareFieldDataAt(doubleField, 10, 20, true)
	s.Error(err)
	_, err = compareFieldDataAt(boolField, 10, 20, true)
	s.Error(err)
}

// Test compareFieldDataAt with nullable fields (ValidData)
func (s *SearchPipelineSuite) TestCompareFieldDataAtNullable() {
	mustCompare := func(field *schemapb.FieldData, i, j int) int {
		cmp, err := compareFieldDataAt(field, i, j, true)
		s.NoError(err)
		return cmp
	}

	// Create field with ValidData (nullable)
	nullableField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "nullable_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{100, 200, 300}},
				},
			},
		},
		ValidData: []bool{true, false, true}, // Index 1 is null
	}

	// NULLS FIRST
	s.Equal(1, mustCompare(nullableField, 0, 1))  // 100 > null
	s.Equal(-1, mustCompare(nullableField, 1, 0)) // null < 100
	s.Equal(-1, mustCompare(nullableField, 1, 2)) // null < 300

	cmp, err := compareFieldDataAt(nullableField, 0, 1, false)
	s.NoError(err)
	s.Equal(-1, cmp) // NULLS LAST: 100 < null
	cmp, err = compareFieldDataAt(nullableField, 1, 0, false)
	s.NoError(err)
	s.Equal(1, cmp) // NULLS LAST: null > 100

	// Create field where both are null
	nullableField2 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "nullable_field2",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{100, 200, 300}},
				},
			},
		},
		ValidData: []bool{false, false, true}, // Index 0 and 1 are null
	}
	s.Equal(0, mustCompare(nullableField2, 0, 1)) // null == null
}

// Test isSameGroupByValue for different types
func (s *SearchPipelineSuite) TestIsSameGroupByValueAllTypes() {
	// Test Int8/Int16/Int32
	intField := &schemapb.FieldData{
		Type: schemapb.DataType_Int32,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{Data: []int32{10, 10, 20}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(intField, 0, 1))  // 10 == 10
	s.False(isSameGroupByValue(intField, 0, 2)) // 10 != 20

	// Test Int64
	int64Field := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{100, 100, 200}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(int64Field, 0, 1))  // 100 == 100
	s.False(isSameGroupByValue(int64Field, 0, 2)) // 100 != 200

	// Test Bool
	boolField := &schemapb.FieldData{
		Type: schemapb.DataType_Bool,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: []bool{true, true, false}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(boolField, 0, 1))  // true == true
	s.False(isSameGroupByValue(boolField, 0, 2)) // true != false

	// Test VarChar (already covered but verify)
	stringField := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"A", "A", "B"}},
				},
			},
		},
	}
	s.True(isSameGroupByValue(stringField, 0, 1))  // "A" == "A"
	s.False(isSameGroupByValue(stringField, 0, 2)) // "A" != "B"

	// Test out of bounds returns false
	s.False(isSameGroupByValue(intField, 10, 20))
	s.False(isSameGroupByValue(int64Field, 10, 20))
	s.False(isSameGroupByValue(boolField, 10, 20))
	s.False(isSameGroupByValue(stringField, 10, 20))

	// Test unsupported type (Float) returns false
	floatField := &schemapb.FieldData{
		Type: schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: []float32{1.0, 1.0}},
				},
			},
		},
	}
	s.False(isSameGroupByValue(floatField, 0, 1)) // Float not supported
}

// Test compareJSONValues with boolean values
func (s *SearchPipelineSuite) TestCompareJSONValuesBool() {
	// Test bool: false < true
	a := extractJSONValue([]byte(`{"v": false}`), "/v")
	b := extractJSONValue([]byte(`{"v": true}`), "/v")
	s.Equal(-1, compareJSONValues(a, b, true)) // false < true
	s.Equal(1, compareJSONValues(b, a, true))  // true > false

	// Test both true
	a = extractJSONValue([]byte(`{"v": true}`), "/v")
	b = extractJSONValue([]byte(`{"v": true}`), "/v")
	s.Equal(0, compareJSONValues(a, b, true)) // true == true

	// Test both false
	a = extractJSONValue([]byte(`{"v": false}`), "/v")
	b = extractJSONValue([]byte(`{"v": false}`), "/v")
	s.Equal(0, compareJSONValues(a, b, true)) // false == false
}

// Test compareJSONValues with mixed types (fallback to raw comparison)
func (s *SearchPipelineSuite) TestCompareJSONValuesMixedTypes() {
	// Number vs String - falls back to raw comparison
	// Raw: "10" vs "\"hello\"" - quote char '"' (34) < '1' (49)
	a := extractJSONValue([]byte(`{"v": 10}`), "/v")
	b := extractJSONValue([]byte(`{"v": "hello"}`), "/v")
	cmp := compareJSONValues(a, b, true)
	// String (with quotes) should sort before number due to quote ASCII
	s.Equal(1, cmp) // "10" > "\"hello\"" because '1' > '"'
}

// Test orderByOperator with Float field type
func (s *SearchPipelineSuite) TestOrderByOperatorFloatField() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Float,
					FieldName: "rating",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_FloatData{
								FloatData: &schemapb.FloatArray{Data: []float32{3.5, 1.5, 4.5, 2.5}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "rating", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort: 1.5, 2.5, 3.5, 4.5
	// Original: 3.5(id=1), 1.5(id=2), 4.5(id=3), 2.5(id=4)
	// Sorted: 1.5(id=2), 2.5(id=4), 3.5(id=1), 4.5(id=3)
	expectedIds := []int64{2, 4, 1, 3}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test orderByOperator with Bool field type
func (s *SearchPipelineSuite) TestOrderByOperatorBoolField() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Bool,
					FieldName: "active",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_BoolData{
								BoolData: &schemapb.BoolArray{Data: []bool{true, false, true, false}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "active", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort: false, false, true, true
	// Original: true(id=1), false(id=2), true(id=3), false(id=4)
	// Sorted (stable): false(id=2), false(id=4), true(id=1), true(id=3)
	expectedIds := []int64{2, 4, 1, 3}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test orderByOperator group_by with Int64 group values
func (s *SearchPipelineSuite) TestOrderByOperatorWithGroupByInt64() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
				},
			},
			Scores: []float32{0.9, 0.85, 0.8, 0.75, 0.7},
			Topks:  []int64{5},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 25, 10, 20, 15}},
							},
						},
					},
				},
			},
			// Group-by column via the plural channel (Step 3.3c.3 redo).
			// Group 100: ids [1,2], prices [30,25]
			// Group 200: ids [3], price [10]
			// Group 300: ids [4,5], prices [20,15]
			GroupByFieldValues: []*schemapb.FieldData{{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{100, 100, 200, 300, 300}},
						},
					},
				},
			}},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: 100,
		groupSize:      2,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After sorting groups by first row's price:
	// Group 200 (price=10) -> Group 300 (price=20) -> Group 100 (price=30)
	// Final order: [3, 4, 5, 1, 2]
	expectedIds := []int64{3, 4, 5, 1, 2}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test sortGroupsByOrderByFields with no GroupByFieldValue (fallback to regular sort)
func (s *SearchPipelineSuite) TestOrderByOperatorWithGroupByNoGroupValue() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
			Topks:  []int64{3},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "price",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{30, 10, 20}},
							},
						},
					},
				},
			},
			// No GroupByFieldValue - should fallback to regular sort
			GroupByFieldValue: nil,
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "price", Ascending: true},
		},
		groupByFieldId: 100, // Says group_by but no GroupByFieldValue
		groupSize:      2,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// Should fallback to regular sort: 10, 20, 30
	expectedIds := []int64{2, 3, 1}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)
}

// Test reorderFieldData for Int8/Int16/Int32 scalar types
func (s *SearchPipelineSuite) TestReorderFieldDataInt32() {
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: "int_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{Data: []int32{10, 20, 30, 40}},
				},
			},
		},
	}

	// Reorder: [2, 0, 3, 1] means new[0]=old[2], new[1]=old[0], etc.
	indices := []int{2, 0, 3, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expected := []int32{30, 10, 40, 20}
	s.Equal(expected, field.GetScalars().GetIntData().Data)
}

// Test reorderFieldData for Array type
func (s *SearchPipelineSuite) TestReorderFieldDataArray() {
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "array_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}},
							{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{3, 4}}}},
							{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{5, 6}}}},
						},
					},
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	result := field.GetScalars().GetArrayData().Data
	s.Len(result, 3)
	s.Equal([]int32{5, 6}, result[0].GetIntData().Data)
	s.Equal([]int32{1, 2}, result[1].GetIntData().Data)
	s.Equal([]int32{3, 4}, result[2].GetIntData().Data)
}

// Test reorderFieldData for BinaryVector type
func (s *SearchPipelineSuite) TestReorderFieldDataBinaryVector() {
	// 3 vectors, dim=16 (2 bytes per vector)
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_BinaryVector,
		FieldName: "binary_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 16,
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06}, // 3 vectors * 2 bytes
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expected := []byte{0x05, 0x06, 0x01, 0x02, 0x03, 0x04}
	s.Equal(expected, field.GetVectors().GetBinaryVector())
}

// Test reorderFieldData for Float16Vector type
func (s *SearchPipelineSuite) TestReorderFieldDataFloat16Vector() {
	// 3 vectors, dim=2 (4 bytes per vector: 2 bytes per float16 * 2 dims)
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Float16Vector,
		FieldName: "fp16_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 2,
				Data: &schemapb.VectorField_Float16Vector{
					Float16Vector: []byte{
						0x01, 0x02, 0x03, 0x04, // vector 0
						0x05, 0x06, 0x07, 0x08, // vector 1
						0x09, 0x0A, 0x0B, 0x0C, // vector 2
					},
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expected := []byte{
		0x09, 0x0A, 0x0B, 0x0C, // was vector 2
		0x01, 0x02, 0x03, 0x04, // was vector 0
		0x05, 0x06, 0x07, 0x08, // was vector 1
	}
	s.Equal(expected, field.GetVectors().GetFloat16Vector())
}

// Test reorderFieldData for BFloat16Vector type
func (s *SearchPipelineSuite) TestReorderFieldDataBFloat16Vector() {
	// 3 vectors, dim=2 (4 bytes per vector: 2 bytes per bfloat16 * 2 dims)
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_BFloat16Vector,
		FieldName: "bf16_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 2,
				Data: &schemapb.VectorField_Bfloat16Vector{
					Bfloat16Vector: []byte{
						0x11, 0x12, 0x13, 0x14, // vector 0
						0x21, 0x22, 0x23, 0x24, // vector 1
						0x31, 0x32, 0x33, 0x34, // vector 2
					},
				},
			},
		},
	}

	// Reorder: [1, 2, 0]
	indices := []int{1, 2, 0}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expected := []byte{
		0x21, 0x22, 0x23, 0x24, // was vector 1
		0x31, 0x32, 0x33, 0x34, // was vector 2
		0x11, 0x12, 0x13, 0x14, // was vector 0
	}
	s.Equal(expected, field.GetVectors().GetBfloat16Vector())
}

// Test reorderFieldData for SparseFloatVector type
func (s *SearchPipelineSuite) TestReorderFieldDataSparseFloatVector() {
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_SparseFloatVector,
		FieldName: "sparse_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 100,
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Contents: [][]byte{
							{0x01, 0x02}, // sparse vector 0
							{0x03, 0x04}, // sparse vector 1
							{0x05, 0x06}, // sparse vector 2
						},
					},
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	result := field.GetVectors().GetSparseFloatVector().GetContents()
	s.Len(result, 3)
	s.Equal([]byte{0x05, 0x06}, result[0])
	s.Equal([]byte{0x01, 0x02}, result[1])
	s.Equal([]byte{0x03, 0x04}, result[2])
}

// Test reorderFieldData for Int8Vector type
func (s *SearchPipelineSuite) TestReorderFieldDataInt8Vector() {
	// 3 vectors, dim=4 (4 bytes per vector)
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Int8Vector,
		FieldName: "int8_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 4,
				Data: &schemapb.VectorField_Int8Vector{
					Int8Vector: []byte{
						0x01, 0x02, 0x03, 0x04, // vector 0
						0x05, 0x06, 0x07, 0x08, // vector 1
						0x09, 0x0A, 0x0B, 0x0C, // vector 2
					},
				},
			},
		},
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expected := []byte{
		0x09, 0x0A, 0x0B, 0x0C, // was vector 2
		0x01, 0x02, 0x03, 0x04, // was vector 0
		0x05, 0x06, 0x07, 0x08, // was vector 1
	}
	s.Equal(expected, field.GetVectors().GetInt8Vector())
}

func (s *SearchPipelineSuite) TestReorderFieldDataNullableVectorCompactData() {
	indices := []int{2, 0, 1}
	expectedValidData := []bool{true, true, false}

	tests := []struct {
		name   string
		field  *schemapb.FieldData
		assert func(*schemapb.FieldData)
	}{
		{
			name: "float_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "nullable_float_vec",
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 5, 6}}},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Equal([]float32{5, 6, 1, 2}, field.GetVectors().GetFloatVector().GetData())
			},
		},
		{
			name: "binary_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_BinaryVector,
				FieldName: "nullable_binary_vec",
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  16,
					Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0x01, 0x02, 0x05, 0x06}},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Equal([]byte{0x05, 0x06, 0x01, 0x02}, field.GetVectors().GetBinaryVector())
			},
		},
		{
			name: "float16_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_Float16Vector,
				FieldName: "nullable_float16_vec",
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{0x01, 0x02, 0x03, 0x04, 0x09, 0x0A, 0x0B, 0x0C}},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Equal([]byte{0x09, 0x0A, 0x0B, 0x0C, 0x01, 0x02, 0x03, 0x04}, field.GetVectors().GetFloat16Vector())
			},
		},
		{
			name: "bfloat16_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_BFloat16Vector,
				FieldName: "nullable_bfloat16_vec",
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{0x11, 0x12, 0x13, 0x14, 0x31, 0x32, 0x33, 0x34}},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Equal([]byte{0x31, 0x32, 0x33, 0x34, 0x11, 0x12, 0x13, 0x14}, field.GetVectors().GetBfloat16Vector())
			},
		},
		{
			name: "int8_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_Int8Vector,
				FieldName: "nullable_int8_vec",
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  4,
					Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{0x01, 0x02, 0x03, 0x04, 0x09, 0x0A, 0x0B, 0x0C}},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Equal([]byte{0x09, 0x0A, 0x0B, 0x0C, 0x01, 0x02, 0x03, 0x04}, field.GetVectors().GetInt8Vector())
			},
		},
		{
			name: "sparse_float_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_SparseFloatVector,
				FieldName: "nullable_sparse_vec",
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: 3,
					Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
						Dim:      3,
						Contents: [][]byte{{0x01}, {0x03}},
					}},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Equal([][]byte{{0x03}, {0x01}}, field.GetVectors().GetSparseFloatVector().GetContents())
			},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			err := reorderFieldData(tt.field, indices)
			s.NoError(err)
			s.Equal(expectedValidData, tt.field.GetValidData())
			tt.assert(tt.field)
		})
	}
}

func (s *SearchPipelineSuite) TestReorderFieldDataNullableVectorAllNullCompactData() {
	indices := []int{1, 0}
	expectedValidData := []bool{false, false}

	tests := []struct {
		name   string
		field  *schemapb.FieldData
		assert func(*schemapb.FieldData)
	}{
		{
			name: "float_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_FloatVector,
				FieldName: "nullable_float_vec",
				ValidData: []bool{false, false},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Empty(field.GetVectors().GetFloatVector().GetData())
			},
		},
		{
			name: "binary_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_BinaryVector,
				FieldName: "nullable_binary_vec",
				ValidData: []bool{false, false},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  16,
					Data: &schemapb.VectorField_BinaryVector{},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Empty(field.GetVectors().GetBinaryVector())
			},
		},
		{
			name: "float16_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_Float16Vector,
				FieldName: "nullable_float16_vec",
				ValidData: []bool{false, false},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_Float16Vector{},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Empty(field.GetVectors().GetFloat16Vector())
			},
		},
		{
			name: "bfloat16_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_BFloat16Vector,
				FieldName: "nullable_bfloat16_vec",
				ValidData: []bool{false, false},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  2,
					Data: &schemapb.VectorField_Bfloat16Vector{},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Empty(field.GetVectors().GetBfloat16Vector())
			},
		},
		{
			name: "int8_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_Int8Vector,
				FieldName: "nullable_int8_vec",
				ValidData: []bool{false, false},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  4,
					Data: &schemapb.VectorField_Int8Vector{},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Empty(field.GetVectors().GetInt8Vector())
			},
		},
		{
			name: "sparse_float_vector",
			field: &schemapb.FieldData{
				Type:      schemapb.DataType_SparseFloatVector,
				FieldName: "nullable_sparse_vec",
				ValidData: []bool{false, false},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  3,
					Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 3}},
				}},
			},
			assert: func(field *schemapb.FieldData) {
				s.Empty(field.GetVectors().GetSparseFloatVector().GetContents())
			},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			err := reorderFieldData(tt.field, indices)
			s.NoError(err)
			s.Equal(expectedValidData, tt.field.GetValidData())
			tt.assert(tt.field)
		})
	}
}

// Test reorderFieldData with ValidData (nullable fields)
func (s *SearchPipelineSuite) TestReorderFieldDataWithValidData() {
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "nullable_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
		},
		ValidData: []bool{true, false, true}, // index 1 is null
	}

	// Reorder: [2, 0, 1]
	indices := []int{2, 0, 1}
	err := reorderFieldData(field, indices)
	s.NoError(err)

	expectedData := []int64{30, 10, 20}
	expectedValid := []bool{true, true, false} // null moves to index 2
	s.Equal(expectedData, field.GetScalars().GetLongData().Data)
	s.Equal(expectedValid, field.ValidData)
}

// Test orderByOperator with Int32 field to cover reorderFieldData Int32 branch
func (s *SearchPipelineSuite) TestOrderByOperatorInt32Field() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7, 0.6},
			Topks:  []int64{4},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int32,
					FieldName: "count",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_IntData{
								IntData: &schemapb.IntArray{Data: []int32{30, 10, 40, 20}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "count", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort: 10, 20, 30, 40
	expectedIds := []int64{2, 4, 1, 3}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedCounts := []int32{10, 20, 30, 40}
	s.Equal(expectedCounts, sortedResult.Results.FieldsData[0].GetScalars().GetIntData().Data)
}

// Test orderByOperator with Double field
func (s *SearchPipelineSuite) TestOrderByOperatorDoubleField() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
			Topks:  []int64{3},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Double,
					FieldName: "value",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_DoubleData{
								DoubleData: &schemapb.DoubleArray{Data: []float64{3.14, 1.41, 2.72}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "value", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort: 1.41, 2.72, 3.14
	expectedIds := []int64{2, 3, 1}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedValues := []float64{1.41, 2.72, 3.14}
	s.Equal(expectedValues, sortedResult.Results.FieldsData[0].GetScalars().GetDoubleData().Data)
}

// Test orderByOperator with VarChar field
func (s *SearchPipelineSuite) TestOrderByOperatorVarCharField() {
	result := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
			Scores: []float32{0.9, 0.8, 0.7},
			Topks:  []int64{3},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_VarChar,
					FieldName: "name",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{"charlie", "alice", "bob"}},
							},
						},
					},
				},
			},
		},
	}

	op := &orderByOperator{
		orderByFields: []OrderByField{
			{FieldName: "name", Ascending: true},
		},
		groupByFieldId: -1,
	}

	outputs, err := op.run(context.Background(), s.span, result)
	s.NoError(err)
	sortedResult := outputs[0].(*milvuspb.SearchResults)

	// After ascending sort: alice, bob, charlie
	expectedIds := []int64{2, 3, 1}
	s.Equal(expectedIds, sortedResult.Results.Ids.GetIntId().Data)

	expectedNames := []string{"alice", "bob", "charlie"}
	s.Equal(expectedNames, sortedResult.Results.FieldsData[0].GetScalars().GetStringData().Data)
}

// Test compareNulls helper function
func (s *SearchPipelineSuite) TestCompareNulls() {
	// Empty ValidData - should return (0, false)
	cmp, handled := compareNulls(nil, 0, 1, true)
	s.Equal(0, cmp)
	s.False(handled)

	cmp, handled = compareNulls([]bool{}, 0, 1, true)
	s.Equal(0, cmp)
	s.False(handled)

	// Both non-null - should return (0, false)
	validData := []bool{true, true, true}
	cmp, handled = compareNulls(validData, 0, 1, true)
	s.Equal(0, cmp)
	s.False(handled)

	// First is null, second is not - should return (-1, true) (nulls first)
	validData = []bool{false, true, true}
	cmp, handled = compareNulls(validData, 0, 1, true)
	s.Equal(-1, cmp)
	s.True(handled)

	// First is not null, second is null - should return (1, true)
	cmp, handled = compareNulls(validData, 1, 0, true)
	s.Equal(1, cmp)
	s.True(handled)

	// Nulls last reverses the null/non-null ordering.
	cmp, handled = compareNulls(validData, 0, 1, false)
	s.Equal(1, cmp)
	s.True(handled)
	cmp, handled = compareNulls(validData, 1, 0, false)
	s.Equal(-1, cmp)
	s.True(handled)

	// Both are null - should return (0, true)
	validData = []bool{false, false, true}
	cmp, handled = compareNulls(validData, 0, 1, true)
	s.Equal(0, cmp)
	s.True(handled)
}

// Test buildJSONValueCache and getCachedJSONValue
func (s *SearchPipelineSuite) TestBuildJSONValueCache() {
	jsonField := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "metadata",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: [][]byte{
							[]byte(`{"score": 100}`),
							[]byte(`{"score": 200}`),
							[]byte(`{"score": 150}`),
						},
					},
				},
			},
		},
	}

	fieldMap := map[string]*schemapb.FieldData{
		"metadata": jsonField,
	}
	orderByFields := []OrderByField{
		{FieldName: "metadata", JSONPath: "/score"},
	}

	cache := buildJSONValueCache(fieldMap, orderByFields, []int{0, 1, 2})

	// Verify cache contains extracted values
	val0 := cache.getCachedJSONValue("metadata", "/score", 0)
	s.True(val0.Exists())
	s.Equal(float64(100), val0.Float())

	val1 := cache.getCachedJSONValue("metadata", "/score", 1)
	s.True(val1.Exists())
	s.Equal(float64(200), val1.Float())

	val2 := cache.getCachedJSONValue("metadata", "/score", 2)
	s.True(val2.Exists())
	s.Equal(float64(150), val2.Float())

	// Non-existent field/path returns empty result
	valMissing := cache.getCachedJSONValue("nonexistent", "/score", 0)
	s.False(valMissing.Exists())

	valMissingPath := cache.getCachedJSONValue("metadata", "/nonexistent", 0)
	s.False(valMissingPath.Exists())

	// Out of bounds index returns empty result
	valOOB := cache.getCachedJSONValue("metadata", "/score", 100)
	s.False(valOOB.Exists())
}

// Test buildJSONValueCache skips non-JSON fields
func (s *SearchPipelineSuite) TestBuildJSONValueCacheSkipsNonJSON() {
	intField := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "age",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
		},
	}

	fieldMap := map[string]*schemapb.FieldData{
		"age": intField,
	}
	orderByFields := []OrderByField{
		{FieldName: "age", JSONPath: ""}, // No JSON path, should be skipped
	}

	cache := buildJSONValueCache(fieldMap, orderByFields, []int{0, 1, 2})
	s.Empty(cache) // No entries should be cached for non-JSON fields
}

// Test compareOrderByField with nullable JSON field
func (s *SearchPipelineSuite) TestCompareOrderByFieldNullableJSON() {
	jsonField := &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: "metadata",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: [][]byte{
							[]byte(`{"score": 100}`),
							[]byte(`{"score": 200}`),
							[]byte(`{"score": 150}`),
						},
					},
				},
			},
		},
		ValidData: []bool{true, false, true}, // Index 1 is null
	}

	fieldMap := map[string]*schemapb.FieldData{
		"metadata": jsonField,
	}
	orderBy := OrderByField{FieldName: "metadata", JSONPath: "/score", Ascending: true, NullsFirst: true}
	cache := buildJSONValueCache(fieldMap, []OrderByField{orderBy}, []int{0, 1, 2})

	// null vs non-null: null should come first
	cmp, err := compareOrderByField(jsonField, orderBy, 1, 0, cache)
	s.NoError(err)
	s.Equal(-1, cmp) // null < 100

	cmp, err = compareOrderByField(jsonField, orderBy, 0, 1, cache)
	s.NoError(err)
	s.Equal(1, cmp) // 100 > null

	// non-null vs non-null: normal comparison
	cmp, err = compareOrderByField(jsonField, orderBy, 0, 2, cache)
	s.NoError(err)
	s.Equal(-1, cmp) // 100 < 150

	cmp, err = compareOrderByField(jsonField, orderBy, 2, 0, cache)
	s.NoError(err)
	s.Equal(1, cmp) // 150 > 100
}

// Test reorderFieldData returns error for unhandled data type
func (s *SearchPipelineSuite) TestReorderFieldDataUnhandledType() {
	// Create a field with an unhandled type (None type is not handled)
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_None,
		FieldName: "unknown",
	}

	indices := []int{0, 1, 2}
	err := reorderFieldData(field, indices)
	s.Error(err)
	s.Contains(err.Error(), "unhandled data type")
}

// TestNewSearchPipelineWithOrderBy tests that newSearchPipeline correctly routes
// to searchWithOrderByPipe when orderByFields is set
func (s *SearchPipelineSuite) TestNewSearchPipelineWithOrderBy() {
	collectionName := "test_collection"

	// Mock requery operation
	f1 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "intField", 20)
	f1.FieldId = 101
	f2 := testutils.GenerateScalarFieldData(schemapb.DataType_Int64, "int64", 20)
	f2.FieldId = 100
	mocker := mockey.Mock((*requeryOperator).requery).Return(&milvuspb.QueryResults{
		FieldsData: []*schemapb.FieldData{f1, f2},
	}, segcore.StorageCost{ScannedRemoteBytes: 100, ScannedTotalBytes: 200}, nil).Build()
	defer mocker.UnPatch()

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: collectionName,
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
			MetricType:   "L2",
			Topk:         10,
			Nq:           2,
			PartitionIDs: []int64{1},
			CollectionID: 1,
			DbID:         1,
			IsAdvanced:   false, // Regular search, not hybrid
		},
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
				},
			},
			pkField: &schemapb.FieldSchema{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		queryInfos:             []*planpb.QueryInfo{{}},
		translatedOutputFields: []string{"intField"},
		request:                &milvuspb.SearchRequest{Namespace: nil},
		// Set orderByFields to trigger searchWithOrderByPipe
		orderByFields: []OrderByField{
			{FieldName: "intField", OutputFieldName: "intField", Ascending: true},
		},
	}

	// Use newSearchPipeline (the actual entry point)
	pipeline, err := newSearchPipeline(task)
	s.NoError(err)
	s.NotNil(pipeline)

	// Run the pipeline with test data
	sr := genTestSearchResultData(2, 10, schemapb.DataType_Int64, "intField", 101, false)
	results, _, err := pipeline.Run(context.Background(), s.span, []*internalpb.SearchResults{sr}, segcore.StorageCost{ScannedRemoteBytes: 100, ScannedTotalBytes: 200})
	s.NoError(err)
	s.NotNil(results)
	s.NotNil(results.Results)
	s.Equal(int64(2), results.Results.NumQueries)
}

func (s *SearchPipelineSuite) TestNewRequeryOperator_WithHighlightDynamicFields() {
	// Test that highlight dynamic fields are added to requery output
	schema := &schemaInfo{
		CollectionSchema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			},
		},
		pkField: &schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}

	// Mock highlighter with dynamic field names
	mockHighlighter := &SemanticHighlighter{highlight: &highlight.SemanticHighlight{}}
	mockDynFields := mockey.Mock((*SemanticHighlighter).DynamicFieldNames).To(func(h *SemanticHighlighter) []string {
		return []string{"dyn_field1", "dyn_field2"}
	}).Build()
	defer mockDynFields.UnPatch()

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
		},
		request: &milvuspb.SearchRequest{
			CollectionName: "test_collection",
		},
		schema:                 schema,
		translatedOutputFields: []string{"title"},
		highlighter:            mockHighlighter,
		tr:                     timerecord.NewTimeRecorder("test"),
	}

	op, err := newRequeryOperator(task, nil)
	s.NoError(err)
	s.NotNil(op)

	reqOp, ok := op.(*requeryOperator)
	s.True(ok)

	// Verify that dynamic fields from highlighter are included in output fields
	s.Contains(reqOp.outputFieldNames, "title")
	s.Contains(reqOp.outputFieldNames, "dyn_field1")
	s.Contains(reqOp.outputFieldNames, "dyn_field2")
}

func (s *SearchPipelineSuite) TestNewRequeryOperatorIncludesOrderByOutputFieldNames() {
	schema := &schemaInfo{
		CollectionSchema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "metadata", DataType: schemapb.DataType_JSON},
				{FieldID: 103, Name: "price", DataType: schemapb.DataType_Int64},
			},
		},
		pkField: &schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}
	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest:  &internalpb.SearchRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Search}},
		request:        &milvuspb.SearchRequest{CollectionName: "test_collection"},
		schema:         schema,
		translatedOutputFields: []string{
			"title",
		},
		orderByFields: []OrderByField{
			{FieldName: "price", OutputFieldName: "price"},
			{FieldName: "metadata", JSONPath: "/score", OutputFieldName: "metadata"},
			{FieldName: "$meta", JSONPath: "/dynamic_price", OutputFieldName: "dynamic_price", IsDynamicField: true},
		},
		tr: timerecord.NewTimeRecorder("test"),
	}

	op, err := newRequeryOperator(task, nil)
	s.NoError(err)
	reqOp := op.(*requeryOperator)
	s.ElementsMatch([]string{"title", "price", "metadata", "dynamic_price"}, reqOp.outputFieldNames)
}

func (s *SearchPipelineSuite) TestNewRequeryOperator_WithoutHighlighter() {
	// Test that without highlighter, only translated output fields are included
	schema := &schemaInfo{
		CollectionSchema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			},
		},
		pkField: &schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
		},
		request: &milvuspb.SearchRequest{
			CollectionName: "test_collection",
		},
		schema:                 schema,
		translatedOutputFields: []string{"title"},
		highlighter:            nil, // No highlighter
		tr:                     timerecord.NewTimeRecorder("test"),
	}

	op, err := newRequeryOperator(task, nil)
	s.NoError(err)
	s.NotNil(op)

	reqOp, ok := op.(*requeryOperator)
	s.True(ok)

	// Verify only translated output fields are included
	s.Equal([]string{"title"}, reqOp.outputFieldNames)
}

func (s *SearchPipelineSuite) TestNewBuiltInPipelineWithAggCtx() {
	// searchWithAggPipe now chains searchReduceOp → aggregateOp, so the task
	// needs a schema with a PK so searchReduceOp's factory can initialize.
	pkField := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
	aggCtx, err := search_agg.NewContext(1, nil, nil, nil)
	s.Require().NoError(err)
	task := &searchTask{
		SearchRequest: &internalpb.SearchRequest{IsAdvanced: false},
		aggCtx:        aggCtx,
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{pkField}},
			pkField:          pkField,
		},
		queryInfos: []*planpb.QueryInfo{{}},
	}

	pipeline, err := newBuiltInPipeline(task)
	s.NoError(err)
	s.NotNil(pipeline)
	s.Equal(searchWithAggPipe.name, pipeline.name)
}

func (s *SearchPipelineSuite) TestNewSearchPipelineWithAggCtxSkipsEndNode() {
	pkField := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
	aggCtx, err := search_agg.NewContext(1, nil, nil, nil)
	s.Require().NoError(err)
	task := &searchTask{
		SearchRequest: &internalpb.SearchRequest{IsAdvanced: false},
		aggCtx:        aggCtx,
		schema: &schemaInfo{
			CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{pkField}},
			pkField:          pkField,
		},
		queryInfos:  []*planpb.QueryInfo{{}},
		highlighter: nil,
	}

	pipeline, err := newSearchPipeline(task)
	s.NoError(err)
	s.NotNil(pipeline)
	s.Equal(searchWithAggPipe.name, pipeline.name)
	s.Len(pipeline.nodes, len(searchWithAggPipe.nodes))
}

func (s *SearchPipelineSuite) TestAggregateOperatorRun() {
	aggCtx, err := search_agg.NewContext(1,
		[]search_agg.LevelContext{{
			OwnFieldIDs: []int64{101},
			Size:        100,
			Metrics: map[string]search_agg.MetricSpec{
				"sum_value": {Op: "sum", FieldID: 102, FieldType: schemapb.DataType_Int64},
			},
			TopHits: &search_agg.TopHitsConfig{Size: 100},
			Order:   []search_agg.OrderCriterion{{Key: "_key", Dir: "asc"}},
		}},
		nil,
		[]int64{102},
	)
	s.Require().NoError(err)
	op := &aggregateOperator{aggCtx: aggCtx}

	data := &schemapb.SearchResultData{
		NumQueries: 1,
		Topks:      []int64{3},
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{
			Data: []int64{1, 2, 3},
		}}},
		Scores: []float32{0.9, 0.8, 0.7},
		FieldsData: []*schemapb.FieldData{
			testutils.GenerateScalarFieldDataWithValue(schemapb.DataType_Int64, "value", 102, []int64{10, 20, 30}),
		},
		GroupByFieldValues: []*schemapb.FieldData{
			testutils.GenerateScalarFieldDataWithValue(schemapb.DataType_VarChar, "brand", 101, []string{"A", "A", "B"}),
		},
	}

	// aggregateOp consumes the single reduced *milvuspb.SearchResults produced
	// upstream by searchReduceOp.
	reduced := &milvuspb.SearchResults{Status: merr.Success(), Results: data}
	outputs, err := op.run(context.Background(), s.span, []*milvuspb.SearchResults{reduced})
	s.NoError(err)
	s.Len(outputs, 1)

	result := outputs[0].(*milvuspb.SearchResults)
	s.NotNil(result)
	s.NotNil(result.GetResults())
	s.Equal(int64(1), result.GetResults().GetNumQueries())
	s.Equal([]int64{2}, result.GetResults().GetAggTopks())
	s.Len(result.GetResults().GetAggBuckets(), 2)
}

func (s *SearchPipelineSuite) TestNewRequeryOperator_WithHighlighterNoDynamicFields() {
	// Test that highlighter with empty dynamic fields doesn't affect output
	schema := &schemaInfo{
		CollectionSchema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "title", DataType: schemapb.DataType_VarChar},
			},
		},
		pkField: &schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}

	// Mock highlighter with empty dynamic field names
	mockHighlighter := &SemanticHighlighter{highlight: &highlight.SemanticHighlight{}}
	mockDynFields := mockey.Mock((*SemanticHighlighter).DynamicFieldNames).To(func(h *SemanticHighlighter) []string {
		return []string{} // Empty dynamic fields
	}).Build()
	defer mockDynFields.UnPatch()

	task := &searchTask{
		ctx:            context.Background(),
		collectionName: "test_collection",
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				Timestamp: uint64(time.Now().UnixNano()),
			},
		},
		request: &milvuspb.SearchRequest{
			CollectionName: "test_collection",
		},
		schema:                 schema,
		translatedOutputFields: []string{"title"},
		highlighter:            mockHighlighter,
		tr:                     timerecord.NewTimeRecorder("test"),
	}

	op, err := newRequeryOperator(task, nil)
	s.NoError(err)
	s.NotNil(op)

	reqOp, ok := op.(*requeryOperator)
	s.True(ok)

	// Verify only translated output fields are included (no dynamic fields added)
	s.Equal([]string{"title"}, reqOp.outputFieldNames)
}

// TestPickFieldDataWithNullableSparseVector tests pickFieldData with nullable sparse vector fields.
// This test ensures that when reordering query results (which may return in different order than requested),
// nullable sparse vectors are correctly handled using FieldDataIdxComputer to map row indices to data indices.
func (s *SearchPipelineSuite) TestPickFieldDataWithNullableSparseVector() {
	// Scenario: Search returns IDs [3, 1, 2], but Query returns them in order [1, 2, 3]
	// with a nullable sparse vector where row 1 (middle) is null.
	// pickFieldData should correctly reorder the data to match search result order.

	// Search result IDs (the order we want)
	searchIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{3, 1, 2}, // Want this order in final result
			},
		},
	}

	// Query returns data in different order: [1, 2, 3]
	// pkOffset maps each PK to its position in the query result
	pkOffset := map[any]int{
		int64(1): 0, // PK 1 is at index 0 in query result
		int64(2): 1, // PK 2 is at index 1 in query result
		int64(3): 2, // PK 3 is at index 2 in query result
	}

	// Create sparse vector data for 3 rows, but row index 1 (PK=2) is null
	// ValidData: [true, false, true] means row 0 and 2 have data, row 1 is null
	// Contents only has 2 entries (for the non-null rows)
	sparseContent0, _ := testutils.GenerateSparseFloatVectorsData(1)
	sparseContent2, _ := testutils.GenerateSparseFloatVectorsData(1)

	queryFields := []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "pk",
			FieldId:   100,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2, 3}, // Query result order
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_SparseFloatVector,
			FieldName: "sparse_vec",
			FieldId:   101,
			ValidData: []bool{true, false, true}, // Row 1 is null
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 700,
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: &schemapb.SparseFloatArray{
							Dim:      700,
							Contents: [][]byte{sparseContent0[0], sparseContent2[0]}, // Only 2 entries for non-null rows
						},
					},
				},
			},
		},
	}

	// Call pickFieldData - this should NOT panic
	result, err := pickFieldData(searchIDs, pkOffset, queryFields, nil, 12345)
	s.NoError(err)
	s.NotNil(result)
	s.Len(result, 2)

	// Verify PK field is reordered correctly: [3, 1, 2]
	pkData := result[0].GetScalars().GetLongData().GetData()
	s.Equal([]int64{3, 1, 2}, pkData)

	// Verify sparse vector ValidData is reordered correctly
	// Original ValidData: [true, false, true] for rows [1, 2, 3]
	// After reorder to [3, 1, 2]: ValidData should be [true, true, false]
	sparseValidData := result[1].GetValidData()
	s.Equal([]bool{true, true, false}, sparseValidData)

	// Verify sparse vector Contents has correct number of entries (2 non-null values)
	sparseContents := result[1].GetVectors().GetSparseFloatVector().GetContents()
	s.Len(sparseContents, 2)
}

// TestPickFieldDataWithAllNullSparseVector tests pickFieldData when all sparse vector values are null.
func (s *SearchPipelineSuite) TestPickFieldDataWithAllNullSparseVector() {
	searchIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{2, 1},
			},
		},
	}

	pkOffset := map[any]int{
		int64(1): 0,
		int64(2): 1,
	}

	queryFields := []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "pk",
			FieldId:   100,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2},
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_SparseFloatVector,
			FieldName: "sparse_vec",
			FieldId:   101,
			ValidData: []bool{false, false}, // All null
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 700,
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: &schemapb.SparseFloatArray{
							Dim:      700,
							Contents: [][]byte{}, // Empty - all null
						},
					},
				},
			},
		},
	}

	result, err := pickFieldData(searchIDs, pkOffset, queryFields, nil, 12345)
	s.NoError(err)
	s.NotNil(result)

	// All should still be null after reorder
	sparseValidData := result[1].GetValidData()
	s.Equal([]bool{false, false}, sparseValidData)
	s.Len(result[1].GetVectors().GetSparseFloatVector().GetContents(), 0)
}

func (s *SearchPipelineSuite) TestPickFieldDataWithNullableSparseVectorMissingValidData() {
	searchIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{2, 1}},
		},
	}
	pkOffset := map[any]int{
		int64(1): 0,
		int64(2): 1,
	}
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "sparse_vec",
				DataType: schemapb.DataType_SparseFloatVector,
				Nullable: true,
			},
		},
	}
	queryFields := []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "pk",
			FieldId:   100,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2}},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_SparseFloatVector,
			FieldName: "sparse_vec",
			FieldId:   101,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: &schemapb.SparseFloatArray{},
					},
				},
			},
		},
	}

	result, err := pickFieldData(searchIDs, pkOffset, queryFields, schema, 12345)
	s.NoError(err)
	s.Equal([]int64{2, 1}, result[0].GetScalars().GetLongData().GetData())
	s.Equal([]bool{false, false}, result[1].GetValidData())
	s.Empty(result[1].GetVectors().GetSparseFloatVector().GetContents())
}
