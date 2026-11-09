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

package dql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/reduce"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRerankStreamingEligibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*SearchTask)
		want   bool
	}{
		{name: "L2 only", want: true},
		{name: "function score", want: true, change: func(task *SearchTask) {
			task.request.FunctionChains = nil
			task.request.FunctionScore = &schemapb.FunctionScore{}
			task.rerankMeta = &funcScoreRerankMeta{}
		}},
		{name: "unprepared", change: func(task *SearchTask) { task.rerankMeta = nil }},
		{name: "function score without Proxy metadata", change: func(task *SearchTask) {
			task.request.FunctionChains = nil
			task.request.FunctionScore = &schemapb.FunctionScore{}
			task.rerankMeta = nil
		}},
		{name: "iterator", change: func(task *SearchTask) { task.IsIterator = true }},
		{name: "order by", change: func(task *SearchTask) { task.orderByFields = []OrderByField{{FieldName: "rank"}} }},
		{name: "unknown classification", change: func(task *SearchTask) { task.SearchType = internalpb.SearchType(99) }},
		{name: "L0 only", want: true, change: func(task *SearchTask) {
			task.request.FunctionChains = []*schemapb.FunctionChain{l0FunctionChain(mapOp("$score", "expr", columnArg("rank")))}
			task.rerankMeta = nil
		}},
		{name: "L1 only", want: true, change: func(task *SearchTask) {
			task.request.FunctionChains = []*schemapb.FunctionChain{l1LimitFunctionChain(2)}
			task.rerankMeta = nil
		}},
		{name: "L0 with L1", want: true, change: func(task *SearchTask) {
			task.request.FunctionChains = []*schemapb.FunctionChain{l0FunctionChain(mapOp("$score", "expr", columnArg("rank"))), l1LimitFunctionChain(2)}
			task.rerankMeta = nil
		}},
		{name: "L0 with L2", want: true, change: func(task *SearchTask) {
			task.request.FunctionChains = append(task.request.FunctionChains, l0FunctionChain(mapOp("$score", "expr", columnArg("rank"))))
		}},
		{name: "L1 with L2", want: true, change: func(task *SearchTask) {
			task.request.FunctionChains = append(task.request.FunctionChains, l1LimitFunctionChain(2))
		}},
		{name: "L0 with L1 and L2", want: true, change: func(task *SearchTask) {
			task.request.FunctionChains = append(task.request.FunctionChains, l0FunctionChain(mapOp("$score", "expr", columnArg("rank"))), l1LimitFunctionChain(2))
		}},
		{name: "L0 with unprepared L2", change: func(task *SearchTask) {
			task.request.FunctionChains = append(task.request.FunctionChains, l0FunctionChain(mapOp("$score", "expr", columnArg("rank"))))
			task.rerankMeta = nil
		}},
		{name: "L1 iterator", change: func(task *SearchTask) {
			task.request.FunctionChains = []*schemapb.FunctionChain{l1LimitFunctionChain(2)}
			task.rerankMeta, task.IsIterator = nil, true
		}},
		{name: "unspecified stage", change: func(task *SearchTask) { task.request.FunctionChains = []*schemapb.FunctionChain{{}} }},
		{name: "hybrid function rerank", change: func(task *SearchTask) {
			task.IsAdvanced = true
			task.SubReqs = []*internalpb.SubSearchRequest{{SearchType: internalpb.SearchType_DEFAULT}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := &SearchTask{
				SearchRequest: &internalpb.SearchRequest{Nq: 1, Topk: 4},
				request:       &milvuspb.SearchRequest{FunctionChains: []*schemapb.FunctionChain{l2LimitFunctionChain(2)}},
				rerankMeta:    &functionChainRerankMeta{},
			}
			if tc.change != nil {
				tc.change(task)
			}
			require.Equal(t, tc.want, task.supportsStreamingReduce())
		})
	}
}

func TestRerankStreamingMatchesBatch(t *testing.T) {
	for _, tc := range []struct {
		name          string
		dynamic       bool
		nullable      bool
		mapScore      bool
		functionScore bool
		model         bool
		modelFailure  bool
		requery       bool
		emptyFirst    bool
		allEmpty      bool
		workerStages  []schemapb.FunctionChainStage
		workerOnly    bool
	}{
		{name: "L2 sort and limit"},
		{name: "L2 score mapping", mapScore: true},
		{name: "nullable input", nullable: true},
		{name: "dynamic input", dynamic: true, nullable: true},
		{name: "function score", functionScore: true},
		{name: "model", functionScore: true, model: true},
		{name: "model failure", functionScore: true, model: true, modelFailure: true},
		{name: "requery", requery: true},
		{name: "empty first vector", emptyFirst: true},
		{name: "all empty", allEmpty: true},
		{name: "L0 only", workerOnly: true, workerStages: []schemapb.FunctionChainStage{schemapb.FunctionChainStage_FunctionChainStageL0Rerank}},
		{name: "L1 only", workerOnly: true, workerStages: []schemapb.FunctionChainStage{schemapb.FunctionChainStage_FunctionChainStageL1Rerank}},
		{name: "L0 and L1", workerOnly: true, workerStages: []schemapb.FunctionChainStage{schemapb.FunctionChainStage_FunctionChainStageL0Rerank, schemapb.FunctionChainStage_FunctionChainStageL1Rerank}},
		{name: "L0 and L2", workerStages: []schemapb.FunctionChainStage{schemapb.FunctionChainStage_FunctionChainStageL0Rerank}},
		{name: "L1 and L2", workerStages: []schemapb.FunctionChainStage{schemapb.FunctionChainStage_FunctionChainStageL1Rerank}},
		{name: "L0 L1 and L2", workerStages: []schemapb.FunctionChainStage{schemapb.FunctionChainStage_FunctionChainStageL0Rerank, schemapb.FunctionChainStage_FunctionChainStageL1Rerank}},
	} {
		for _, metric := range []string{"IP", "L2"} {
			for _, offset := range []int64{0, 1} {
				for _, chunkBytes := range []int{1, 256 * 1024} {
					t.Run(fmt.Sprintf("%s/%s/offset_%d/bytes_%d", tc.name, metric, offset, chunkBytes), func(t *testing.T) {
						ctx := context.Background()
						legacy := &fakeLegacyQueryClient{}
						task := newQueryViewSearchTask(t, legacy, 4)
						task.ctx, task.IsIterator, task.Nq, task.Offset, task.MetricType = ctx, false, 2, offset, metric
						task.schema = mustNewSchemaInfo(&schemapb.CollectionSchema{EnableDynamicField: true, Fields: []*schemapb.FieldSchema{
							{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
							{FieldID: 101, Name: "rank", DataType: schemapb.DataType_Double, Nullable: true},
							{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
							{FieldID: 103, Name: "text", DataType: schemapb.DataType_VarChar},
						}})
						task.queryInfos = []*planpb.QueryInfo{{Topk: 4, RoundDecimal: -1}}
						task.userOutputFields, task.translatedOutputFields = []string{"rank"}, []string{"rank"}
						input := "rank"
						if tc.dynamic {
							input = `$meta["rank"]`
						}
						sortOp := &schemapb.FunctionChainOp{Op: "sort", Inputs: []string{input}, Params: map[string]*schemapb.FunctionParamValue{
							"desc": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}},
						}}
						if tc.dynamic {
							sortOp.Params[chaintypes.InputDataTypesParam] = chainDataTypesParam(schemapb.DataType_Double)
						}
						ops := []*schemapb.FunctionChainOp{sortOp}
						if tc.mapScore {
							ops = append(ops, mapOp("$score", "num_combine", columnArg("$score"), columnArg("rank")))
						}
						ops = append(ops, l2LimitFunctionChain(2).GetOps()...)
						task.request.FunctionChains = []*schemapb.FunctionChain{l2FunctionChain(ops...)}
						meta, err := newFunctionChainRerankMeta(task.request.FunctionChains, task.schema)
						require.NoError(t, err)
						task.rerankMeta = meta
						if tc.functionScore {
							function := &schemapb.FunctionSchema{Type: schemapb.FunctionType_Rerank, InputFieldNames: []string{"rank"}, Params: []*commonpb.KeyValuePair{
								{Key: "reranker", Value: "decay"},
								{Key: "origin", Value: "4"},
								{Key: "scale", Value: "10"},
								{Key: "offset", Value: "0"},
								{Key: "decay", Value: "0.5"},
								{Key: "function", Value: "gauss"},
							}}
							if tc.model {
								server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
									var request struct {
										Texts []string `json:"texts"`
									}
									if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
										t.Error(err)
										w.WriteHeader(http.StatusBadRequest)
										return
									}
									if tc.modelFailure {
										_, _ = w.Write([]byte(`invalid model response`))
										return
									}
									var scores []map[string]any
									for i, text := range request.Texts {
										value, err := strconv.Atoi(text)
										if err != nil {
											t.Error(err)
										}
										scores = append(scores, map[string]any{"index": i, "score": value})
									}
									if err := json.NewEncoder(w).Encode(scores); err != nil {
										t.Error(err)
									}
								}))
								t.Cleanup(server.Close)
								providers := &paramtable.Get().FunctionCfg.RerankModelProviders
								previous := providers.GetFunc
								providers.GetFunc = func() map[string]string { return map[string]string{"tei.enable": "true"} }
								t.Cleanup(func() { providers.GetFunc = previous })
								function.InputFieldNames = []string{"text"}
								function.Params = []*commonpb.KeyValuePair{
									{Key: "reranker", Value: "model"},
									{Key: "provider", Value: "tei"},
									{Key: "endpoint", Value: server.URL},
									{Key: "queries", Value: `["q0","q1"]`},
								}
							}
							task.request.FunctionChains = nil
							task.request.FunctionScore = &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{function}}
							task.rerankMeta = mustNewRerankMeta(t, task.schema.CollectionSchema, task.request.FunctionScore)
						}
						if tc.workerOnly {
							task.request.FunctionChains, task.rerankMeta = nil, nil
						}
						// Worker results below are already reranked. Native worker execution is
						// covered by TestWorkerRerankStreamingMatchesBatch in querynodev2/tasks.
						for _, stage := range tc.workerStages {
							task.request.FunctionChains = append(task.request.FunctionChains, &schemapb.FunctionChain{
								Stage: stage, Ops: []*schemapb.FunctionChainOp{mapOp("$score", "num_combine", columnArg("$score"), columnArg("rank"))},
							})
						}

						var batchShards []*internalpb.SearchResults
						var vchannels []searchreducestream.ReduceStream
						var leaves []*fakeLegacySearchStream
						info := reduce.NewReduceSearchResultInfo(2, 4).WithMetricType(metric)
						for channel := int64(0); channel < 2; channel++ {
							data := &schemapb.SearchResultData{NumQueries: 2, TopK: 4, Ids: &schemapb.IDs{}}
							ranks, roots, texts := &schemapb.DoubleArray{}, &schemapb.JSONArray{}, &schemapb.StringArray{}
							rankField := &schemapb.FieldData{FieldId: 101, Type: schemapb.DataType_Double, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: ranks}}}}
							data.FieldsData = []*schemapb.FieldData{
								rankField,
								{FieldId: 102, Type: schemapb.DataType_JSON, IsDynamic: true, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: roots}}}},
								{FieldId: 103, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: texts}}}},
							}
							for query := int64(0); query < 2; query++ {
								count := int64(2)
								if tc.allEmpty || (tc.emptyFirst && query == 0) {
									count = 0
								}
								data.Topks = append(data.Topks, count)
								for row := int64(0); row < count; row++ {
									rank := 1 + channel + row*2
									typeutil.AppendPKs(data.Ids, query*10+rank)
									score := 1 - float32(rank)/10
									if metric == "L2" {
										score -= 1
									}
									data.Scores = append(data.Scores, score)
									valid := !tc.nullable || rank != 3
									rankField.ValidData = append(rankField.ValidData, valid)
									ranks.Data = append(ranks.Data, float64(rank))
									root := []byte(fmt.Sprintf(`{"rank":%d}`, rank))
									if !valid {
										root = []byte(`{}`)
									}
									roots.Data = append(roots.Data, root)
									texts.Data = append(texts.Data, strconv.FormatInt(rank, 10))
								}
							}
							data.FieldsData = append(data.FieldsData, &schemapb.FieldData{
								FieldId: 100, Type: schemapb.DataType_Int64,
								Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
									Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data.GetIds().GetIntId().GetData()}},
								}},
							})
							var workers []searchreducestream.ReduceStream
							var batchWorkers []*internalpb.SearchResults
							// Two worker versions per vchannel exercise shard deduplication.
							for worker := 0; worker < 2; worker++ {
								result := &internalpb.SearchResults{Status: merr.Success(), MetricType: metric, NumQueries: 2, TopK: 4, ResultData: proto.Clone(data).(*schemapb.SearchResultData)}
								batchWorkers = append(batchWorkers, proto.Clone(result).(*internalpb.SearchResults))
								chunks, err := searchreducestream.SplitSearchResult(result, chunkBytes)
								require.NoError(t, err)
								leaf := &fakeLegacySearchStream{}
								for _, chunk := range chunks {
									leaf.recv = append(leaf.recv, fakeLegacySearchRecv{chunk: chunk})
								}
								leaves = append(leaves, leaf)
								workers = append(workers, leaf)
							}
							batch, err := segments.ReduceSearchOnQueryNode(ctx, batchWorkers, info)
							require.NoError(t, err)
							batchShards = append(batchShards, batch)
							stream, err := searchreducestream.NewShardReduceStream(task.SearchRequest, workers, chunkBytes)
							require.NoError(t, err)
							vchannels = append(vchannels, stream)
						}
						stream, err := searchreducestream.NewReduceStream(task.SearchRequest, vchannels, chunkBytes)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, stream.Close()) })
						legacy.searchResult = &queryclient.LegacySearchResult{Stream: stream}
						require.True(t, task.supportsStreamingReduce())
						require.NoError(t, task.executeByQueryView(ctx))
						require.True(t, legacy.lastSearchRequest.AllowStreaming)
						require.Empty(t, task.resultBuf.Collect())
						for _, leaf := range leaves {
							require.Equal(t, 1, leaf.closeCalls)
						}
						if !tc.allEmpty {
							wantCounts := []int64{4 - offset, 4 - offset}
							if tc.emptyFirst {
								wantCounts[0] = 0
							}
							require.Equal(t, wantCounts, task.reducedResult.GetResults().GetTopks(), "L2 limit must not truncate ANN candidates")
						}
						task.needRequery = tc.requery
						if tc.requery {
							fields := []*schemapb.FieldData{
								{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 11, 12, 13, 14}}}}}},
								{FieldId: 101, Type: schemapb.DataType_Double, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1, 2, 3, 4, 1, 2, 3, 4}}}}}},
							}
							mock := mockTest(t, (*requeryOperator).requery, &milvuspb.QueryResults{FieldsData: fields}, segcore.StorageCost{}, nil)
							t.Cleanup(func() { mock.UnPatch() })
						}
						pipeline, err := newSearchPipeline(task)
						require.NoError(t, err)
						expected, _, batchErr := pipeline.Run(ctx, trace.SpanFromContext(ctx), batchShards, segcore.StorageCost{})
						streamErr := task.PostExecute(ctx)
						if tc.modelFailure {
							require.ErrorContains(t, batchErr, "call service failed")
							require.ErrorContains(t, streamErr, "call service failed")
							require.Equal(t, merr.Code(batchErr), merr.Code(streamErr))
							return
						}
						require.NoError(t, batchErr)
						require.NoError(t, streamErr)
						expected.Results.PrimaryFieldName, expected.Results.OutputFields = "pk", task.userOutputFields
						require.True(t, proto.Equal(expected.GetResults(), task.result.GetResults()), "Batch=%s\nStreaming=%s", expected.GetResults(), task.result.GetResults())
						if !tc.functionScore && !tc.allEmpty && !tc.workerOnly {
							winner := int64(3)
							if tc.nullable {
								winner = 2
							}
							want := []int64{4, winner, 14, 10 + winner}
							if tc.emptyFirst {
								want = want[2:]
							}
							require.Equal(t, want, task.result.GetResults().GetIds().GetIntId().GetData(), "a late ANN candidate must be eligible to win reranking")
						}
					})
				}
			}
		}
	}
}
