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
package dql

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/featureusage"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSearchTaskStreamingConfigRefresh(t *testing.T) {
	for _, hybrid := range []bool{false, true} {
		t.Run("hybrid="+strconv.FormatBool(hybrid), func(t *testing.T) {
			params := paramtable.Get()
			cfg := &params.ProxyCfg
			oldBytes := cfg.ReduceStreamChunkBytes.GetValue()
			t.Cleanup(func() { require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, oldBytes)) })
			legacy := &fakeLegacyQueryClient{}
			for i, enabled := range []bool{false, true, true, false} {
				chunkBytes := 32 << i
				task := newQueryViewSearchTask(t, legacy, 1)
				require.NoError(t, params.Save(cfg.EnableReduceStream.Key, strconv.FormatBool(enabled)))
				require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, strconv.Itoa(chunkBytes)))
				// Obsolete SDK parameters cannot override server configuration.
				task.request.SearchParams = []*commonpb.KeyValuePair{
					{Key: "enableReduceStream", Value: strconv.FormatBool(!enabled)},
					{Key: "reduceStreamChunkBytes", Value: "1"},
				}
				if hybrid {
					task.IsIterator, task.IsAdvanced = false, true
					task.rerankMeta = &legacyRerankMeta{}
					task.SubReqs = []*internalpb.SubSearchRequest{{Nq: 1, Topk: 1, SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER}}
				}
				legacy.searchResult = &queryclient.LegacySearchResult{}
				if enabled {
					stream := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: newFinalSearchChunk([]int64{1}, []float32{.9}, "channel")}}}
					if hybrid {
						legacy.searchResult.SubStreams = []searchreducestream.ReduceStream{stream}
					} else {
						legacy.searchResult.Stream = stream
					}
				}
				require.NoError(t, task.Execute(context.Background()))
				require.Equal(t, enabled, legacy.lastSearchRequest.AllowStreaming)
				require.Equal(t, chunkBytes, legacy.lastSearchRequest.ReduceStreamChunkBytes)
				require.Equal(t, enabled, task.reducedResult != nil || task.reducedSubResults != nil)
			}
		})
	}
}

func TestQueryTaskStreamingConfigRefresh(t *testing.T) {
	for _, shape := range []string{"plain", "iterator", "grouped", "grouped_order_by", "global_count", "global_sum"} {
		t.Run(shape, func(t *testing.T) {
			params := paramtable.Get()
			cfg := &params.ProxyCfg
			oldBytes := cfg.ReduceStreamChunkBytes.GetValue()
			t.Cleanup(func() { require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, oldBytes)) })
			legacy := &fakeLegacyQueryClient{}
			for i, enabled := range []bool{false, true, true, false} {
				chunkBytes := 32 << i
				task := newQueryViewQueryTask(t, legacy, 1)
				task.IsIterator = shape == "iterator"
				if shape == "grouped" || shape == "grouped_order_by" {
					task.GroupByFieldIds = []int64{101}
				}
				if shape == "grouped_order_by" {
					task.OrderByFields = []*planpb.OrderByField{{FieldId: 101}}
				}
				if shape == "global_count" || shape == "global_sum" {
					op := planpb.AggregateOp_sum
					if shape == "global_count" {
						op = planpb.AggregateOp_count
					}
					task.Aggregates = []*planpb.Aggregate{{Op: op, FieldId: 101}}
				}
				require.NoError(t, params.Save(cfg.EnableReduceStream.Key, strconv.FormatBool(enabled)))
				require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, strconv.Itoa(chunkBytes)))
				task.request.QueryParams = []*commonpb.KeyValuePair{
					{Key: "enableReduceStream", Value: strconv.FormatBool(!enabled)},
					{Key: "reduceStreamChunkBytes", Value: "1"},
				}
				legacy.queryResult = &queryclient.LegacyQueryResult{}
				if enabled {
					legacy.queryResult.Stream = &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: newFinalQueryChunk([]int64{1}, 10)}}}
				}
				require.NoError(t, task.Execute(context.Background()))
				require.Equal(t, enabled, legacy.lastQueryRequest.AllowStreaming)
				require.Equal(t, chunkBytes, legacy.lastQueryRequest.ReduceStreamChunkBytes)
				require.Equal(t, enabled, len(task.resultBuf.Collect()) > 0)
			}
		})
	}
}

func TestStreamingConfigStableAcrossRetries(t *testing.T) {
	for _, operation := range []string{"Search", "Hybrid", "Query"} {
		for _, enabled := range []bool{false, true} {
			t.Run(operation+"/"+strconv.FormatBool(enabled), func(t *testing.T) {
				legacy := &fakeLegacyQueryClient{maxRetries: 2}
				search := newQueryViewSearchTask(t, legacy, 1)
				query := newQueryViewQueryTask(t, legacy, 1)
				if operation == "Hybrid" {
					search.IsIterator, search.IsAdvanced = false, true
					search.rerankMeta = &legacyRerankMeta{}
					search.SubReqs = []*internalpb.SubSearchRequest{{Nq: 1, Topk: 1, SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER}}
				}
				params := paramtable.Get()
				cfg := &params.ProxyCfg
				oldBytes := cfg.ReduceStreamChunkBytes.GetValue()
				t.Cleanup(func() { require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, oldBytes)) })
				require.NoError(t, params.Save(cfg.EnableReduceStream.Key, strconv.FormatBool(enabled)))
				require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, "64"))
				legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
					require.Equal(t, enabled, legacy.lastSearchRequest.AllowStreaming)
					require.Equal(t, 64, legacy.lastSearchRequest.ReduceStreamChunkBytes)
					require.NoError(t, params.Save(cfg.EnableReduceStream.Key, strconv.FormatBool(!enabled)))
					require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, "128"))
					if attempt == 1 {
						return nil, streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, "retry")
					}
					stream := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: newFinalSearchChunk([]int64{1}, []float32{.9}, "channel")}}}
					if operation == "Hybrid" {
						return &queryclient.LegacySearchResult{SubStreams: []searchreducestream.ReduceStream{stream}}, nil
					}
					return &queryclient.LegacySearchResult{Stream: stream}, nil
				}
				legacy.query = func(attempt int) (*queryclient.LegacyQueryResult, error) {
					require.Equal(t, enabled, legacy.lastQueryRequest.AllowStreaming)
					require.Equal(t, 64, legacy.lastQueryRequest.ReduceStreamChunkBytes)
					require.NoError(t, params.Save(cfg.EnableReduceStream.Key, strconv.FormatBool(!enabled)))
					require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, "128"))
					if attempt == 1 {
						return nil, streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, "retry")
					}
					return &queryclient.LegacyQueryResult{Stream: &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: newFinalQueryChunk([]int64{1}, 10)}}}}, nil
				}
				var err error
				if operation == "Query" {
					err = query.Execute(t.Context())
				} else {
					err = search.Execute(t.Context())
				}
				if enabled {
					require.NoError(t, err)
					require.Equal(t, 2, legacy.searchCalled+legacy.queryCalled)
				} else {
					require.Error(t, err)
					require.Equal(t, 1, legacy.searchCalled+legacy.queryCalled)
				}
			})
		}
	}
}

func TestStreamingConfigLeavesUnsupportedRequestsOnBatch(t *testing.T) {
	legacy := &fakeLegacyQueryClient{searchResult: &queryclient.LegacySearchResult{}, queryResult: &queryclient.LegacyQueryResult{}}
	search := newQueryViewSearchTask(t, legacy, 1)
	search.IsIterator = false // DEFAULT without Range/iterative-filter QueryInfo is unsupported.
	search.request.SearchParams = []*commonpb.KeyValuePair{{Key: "enableReduceStream", Value: "true"}, {Key: "reduceStreamChunkBytes", Value: "invalid"}}
	require.NoError(t, search.Execute(t.Context()))
	require.False(t, legacy.lastSearchRequest.AllowStreaming)
	for _, req := range []*internalpb.RetrieveRequest{
		{},
		{Limit: 1, IsCount: true},
		{Limit: 1, Aggregates: []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 101}}, OrderByFields: []*planpb.OrderByField{{FieldId: 101}}},
	} {
		query := newQueryViewQueryTask(t, legacy, 1)
		req.Base = query.Base
		query.RetrieveRequest = req
		query.request.QueryParams = search.request.SearchParams
		require.NoError(t, query.Execute(t.Context()))
		require.False(t, legacy.lastQueryRequest.AllowStreaming)
	}
	query := newQueryViewQueryTask(t, legacy, 1)
	query.request.FunctionChains = []*schemapb.FunctionChain{{Stage: schemapb.FunctionChainStage_FunctionChainStageQueryScan}}
	require.NoError(t, query.Execute(t.Context()))
	require.False(t, legacy.lastQueryRequest.AllowStreaming)
}

func TestStreamingConfigLeavesRetainedPlansOnBatch(t *testing.T) {
	for _, plans := range [][]queryclient.ShardPlan{{}, {{Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 10}}}} {
		legacy := &fakeLegacyQueryClient{maxRetries: 2, err: streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, "do not retry requery")}
		task := newQueryViewQueryTask(t, legacy, 1)
		task.queryViewPlans = plans
		require.Error(t, task.Execute(context.Background()))
		require.Equal(t, 1, legacy.queryCalled)
		require.Equal(t, plans, legacy.lastQueryRequest.Plans)
		require.False(t, legacy.lastQueryRequest.AllowStreaming)
	}
}

func TestStreamingResultsPreservePlanExecutionSummary(t *testing.T) {
	plan := queryclient.ShardPlan{ExecutionSummary: &viewpb.ExecutionSummary{GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{
		Flags: 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering,
	}}}
	bit, _ := featureusage.ExecBit(featureusage.FeatureSegmentPrune)
	legacy := &fakeLegacyQueryClient{
		searchResult: &queryclient.LegacySearchResult{Plans: []queryclient.ShardPlan{plan}, Stream: &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: newFinalSearchChunk([]int64{1}, []float32{.9}, "channel")}}}},
		queryResult:  &queryclient.LegacyQueryResult{Plans: []queryclient.ShardPlan{plan}, Stream: &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: newFinalQueryChunk([]int64{1}, 10)}}}},
	}
	search := newQueryViewSearchTask(t, legacy, 1)
	require.NoError(t, search.Execute(t.Context()))
	require.Equal(t, bit, search.reducedFeatureBits&bit)
	query := newQueryViewQueryTask(t, legacy, 1)
	require.NoError(t, query.Execute(t.Context()))
	var featureBits uint64
	query.resultBuf.Range(func(result *internalpb.RetrieveResults) bool {
		featureBits |= result.GetFeatureBits()
		return true
	})
	require.Equal(t, bit, featureBits&bit)
}
