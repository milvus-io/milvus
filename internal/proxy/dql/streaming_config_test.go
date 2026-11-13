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
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/mocks"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
			for _, tc := range []struct {
				enabled    bool
				chunkBytes string
				wantBytes  int
			}{
				{false, "32", 32},
				{true, "64", 64},
				{true, "0", 0},
				{true, "128", 128},
				{true, "-1", -1},
				{true, "256KB", 0},
				{true, "1", 1},
				{true, "262144", 262144},
				{false, "0", 0},
			} {
				wantStream := tc.enabled && tc.wantBytes > 0
				task := newQueryViewSearchTask(t, legacy, 1)
				require.NoError(t, params.Save(cfg.EnableReduceStream.Key, strconv.FormatBool(tc.enabled)))
				require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, tc.chunkBytes))
				// Obsolete SDK parameters cannot override server configuration.
				task.request.SearchParams = []*commonpb.KeyValuePair{
					{Key: "enableReduceStream", Value: strconv.FormatBool(!tc.enabled)},
					{Key: "reduceStreamChunkBytes", Value: "1"},
				}
				if hybrid {
					task.IsIterator, task.IsAdvanced = false, true
					task.rerankMeta = &legacyRerankMeta{}
					task.SubReqs = []*internalpb.SubSearchRequest{{Nq: 1, Topk: 1, SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER}}
				}
				legacy.searchResult = &queryclient.LegacySearchResult{}
				if wantStream {
					stream := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: newFinalSearchChunk([]int64{1}, []float32{.9}, "channel")}}}
					if hybrid {
						legacy.searchResult.SubStreams = []searchreducestream.ReduceStream{stream}
					} else {
						legacy.searchResult.Stream = stream
					}
				}
				require.NoError(t, task.Execute(context.Background()))
				require.Equal(t, wantStream, legacy.lastSearchRequest.AllowStreaming, tc)
				require.Equal(t, tc.wantBytes, legacy.lastSearchRequest.ReduceStreamChunkBytes)
				require.Equal(t, wantStream, task.reducedResult != nil || task.reducedSubResults != nil)
				require.Equal(t, tc.chunkBytes, cfg.ReduceStreamChunkBytes.GetValue())
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
			for _, tc := range []struct {
				enabled    bool
				chunkBytes string
				wantBytes  int
			}{
				{false, "32", 32},
				{true, "64", 64},
				{true, "0", 0},
				{true, "128", 128},
				{true, "-1", -1},
				{true, "256KB", 0},
				{true, "1", 1},
				{true, "262144", 262144},
				{false, "0", 0},
			} {
				wantStream := tc.enabled && tc.wantBytes > 0
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
				require.NoError(t, params.Save(cfg.EnableReduceStream.Key, strconv.FormatBool(tc.enabled)))
				require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, tc.chunkBytes))
				task.request.QueryParams = []*commonpb.KeyValuePair{
					{Key: "enableReduceStream", Value: strconv.FormatBool(!tc.enabled)},
					{Key: "reduceStreamChunkBytes", Value: "1"},
				}
				legacy.queryResult = &queryclient.LegacyQueryResult{}
				if wantStream {
					legacy.queryResult.Stream = &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: newFinalQueryChunk([]int64{1}, 10)}}}
				}
				require.NoError(t, task.Execute(context.Background()))
				require.Equal(t, wantStream, legacy.lastQueryRequest.AllowStreaming, tc)
				require.Equal(t, tc.wantBytes, legacy.lastQueryRequest.ReduceStreamChunkBytes)
				require.Equal(t, wantStream, len(task.resultBuf.Collect()) > 0)
				require.Equal(t, tc.chunkBytes, cfg.ReduceStreamChunkBytes.GetValue())
			}
		})
	}
}

func TestStreamingConfigStableAcrossRetries(t *testing.T) {
	mixCoord := mockey.Mock((*namespaceRequeryMockNode).MixCoord).Return(&mocks.MockMixCoordClient{}).Build()
	defer mixCoord.UnPatch()
	lookup := mockey.Mock((*mocks.MockMixCoordClient).GetQueryViewLoadInfo).Return(
		&querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil).Build()
	defer lookup.UnPatch()
	for _, operation := range []string{"Search", "Hybrid", "Query"} {
		for _, tc := range []struct {
			enabled        bool
			chunkBytes     string
			wantBytes      int
			refreshedBytes string
		}{
			{false, "64", 64, "128"},
			{true, "64", 64, "128"},
			{true, "64", 64, "0"},
			{true, "64", 64, "256KB"},
			{true, "0", 0, "64"},
			{true, "-1", -1, "64"},
			{true, "256KB", 0, "64"},
		} {
			t.Run(operation+"/"+strconv.FormatBool(tc.enabled)+"/"+tc.chunkBytes+"->"+tc.refreshedBytes, func(t *testing.T) {
				wantStream := tc.enabled && tc.wantBytes > 0
				legacy := &fakeLegacyQueryClient{maxRetries: 2}
				search := newQueryViewSearchTask(t, legacy, 1)
				query := newQueryViewQueryTask(t, legacy, 1)
				search.node, query.node = &namespaceRequeryMockNode{}, &namespaceRequeryMockNode{}
				if operation == "Hybrid" {
					search.IsIterator, search.IsAdvanced = false, true
					search.rerankMeta = &legacyRerankMeta{}
					search.SubReqs = []*internalpb.SubSearchRequest{{Nq: 1, Topk: 1, SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER}}
				}
				params := paramtable.Get()
				cfg := &params.ProxyCfg
				oldBytes := cfg.ReduceStreamChunkBytes.GetValue()
				t.Cleanup(func() { require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, oldBytes)) })
				require.NoError(t, params.Save(cfg.EnableReduceStream.Key, strconv.FormatBool(tc.enabled)))
				require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, tc.chunkBytes))
				legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
					require.Equal(t, wantStream, legacy.lastSearchRequest.AllowStreaming)
					require.Equal(t, tc.wantBytes, legacy.lastSearchRequest.ReduceStreamChunkBytes)
					require.NoError(t, params.Save(cfg.EnableReduceStream.Key, strconv.FormatBool(!tc.enabled)))
					require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, tc.refreshedBytes))
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
					require.Equal(t, wantStream, legacy.lastQueryRequest.AllowStreaming)
					require.Equal(t, tc.wantBytes, legacy.lastQueryRequest.ReduceStreamChunkBytes)
					require.NoError(t, params.Save(cfg.EnableReduceStream.Key, strconv.FormatBool(!tc.enabled)))
					require.NoError(t, params.Save(cfg.ReduceStreamChunkBytes.Key, tc.refreshedBytes))
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
				if wantStream {
					require.NoError(t, err)
					require.Equal(t, 2, legacy.searchCalled+legacy.queryCalled)
				} else {
					require.Error(t, err)
					require.Equal(t, 1, legacy.searchCalled+legacy.queryCalled)
				}
				require.Equal(t, tc.refreshedBytes, cfg.ReduceStreamChunkBytes.GetValue())
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
		{Limit: 1, ReduceType: 99},
		{GroupByFieldIds: []int64{100}, Aggregates: []*planpb.Aggregate{{Op: planpb.AggregateOp(99)}}},
		{Limit: 1, Aggregates: []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 101}}, OrderByFields: []*planpb.OrderByField{{FieldId: 101}}},
	} {
		query := newQueryViewQueryTask(t, legacy, 1)
		req.Base = query.Base
		query.RetrieveRequest = req
		query.request.QueryParams = search.request.SearchParams
		require.NoError(t, query.Execute(t.Context()))
		require.False(t, legacy.lastQueryRequest.AllowStreaming)
	}
	for _, hybrid := range []bool{false, true} {
		search := newQueryViewSearchTask(t, legacy, 1)
		search.IsIterator, search.IsAdvanced = false, hybrid
		search.GroupByFieldIds = []int64{100}
		search.request.FunctionChains = []*schemapb.FunctionChain{{Stage: schemapb.FunctionChainStage(99)}}
		if hybrid {
			search.rerankMeta = &legacyRerankMeta{}
			search.SubReqs = []*internalpb.SubSearchRequest{{SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER}}
			search.request.SubReqs = []*milvuspb.SubSearchRequest{{FunctionChains: search.request.FunctionChains}}
			search.request.FunctionChains = nil
		}
		require.NoError(t, search.Execute(t.Context()))
		require.False(t, legacy.lastSearchRequest.AllowStreaming)
	}
}

func TestStreamingConfigAllowsQueryScan(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		legacy := &fakeLegacyQueryClient{queryResult: &queryclient.LegacyQueryResult{}}
		query := newQueryViewQueryTask(t, legacy, 1)
		cfg := &paramtable.Get().ProxyCfg.EnableReduceStream
		old := cfg.GetValue()
		t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(cfg.Key, old)) })
		require.NoError(t, paramtable.Get().Save(cfg.Key, strconv.FormatBool(enabled)))
		prepared := queryStageTestTask()
		require.NoError(t, prepared.createPlan(t.Context()))
		query.request.FunctionChains, query.plan = prepared.request.FunctionChains, prepared.plan
		require.NotEmpty(t, query.plan.GetQuerynodeFunctionChains())
		require.NoError(t, query.Execute(t.Context()))
		require.Equal(t, enabled, legacy.lastQueryRequest.AllowStreaming)
	}
}

func TestStreamingEligibilitySchemaCoverage(t *testing.T) {
	for _, tc := range []struct {
		message   proto.Message
		reduction string
		other     string
	}{
		{
			message: &internalpb.RetrieveRequest{},
			reduction: `serialized_expr_plan output_fields_id limit is_count reduce_type is_iterator
				group_by_field_ids aggregates order_by_fields`,
			other: `base reqID dbID collectionID partitionIDs mvcc_timestamp guarantee_timestamp timeout_timestamp
				ignoreGrowing iteration_extension_reduce_rate username reduce_stop_for_best consistency_level
				collection_ttl_timestamps entity_ttl_physical_time query_label pk_filter schema_version`,
		},
		{
			message: &internalpb.SearchRequest{},
			reduction: `serialized_expr_plan output_fields_id nq topk metricType sub_reqs is_advanced offset
				group_by_field_id group_size is_topk_reduce is_recall_evaluation is_iterator search_type group_by_field_ids`,
			other: `base reqID dbID collectionID partitionIDs dsl placeholder_group dsl_type mvcc_timestamp
				guarantee_timestamp timeout_timestamp ignoreGrowing username consistency_level field_id analyzer_name
				collection_ttl_timestamps entity_ttl_physical_time pk_filter schema_version`,
		},
		{
			message:   &internalpb.SubSearchRequest{},
			reduction: `serialized_expr_plan nq topk offset metricType group_by_field_id group_size search_type skip`,
			other:     `dsl placeholder_group dsl_type partitionIDs field_id ignore_growing analyzer_name`,
		},
		{
			message:   &planpb.PlanNode{},
			reduction: `vector_anns query output_field_ids dynamic_fields scorers score_option querynode_function_chains`,
			other:     `predicates plan_options namespace`,
		},
		{
			message:   &planpb.QueryPlanNode{},
			reduction: `is_count limit group_by_field_ids aggregates order_by_fields query_iterator_cursor`,
			other:     `predicates`,
		},
		{
			message: &planpb.QueryInfo{},
			reduction: `topk metric_type search_params round_decimal group_by_field_id group_size strict_group_size
				hints search_iterator_v2_info json_path json_type strict_cast group_by_field_ids`,
			other: `materialized_view_involved bm25_avgdl query_field_id search_topk_ratio refine_topk_ratio`,
		},
		{
			message:   &milvuspb.QueryRequest{},
			reduction: `output_fields query_params function_chains`,
			other: `base db_name collection_name expr partition_names travel_timestamp guarantee_timestamp
				not_return_all_meta consistency_level use_default_consistency expr_template_values namespace rls_principal skip_rls`,
		},
		{
			message:   &milvuspb.SearchRequest{},
			reduction: `output_fields search_params nq sub_reqs function_score search_aggregation function_chains`,
			other: `base db_name collection_name partition_names dsl placeholder_group ids dsl_type travel_timestamp
				guarantee_timestamp not_return_all_meta consistency_level use_default_consistency search_by_primary_keys
				expr_template_values namespace highlighter rls_principal skip_rls`,
		},
		{
			message:   &milvuspb.SubSearchRequest{},
			reduction: `search_params nq function_chains`,
			other:     `dsl placeholder_group dsl_type expr_template_values namespace`,
		},
	} {
		descriptor := tc.message.ProtoReflect().Descriptor()
		t.Run(string(descriptor.FullName()), func(t *testing.T) {
			fields := descriptor.Fields()
			names := make([]string, fields.Len())
			for i := range names {
				names[i] = string(fields.Get(i).Name())
			}
			require.ElementsMatch(t, strings.Fields(tc.reduction+" "+tc.other), names,
				"Review new fields for streaming compatibility and classify them as reduction-related or independent; update gate/parity tests where needed")
		})
	}
	require.Equal(t, map[int32]string{0: "sum", 1: "count", 2: "avg", 3: "min", 4: "max"}, planpb.AggregateOp_name)
	require.Equal(t, map[int32]string{0: "DEFAULT", 1: "PURE_ANN_SEARCH_NO_FILTER", 2: "PURE_ANN_SEARCH_WITH_FILTER"}, internalpb.SearchType_name)
	require.Equal(t, map[int32]string{
		0: "FunctionChainStageUnspecified", 1: "FunctionChainStageIngestion", 2: "FunctionChainStagePreProcess",
		3: "FunctionChainStageL0Rerank", 4: "FunctionChainStageL1Rerank", 5: "FunctionChainStageL2Rerank",
		6: "FunctionChainStagePostProcess", 7: "FunctionChainStageQueryScan",
	}, schemapb.FunctionChainStage_name)
}

func TestStreamingConfigLeavesRetainedPlansOnBatch(t *testing.T) {
	mixCoord := mockey.Mock((*namespaceRequeryMockNode).MixCoord).Return(&mocks.MockMixCoordClient{}).Build()
	defer mixCoord.UnPatch()
	lookup := mockey.Mock((*mocks.MockMixCoordClient).GetQueryViewLoadInfo).Return(
		&querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil).Build()
	defer lookup.UnPatch()
	for _, plans := range [][]queryclient.ShardPlan{{}, {{Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 10}}}} {
		legacy := &fakeLegacyQueryClient{maxRetries: 2, err: streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, "do not retry requery")}
		task := newQueryViewQueryTask(t, legacy, 1)
		task.node = &namespaceRequeryMockNode{}
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
