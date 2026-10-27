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
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSearchTaskStreamingOverrides(t *testing.T) {
	enabled, disabled := true, false
	for _, tc := range []struct {
		name       string
		defaultOn  bool
		override   *bool
		chunkBytes int
		hybrid     bool
	}{
		{name: "default-Batch"},
		{name: "default-Streaming", defaultOn: true},
		{name: "force-Batch", defaultOn: true, override: &disabled},
		{name: "force-Streaming", override: &enabled, chunkBytes: 128},
		{name: "Chunk-only", defaultOn: true, chunkBytes: 64},
		{name: "Chunk-does-not-enable", chunkBytes: 64},
		{name: "Hybrid", override: &enabled, chunkBytes: 32, hybrid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacy := &fakeLegacyQueryClient{reduceStreamEnabled: tc.defaultOn, searchResult: &queryclient.LegacySearchResult{}}
			task := newQueryViewSearchTask(legacy, 1)
			if tc.override != nil {
				value := "false"
				if *tc.override {
					value = "true"
				}
				task.request.SearchParams = append(task.request.SearchParams, &commonpb.KeyValuePair{Key: "enableReduceStream", Value: value})
			}
			if tc.chunkBytes > 0 {
				task.request.SearchParams = append(task.request.SearchParams, &commonpb.KeyValuePair{Key: "reduceStreamChunkBytes", Value: strconv.Itoa(tc.chunkBytes)})
			}
			if tc.hybrid {
				task.IsIterator, task.IsAdvanced = false, true
				task.rerankMeta = &legacyRerankMeta{}
				task.SubReqs = []*internalpb.SubSearchRequest{{Nq: 1, Topk: 1, SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER}}
			}
			streaming := tc.defaultOn
			if tc.override != nil {
				streaming = *tc.override
			}
			if streaming {
				stream := &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: newFinalSearchChunk([]int64{1}, []float32{.9}, "channel")}}}
				if tc.hybrid {
					legacy.searchResult.SubStreams = []searchutil.ReduceStream{stream}
				} else {
					legacy.searchResult.Stream = stream
				}
			}
			require.NoError(t, task.Execute(context.Background()))
			require.Equal(t, tc.override, legacy.lastSearchRequest.EnableReduceStream)
			require.Equal(t, tc.chunkBytes, legacy.lastSearchRequest.ReduceStreamChunkBytes)
			require.True(t, legacy.lastSearchRequest.AllowStreaming)
			require.Equal(t, tc.defaultOn, legacy.ReduceStreamEnabled())
		})
	}
}

func TestSearchTaskRejectsInvalidStreamingOverrides(t *testing.T) {
	for _, tc := range []struct {
		key, value string
	}{
		{"enableReduceStream", "yes"}, {"enableReduceStream", ""},
		{"reduceStreamChunkBytes", "0"}, {"reduceStreamChunkBytes", "-1"},
		{"reduceStreamChunkBytes", "1.5"}, {"reduceStreamChunkBytes", ""},
		{"reduceStreamChunkBytes", "9223372036854775808"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			legacy := &fakeLegacyQueryClient{}
			task := newQueryViewSearchTask(legacy, 1)
			task.request.SearchParams = []*commonpb.KeyValuePair{{Key: tc.key, Value: tc.value}}
			require.ErrorContains(t, task.Execute(context.Background()), tc.key)
			require.Zero(t, legacy.searchCalled)
		})
	}
	for _, force := range []string{"true", "false"} {
		t.Run("unsupported="+force, func(t *testing.T) {
			legacy := &fakeLegacyQueryClient{searchResult: &queryclient.LegacySearchResult{}}
			task := newQueryViewSearchTask(legacy, 1)
			task.IsIterator = false // DEFAULT without Range/iterative-filter QueryInfo is unsupported.
			task.request.SearchParams = []*commonpb.KeyValuePair{{Key: "enableReduceStream", Value: force}}
			err := task.Execute(context.Background())
			if force == "true" {
				require.ErrorContains(t, err, "not supported")
				require.Zero(t, legacy.searchCalled)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, legacy.searchCalled)
			}
		})
	}
}

func TestSearchTaskStreamingOverrideControlsRetries(t *testing.T) {
	for _, force := range []string{"true", "false"} {
		t.Run(force, func(t *testing.T) {
			legacy := &fakeLegacyQueryClient{maxRetries: 2, reduceStreamEnabled: force == "false"}
			legacy.search = func(attempt int) (*queryclient.LegacySearchResult, error) {
				if attempt == 1 {
					return nil, streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, "retry")
				}
				return &queryclient.LegacySearchResult{Stream: &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: newFinalSearchChunk([]int64{1}, []float32{.9}, "channel")}}}}, nil
			}
			task := newQueryViewSearchTask(legacy, 1)
			task.request.SearchParams = []*commonpb.KeyValuePair{{Key: "enableReduceStream", Value: force}, {Key: "reduceStreamChunkBytes", Value: "64"}}
			err := task.Execute(context.Background())
			if force == "true" {
				require.NoError(t, err)
				require.Equal(t, 2, legacy.searchCalled)
			} else {
				require.Error(t, err)
				require.Equal(t, 1, legacy.searchCalled)
			}
			require.Equal(t, 64, legacy.lastSearchRequest.ReduceStreamChunkBytes)
		})
	}
}

func TestQueryTaskStreamingOverrides(t *testing.T) {
	enabled, disabled := true, false
	for _, tc := range []struct {
		name       string
		defaultOn  bool
		override   *bool
		chunkBytes int
		iterator   bool
		grouped    bool
	}{
		{name: "default-Batch"},
		{name: "default-Streaming", defaultOn: true},
		{name: "force-Batch", defaultOn: true, override: &disabled},
		{name: "force-Streaming", override: &enabled, chunkBytes: 128},
		{name: "Chunk-only", defaultOn: true, chunkBytes: 64},
		{name: "Chunk-does-not-enable", chunkBytes: 64},
		{name: "iterator", override: &enabled, chunkBytes: 32, iterator: true},
		{name: "Group-By", override: &enabled, chunkBytes: 32, grouped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacy := &fakeLegacyQueryClient{reduceStreamEnabled: tc.defaultOn, queryResult: &queryclient.LegacyQueryResult{}}
			task := newQueryViewQueryTask(legacy, 1)
			task.IsIterator = tc.iterator
			if tc.grouped {
				task.GroupByFieldIds = []int64{101}
			}
			streaming := tc.defaultOn
			if tc.override != nil {
				streaming = *tc.override
				task.request.QueryParams = append(task.request.QueryParams, &commonpb.KeyValuePair{Key: "enableReduceStream", Value: strconv.FormatBool(streaming)})
			}
			if tc.chunkBytes > 0 {
				task.request.QueryParams = append(task.request.QueryParams, &commonpb.KeyValuePair{Key: "reduceStreamChunkBytes", Value: strconv.Itoa(tc.chunkBytes)})
			}
			if streaming {
				legacy.queryResult.Stream = &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: newFinalQueryChunk([]int64{1}, 10)}}}
			}
			require.NoError(t, task.Execute(context.Background()))
			require.Equal(t, tc.override, legacy.lastQueryRequest.EnableReduceStream)
			require.Equal(t, tc.chunkBytes, legacy.lastQueryRequest.ReduceStreamChunkBytes)
			require.True(t, legacy.lastQueryRequest.AllowStreaming)
			require.Equal(t, tc.defaultOn, legacy.ReduceStreamEnabled())
		})
	}
}

func TestQueryTaskRejectsInvalidStreamingOverrides(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"enableReduceStream", "yes"}, {"enableReduceStream", ""},
		{"reduceStreamChunkBytes", "0"}, {"reduceStreamChunkBytes", "-1"},
		{"reduceStreamChunkBytes", "1.5"}, {"reduceStreamChunkBytes", ""},
		{"reduceStreamChunkBytes", "9223372036854775808"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			legacy := &fakeLegacyQueryClient{}
			task := newQueryViewQueryTask(legacy, 1)
			task.request.QueryParams = []*commonpb.KeyValuePair{{Key: tc.key, Value: tc.value}}
			require.ErrorContains(t, task.Execute(context.Background()), tc.key)
			require.Zero(t, legacy.queryCalled)
		})
	}
	for _, req := range []*internalpb.RetrieveRequest{
		{}, {Limit: 1, IsCount: true},
		{Limit: 1, OrderByFields: []*planpb.OrderByField{{FieldId: 101}}},
		{Limit: 1, Aggregates: []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 101}}},
	} {
		for _, force := range []string{"true", "false"} {
			legacy := &fakeLegacyQueryClient{queryResult: &queryclient.LegacyQueryResult{}}
			task := newQueryViewQueryTask(legacy, 1)
			req.Base = task.RetrieveRequest.Base
			task.RetrieveRequest = req
			task.request.QueryParams = []*commonpb.KeyValuePair{{Key: "enableReduceStream", Value: force}}
			err := task.Execute(context.Background())
			if force == "true" {
				require.ErrorContains(t, err, "not supported")
				require.Zero(t, legacy.queryCalled)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, legacy.queryCalled)
			}
		}
	}
}

func TestQueryTaskStreamingOverrideControlsRetries(t *testing.T) {
	for _, force := range []string{"true", "false"} {
		t.Run(force, func(t *testing.T) {
			legacy := &fakeLegacyQueryClient{maxRetries: 2, reduceStreamEnabled: force == "false"}
			legacy.query = func(attempt int) (*queryclient.LegacyQueryResult, error) {
				if attempt == 1 {
					return nil, streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, "retry")
				}
				return &queryclient.LegacyQueryResult{Stream: &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: newFinalQueryChunk([]int64{1}, 10)}}}}, nil
			}
			task := newQueryViewQueryTask(legacy, 1)
			task.request.QueryParams = []*commonpb.KeyValuePair{{Key: "enableReduceStream", Value: force}, {Key: "reduceStreamChunkBytes", Value: "64"}}
			err := task.Execute(context.Background())
			if force == "true" {
				require.NoError(t, err)
				require.Equal(t, 2, legacy.queryCalled)
			} else {
				require.Error(t, err)
				require.Equal(t, 1, legacy.queryCalled)
			}
			require.Equal(t, 64, legacy.lastQueryRequest.ReduceStreamChunkBytes)
		})
	}
}

func TestQueryTaskStreamingOverridesLeaveRetainedPlansUnchanged(t *testing.T) {
	for _, plans := range [][]queryclient.ShardPlan{{}, {{Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 10}}}} {
		legacy := &fakeLegacyQueryClient{maxRetries: 2, reduceStreamEnabled: true, err: streamViewError(viewpb.ViewCode_VIEW_CODE_VIEW_NOT_FOUND, "do not retry requery")}
		task := newQueryViewQueryTask(legacy, 1)
		task.queryViewPlans = plans
		task.request.QueryParams = []*commonpb.KeyValuePair{{Key: "enableReduceStream", Value: "true"}, {Key: "reduceStreamChunkBytes", Value: "1"}}
		require.Error(t, task.Execute(context.Background()))
		require.Equal(t, 1, legacy.queryCalled)
		require.Equal(t, plans, legacy.lastQueryRequest.Plans)
		require.Nil(t, legacy.lastQueryRequest.EnableReduceStream)
		require.Zero(t, legacy.lastQueryRequest.ReduceStreamChunkBytes)
	}
}

func TestStreamingResultsPreservePlanExecutionSummary(t *testing.T) {
	plan := queryclient.ShardPlan{ExecutionSummary: &viewpb.ExecutionSummary{GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{
		Flags: 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering,
	}}}
	bit, _ := featureusage.ExecBit(featureusage.FeatureSegmentPrune)
	legacy := &fakeLegacyQueryClient{reduceStreamEnabled: true,
		searchResult: &queryclient.LegacySearchResult{Plans: []queryclient.ShardPlan{plan}, Stream: &fakeLegacySearchStream{recv: []fakeLegacySearchRecv{{chunk: newFinalSearchChunk([]int64{1}, []float32{.9}, "channel")}}}},
		queryResult:  &queryclient.LegacyQueryResult{Plans: []queryclient.ShardPlan{plan}, Stream: &fakeLegacyQueryStream{recv: []fakeLegacyQueryRecv{{chunk: newFinalQueryChunk([]int64{1}, 10)}}}},
	}
	search := newQueryViewSearchTask(legacy, 1)
	require.NoError(t, search.Execute(t.Context()))
	require.Equal(t, bit, search.reducedFeatureBits&bit)
	query := newQueryViewQueryTask(legacy, 1)
	require.NoError(t, query.Execute(t.Context()))
	var featureBits uint64
	query.resultBuf.Range(func(result *internalpb.RetrieveResults) bool {
		featureBits |= result.GetFeatureBits()
		return true
	})
	require.Equal(t, bit, featureBits&bit)
}
