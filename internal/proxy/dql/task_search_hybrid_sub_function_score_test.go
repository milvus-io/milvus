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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

const subBoostTestCollection = "hybrid_sub_boost"

func subBoostTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: subBoostTestCollection,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "tag", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "8"}}},
			{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
		},
	}
}

// boostFunctionScore is the per-sub-request ranker from the #52956 repro:
// rows with tag == "vip" get their sub-search score multiplied by 100.
func boostFunctionScore() *schemapb.FunctionScore {
	return &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{
		rerankFunction("boost", map[string]string{"filter": `tag == "vip"`, "weight": "100"}),
	}}
}

func weightedFunctionScore() *schemapb.FunctionScore {
	return &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{
		rerankFunction("weighted", map[string]string{"weights": "[0.5, 0.5]", "norm_score": "true"}),
	}}
}

func rerankFunction(name string, params map[string]string) *schemapb.FunctionSchema {
	function := &schemapb.FunctionSchema{
		Type:   schemapb.FunctionType_Rerank,
		Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: name}},
	}
	for key, value := range params {
		function.Params = append(function.Params, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	return function
}

// newSubBoostHybridRequest returns a two-leg hybrid search fused by a top-level
// weighted ranker. Callers attach sub-request function scores.
func newSubBoostHybridRequest(t *testing.T) *milvuspb.HybridSearchRequest {
	t.Helper()
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 16)},
	}}})
	require.NoError(t, err)
	request := &milvuspb.HybridSearchRequest{
		CollectionName: subBoostTestCollection,
		RankParams:     []*commonpb.KeyValuePair{{Key: LimitKey, Value: "5"}},
		FunctionScore:  weightedFunctionScore(),
	}
	for range 2 {
		request.Requests = append(request.Requests, &milvuspb.SearchRequest{
			Nq:          1,
			SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder},
			SearchParams: []*commonpb.KeyValuePair{
				{Key: AnnsFieldKey, Value: "vec"},
				{Key: TopKKey, Value: "5"},
				{Key: common.MetricTypeKey, Value: metric.IP},
				{Key: ParamsKey, Value: `{}`},
			},
		})
	}
	return request
}

// newSubBoostSearchTask builds the task the way NewHybridSearchTask does,
// without a proxy node, and runs initAdvancedSearchRequest.
func newSubBoostSearchTask(t *testing.T, request *milvuspb.HybridSearchRequest) (*SearchTask, error) {
	t.Helper()
	searchRequest, subSearches := convertHybridSearch(request)
	task := &SearchTask{
		ctx:               context.Background(),
		collectionName:    subBoostTestCollection,
		SearchRequest:     &internalpb.SearchRequest{Base: &commonpb.MsgBase{Timestamp: 1}, CollectionID: 1, PartitionIDs: []int64{1}, IsAdvanced: true, Nq: 1},
		request:           searchRequest,
		hybridSubSearches: subSearches,
		schema:            mustNewSchemaInfo(subBoostTestSchema()),
		tr:                timerecord.NewTimeRecorder("hybrid-sub-boost-test"),
	}
	return task, task.initAdvancedSearchRequest(task.ctx)
}

func subSearchPlan(t *testing.T, task *SearchTask, index int) *planpb.PlanNode {
	t.Helper()
	plan := &planpb.PlanNode{}
	require.NoError(t, proto.Unmarshal(task.SubReqs[index].GetSerializedExprPlan(), plan))
	return plan
}

func TestConvertHybridSearchPairsSubFunctionScore(t *testing.T) {
	request := newSubBoostHybridRequest(t)
	request.Requests[1].FunctionScore = boostFunctionScore()

	searchRequest, subSearches := convertHybridSearch(request)
	require.Len(t, subSearches, 2)
	for i, subSearch := range subSearches {
		assert.Same(t, searchRequest.GetSubReqs()[i], subSearch.request)
	}
	assert.Nil(t, subSearches[0].functionScore)
	assert.Same(t, request.Requests[1].FunctionScore, subSearches[1].functionScore)
	assert.Same(t, request.FunctionScore, searchRequest.GetFunctionScore())
}

func TestHybridSubFunctionScorePlan(t *testing.T) {
	paramtable.Init()

	t.Run("boost lands only in its own sub-search plan", func(t *testing.T) {
		request := newSubBoostHybridRequest(t)
		request.Requests[1].FunctionScore = boostFunctionScore()
		task, err := newSubBoostSearchTask(t, request)
		require.NoError(t, err)
		require.Len(t, task.SubReqs, 2)

		assert.Empty(t, subSearchPlan(t, task, 0).GetScorers())
		boosted := subSearchPlan(t, task, 1)
		require.Len(t, boosted.GetScorers(), 1)
		assert.Equal(t, float32(100), boosted.GetScorers()[0].GetWeight())
		assert.NotNil(t, boosted.GetScorers()[0].GetFilter(), "the boost filter must be parsed into the sub-search plan")
		// Fusion still uses the top-level ranker.
		assert.IsType(t, &funcScoreRerankMeta{}, task.rerankMeta)
	})

	t.Run("boost with legacy rank params", func(t *testing.T) {
		request := newSubBoostHybridRequest(t)
		request.FunctionScore = nil
		request.RankParams = append(request.RankParams, &commonpb.KeyValuePair{Key: RankTypeKey, Value: "rrf"})
		request.Requests[0].FunctionScore = boostFunctionScore()
		task, err := newSubBoostSearchTask(t, request)
		require.NoError(t, err)

		require.Len(t, subSearchPlan(t, task, 0).GetScorers(), 1)
		assert.Empty(t, subSearchPlan(t, task, 1).GetScorers())
	})

	t.Run("without sub function score nothing changes", func(t *testing.T) {
		task, err := newSubBoostSearchTask(t, newSubBoostHybridRequest(t))
		require.NoError(t, err)
		for i := range task.SubReqs {
			assert.Empty(t, subSearchPlan(t, task, i).GetScorers())
		}
	})
}

func TestHybridSubFunctionScoreRejected(t *testing.T) {
	paramtable.Init()

	cases := []struct {
		name    string
		mutate  func(*milvuspb.HybridSearchRequest)
		message string
	}{
		{
			name: "empty function score",
			mutate: func(r *milvuspb.HybridSearchRequest) {
				r.Requests[1].FunctionScore = &schemapb.FunctionScore{}
			},
			message: "must contain at least one boost function",
		},
		{
			name: "nil function",
			mutate: func(r *milvuspb.HybridSearchRequest) {
				r.Requests[1].FunctionScore = &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{nil}}
			},
			message: "function[0] is nil",
		},
		{
			name: "boost mixed with decay",
			mutate: func(r *milvuspb.HybridSearchRequest) {
				fs := boostFunctionScore()
				fs.Functions = append(fs.Functions, rerankFunction("decay", map[string]string{"function": "gauss", "origin": "0", "scale": "10"}))
				r.Requests[1].FunctionScore = fs
			},
			message: `only supports the boost reranker, got "decay"`,
		},
		{
			name: "nested function chains",
			mutate: func(r *milvuspb.HybridSearchRequest) {
				// Legacy top-level ranker: top-level function_score with nested
				// chains is already rejected before the sub-search loop.
				r.FunctionScore = nil
				r.Requests[1].FunctionScore = boostFunctionScore()
				r.Requests[1].FunctionChains = []*schemapb.FunctionChain{l0FunctionChain(&schemapb.FunctionChainOp{Op: "map", Outputs: []string{"$score"}})}
			},
			message: "function_score cannot be used with function_chains",
		},
		{
			name: "top-level function score also has boost",
			mutate: func(r *milvuspb.HybridSearchRequest) {
				r.FunctionScore.Functions = append(r.FunctionScore.Functions, boostFunctionScore().Functions...)
				r.Requests[1].FunctionScore = boostFunctionScore()
			},
			message: "top-level function_score contains a boost function",
		},
	}
	for _, name := range []string{"decay", "weighted", "rrf", "model", ""} {
		cases = append(cases, struct {
			name    string
			mutate  func(*milvuspb.HybridSearchRequest)
			message string
		}{
			name: "reranker " + name,
			mutate: func(r *milvuspb.HybridSearchRequest) {
				function := &schemapb.FunctionSchema{Type: schemapb.FunctionType_Rerank}
				if name != "" {
					function.Params = []*commonpb.KeyValuePair{{Key: "reranker", Value: name}}
				}
				r.Requests[1].FunctionScore = &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{function}}
			},
			message: "only supports the boost reranker",
		})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := newSubBoostHybridRequest(t)
			tc.mutate(request)
			_, err := newSubBoostSearchTask(t, request)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.ErrorContains(t, err, "sub-search[1]")
			assert.ErrorContains(t, err, tc.message)
		})
	}
}

func TestHybridSubFunctionScoreUnpaired(t *testing.T) {
	paramtable.Init()
	request := newSubBoostHybridRequest(t)
	request.Requests[1].FunctionScore = boostFunctionScore()
	searchRequest, subSearches := convertHybridSearch(request)
	// Something rebuilt SubReqs after conversion: the pairing no longer holds.
	searchRequest.SubReqs[1] = proto.Clone(searchRequest.SubReqs[1]).(*milvuspb.SubSearchRequest)
	task := &SearchTask{
		ctx:               context.Background(),
		collectionName:    subBoostTestCollection,
		SearchRequest:     &internalpb.SearchRequest{Base: &commonpb.MsgBase{Timestamp: 1}, CollectionID: 1, PartitionIDs: []int64{1}, IsAdvanced: true, Nq: 1},
		request:           searchRequest,
		hybridSubSearches: subSearches,
		schema:            mustNewSchemaInfo(subBoostTestSchema()),
		tr:                timerecord.NewTimeRecorder("hybrid-sub-boost-test"),
	}
	err := task.initAdvancedSearchRequest(task.ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.ErrorContains(t, err, "sub-search[1] is not paired")
}

// TestHybridSubBoostSurvivesFusion runs the proxy reduce and weighted fusion on
// QueryNode-style sub-search results, as in the #52956 repro: row 0 is the
// weakest match in both legs, and the QueryNode applies the 100x boost to it.
// The boosted row must come first after fusion; without the boost it is last.
func TestHybridSubBoostSurvivesFusion(t *testing.T) {
	paramtable.Init()

	leg := func(reqIndex int64, ids []int64, scores []float32) *internalpb.SubSearchResults {
		return &internalpb.SubSearchResults{
			ReqIndex: reqIndex, MetricType: metric.IP, NumQueries: 1, TopK: 5,
			ResultData: &schemapb.SearchResultData{
				NumQueries: 1, TopK: 5, Topks: []int64{int64(len(ids))},
				Ids: testSearchResultIDs(ids...), Scores: scores,
			},
		}
	}

	cases := []struct {
		name    string
		boosted bool
		legs    []*internalpb.SubSearchResults
		want    []int64
	}{
		{
			name:    "without boost",
			boosted: false,
			legs: []*internalpb.SubSearchResults{
				leg(0, []int64{4, 3, 2, 1, 0}, []float32{0.144, 0.141, 0.137, 0.129, 0.109}),
				leg(1, []int64{4, 3, 2, 1, 0}, []float32{0.102, 0.098, 0.091, 0.085, 0.062}),
			},
			want: []int64{4, 3, 2, 1, 0},
		},
		{
			name:    "with boost on both legs",
			boosted: true,
			legs: []*internalpb.SubSearchResults{
				leg(0, []int64{0, 4, 3, 2, 1}, []float32{10.9, 0.144, 0.141, 0.137, 0.129}),
				leg(1, []int64{0, 4, 3, 2, 1}, []float32{6.2, 0.102, 0.098, 0.091, 0.085}),
			},
			want: []int64{0, 4, 3, 2, 1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := newSubBoostHybridRequest(t)
			if tc.boosted {
				for _, sub := range request.Requests {
					sub.FunctionScore = boostFunctionScore()
				}
			}
			task, err := newSubBoostSearchTask(t, request)
			require.NoError(t, err)

			span := trace.SpanFromContext(task.ctx)
			reduceOp, err := newHybridSearchReduceOperator(task, nil)
			require.NoError(t, err)
			reduced, err := reduceOp.run(task.ctx, span, []*internalpb.SearchResults{{IsAdvanced: true, SubResults: tc.legs}})
			require.NoError(t, err)

			rerankOp, err := newRerankOperator(task, nil)
			require.NoError(t, err)
			fused, err := rerankOp.run(task.ctx, span, reduced...)
			require.NoError(t, err)
			result := fused[0].(*milvuspb.SearchResults).GetResults()
			assert.Equal(t, tc.want, result.GetIds().GetIntId().GetData())
		})
	}
}
