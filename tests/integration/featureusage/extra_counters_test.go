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
package featureusage

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestMoreSearchCounters covers the request options the first pass left at
// zero: the remaining consistency levels, the rerank paths in both their
// forms, the semantic highlighter and the two null predicates.
//
// Several of these are recorded at the top of PreExecute, before the request
// is validated, which is deliberate: the counter answers "how many clients
// asked for this", and a client that asks with a bad argument still asked.
func (s *Suite) TestMoreSearchCounters() {
	ctx := context.Background()
	s.ensureCollection(ctx)

	cases := []struct {
		name string
		run  func()
		want map[string]int64
	}{
		{
			name: "strong consistency",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
					r.UseDefaultConsistency = false
					r.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
				})
			},
			want: map[string]int64{"consistency_level=Strong": 1},
		},
		{
			name: "session consistency",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
					r.UseDefaultConsistency = false
					r.ConsistencyLevel = commonpb.ConsistencyLevel_Session
				})
			},
			want: map[string]int64{"consistency_level=Session": 1},
		},
		{
			name: "customized consistency",
			run: func() {
				s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
					r.UseDefaultConsistency = false
					r.ConsistencyLevel = commonpb.ConsistencyLevel_Customized
					r.GuaranteeTimestamp = 1
				})
			},
			want: map[string]int64{"consistency_level=Customized": 1},
		},
		{
			name: "is null",
			run: func() {
				s.queryWith(ctx, func(r *milvuspb.QueryRequest) { r.Expr = nullableField + " is null" })
			},
			want: map[string]int64{"is_null": 1},
		},
		{
			name: "regex match",
			run: func() {
				// "like" with an optimizable pattern becomes a prefix match; the
				// =~ operator is what reaches the regex node.
				s.queryWith(ctx, func(r *milvuspb.QueryRequest) { r.Expr = textField + ` =~ "row [0-9]+"` })
			},
			want: map[string]int64{"regex_match": 1},
		},
		{
			name: "json stats hint travels with the template values",
			run: func() {
				s.queryWith(ctx, func(r *milvuspb.QueryRequest) {
					r.Expr = pkField + " >= 0"
					r.ExprTemplateValues = map[string]*schemapb.TemplateValue{
						common.ExprUseJSONStatsKey: {Val: &schemapb.TemplateValue_BoolVal{BoolVal: true}},
					}
				})
			},
			want: map[string]int64{"expr_use_json_stats": 1, "comparison_operators=relational": 1},
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			tc.run()
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, tc.want)
		})
	}
}

// TestRerankCounters covers both rerank paths. The legacy path sends a
// rank_params strategy; the function-score path sends a FunctionScore naming a
// reranker. They are distinct client APIs and are counted separately, so a
// deprecation decision is not misled by summing them.
func (s *Suite) TestRerankCounters() {
	ctx := context.Background()
	s.ensureCollection(ctx)

	for _, tc := range []struct {
		name       string
		rankParams []*commonpb.KeyValuePair
		want       map[string]int64
	}{
		{
			name: "legacy rrf strategy",
			rankParams: []*commonpb.KeyValuePair{
				{Key: "strategy", Value: "rrf"},
				{Key: "params", Value: `{"k": 60}`},
				{Key: "limit", Value: "5"},
			},
			want: map[string]int64{"strategy=rrf": 1},
		},
		{
			// norm_score belongs inside the params object: convertLegacyParams
			// recognizes only strategy and params at the top level and drops the
			// rest, so a top-level key never reaches the reranker.
			name: "legacy weighted strategy with norm_score",
			rankParams: []*commonpb.KeyValuePair{
				{Key: "strategy", Value: "weighted"},
				{Key: "params", Value: `{"weights": [1.0], "norm_score": true}`},
				{Key: "limit", Value: "5"},
			},
			want: map[string]int64{"strategy=weighted": 1, "norm_score": 1},
		},
		{
			name: "a top level norm_score is dropped and does not count",
			rankParams: []*commonpb.KeyValuePair{
				{Key: "strategy", Value: "weighted"},
				{Key: "params", Value: `{"weights": [1.0]}`},
				{Key: "norm_score", Value: "true"},
				{Key: "limit", Value: "5"},
			},
			want: map[string]int64{"strategy=weighted": 1},
		},
	} {
		s.Run(tc.name, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			s.hybridSearch(ctx, tc.rankParams, nil)
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, withOneDenseHybrid(tc.want))
		})
	}

	for _, reranker := range []string{"rrf", "weighted", "model", "boost"} {
		s.Run("reranker="+reranker, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
				r.FunctionScore = &schemapb.FunctionScore{
					Functions: []*schemapb.FunctionSchema{{
						Name:   "rerank",
						Type:   schemapb.FunctionType_Rerank,
						Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: reranker}},
					}},
				}
			})
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, map[string]int64{"reranker=" + reranker: 1})
		})
	}

	// A model reranker names a provider; the counter is the request-side form
	// of the providers group, since a rerank function cannot live in a schema.
	for _, provider := range []string{"ali", "cohere", "huggingface", "siliconflow", "tei", "vllm", "voyageai", "zilliz", "not_a_provider"} {
		want := "rerank_provider=" + provider
		if provider == "not_a_provider" {
			want = "rerank_provider=_other"
		}
		s.Run(want, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
				r.FunctionScore = &schemapb.FunctionScore{
					Functions: []*schemapb.FunctionSchema{{
						Name: "rerank", Type: schemapb.FunctionType_Rerank,
						Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "model"}, {Key: "Provider", Value: provider}},
					}},
				}
			})
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, map[string]int64{"reranker=model": 1, want: 1})
		})
	}

	// function_chains is the third client shape for reranking. It shares the
	// reranker=* counters: the merge strategy of a hybrid chain, and the
	// rerank function a map operator evaluates, count as the same reranker
	// under function_score would.
	strParam := func(v string) *schemapb.FunctionParamValue {
		return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: v}}
	}
	numParam := func(v float64) *schemapb.FunctionParamValue {
		return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_DoubleValue{DoubleValue: v}}
	}
	for _, tc := range []struct {
		strategy string
		params   map[string]*schemapb.FunctionParamValue
		want     string
	}{
		{"rrf", map[string]*schemapb.FunctionParamValue{"k": numParam(60)}, "reranker=rrf"},
		{"weighted", map[string]*schemapb.FunctionParamValue{"weights": {Value: &schemapb.FunctionParamValue_ArrayValue{
			ArrayValue: &schemapb.FunctionParamArray{Values: []*schemapb.FunctionParamValue{numParam(1)}},
		}}}, "reranker=weighted"},
		{"max", nil, "reranker=_other"},
	} {
		s.Run("hybrid function chain merge "+tc.strategy, func() {
			params := map[string]*schemapb.FunctionParamValue{"strategy": strParam(tc.strategy)}
			for k, v := range tc.params {
				params[k] = v
			}
			sub := s.baseSearchRequest()
			before := counters(s.report(ctx), typeutil.ProxyRole)
			_, err := s.Cluster.MilvusClient.HybridSearch(ctx, &milvuspb.HybridSearchRequest{
				CollectionName:        s.collection,
				Requests:              []*milvuspb.SearchRequest{sub},
				UseDefaultConsistency: true,
				RankParams:            []*commonpb.KeyValuePair{{Key: "limit", Value: "5"}},
				FunctionChains: []*schemapb.FunctionChain{{
					Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
					Ops:   []*schemapb.FunctionChainOp{{Op: "merge", Params: params}},
				}},
			})
			s.Require().NoError(err, "transport error")
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, withOneDenseHybrid(map[string]int64{tc.want: 1}))
		})
	}

	s.Run("search function chain decay", func() {
		before := counters(s.report(ctx), typeutil.ProxyRole)
		s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
			r.FunctionChains = []*schemapb.FunctionChain{{
				Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
				Ops: []*schemapb.FunctionChainOp{{
					Op:      "map",
					Outputs: []string{"$score"},
					Expr: &schemapb.FunctionChainExpr{
						Name: "decay",
						Args: []*schemapb.FunctionChainExprArg{{Arg: &schemapb.FunctionChainExprArg_Column{
							Column: &schemapb.FunctionChainColumnArg{Name: pkField},
						}}},
						Params: map[string]*schemapb.FunctionParamValue{
							"function": strParam("gauss"), "origin": numParam(0), "scale": numParam(100),
						},
					},
				}},
			}}
		})
		after := counters(s.report(ctx), typeutil.ProxyRole)
		requireOnlyDelta(s.T(), before, after, map[string]int64{"reranker=decay": 1})
	})

	s.Run("search function chain without a reranker folds to _other", func() {
		before := counters(s.report(ctx), typeutil.ProxyRole)
		s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
			r.FunctionChains = []*schemapb.FunctionChain{{
				Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
				Ops: []*schemapb.FunctionChainOp{{
					Op: "map", Outputs: []string{"$score"},
					Expr: &schemapb.FunctionChainExpr{
						Name: "round_decimal",
						Args: []*schemapb.FunctionChainExprArg{{Arg: &schemapb.FunctionChainExprArg_Column{
							Column: &schemapb.FunctionChainColumnArg{Name: "$score"},
						}}},
						Params: map[string]*schemapb.FunctionParamValue{"decimal": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 2}}},
					},
				}},
			}}
		})
		after := counters(s.report(ctx), typeutil.ProxyRole)
		requireOnlyDelta(s.T(), before, after, map[string]int64{"reranker=_other": 1})
	})

	s.Run("semantic highlighter", func() {
		before := counters(s.report(ctx), typeutil.ProxyRole)
		s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
			r.Highlighter = &commonpb.Highlighter{Type: commonpb.HighlightType_Semantic}
		})
		after := counters(s.report(ctx), typeutil.ProxyRole)
		requireOnlyDelta(s.T(), before, after, map[string]int64{"highlighter=Semantic": 1})
	})
}

// withOneDenseHybrid adds, to a case's expected counters, the hybrid search
// shape every hybridSearch call has: one dense vector subrequest.
func withOneDenseHybrid(want map[string]int64) map[string]int64 {
	out := map[string]int64{"hybrid_search_reqs|<=2": 1, "hybrid_search=dense_vector": 1}
	for k, v := range want {
		out[k] += v
	}
	return out
}

// hybridSearch issues a one-request hybrid search with the given rank params.
func (s *Suite) hybridSearch(ctx context.Context, rankParams []*commonpb.KeyValuePair, shape func(*milvuspb.SearchRequest)) {
	sub := s.baseSearchRequest()
	sub.UseDefaultConsistency = false
	if shape != nil {
		shape(sub)
	}
	_, err := s.Cluster.MilvusClient.HybridSearch(ctx, &milvuspb.HybridSearchRequest{
		CollectionName:        s.collection,
		Requests:              []*milvuspb.SearchRequest{sub},
		RankParams:            rankParams,
		UseDefaultConsistency: true,
	})
	s.Require().NoError(err, "transport error")
}
