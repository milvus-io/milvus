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
			want: map[string]int64{"expr_use_json_stats": 1},
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
			name: "legacy weighted strategy with norm_score",
			rankParams: []*commonpb.KeyValuePair{
				{Key: "strategy", Value: "weighted"},
				{Key: "params", Value: `{"weights": [1.0]}`},
				{Key: "norm_score", Value: "true"},
				{Key: "limit", Value: "5"},
			},
			want: map[string]int64{"strategy=weighted": 1, "norm_score": 1},
		},
	} {
		s.Run(tc.name, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			s.hybridSearch(ctx, tc.rankParams, nil)
			after := counters(s.report(ctx), typeutil.ProxyRole)
			requireOnlyDelta(s.T(), before, after, tc.want)
		})
	}

	for _, reranker := range []string{"rrf", "weighted", "model", "boost"} {
		s.Run("function_score="+reranker, func() {
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
			requireOnlyDelta(s.T(), before, after, map[string]int64{"function_score=" + reranker: 1})
		})
	}

	s.Run("semantic highlighter", func() {
		before := counters(s.report(ctx), typeutil.ProxyRole)
		s.searchWith(ctx, func(r *milvuspb.SearchRequest) {
			r.Highlighter = &commonpb.Highlighter{Type: commonpb.HighlightType_Semantic}
		})
		after := counters(s.report(ctx), typeutil.ProxyRole)
		requireOnlyDelta(s.T(), before, after, map[string]int64{"highlighter=Semantic": 1})
	})
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
