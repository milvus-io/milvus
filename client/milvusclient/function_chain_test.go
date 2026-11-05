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

package milvusclient

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

func scoreChain(stage entity.FunctionChainStage) *entity.FunctionChain {
	return entity.NewFunctionChain(stage).Map("$score", entity.NewRoundDecimalExpr(entity.Col("$score"), 3))
}

func TestFunctionChainSearchRequest(t *testing.T) {
	l0, l1, l2 := scoreChain(entity.FunctionChainStageL0Rerank), scoreChain(entity.FunctionChainStageL1Rerank), scoreChain(entity.FunctionChainStageL2Rerank)
	opt := NewSearchOption("articles", 10, []entity.Vector{entity.FloatVector{1, 2}}).WithFunctionChains(l2, l0, l1)
	req, err := opt.Request()
	require.NoError(t, err)
	require.Len(t, req.FunctionChains, 3)
	require.Equal(t, entity.FunctionChainStageL0Rerank, req.FunctionChains[0].Stage)
	require.Equal(t, entity.FunctionChainStageL2Rerank, req.FunctionChains[2].Stage)
	l0.Ops[0].Outputs[0] = "changed"
	require.Equal(t, "$score", req.FunctionChains[0].Ops[0].Outputs[0])
	req, err = opt.WithFunctionChains().Request()
	require.NoError(t, err)
	require.Empty(t, req.FunctionChains)
}

func TestFunctionChainRequestValidation(t *testing.T) {
	l0, l1, l2 := scoreChain(entity.FunctionChainStageL0Rerank), scoreChain(entity.FunctionChainStageL1Rerank), scoreChain(entity.FunctionChainStageL2Rerank)
	fs := entity.NewFunctionScore().AddFunction(entity.NewFunction().WithType(entity.FunctionTypeRerank))
	search := func() *searchOption { return NewSearchOption("c", 10, []entity.Vector{entity.FloatVector{1, 2}}) }
	ann := func() *AnnRequest { return NewAnnRequest("v", 10, entity.FloatVector{1, 2}) }
	query := func() *entity.FunctionChain {
		return entity.NewFunctionChain(entity.FunctionChainStageQueryScan).Grep(entity.Col("text"), "-e hi", "matches")
	}
	cases := map[string]func() error{
		"nil": func() error { _, e := search().WithFunctionChains(nil).Request(); return e },
		"empty": func() error {
			_, e := search().WithFunctionChains(entity.NewFunctionChain(entity.FunctionChainStageL2Rerank)).Request()
			return e
		},
		"reserved_stage": func() error {
			_, e := search().WithFunctionChains(scoreChain(entity.FunctionChainStageIngestion)).Request()
			return e
		},
		"duplicate_stage":  func() error { _, e := search().WithFunctionChains(l2, l2).Request(); return e },
		"score_then_chain": func() error { _, e := search().WithFunctionScore(fs).WithFunctionChains(l2).Request(); return e },
		"chain_then_score": func() error { _, e := search().WithFunctionChains(l2).WithFunctionScore(fs).Request(); return e },
		"legacy_function": func() error {
			_, e := search().WithFunctionChains(l2).WithFunctionReranker(fs.Functions[0]).Request()
			return e
		},
		"order_by": func() error {
			_, e := search().WithFunctionChains(l2).WithSearchParam(spOrderByFields, "id").Request()
			return e
		},
		"iterator": func() error {
			_, e := search().WithFunctionChains(l2).WithSearchParam(IteratorSearchV2Key, "true").Request()
			return e
		},
		"merge_in_search": func() error {
			_, e := search().WithFunctionChains(entity.NewFunctionChain(entity.FunctionChainStageL2Rerank).Merge("rrf", nil)).Request()
			return e
		},
		"l0_sort": func() error {
			_, e := search().WithFunctionChains(entity.NewFunctionChain(entity.FunctionChainStageL0Rerank).Sort(entity.Col("$score"), true)).Request()
			return e
		},
		"l1_pyudf": func() error {
			_, e := search().WithFunctionChains(entity.NewFunctionChain(entity.FunctionChainStageL1Rerank).Map("x", entity.NewPyUDFExpr("wheel", nil, entity.Col("text")))).Request()
			return e
		},
		"hybrid_top_l0": func() error {
			_, e := NewHybridSearchOption("c", 10, ann()).WithFunctionChains(l0).HybridRequest()
			return e
		},
		"hybrid_nested_l2": func() error {
			_, e := NewHybridSearchOption("c", 10, ann().WithFunctionChains(l2)).HybridRequest()
			return e
		},
		"hybrid_nested_score": func() error {
			_, e := NewHybridSearchOption("c", 10, ann().WithFunctionChains(l1)).WithFunctionScore(fs).HybridRequest()
			return e
		},
		"hybrid_missing_merge": func() error {
			_, e := NewHybridSearchOption("c", 10, ann()).WithFunctionChains(l2).HybridRequest()
			return e
		},
		"hybrid_legacy_ranker": func() error {
			_, e := NewHybridSearchOption("c", 10, ann()).WithFunctionChains(entity.NewFunctionChain(entity.FunctionChainStageL2Rerank).Merge("rrf", nil)).WithReranker(NewRRFReranker()).HybridRequest()
			return e
		},
		"query_search_stage": func() error { _, e := NewQueryOption("c").WithFunctionChains(l2).Request(); return e },
		"query_count": func() error {
			_, e := NewQueryOption("c").WithOutputFields("count(*)").WithFunctionChains(query()).Request()
			return e
		},
		"query_order": func() error {
			_, e := NewQueryOption("c").WithOrderByFields("id").WithFunctionChains(query()).Request()
			return e
		},
		"query_multiple_grep": func() error {
			_, e := NewQueryOption("c").WithFunctionChains(query().Grep(entity.Col("text"), "x", "more")).Request()
			return e
		},
		"query_invalid_export": func() error {
			_, e := NewQueryOption("c").WithFunctionChains(query().WithName("grep").WithOutputColumns("unknown")).Request()
			return e
		},
	}
	for name, call := range cases {
		t.Run(name, func(t *testing.T) { require.ErrorIs(t, call(), merr.ErrParameterInvalid) })
	}
	t.Run("query_json_path_with_parenthesis", func(t *testing.T) {
		_, err := NewQueryOption("c").WithOutputFields(`metadata["a(b)"]`).WithFunctionChains(query()).Request()
		require.NoError(t, err)
	})
}

func TestFunctionChainHybridRequest(t *testing.T) {
	for _, strategy := range []string{"rrf", "weighted", "max", "sum", "avg"} {
		t.Run(strategy, func(t *testing.T) {
			params := map[string]any{}
			if strategy == "weighted" || strategy == "rrf" {
				params["weights"] = []float64{0.7, 0.3}
			}
			chain := entity.NewFunctionChain(entity.FunctionChainStageL2Rerank).Merge(strategy, params).Sort(entity.Col("$score"), true).Limit(3, 1)
			a := NewAnnRequest("v", 10, entity.FloatVector{1, 2}).WithFunctionChains(scoreChain(entity.FunctionChainStageL1Rerank), scoreChain(entity.FunctionChainStageL0Rerank))
			b := NewAnnRequest("v", 10, entity.FloatVector{2, 1})
			req, err := NewHybridSearchOption("c", 20, a, b).WithFunctionChains(chain).WithOffset(2).HybridRequest()
			require.NoError(t, err)
			require.Len(t, req.FunctionChains, 1)
			require.Len(t, req.Requests[0].FunctionChains, 2)
			require.Empty(t, req.Requests[1].FunctionChains)
			require.Equal(t, entity.FunctionChainStageL0Rerank, req.Requests[0].FunctionChains[0].Stage)
			require.NotContains(t, entity.KvPairsMap(req.RankParams), "strategy")
			require.Equal(t, "20", entity.KvPairsMap(req.RankParams)["limit"])
			require.Equal(t, "2", entity.KvPairsMap(req.RankParams)["offset"])
		})
	}
	for name, params := range map[string]map[string]any{
		"missing": {}, "length": {"weights": []float64{1}}, "negative": {"weights": []float64{-1, 1}},
		"nan": {"weights": []float64{math.NaN(), 1}}, "wrong_type": {"weights": []string{"1", "0"}},
		"unknown": {"weights": []float64{1, 0}, "metrics": "IP"},
	} {
		t.Run(name, func(t *testing.T) {
			chain := entity.NewFunctionChain(entity.FunctionChainStageL2Rerank).Merge("weighted", params)
			_, err := NewHybridSearchOption("c", 10, NewAnnRequest("v", 10), NewAnnRequest("v", 10)).WithFunctionChains(chain).HybridRequest()
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
	// Nested chains can coexist with an explicit legacy fusion ranker.
	_, err := NewHybridSearchOption("c", 10, NewAnnRequest("v", 10, entity.FloatVector{1, 2}).WithFunctionChains(scoreChain(entity.FunctionChainStageL0Rerank))).WithReranker(NewRRFReranker()).HybridRequest()
	require.NoError(t, err)
}

func (s *ReadSuite) TestFunctionChainRPC() {
	name := "function_chain_rpc"
	s.setupCache(name, s.schema)
	chain := scoreChain(entity.FunctionChainStageL2Rerank).Limit(1, 0)
	expected, err := chain.ProtoMessage()
	s.Require().NoError(err)
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Require().Len(req.FunctionChains, 1)
		s.True(proto.Equal(expected, req.FunctionChains[0]))
		return &milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{
			NumQueries: 2, Topks: []int64{1, 0}, Scores: []float32{0.25},
			Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{42}}}},
		}}, nil
	}).Once()
	results, err := s.client.Search(s.T().Context(), NewSearchOption(name, 10, []entity.Vector{entity.FloatVector{1, 2}, entity.FloatVector{2, 1}}).WithFunctionChains(chain))
	s.Require().NoError(err)
	s.Require().Len(results, 2)
	s.Equal([]float32{0.25}, results[0].Scores)
	s.Equal(1, results[0].ResultCount)
	s.NoError(results[0].Err)
	s.Zero(results[1].ResultCount)
	s.NoError(results[1].Err)
}

func (s *ReadSuite) TestFunctionChainQueryComputedResults() {
	name := "function_chain_query"
	s.setupCache(name, s.schema)
	for _, rows := range []int{0, 1} {
		s.Run(map[int]string{0: "empty", 1: "matches"}[rows], func() {
			data := [][]byte{[]byte(`[{"content":"timeout","line_number":1,"starts":[0],"ends":[7]}]`)}[:rows]
			ids := []int64{42}[:rows]
			s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
				s.Require().Len(req.FunctionChains, 1)
				s.Equal([]string{"matches"}, req.FunctionChains[0].OutputColumns)
				s.Equal("-i -e timeout", req.FunctionChains[0].Ops[0].Params["arguments"].GetStringValue())
				return &milvuspb.QueryResults{Status: merr.Success(), FieldsData: []*schemapb.FieldData{
					column.NewColumnInt64("ID", ids).FieldData(), column.NewColumnJSONBytes("matches", data).FieldData(),
				}}, nil
			}).Once()
			chain := entity.NewFunctionChain(entity.FunctionChainStageQueryScan).WithName("scan").WithOutputColumns("matches").Grep(entity.Col("text"), "-i -e timeout", "matches")
			result, err := s.client.Query(s.T().Context(), NewQueryOption(name).WithOutputFields("ID").WithFunctionChains(chain))
			s.Require().NoError(err)
			s.Equal(rows, result.ResultCount)
			s.Require().NotNil(result.GetColumn("matches"))
			s.Equal(rows, result.GetColumn("matches").Len())
			if rows > 0 {
				s.Equal(data, result.GetColumn("matches").(*column.ColumnJSONBytes).Data())
			}
		})
	}
}
