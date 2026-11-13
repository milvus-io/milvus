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

package functionchain

import (
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestPythonParity(t *testing.T) {
	chains := map[string]*FunctionChain{
		"rerank": NewFunctionChain(L2Rerank).WithName("fresh_popular").
			Map("freshness", Decay(Col("published_at"), "exp", 1700000000, 86400)).
			Map("$score", NumCombine(Col("$score"), Col("freshness")).WithParam("mode", "weighted").WithParam("weights", []float64{0.7, 0.3})).
			Map("$score", RoundDecimal(Col("$score"), 4)).SortBy(Col("$score"), true, Col("$id")).Limit(10, 2),
		"grep":     NewFunctionChain(QueryScan).WithName("grep").WithOutputColumns("matches").Grep("content", "-i -e 'timeout'"),
		"rrf":      NewFunctionChain(L2Rerank).Merge("rrf", MergeOptions{Weights: []float64{0.7, 0.3}}),
		"weighted": NewFunctionChain(L2Rerank).Merge("weighted", MergeOptions{Weights: []float64{0.7, 0.3}}),
		"xgboost":  NewFunctionChain(L0Rerank).Map("$score", XGBoost("model", Col("price"), Col("popularity")).WithParam("output", "raw")),
		"model":    NewFunctionChain(L2Rerank).Map("$score", RerankModel(Col("text"), []string{"query"}).WithParam("provider", "voyageai")),
		"literals": NewFunctionChain(PostProcess).Map("out", NewExpr("custom", Col("text"), "literal", true, int64(-12), 1.25, []byte{0, 255}, []any{1, "x"}, map[string]any{"nested": []bool{true, false}})),
	}
	for name, c := range chains {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + name + ".json")
			require.NoError(t, err)
			expected := &schemapb.FunctionChain{}
			require.NoError(t, protojson.Unmarshal(data, expected))
			actual, err := c.ProtoMessage()
			require.NoError(t, err)
			require.True(t, proto.Equal(expected, actual), "expected %s, got %s", expected, actual)
		})
	}
}

func TestValidation(t *testing.T) {
	tests := map[string]*FunctionChain{
		"stage":        NewFunctionChain(Stage(123)),
		"empty column": NewFunctionChain(L2Rerank).Map("x", NewExpr("custom", Col(""))),
		"map":          NewFunctionChain(L2Rerank).Map("", NewExpr("x")),
		"nil expr":     NewFunctionChain(L2Rerank).Map("x", nil),
		"limit":        NewFunctionChain(L2Rerank).Limit(0),
		"offset":       NewFunctionChain(L2Rerank).Limit(1, -1),
		"sort":         NewFunctionChain(L2Rerank).Sort(""),
		"tie":          NewFunctionChain(L2Rerank).SortBy("x", true, ""),
		"grep stage":   NewFunctionChain(L2Rerank).Grep("text", "x"),
		"output stage": NewFunctionChain(L2Rerank).WithName("x").WithOutputColumns("out"),
		"output name":  NewFunctionChain(QueryScan).WithOutputColumns("out"),
		"duplicate":    NewFunctionChain(QueryScan).WithName("x").WithOutputColumns("out", "out"),
		"reserved":     NewFunctionChain(QueryScan).WithName("x").WithOutputColumns("_original_idx"),
		"nil literal":  NewFunctionChain(L2Rerank).Map("x", NewExpr("x", nil)),
		"overflow":     NewFunctionChain(L2Rerank).Map("x", NewExpr("x", uint64(math.MaxUint64))),
		"object key":   NewFunctionChain(L2Rerank).Map("x", NewExpr("x", map[string]any{"": 1})),
		"unsupported":  NewFunctionChain(L2Rerank).Map("x", NewExpr("x", make(chan int))),
	}
	var nilChain *FunctionChain
	tests["nil chain"] = nilChain
	for name, c := range tests {
		t.Run(name, func(t *testing.T) { _, err := c.ProtoMessage(); require.Error(t, err) })
	}
	cycle := map[string]any{}
	cycle["self"] = cycle
	_, err := NewExpr("x", cycle).ProtoMessage()
	require.Error(t, err)
}

func TestHelperValidation(t *testing.T) {
	for _, e := range []*Expr{NumCombine(Col("a")), NumCombine(Col("a"), Col("b")).WithParam("mode", "weighted"), NumCombine(Col("a"), Col("b")).WithParam("weights", []float64{1, 2}), NumCombine(Col("a"), Col("b")).WithParam("mode", "weighted").WithParam("weights", []bool{true, false}), Decay(Col("x"), "bad", 0, 1), Decay(Col("x"), "exp", true, 1), RoundDecimal(Col("x"), 7), XGBoost("", Col("x")), XGBoost("model"), XGBoost("model", Col("x")).WithParam("output", "bad"), RerankModel(Col("x"), nil), RerankModel(Col("x"), []string{""})} {
		_, err := e.ProtoMessage()
		require.Error(t, err)
	}
}

func TestSerializationSnapshot(t *testing.T) {
	b := []byte{1, 2}
	c := NewFunctionChain(L2Rerank).Map("x", NewExpr("custom", b)).Sort("x").Limit(2)
	p, err := c.ProtoMessage()
	require.NoError(t, err)
	b[0] = 9
	c.Limit(1)
	p.Ops[1].Inputs[0] = "changed"
	require.Equal(t, byte(1), p.Ops[0].Expr.Args[0].GetLiteral().GetBytesValue()[0])
	q, err := c.ProtoMessage()
	require.NoError(t, err)
	require.Equal(t, "x", q.Ops[1].Inputs[0])
	require.Len(t, p.Ops, 3)
	require.Len(t, q.Ops, 4)
	k := 0
	norm := false
	p, err = NewFunctionChain(L2Rerank).Merge("rrf", MergeOptions{K: &k, NormScore: &norm}).ProtoMessage()
	require.NoError(t, err)
	require.Zero(t, p.Ops[0].Params["k"].GetInt64Value())
	require.False(t, p.Ops[0].Params["norm_score"].GetBoolValue())
}

func TestIntegerWeightsNormalizeToDouble(t *testing.T) {
	p, err := NumCombine(Col("a"), Col("b")).WithParam("mode", "weighted").WithParam("weights", []any{1, 2}).ProtoMessage()
	require.NoError(t, err)
	require.IsType(t, &schemapb.FunctionParamValue_DoubleValue{}, p.Params["weights"].GetArrayValue().Values[0].Value)
	require.Equal(t, 2.0, p.Params["weights"].GetArrayValue().Values[1].GetDoubleValue())
}
