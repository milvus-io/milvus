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

package entity

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

func TestFunctionChainWireContract(t *testing.T) {
	chain := NewFunctionChain(FunctionChainStageL2Rerank).WithName("rank").
		Map("$score", NewNumCombineExpr("weighted", Col("$score"), Col("popularity")).WithParam("weights", []float64{0.7, 0.3})).
		Sort(Col("$score"), true, Col("$id")).Limit(5, 1)
	actual, err := chain.ProtoMessage()
	require.NoError(t, err)
	// The public PyMilvus map/sort/limit protobuf shape, independent of Go builders.
	expected := &schemapb.FunctionChain{}
	require.NoError(t, protojson.Unmarshal([]byte(`{
 "name":"rank", "stage":"FunctionChainStageL2Rerank", "ops":[
 {"op":"map","outputs":["$score"],"expr":{"name":"num_combine","args":[{"column":{"name":"$score"}},{"column":{"name":"popularity"}}],"params":{"mode":{"stringValue":"weighted"},"weights":{"arrayValue":{"values":[{"doubleValue":0.7},{"doubleValue":0.3}]}}}}},
 {"op":"sort","inputs":["$score","$id"],"params":{"column":{"stringValue":"$score"},"desc":{"boolValue":true},"tie_break_col":{"stringValue":"$id"}}},
 {"op":"limit","params":{"limit":{"int64Value":"5"},"offset":{"int64Value":"1"}}}
 ]}`), expected))
	require.True(t, proto.Equal(expected, actual), "actual: %s", actual)
	wire, err := proto.Marshal(actual)
	require.NoError(t, err)
	decoded := &schemapb.FunctionChain{}
	require.NoError(t, proto.Unmarshal(wire, decoded))
	require.True(t, proto.Equal(expected, decoded))
}

func TestFunctionChainTypedParametersAndSnapshot(t *testing.T) {
	blob := []byte{0, 255, 1}
	nested := map[string]any{"array": []any{true, int64(math.MaxInt64), 1.25, "literal", blob, map[string]any{"x": uint16(7)}}}
	expr := NewFunctionChainExpr("custom", Col("field"), "a literal", int64(math.MinInt64)).WithParam("nested", nested)
	pb, err := expr.ProtoMessage()
	require.NoError(t, err)
	require.NotNil(t, pb.Args[0].GetColumn())
	require.Equal(t, "a literal", pb.Args[1].GetLiteral().GetStringValue())
	require.Equal(t, int64(math.MinInt64), pb.Args[2].GetLiteral().GetInt64Value())
	values := pb.Params["nested"].GetObjectValue().Fields["array"].GetArrayValue().Values
	require.True(t, values[0].GetBoolValue())
	require.Equal(t, int64(math.MaxInt64), values[1].GetInt64Value())
	require.Equal(t, 1.25, values[2].GetDoubleValue())
	require.Equal(t, int64(7), values[5].GetObjectValue().Fields["x"].GetInt64Value())
	blob[0] = 42
	nested["array"] = false
	require.Equal(t, []byte{0, 255, 1}, values[4].GetBytesValue())
	require.Len(t, pb.Params["nested"].GetObjectValue().Fields["array"].GetArrayValue().Values, 6)
}

func TestFunctionChainJSONHints(t *testing.T) {
	path := Col(`meta["price"]`).WithDataType(FieldTypeDouble)
	expr := NewFunctionChainExpr("custom", path, "literal", Col("$score"), path)
	op := &FunctionChainOp{Op: "map", Expr: expr, Outputs: []string{"out"}, Params: map[string]any{"other": true}}
	pb, err := op.ProtoMessage()
	require.NoError(t, err)
	hints := pb.Params[functionChainInputTypes].GetArrayValue().Values
	require.Len(t, hints, 3)
	require.Equal(t, int64(schemapb.DataType_Double), hints[0].GetInt64Value())
	require.Zero(t, hints[1].GetInt64Value())
	require.Equal(t, hints[0], hints[2])
	require.NotContains(t, op.Params, functionChainInputTypes)
	pb.Params["other"] = nil
	require.Equal(t, true, op.Params["other"])
	sorted, err := NewFunctionChain(FunctionChainStageL2Rerank).Sort(path, false, Col("$id")).ProtoMessage()
	require.NoError(t, err)
	require.Len(t, sorted.Ops[0].Params[functionChainInputTypes].GetArrayValue().Values, 2)
}

func TestFunctionChainInvalidValues(t *testing.T) {
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	for name, value := range map[string]any{
		"nil": nil, "pointer": new(int), "overflow": uint64(math.MaxUint64),
		"non_string_key": map[int]string{1: "x"}, "empty_key": map[string]any{"": 1},
		"nested_nil": []any{true, nil}, "cycle": cyclic, "struct": struct{}{},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewFunctionChainExpr("custom").WithParam("value", value).ProtoMessage()
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
	cases := map[string]*FunctionChain{
		"nil_chain":         nil,
		"unknown_stage":     NewFunctionChain(999),
		"nil_op":            NewFunctionChain(FunctionChainStageL2Rerank).AddOp(nil),
		"nil_expr":          NewFunctionChain(FunctionChainStageL2Rerank).Map("score", nil),
		"empty_column":      NewFunctionChain(FunctionChainStageL2Rerank).Map("score", NewRoundDecimalExpr(Col(""), 2)),
		"empty_output":      NewFunctionChain(FunctionChainStageL2Rerank).Map("", NewRoundDecimalExpr(Col("$score"), 2)),
		"limit":             NewFunctionChain(FunctionChainStageL2Rerank).Limit(0, 0),
		"offset":            NewFunctionChain(FunctionChainStageL2Rerank).Limit(1, -1),
		"system_hint":       NewFunctionChain(FunctionChainStageL2Rerank).Sort(Col("$score").WithDataType(FieldTypeDouble), true),
		"unsupported_hint":  NewFunctionChain(FunctionChainStageL2Rerank).Sort(Col("json").WithDataType(FieldTypeJSON), true),
		"conflicting_hint":  NewFunctionChain(FunctionChainStageL2Rerank).Map("x", NewFunctionChainExpr("custom", Col("x").WithDataType(FieldTypeDouble), Col("x"))),
		"exports_on_search": NewFunctionChain(FunctionChainStageL2Rerank).WithName("x").WithOutputColumns("x"),
		"unnamed_export":    NewFunctionChain(FunctionChainStageQueryScan).WithOutputColumns("x"),
		"duplicate_export":  NewFunctionChain(FunctionChainStageQueryScan).WithName("x").WithOutputColumns("x", "x"),
		"reserved_export":   NewFunctionChain(FunctionChainStageQueryScan).WithName("x").WithOutputColumns("_original_idx"),
	}
	for name, chain := range cases {
		t.Run(name, func(t *testing.T) { _, err := chain.ProtoMessage(); require.ErrorIs(t, err, merr.ErrParameterInvalid) })
	}
}

func TestFunctionChainExpressions(t *testing.T) {
	for name, expr := range map[string]*FunctionChainExpr{
		"decay":   NewDecayExpr(Col("ts"), "exp", 100, 10),
		"combine": NewNumCombineExpr("sum", Col("$score"), Col("popularity")),
		"round":   NewRoundDecimalExpr(Col("$score"), 4),
		"model":   NewRerankModelExpr(Col("text"), "query").WithParam("provider", "voyageai"),
		"xgboost": NewXGBoostExpr("model", Col("f1"), Col("f2")).WithParam("output", "raw"),
		"py_udf":  NewPyUDFExpr("wheel", map[string]any{"nested": []int{1, 2}}, Col("text")),
	} {
		t.Run(name, func(t *testing.T) { _, err := expr.ProtoMessage(); require.NoError(t, err) })
	}
	for name, expr := range map[string]*FunctionChainExpr{
		"mode":                NewNumCombineExpr("bad", Col("a"), Col("b")),
		"missing_weights":     NewNumCombineExpr("weighted", Col("a"), Col("b")),
		"extra_weights":       NewNumCombineExpr("sum", Col("a"), Col("b")).WithParam("weights", []float64{1, 1}),
		"non_numeric_weights": NewNumCombineExpr("weighted", Col("a"), Col("b")).WithParam("weights", []any{true, 1}),
		"decimal":             NewRoundDecimalExpr(Col("$score"), 7),
		"decay":               NewDecayExpr(Col("x"), "unknown", 0, 1),
		"nonfinite":           NewDecayExpr(Col("x"), "gauss", math.Inf(1), 1),
		"queries":             NewRerankModelExpr(Col("text")),
		"model":               NewXGBoostExpr("", Col("x")),
		"output":              NewXGBoostExpr("model", Col("x")).WithParam("output", "wrong"),
		"udf":                 NewPyUDFExpr("", nil, Col("x")),
	} {
		t.Run(name, func(t *testing.T) { _, err := expr.ProtoMessage(); require.ErrorIs(t, err, merr.ErrParameterInvalid) })
	}
}

func TestFunctionChainGenericExpressionPreservesServerDefaults(t *testing.T) {
	// The server defaults an omitted num_combine mode to multiply. The generic
	// expression must preserve this rather than imposing helper defaults.
	pb, err := NewFunctionChainExpr("num_combine", Col("a"), Col("b")).ProtoMessage()
	require.NoError(t, err)
	require.Empty(t, pb.Params)
	helper := NewRoundDecimalExpr(Col("a"), 2)
	helper.Args = []any{"literal"}
	_, err = helper.ProtoMessage()
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}
