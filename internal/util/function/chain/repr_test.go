/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	chainexpr "github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

// MockFunctionExpr is a mock implementation of FunctionExpr for testing.
type MockFunctionExpr struct {
	name string
}

func (m *MockFunctionExpr) Name() string {
	return m.name
}

func (m *MockFunctionExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

func (m *MockFunctionExpr) IsRunnable(stage string) bool {
	return true
}

func (m *MockFunctionExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return inputs, nil
}

// MockBooleanFunctionExpr is a mock implementation that returns boolean type.
type MockBooleanFunctionExpr struct {
	name string
}

func (m *MockBooleanFunctionExpr) Name() string {
	return m.name
}

func (m *MockBooleanFunctionExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.FixedWidthTypes.Boolean}
}

func (m *MockBooleanFunctionExpr) IsRunnable(stage string) bool {
	return true
}

func (m *MockBooleanFunctionExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return inputs, nil
}

func init() {
	// Register mock_filter function for testing
	types.RegisterFunction("mock_filter", func(_ types.FunctionBuildContext, _ types.FunctionConfig) (types.FunctionExpr, error) {
		return &MockBooleanFunctionExpr{name: "mock_filter"}, nil
	})
}

func TestParseFuncChainProto_BasicOperators(t *testing.T) {
	pb := &schemapb.FunctionChain{
		Name:  "test-chain",
		Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
		Ops: []*schemapb.FunctionChainOp{
			{
				Op: "filter",
				Expr: &schemapb.FunctionChainExpr{
					Name:   "mock_filter",
					Args:   []*schemapb.FunctionChainExprArg{columnArg("score")},
					Params: map[string]*schemapb.FunctionParamValue{},
				},
			},
			{
				Op: "select",
				Params: map[string]*schemapb.FunctionParamValue{
					"columns": arrayParam(stringParam("id"), stringParam("score"), stringParam("name")),
				},
			},
			{
				Op:     "sort",
				Inputs: []string{"score"},
				Params: map[string]*schemapb.FunctionParamValue{
					"desc": boolParam(true),
				},
			},
			{
				Op: "limit",
				Params: map[string]*schemapb.FunctionParamValue{
					"limit":  intParam(10),
					"offset": intParam(5),
				},
			},
		},
	}

	chain, err := ParseFuncChainProto(pb, memory.NewGoAllocator())
	assert.NoError(t, err)
	assert.NotNil(t, chain)
	assert.Equal(t, "test-chain", chain.name)
	assert.Equal(t, types.StageL2Rerank, chain.Stage())
	assert.Len(t, chain.operators, 4)

	assert.IsType(t, &FilterOp{}, chain.operators[0])
	assert.IsType(t, &SelectOp{}, chain.operators[1])
	assert.IsType(t, &SortOp{}, chain.operators[2])
	assert.IsType(t, &LimitOp{}, chain.operators[3])
}

func TestParseFuncChainProto_UnknownOperator(t *testing.T) {
	_, err := ParseFuncChainProto(&schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
		Ops: []*schemapb.FunctionChainOp{
			{Op: "unknown_op"},
		},
	}, memory.NewGoAllocator())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown operator type")
}

func TestParseFuncChainProto_MissingStage(t *testing.T) {
	_, err := ParseFuncChainProto(&schemapb.FunctionChain{
		Ops: []*schemapb.FunctionChainOp{
			{
				Op: "select",
				Params: map[string]*schemapb.FunctionParamValue{
					"columns": arrayParam(stringParam("test")),
				},
			},
		},
	}, memory.NewGoAllocator())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported function chain stage")
}

func TestParseFuncChainProto_MissingParams(t *testing.T) {
	testCases := []struct {
		name   string
		chain  *schemapb.FunctionChain
		errMsg string
	}{
		{
			name: "filter missing function",
			chain: &schemapb.FunctionChain{
				Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
				Ops:   []*schemapb.FunctionChainOp{{Op: "filter"}},
			},
			errMsg: "filter_op: function is required",
		},
		{
			name: "filter missing inputs",
			chain: &schemapb.FunctionChain{
				Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
				Ops: []*schemapb.FunctionChainOp{{
					Op:   "filter",
					Expr: &schemapb.FunctionChainExpr{Name: "mock_filter"},
				}},
			},
			errMsg: "filter_op: inputs is required",
		},
		{
			name: "sort missing column",
			chain: &schemapb.FunctionChain{
				Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
				Ops:   []*schemapb.FunctionChainOp{{Op: "sort"}},
			},
			errMsg: "sort_op: column is required",
		},
		{
			name: "select missing columns",
			chain: &schemapb.FunctionChain{
				Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
				Ops:   []*schemapb.FunctionChainOp{{Op: "select"}},
			},
			errMsg: "select_op: columns is required",
		},
		{
			name: "limit invalid limit",
			chain: &schemapb.FunctionChain{
				Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
				Ops: []*schemapb.FunctionChainOp{{
					Op: "limit",
					Params: map[string]*schemapb.FunctionParamValue{
						"limit": intParam(0),
					},
				}},
			},
			errMsg: "limit_op: limit must be positive",
		},
		{
			name: "map missing function",
			chain: &schemapb.FunctionChain{
				Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
				Ops:   []*schemapb.FunctionChainOp{{Op: "map"}},
			},
			errMsg: "map operator requires function",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseFuncChainProto(tc.chain, memory.NewGoAllocator())
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestParseFuncChainProto_UnknownFunction(t *testing.T) {
	_, err := ParseFuncChainProto(&schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
		Ops: []*schemapb.FunctionChainOp{
			{
				Op: "map",
				Expr: &schemapb.FunctionChainExpr{
					Name:   "UNKNOWN_FUNC",
					Params: map[string]*schemapb.FunctionParamValue{},
				},
			},
		},
	}, memory.NewGoAllocator())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown function")
}

func TestParseFuncChainProto_RoundDecimalFunction(t *testing.T) {
	chain, err := ParseFuncChainProto(&schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
		Ops: []*schemapb.FunctionChainOp{
			{
				Op:      types.OpTypeMap,
				Outputs: []string{types.ScoreFieldName},
				Expr: &schemapb.FunctionChainExpr{
					Name: "round_decimal",
					Args: []*schemapb.FunctionChainExprArg{columnArg(types.ScoreFieldName)},
					Params: map[string]*schemapb.FunctionParamValue{
						"decimal": intParam(2),
					},
				},
			},
		},
	}, memory.NewGoAllocator())
	require.NoError(t, err)
	require.NotNil(t, chain)
	assert.Len(t, chain.operators, 1)
	assert.IsType(t, &MapOp{}, chain.operators[0])
}

func TestProtoOpToReprDerivesInputsFromExprArgs(t *testing.T) {
	repr, err := ProtoOpToRepr(&schemapb.FunctionChainOp{
		Op: types.OpTypeMap,
		Expr: &schemapb.FunctionChainExpr{
			Name: "expr",
			Args: []*schemapb.FunctionChainExprArg{
				columnArg("$score"),
				literalArg(intParam(100)),
				columnArg("tag"),
				columnArg("$score"),
			},
			Params: map[string]*schemapb.FunctionParamValue{
				"expr": stringParam("($0 > $1) && ($2 != \"dog\")"),
			},
		},
		Outputs: []string{"new_score"},
	})
	require.NoError(t, err)
	require.NotNil(t, repr)
	assert.Equal(t, []string{"$score", "tag"}, repr.Inputs)
	assert.Equal(t, []string{"new_score"}, repr.Outputs)
	require.NotNil(t, repr.Function)
	assert.Equal(t, "expr", repr.Function.Name)
	require.Len(t, repr.Function.Args, 4)
	assert.Equal(t, "($0 > $1) && ($2 != \"dog\")", repr.Function.Params["expr"].GetStringValue())
}

func TestProtoChainToReprBuildsInfo(t *testing.T) {
	repr, err := ProtoChainToRepr(&schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
		Ops: []*schemapb.FunctionChainOp{
			{
				Op: types.OpTypeMap,
				Expr: &schemapb.FunctionChainExpr{
					Name: "decay",
					Args: []*schemapb.FunctionChainExprArg{columnArg("ts")},
				},
				Outputs: []string{"score1"},
			},
			{
				Op: types.OpTypeMap,
				Expr: &schemapb.FunctionChainExpr{
					Name: "sum",
					Args: []*schemapb.FunctionChainExprArg{
						columnArg("score1"),
						columnArg("$score"),
					},
				},
				Outputs: []string{"$score"},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, repr)
	assert.Equal(t, []string{"ts", "$score"}, repr.Info.RequiredInputs)
	assert.Equal(t, []string{"score1", "$score"}, repr.Info.WrittenNames)
	require.Len(t, repr.Info.Ops, 2)
	assert.Equal(t, []string{"ts"}, repr.Info.Ops[0].ReadNames)
	assert.Equal(t, []string{"score1"}, repr.Info.Ops[0].WriteNames)
	assert.Equal(t, []string{"score1", "$score"}, repr.Info.Ops[1].ReadNames)
	assert.Equal(t, []string{"$score"}, repr.Info.Ops[1].WriteNames)
}

func TestProtoStageToReprStage(t *testing.T) {
	tests := []struct {
		stage    schemapb.FunctionChainStage
		expected string
	}{
		{schemapb.FunctionChainStage_FunctionChainStageIngestion, types.StageIngestion},
		{schemapb.FunctionChainStage_FunctionChainStagePreProcess, types.StagePreProcess},
		{schemapb.FunctionChainStage_FunctionChainStageL0Rerank, types.StageL0Rerank},
		{schemapb.FunctionChainStage_FunctionChainStageL1Rerank, types.StageL1Rerank},
		{schemapb.FunctionChainStage_FunctionChainStageL2Rerank, types.StageL2Rerank},
		{schemapb.FunctionChainStage_FunctionChainStagePostProcess, types.StagePostProcess},
	}
	for _, tc := range tests {
		t.Run(tc.stage.String(), func(t *testing.T) {
			stage, err := ProtoStageToReprStage(tc.stage)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, stage)
		})
	}

	_, err := ProtoStageToReprStage(schemapb.FunctionChainStage_FunctionChainStageUnspecified)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported function chain stage")
}

func TestIsFunctionChainSystemName(t *testing.T) {
	assert.True(t, IsFunctionChainSystemName("$score"))
	assert.True(t, IsFunctionChainSystemName("$id"))
	assert.False(t, IsFunctionChainSystemName("score"))
	assert.False(t, IsFunctionChainSystemName(""))
}

func TestFunctionChainExprArgInput(t *testing.T) {
	input, err := FunctionChainExprArgInput(columnArg(" ts "))
	require.NoError(t, err)
	assert.Equal(t, "ts", input)

	input, err = FunctionChainExprArgInput(literalArg(stringParam("value")))
	require.NoError(t, err)
	assert.Empty(t, input)

	_, err = FunctionChainExprArgInput(nil)
	assert.ErrorContains(t, err, "function chain expr arg is nil")

	_, err = FunctionChainExprArgInput(columnArg(" "))
	assert.ErrorContains(t, err, "column name is empty")

	_, err = FunctionChainExprArgInput(&schemapb.FunctionChainExprArg{})
	assert.ErrorContains(t, err, "function chain expr arg is unset")
}

func TestProtoConversionErrors(t *testing.T) {
	_, err := ProtoChainToRepr(nil)
	assert.ErrorContains(t, err, "function chain proto is nil")

	_, err = ProtoOpToRepr(nil)
	assert.ErrorContains(t, err, "op proto is nil")

	_, err = ProtoOpToRepr(&schemapb.FunctionChainOp{Op: " "})
	assert.ErrorContains(t, err, "op name is empty")

	_, err = ProtoOpToRepr(&schemapb.FunctionChainOp{Op: "map", Inputs: []string{" "}})
	assert.ErrorContains(t, err, "input name is empty")

	_, _, err = ProtoExprToRepr(&schemapb.FunctionChainExpr{Name: " "})
	assert.ErrorContains(t, err, "expr name is empty")
}

func TestFunctionFromReprWithContextPassesTypedConfigAndContext(t *testing.T) {
	funcName := "mock_context_function"
	extraInfo := &models.ModelExtraInfo{ClusterID: "cluster-1", DBName: "db-1", BatchFactor: 7}
	params := map[string]*schemapb.FunctionParamValue{"name": stringParam("custom")}
	args := []*schemapb.FunctionChainExprArg{columnArg("text"), literalArg(intParam(10))}

	called := false
	err := types.RegisterFunction(funcName, func(ctx types.FunctionBuildContext, cfg types.FunctionConfig) (types.FunctionExpr, error) {
		called = true
		assert.Same(t, extraInfo, ctx.ModelExtraInfo)
		assert.Equal(t, funcName, cfg.Name)
		assert.Same(t, params["name"], cfg.Params["name"])
		require.Len(t, cfg.Args, 2)
		assert.Same(t, args[0], cfg.Args[0])
		assert.Same(t, args[1], cfg.Args[1])
		return &MockFunctionExpr{name: cfg.Name}, nil
	})
	require.NoError(t, err)

	fn, err := FunctionFromReprWithContext(&FunctionRepr{
		Name:   funcName,
		Params: params,
		Args:   args,
	}, types.FunctionBuildContext{ModelExtraInfo: extraInfo})
	require.NoError(t, err)
	require.NotNil(t, fn)
	assert.True(t, called)
	assert.Equal(t, funcName, fn.Name())
}

func TestFunctionFromReprWithContextRejectsLiteralArgsByDefault(t *testing.T) {
	_, err := FunctionFromReprWithContext(&FunctionRepr{
		Name: chainexpr.NumCombineFuncName,
		Args: []*schemapb.FunctionChainExprArg{
			columnArg("score"),
			literalArg(intParam(10)),
		},
	}, types.FunctionBuildContext{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "num_combine: literal expr arg[1] is not supported")
}

func TestFunctionFromReprWithContextErrors(t *testing.T) {
	_, err := FunctionFromReprWithContext(nil, types.FunctionBuildContext{})
	assert.ErrorContains(t, err, "function repr is nil")

	_, err = FunctionFromReprWithContext(&FunctionRepr{}, types.FunctionBuildContext{})
	assert.ErrorContains(t, err, "function name is required")
}

func TestFuncChainFromReprWithContextPassesBuildContext(t *testing.T) {
	funcName := "mock_chain_context_function"
	extraInfo := &models.ModelExtraInfo{ClusterID: "cluster-2", DBName: "db-2", BatchFactor: 11}
	params := map[string]*schemapb.FunctionParamValue{"flag": boolParam(true)}
	args := []*schemapb.FunctionChainExprArg{columnArg("score")}

	called := false
	err := types.RegisterFunction(funcName, func(ctx types.FunctionBuildContext, cfg types.FunctionConfig) (types.FunctionExpr, error) {
		called = true
		assert.Same(t, extraInfo, ctx.ModelExtraInfo)
		assert.Equal(t, funcName, cfg.Name)
		assert.Same(t, params["flag"], cfg.Params["flag"])
		require.Len(t, cfg.Args, 1)
		assert.Same(t, args[0], cfg.Args[0])
		return &MockFunctionExpr{name: cfg.Name}, nil
	})
	require.NoError(t, err)

	repr := &ChainRepr{
		Name:  "context-chain",
		Stage: types.StageL2Rerank,
		Operators: []OperatorRepr{
			{
				Type: types.OpTypeMap,
				Function: &FunctionRepr{
					Name:   funcName,
					Params: params,
					Args:   args,
				},
				Inputs:  []string{"score"},
				Outputs: []string{"new_score"},
			},
		},
	}

	chain, err := FuncChainFromReprWithContext(repr, memory.NewGoAllocator(), types.FunctionBuildContext{ModelExtraInfo: extraInfo})
	require.NoError(t, err)
	require.NotNil(t, chain)
	assert.True(t, called)
	assert.Len(t, chain.operators, 1)
}

func TestFuncChainFromRepr(t *testing.T) {
	repr := &ChainRepr{
		Name:  "repr-chain",
		Stage: types.StageL2Rerank,
		Operators: []OperatorRepr{
			{
				Type: types.OpTypeFilter,
				Function: &FunctionRepr{
					Name:   "mock_filter",
					Params: map[string]*schemapb.FunctionParamValue{},
				},
				Inputs: []string{"score"},
			},
			{
				Type: types.OpTypeSelect,
				Params: map[string]*schemapb.FunctionParamValue{
					"columns": arrayParam(stringParam("a"), stringParam("b")),
				},
			},
			{
				Type:   types.OpTypeSort,
				Inputs: []string{"a"},
				Params: map[string]*schemapb.FunctionParamValue{
					"desc": boolParam(true),
				},
			},
			{
				Type: types.OpTypeLimit,
				Params: map[string]*schemapb.FunctionParamValue{
					"limit":  intParam(100),
					"offset": intParam(10),
				},
			},
		},
	}

	chain, err := FuncChainFromRepr(repr, memory.NewGoAllocator())
	assert.NoError(t, err)
	assert.NotNil(t, chain)
	assert.Equal(t, "repr-chain", chain.name)
	assert.Len(t, chain.operators, 4)
}

func columnArg(name string) *schemapb.FunctionChainExprArg {
	return &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: name}}}
}

func literalArg(value *schemapb.FunctionParamValue) *schemapb.FunctionChainExprArg {
	return &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Literal{Literal: value}}
}

func boolParam(value bool) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: value}}
}

func intParam(value int64) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: value}}
}

func doubleParam(value float64) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_DoubleValue{DoubleValue: value}}
}

func stringParam(value string) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: value}}
}

func bytesParam(value []byte) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BytesValue{BytesValue: value}}
}

func arrayParam(values ...*schemapb.FunctionParamValue) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{Values: values}}}
}

func objectParam(fields map[string]*schemapb.FunctionParamValue) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: fields}}}
}
