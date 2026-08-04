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

package tasks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	chainexpr "github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPrepareQueryNodeFunctionChainsFromPlan(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "tag", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
	}

	t.Run("empty plan", func(t *testing.T) {
		prepared, err := prepareQueryNodeFunctionChainsFromPlan(nil, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared)
		assert.Nil(t, prepared.l0)
		assert.Nil(t, prepared.l1)
	})

	t.Run("l0 chain derives schema input field ids", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(
					mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest("ts"), columnArgForTest("tag")),
				),
			},
		}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		require.NotNil(t, prepared.l0.chain)
		assert.Equal(t, []int64{101, 102}, prepared.l0.inputFieldIDs)
	})

	t.Run("readable system inputs do not become extra fields", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest(types.ScoreFieldName), columnArgForTest(types.IDFieldName))),
			},
		}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		assert.Empty(t, prepared.l0.inputFieldIDs)
	})

	t.Run("internal system input is not readable", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest(types.SegOffsetFieldName))),
			},
		}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "system input \"$seg_offset\" is not readable")
	})

	t.Run("unknown system input is not readable", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest("$unknown"))),
			},
		}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "system input \"$unknown\" is not readable")
	})

	t.Run("duplicate inputs are planned once", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(
					mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest("ts"), columnArgForTest("ts")),
				),
			},
		}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		assert.Equal(t, []int64{101}, prepared.l0.inputFieldIDs)
	})

	t.Run("boost score is prepared as L0 rerank", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Scorers: []*planpb.ScoreFunction{{Weight: 2}},
			ScoreOption: &planpb.ScoreOption{
				FunctionMode: planpb.FunctionMode_FunctionModeSum,
				BoostMode:    planpb.BoostMode_BoostModeMultiply,
			},
		}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		assert.Nil(t, prepared.l0.chain)
		assert.Empty(t, prepared.l0.inputFieldIDs)
		require.NotNil(t, prepared.l0.boostScore)
		assert.Equal(t, plan.GetScorers(), prepared.l0.boostScore.scorers)
		assert.Equal(t, chainexpr.ModeSum, prepared.l0.boostScore.functionMode)
		assert.Equal(t, chainexpr.ModeMultiply, prepared.l0.boostScore.boostMode)
		assert.Nil(t, prepared.l1)
	})

	t.Run("boost score and l0 are mutually exclusive", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Scorers: []*planpb.ScoreFunction{{}},
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest(types.ScoreFieldName))),
			},
		}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "boost score and querynode rerank function chains cannot be used together")
	})

	t.Run("boost score and l1 are mutually exclusive", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Scorers: []*planpb.ScoreFunction{{}},
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l1FunctionChainForTest(mapOpWithParamsForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)}, columnArgForTest(types.ScoreFieldName), columnArgForTest(types.IDFieldName))),
			},
		}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "boost score and querynode rerank function chains cannot be used together")
	})

	t.Run("l1 chain derives schema input field ids", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(mapOpWithParamsForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)}, columnArgForTest("ts"), columnArgForTest(types.ScoreFieldName))),
		}}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		assert.Nil(t, prepared.l0)
		require.NotNil(t, prepared.l1)
		require.NotNil(t, prepared.l1.chain)
		assert.Equal(t, []int64{101}, prepared.l1.inputFieldIDs)
	})

	t.Run("l0 and l1 inputs are planned separately", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest("ts"))),
			l1FunctionChainForTest(mapOpWithParamsForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)}, columnArgForTest("ts"), columnArgForTest("tag"))),
		}}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		require.NotNil(t, prepared.l0.chain)
		require.NotNil(t, prepared.l1)
		require.NotNil(t, prepared.l1.chain)
		assert.Equal(t, []int64{101}, prepared.l0.inputFieldIDs)
		assert.Equal(t, []int64{101, 102}, prepared.l1.inputFieldIDs)
	})

	t.Run("duplicate stage is rejected before preparing singleton state", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest(types.ScoreFieldName))),
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest(types.ScoreFieldName))),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "appears more than once")
	})

	t.Run("l1 allows map sort and limit", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(
				mapOpWithParamsForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)}, columnArgForTest(types.ScoreFieldName), columnArgForTest(types.IDFieldName)),
				&schemapb.FunctionChainOp{Op: types.OpTypeSort, Inputs: []string{types.ScoreFieldName}},
				&schemapb.FunctionChainOp{Op: types.OpTypeLimit, Params: map[string]*schemapb.FunctionParamValue{
					"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 10}},
				}},
			),
		}}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l1)
		require.NotNil(t, prepared.l1.chain)
	})

	t.Run("l1 rejects expression on sort", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(&schemapb.FunctionChainOp{
				Op: types.OpTypeSort,
				Expr: &schemapb.FunctionChainExpr{
					Name: "unknown_function",
					Args: []*schemapb.FunctionChainExprArg{columnArgForTest(types.ScoreFieldName)},
				},
			}),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sort does not accept expression or outputs")
	})

	t.Run("l1 rejects inputs on limit", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(&schemapb.FunctionChainOp{
				Op:     types.OpTypeLimit,
				Inputs: []string{types.ScoreFieldName},
				Params: map[string]*schemapb.FunctionParamValue{
					"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 10}},
				},
			}),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "limit does not accept expression, inputs, or outputs")
	})

	t.Run("l1 rejects unsupported op", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(&schemapb.FunctionChainOp{Op: types.OpTypeFilter}),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "type \"filter\" is not supported by L1 rerank function chain")
	})

	t.Run("l1 rejects invalid limit as input error", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(&schemapb.FunctionChainOp{Op: types.OpTypeLimit, Params: map[string]*schemapb.FunctionParamValue{
				"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 0}},
			}}),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "limit must be positive")
	})

	t.Run("l1 only score is writable system output", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(mapOpWithParamsForTest(
				types.IDFieldName,
				chainexpr.NumCombineFuncName,
				map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)},
				columnArgForTest(types.ScoreFieldName),
				columnArgForTest(types.IDFieldName),
			)),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "system output \"$id\" is not writable by L1")
	})

	t.Run("l1 provenance output name is reserved", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(mapOpWithParamsForTest(
				l1SourceIndexColumn,
				chainexpr.NumCombineFuncName,
				map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)},
				columnArgForTest(types.ScoreFieldName),
				columnArgForTest(types.IDFieldName),
			)),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "system output \"$l1_source_index\" is not writable by L1")
	})

	t.Run("l1 collection fields are writable", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(
				mapOpWithParamsForTest(
					"ts",
					chainexpr.NumCombineFuncName,
					map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)},
					columnArgForTest(types.ScoreFieldName),
					columnArgForTest(types.IDFieldName),
				),
				mapOpWithParamsForTest(
					types.ScoreFieldName,
					chainexpr.NumCombineFuncName,
					map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)},
					columnArgForTest("ts"),
				),
			),
		}}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l1)
		assert.Empty(t, prepared.l1.inputFieldIDs)
	})

	t.Run("l1 rejects function not runnable at stage", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(mapOpWithParamsForTest(
				types.ScoreFieldName,
				chainexpr.XGBoostFuncName,
				map[string]*schemapb.FunctionParamValue{
					"model_resource": stringParamForTest("model.json"),
				},
				columnArgForTest("ts"),
			)),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not support stage \"L1_rerank\"")
	})

	t.Run("l1 internal system input is not readable", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(mapOpWithParamsForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)}, columnArgForTest(types.SegOffsetFieldName), columnArgForTest(types.ScoreFieldName))),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "system input \"$seg_offset\" is not readable by L1")
	})

	t.Run("unsupported querynode stage", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{{
			Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
			Ops:   []*schemapb.FunctionChainOp{mapOpWithParamsForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)}, columnArgForTest(types.ScoreFieldName))},
		}}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not supported")
	})

	t.Run("empty l0 chain", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{l0FunctionChainForTest()}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must contain at least one op")
	})

	t.Run("only map op is supported", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(&schemapb.FunctionChainOp{Op: types.OpTypeLimit}),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "type \"limit\" is not supported by L0 rerank function chain")
	})

	t.Run("only score is writable system output", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(types.IDFieldName, "expr", columnArgForTest(types.ScoreFieldName))),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "output \"$id\" is not writable")
	})

	t.Run("l0 collection fields are writable", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest("ts", "expr", columnArgForTest(types.ScoreFieldName))),
		}}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		assert.Empty(t, prepared.l0.inputFieldIDs)
	})

	t.Run("l0 temporary outputs are writable", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(
				mapOpForTest("temporary_score", "expr", columnArgForTest("ts")),
				mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest("temporary_score")),
			),
		}}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		assert.Equal(t, []int64{101}, prepared.l0.inputFieldIDs)
	})

	t.Run("unknown input field", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest("unknown"))),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown")
		assert.Contains(t, err.Error(), "neither a previous output nor a collection field")
	})

	t.Run("unsupported input field type", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest("vec"))),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported field type")
	})
}

func l0FunctionChainForTest(ops ...*schemapb.FunctionChainOp) *schemapb.FunctionChain {
	return &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL0Rerank,
		Ops:   ops,
	}
}

func l1FunctionChainForTest(ops ...*schemapb.FunctionChainOp) *schemapb.FunctionChain {
	return &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
		Ops:   ops,
	}
}

func mapOpForTest(output string, exprName string, args ...*schemapb.FunctionChainExprArg) *schemapb.FunctionChainOp {
	return mapOpWithParamsForTest(output, exprName, map[string]*schemapb.FunctionParamValue{}, args...)
}

func mapOpWithParamsForTest(output string, exprName string, params map[string]*schemapb.FunctionParamValue, args ...*schemapb.FunctionChainExprArg) *schemapb.FunctionChainOp {
	return &schemapb.FunctionChainOp{
		Op:      types.OpTypeMap,
		Outputs: []string{output},
		Expr: &schemapb.FunctionChainExpr{
			Name:   exprName,
			Args:   args,
			Params: params,
		},
	}
}

func columnArgForTest(name string) *schemapb.FunctionChainExprArg {
	return &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: name}}}
}

func stringParamForTest(value string) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: value}}
}
