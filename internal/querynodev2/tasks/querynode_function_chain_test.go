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
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chainexpr "github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPrepareQueryNodePyUDFRejectsStageBeforeRuntimeInitialization(t *testing.T) {
	item := &paramtable.Get().FunctionCfg.PyUDFEnabled
	old := item.SwapTempValue("false")
	t.Cleanup(func() { item.SwapTempValue(old) })
	configCalls, clientCalls := 0, 0
	defer mockey.Mock(pyudf.NewConfig).To(func(context.Context) (pyudf.Config, error) {
		configCalls++
		return pyudf.Config{}, merr.ErrServiceInternal
	}).Build().UnPatch()
	defer mockey.Mock(pyudf.NewClient).To(func(pyudf.Config) (*pyudf.Client, error) {
		clientCalls++
		return nil, merr.ErrServiceUnavailable
	}).Build().UnPatch()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	for _, enabled := range []string{"false", "true"} {
		for _, stage := range []schemapb.FunctionChainStage{
			schemapb.FunctionChainStage_FunctionChainStageL0Rerank,
			schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
		} {
			t.Run(enabled+"/"+stage.String(), func(t *testing.T) {
				item.SwapTempValue(enabled)
				op := mapOpForTest(types.ScoreFieldName, chainexpr.PyUDFFuncName, columnArgForTest(types.ScoreFieldName))
				op.Expr.Params = map[string]*schemapb.FunctionParamValue{
					"resource_name": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "rank_udf"}},
				}
				plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{{Stage: stage, Ops: []*schemapb.FunctionChainOp{op}}}}
				_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.ErrorContains(t, err, `function "py_udf" does not support stage`)
				require.NotContains(t, err.Error(), "enabled")
				status := merr.Status(err)
				require.EqualValues(t, 1100, status.Code)
				require.Equal(t, "true", status.ExtraInfo[merr.InputErrorFlagKey])
				require.False(t, status.Retriable)
				require.Zero(t, configCalls)
				require.Zero(t, clientCalls)
			})
		}
	}
}

func TestPrepareQueryNodeFunctionChainsFromPlan(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "tag", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			{FieldID: 104, Name: "metadata", DataType: schemapb.DataType_JSON, Nullable: true},
			{FieldID: 105, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}

	t.Run("empty plan", func(t *testing.T) {
		prepared, err := prepareQueryNodeFunctionChainsFromPlan(nil, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared)
		assert.Nil(t, prepared.l0)
		assert.Nil(t, prepared.l1)
	})

	t.Run("invalid internal schema remains a system error", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest(types.ScoreFieldName))),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	t.Run("l0 chain derives schema input field ids", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(
					mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest("ts"), columnArgForTest("tag")),
				),
			},
		}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		require.NotNil(t, prepared.l0.chain)
		assert.Equal(t, []int64{101, 102}, prepared.l0.inputPlan.PhysicalFieldIDs())
	})

	for _, test := range []struct {
		name  string
		stage schemapb.FunctionChainStage
	}{
		{name: "l0", stage: schemapb.FunctionChainStage_FunctionChainStageL0Rerank},
		{name: "l1", stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank},
	} {
		t.Run(test.name+" compiles scalar JSON and dynamic inputs", func(t *testing.T) {
			op := withInputDataTypesForTest(
				mapOpWithParamsForTest(
					types.ScoreFieldName,
					chainexpr.NumCombineFuncName,
					map[string]*schemapb.FunctionParamValue{
						types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum),
					},
					columnArgForTest(`metadata["rank"]`),
					columnArgForTest(`$meta["profile"]["bonus"]`),
					columnArgForTest("ts"),
				),
				schemapb.DataType_Int64,
				schemapb.DataType_Double,
				schemapb.DataType_None,
			)
			chainPB := l0FunctionChainForTest(op)
			if test.stage == schemapb.FunctionChainStage_FunctionChainStageL1Rerank {
				chainPB = l1FunctionChainForTest(op)
			}

			prepared, err := prepareQueryNodeFunctionChainsFromPlan(
				&planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{chainPB}},
				schema,
			)
			require.NoError(t, err)

			var inputPlan *chain.DataFrameInputPlan
			if test.stage == schemapb.FunctionChainStage_FunctionChainStageL0Rerank {
				require.NotNil(t, prepared.l0)
				inputPlan = prepared.l0.inputPlan
			} else {
				require.NotNil(t, prepared.l1)
				inputPlan = prepared.l1.inputPlan
			}
			require.NotNil(t, inputPlan)
			assert.Equal(t, []int64{104, 105, 101}, inputPlan.PhysicalFieldIDs())
			require.Len(t, inputPlan.Inputs, 3)
			assert.Equal(t, []string{"rank"}, inputPlan.Inputs[0].NestedPath)
			assert.Equal(t, schemapb.DataType_Int64, inputPlan.Inputs[0].DataTypeHint)
			assert.Equal(t, []string{"profile", "bonus"}, inputPlan.Inputs[1].NestedPath)
			assert.Equal(t, schemapb.DataType_Double, inputPlan.Inputs[1].DataTypeHint)
			assert.Equal(t, "ts", inputPlan.Inputs[2].LogicalName)
			assert.Equal(t, schemapb.DataType_None, inputPlan.Inputs[2].DataTypeHint)
		})
	}

	t.Run("querynode rejects JSON path output", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(`metadata["score"]`, chainexpr.NumCombineFuncName, columnArgForTest(types.ScoreFieldName))),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "JSON root or path cannot be used as a function chain output")
	})

	t.Run("readable system inputs do not become extra fields", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest(types.ScoreFieldName), columnArgForTest(types.IDFieldName))),
			},
		}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		assert.Empty(t, prepared.l0.inputPlan.PhysicalFieldIDs())
	})

	t.Run("internal system input is not readable", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest(types.SegOffsetFieldName))),
			},
		}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported function chain system input \"$seg_offset\"")
	})

	t.Run("unknown system input is not readable", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest("$unknown"))),
			},
		}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported function chain system input \"$unknown\"")
	})

	t.Run("duplicate inputs are planned once", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(
					mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest("ts"), columnArgForTest("ts")),
				),
			},
		}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		assert.Equal(t, []int64{101}, prepared.l0.inputPlan.PhysicalFieldIDs())
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
		assert.Nil(t, prepared.l0.inputPlan)
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
				l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest(types.ScoreFieldName))),
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
		assert.Equal(t, []int64{101}, prepared.l1.inputPlan.PhysicalFieldIDs())
	})

	t.Run("l0 and l1 inputs are planned separately", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest("ts"))),
			l1FunctionChainForTest(mapOpWithParamsForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)}, columnArgForTest("ts"), columnArgForTest("tag"))),
		}}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		require.NotNil(t, prepared.l0.chain)
		require.NotNil(t, prepared.l1)
		require.NotNil(t, prepared.l1.chain)
		assert.Equal(t, []int64{101}, prepared.l0.inputPlan.PhysicalFieldIDs())
		assert.Equal(t, []int64{101, 102}, prepared.l1.inputPlan.PhysicalFieldIDs())
	})

	t.Run("duplicate stage is rejected before preparing singleton state", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest(types.ScoreFieldName))),
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest(types.ScoreFieldName))),
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
		assert.Empty(t, prepared.l1.inputPlan.PhysicalFieldIDs())
	})

	t.Run("l1 accepts xgboost", func(t *testing.T) {
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

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l1)
		assert.Equal(t, []int64{101}, prepared.l1.inputPlan.PhysicalFieldIDs())
	})

	t.Run("l1 internal system input is not readable", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l1FunctionChainForTest(mapOpWithParamsForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)}, columnArgForTest(types.SegOffsetFieldName), columnArgForTest(types.ScoreFieldName))),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported function chain system input \"$seg_offset\"")
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
			l0FunctionChainForTest(mapOpForTest(types.IDFieldName, chainexpr.NumCombineFuncName, columnArgForTest(types.ScoreFieldName))),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "output \"$id\" is not writable")
	})

	t.Run("l0 collection fields are writable", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest("ts", chainexpr.NumCombineFuncName, columnArgForTest(types.ScoreFieldName))),
		}}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		assert.Empty(t, prepared.l0.inputPlan.PhysicalFieldIDs())
	})

	t.Run("l0 temporary outputs are writable", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(
				mapOpForTest("temporary_score", chainexpr.NumCombineFuncName, columnArgForTest("ts")),
				mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest("temporary_score")),
			),
		}}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.NotNil(t, prepared.l0)
		assert.Equal(t, []int64{101}, prepared.l0.inputPlan.PhysicalFieldIDs())
	})

	t.Run("unknown input field", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest("unknown"))),
		}}

		_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown")
		assert.Contains(t, err.Error(), "must use explicit $meta[...] syntax")
	})

	t.Run("unsupported input field type", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{
			l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest("vec"))),
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

func TestPrepareQueryNodeMapFunctionStageAndErrors(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	for _, stage := range []schemapb.FunctionChainStage{schemapb.FunctionChainStage_FunctionChainStageL0Rerank, schemapb.FunctionChainStage_FunctionChainStageL1Rerank} {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{{
			Stage: stage,
			Ops:   []*schemapb.FunctionChainOp{mapOpForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName, columnArgForTest(types.ScoreFieldName))},
		}}}
		t.Run(stage.String()+"/unsupported function stage", func(t *testing.T) {
			fn, err := chainexpr.NewNumCombineExpr(chainexpr.ModeSum, nil)
			require.NoError(t, err)
			fn.BaseExpr = *chainexpr.NewBaseExpr(fn.Name(), []string{types.StageL2Rerank})
			factory := mockey.Mock(chain.FunctionFromReprWithContext).Return(fn, nil).Build()
			defer factory.UnPatch()
			_, err = prepareQueryNodeFunctionChainsFromPlan(plan, schema)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "does not support stage")
		})
		t.Run(stage.String()+"/preserve factory error", func(t *testing.T) {
			factoryErr := merr.WrapErrServiceUnavailableMsg("function dependency unavailable")
			factory := mockey.Mock(chain.FunctionFromReprWithContext).Return(nil, factoryErr).Build()
			defer factory.UnPatch()
			_, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			assert.Equal(t, merr.Status(factoryErr).GetCode(), merr.Status(err).GetCode())
			assert.Equal(t, merr.Status(factoryErr).GetRetriable(), merr.Status(err).GetRetriable())
		})
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

func withInputDataTypesForTest(op *schemapb.FunctionChainOp, dataTypes ...schemapb.DataType) *schemapb.FunctionChainOp {
	if op.Params == nil {
		op.Params = make(map[string]*schemapb.FunctionParamValue)
	}
	values := make([]*schemapb.FunctionParamValue, 0, len(dataTypes))
	for _, dataType := range dataTypes {
		values = append(values, &schemapb.FunctionParamValue{
			Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: int64(dataType)},
		})
	}
	op.Params[types.InputDataTypesParam] = &schemapb.FunctionParamValue{
		Value: &schemapb.FunctionParamValue_ArrayValue{
			ArrayValue: &schemapb.FunctionParamArray{Values: values},
		},
	}
	return op
}

func inputPlanForScalarFieldForTest(fieldID int64, fieldName string, dataType schemapb.DataType) *chain.DataFrameInputPlan {
	return &chain.DataFrameInputPlan{Inputs: []chain.ResolvedChainInput{{
		LogicalName:   fieldName,
		SourceFieldID: fieldID,
		FieldName:     fieldName,
		DataType:      dataType,
	}}}
}

func TestPrepareQueryNodeFunctionChainsScalarKeywordNames(t *testing.T) {
	for _, stage := range []schemapb.FunctionChainStage{
		schemapb.FunctionChainStage_FunctionChainStageL0Rerank,
		schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
	} {
		for _, name := range []string{"threshold", "interval", "iso"} {
			t.Run(stage.String()+"/"+name, func(t *testing.T) {
				schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: name, DataType: schemapb.DataType_Double},
				}}
				plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{{
					Stage: stage,
					Ops: []*schemapb.FunctionChainOp{
						mapOpWithParamsForTest(types.ScoreFieldName, chainexpr.NumCombineFuncName,
							map[string]*schemapb.FunctionParamValue{types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum)},
							columnArgForTest(name)),
					},
				}}}
				prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
				require.NoError(t, err)
				var inputPlan *chain.DataFrameInputPlan
				if stage == schemapb.FunctionChainStage_FunctionChainStageL0Rerank {
					require.NotNil(t, prepared.l0)
					inputPlan = prepared.l0.inputPlan
				} else {
					require.NotNil(t, prepared.l1)
					inputPlan = prepared.l1.inputPlan
				}
				require.Len(t, inputPlan.Inputs, 1)
				assert.Equal(t, name, inputPlan.Inputs[0].LogicalName)
				assert.Equal(t, []int64{101}, inputPlan.PhysicalFieldIDs())
			})
		}
	}
}

func TestMaterializedInputContracts(t *testing.T) {
	scalar := chain.ResolvedChainInput{LogicalName: "value", SourceFieldID: 101, DataType: schemapb.DataType_Int64}
	jsonPath := chain.ResolvedChainInput{
		LogicalName: `metadata["value"]`, SourceFieldID: 102,
		DataType: schemapb.DataType_JSON, DataTypeHint: schemapb.DataType_Int64,
	}
	metadata := func(dataType, fieldID string) arrow.Metadata {
		values := map[string]string{}
		if dataType != "" {
			values[arrowMetadataDataTypeKey] = dataType
		}
		if fieldID != "" {
			values[arrowMetadataFieldIDKey] = fieldID
		}
		return arrow.MetadataFrom(values)
	}
	tests := []struct {
		name    string
		input   chain.ResolvedChainInput
		field   arrow.Field
		message string
	}{
		{"scalar", scalar, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata("5", "101")}, ""},
		{"JSON path", jsonPath, arrow.Field{Name: jsonPath.LogicalName, Type: arrow.PrimitiveTypes.Int64, Metadata: metadata("5", "")}, ""},
		{"missing column", scalar, arrow.Field{Name: "other", Type: arrow.PrimitiveTypes.Int64}, "is missing"},
		{"wrong Arrow type", scalar, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Float64, Metadata: metadata("5", "101")}, "type mismatch"},
		{"Milvus type inferred from Arrow", scalar, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata("", "101")}, ""},
		{"wrong Milvus type", scalar, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata("11", "101")}, "invalid Milvus data type metadata"},
		{"missing scalar FieldID", scalar, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata("5", "")}, "invalid field id metadata"},
		{"wrong scalar FieldID", scalar, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata("5", "102")}, "invalid field id metadata"},
		{"JSON path with FieldID", jsonPath, arrow.Field{Name: jsonPath.LogicalName, Type: arrow.PrimitiveTypes.Int64, Metadata: metadata("5", "102")}, "unexpectedly has field id metadata"},
		{"Int8 uses Int8 storage", chain.ResolvedChainInput{LogicalName: "value", SourceFieldID: 101, DataType: schemapb.DataType_Int8}, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int8, Metadata: metadata("2", "101")}, ""},
		{"Int8 rejects Int32 storage", chain.ResolvedChainInput{LogicalName: "value", SourceFieldID: 101, DataType: schemapb.DataType_Int8}, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Metadata: metadata("2", "101")}, "type mismatch"},
		{"Int16 uses Int16 storage", chain.ResolvedChainInput{LogicalName: "value", SourceFieldID: 101, DataType: schemapb.DataType_Int16}, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int16, Metadata: metadata("3", "101")}, ""},
		{"Int16 rejects Int32 storage", chain.ResolvedChainInput{LogicalName: "value", SourceFieldID: 101, DataType: schemapb.DataType_Int16}, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Metadata: metadata("3", "101")}, "type mismatch"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)
			for _, rows := range []int64{0, 2} {
				column := array.MakeArrayOfNull(pool, test.field.Type, int(rows))
				record := array.NewRecord(arrow.NewSchema([]arrow.Field{test.field}, nil), []arrow.Array{column}, rows)
				column.Release()
				df, err := dataFrameFromArrowRecordBatch(record, []int64{rows})
				record.Release()
				require.NoError(t, err)
				plan := &chain.DataFrameInputPlan{Inputs: []chain.ResolvedChainInput{test.input}}
				err = validateL0InputDataFrames([]*chain.DataFrame{df}, plan)
				df.Release()
				if test.message == "" {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, merr.ErrServiceInternal)
					assert.Contains(t, err.Error(), test.message)
					assert.Contains(t, err.Error(), "l0_rerank: segment dataframe 0")
				}
			}
		})
	}
}
