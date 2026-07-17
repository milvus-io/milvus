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
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chainexpr "github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func histogramSampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, observer.(prometheus.Metric).Write(metric))
	return metric.GetHistogram().GetSampleCount()
}

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
		assert.Empty(t, prepared.l0Chains)
		assert.Empty(t, prepared.extraFieldIDs)
	})

	t.Run("l0 chain derives schema input field ids", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(
					mapOpForTest("score1", "expr", columnArgForTest("ts"), columnArgForTest(types.ScoreFieldName)),
					mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest("score1"), columnArgForTest("tag")),
				),
			},
		}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		require.Len(t, prepared.l0Chains, 1)
		assert.Equal(t, []int64{101, 102}, prepared.extraFieldIDs)
	})

	t.Run("readable system inputs do not become extra fields", func(t *testing.T) {
		plan := &planpb.PlanNode{
			QuerynodeFunctionChains: []*schemapb.FunctionChain{
				l0FunctionChainForTest(mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest(types.ScoreFieldName), columnArgForTest(types.IDFieldName))),
			},
		}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		assert.Empty(t, prepared.extraFieldIDs)
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
					mapOpForTest("score1", "expr", columnArgForTest("ts")),
					mapOpForTest(types.ScoreFieldName, "expr", columnArgForTest("ts"), columnArgForTest("score1")),
				),
			},
		}

		prepared, err := prepareQueryNodeFunctionChainsFromPlan(plan, schema)
		require.NoError(t, err)
		assert.Equal(t, []int64{101}, prepared.extraFieldIDs)
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
		assert.Contains(t, err.Error(), "boost score and L0 rerank function chain cannot be used together")
	})

	t.Run("l1 is unsupported", func(t *testing.T) {
		plan := &planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{{
			Stage: schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
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
		assert.Contains(t, err.Error(), "system output \"$id\" is not writable")
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

func TestApplyL0RerankRejectsNilPreparedChains(t *testing.T) {
	task := &SearchTask{ctx: t.Context()}

	err := task.applyL0Rerank(nil, nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prepared querynode function chains is nil")
}

func TestApplyPublicL0RerankPrunesInputsAndPreservesReduceSystemColumns(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	df := makeBoostScoreTestDF(t,
		[]int64{1, 2, 3},
		[]float32{0.5, 0.2, 0.9},
		[]int64{10, 20, 30},
		[]int64{3},
	)
	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes(df.ChunkSizes())
	require.NoError(t, builder.AddColumnFrom(df, types.IDFieldName))
	require.NoError(t, builder.AddColumnFrom(df, types.ScoreFieldName))
	require.NoError(t, builder.AddColumnFrom(df, types.SegOffsetFieldName))
	elementIndicesBuilder := array.NewInt32Builder(defaultAllocator)
	elementIndicesBuilder.AppendValues([]int32{0, 1, 2}, nil)
	elementIndicesArr := elementIndicesBuilder.NewArray()
	elementIndicesBuilder.Release()
	require.NoError(t, builder.AddColumnFromChunks(elementIndicesCol, []arrow.Array{elementIndicesArr}))
	groupByCol := groupByColumnName(100)
	groupByBuilder := array.NewInt64Builder(defaultAllocator)
	groupByBuilder.AppendValues([]int64{1000, 2000, 3000}, nil)
	groupByArr := groupByBuilder.NewArray()
	groupByBuilder.Release()
	require.NoError(t, builder.AddColumnFromChunks(groupByCol, []arrow.Array{groupByArr}))
	tsBuilder := array.NewFloat32Builder(defaultAllocator)
	tsBuilder.AppendValues([]float32{0.1, 3.0, 0.1}, nil)
	tsArr := tsBuilder.NewArray()
	tsBuilder.Release()
	require.NoError(t, builder.AddColumnFromChunks("ts", []arrow.Array{tsArr}))
	df.Release()
	df = builder.Build()
	segDFs := []*chain.DataFrame{df}

	repr, err := chain.ProtoChainToRepr(l0FunctionChainForTest(mapOpWithParamsForTest(
		types.ScoreFieldName,
		chainexpr.NumCombineFuncName,
		map[string]*schemapb.FunctionParamValue{
			types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum),
		},
		columnArgForTest(types.ScoreFieldName),
		columnArgForTest("ts"),
	)))
	require.NoError(t, err)
	task := &SearchTask{ctx: t.Context()}

	require.NoError(t, task.applyPublicL0Rerank(segDFs, &preparedQueryNodeFunctionChains{l0Chains: []*chain.ChainRepr{repr}}))
	defer segDFs[0].Release()

	result := segDFs[0]
	ids := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	require.False(t, result.HasColumn("ts"))
	require.True(t, result.HasColumn(types.IDFieldName))
	require.True(t, result.HasColumn(types.ScoreFieldName))
	require.True(t, result.HasColumn(types.SegOffsetFieldName))
	require.True(t, result.HasColumn(elementIndicesCol))
	require.True(t, result.HasColumn(groupByCol))
	require.Equal(t, int64(2), ids.Value(0))
	require.InDelta(t, 3.2, scores.Value(0), 1e-6)
	require.Equal(t, int64(3), ids.Value(1))
	require.InDelta(t, 1.0, scores.Value(1), 1e-6)
	require.Equal(t, int64(1), ids.Value(2))
	require.InDelta(t, 0.6, scores.Value(2), 1e-6)
}

func TestApplyL0RerankMetrics(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	repr, err := chain.ProtoChainToRepr(l0FunctionChainForTest(mapOpWithParamsForTest(
		types.ScoreFieldName,
		chainexpr.NumCombineFuncName,
		map[string]*schemapb.FunctionParamValue{
			types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum),
		},
		columnArgForTest(types.ScoreFieldName),
		columnArgForTest(types.IDFieldName),
	)))
	require.NoError(t, err)
	publicPrepared := &preparedQueryNodeFunctionChains{l0Chains: []*chain.ChainRepr{repr}}
	boostPlan := &planpb.PlanNode{
		Scorers: []*planpb.ScoreFunction{{Weight: 1}},
		ScoreOption: &planpb.ScoreOption{
			FunctionMode: planpb.FunctionMode_FunctionModeSum,
			BoostMode:    planpb.BoostMode_BoostModeMultiply,
		},
	}
	boostPrepared := &preparedQueryNodeFunctionChains{plan: boostPlan}
	task := &SearchTask{ctx: t.Context()}
	nodeID := fmt.Sprint(task.GetNodeID())

	successObserver := metrics.QueryNodeFunctionChainLatency.WithLabelValues(
		nodeID,
		metrics.FunctionChainLevelL0,
		metrics.SuccessLabel,
	)
	failObserver := metrics.QueryNodeFunctionChainLatency.WithLabelValues(
		nodeID,
		metrics.FunctionChainLevelL0,
		metrics.FailLabel,
	)

	t.Run("public L0 success", func(t *testing.T) {
		before := histogramSampleCount(t, successObserver)
		segDFs := []*chain.DataFrame{
			makeBoostScoreTestDF(t, []int64{1, 2}, []float32{0.5, 0.2}, []int64{10, 20}, []int64{2}),
			makeBoostScoreTestDF(t, []int64{3, 4}, []float32{0.9, 0.1}, []int64{30, 40}, []int64{2}),
		}
		defer func() {
			for _, df := range segDFs {
				df.Release()
			}
		}()

		require.NoError(t, task.applyL0Rerank(segDFs, publicPrepared, nil, nil))
		require.Equal(t, before+1, histogramSampleCount(t, successObserver))
	})

	t.Run("public L0 failure", func(t *testing.T) {
		before := histogramSampleCount(t, failObserver)
		err := task.applyL0Rerank([]*chain.DataFrame{nil}, publicPrepared, nil, nil)
		require.Error(t, err)
		require.Equal(t, before+1, histogramSampleCount(t, failObserver))
	})

	t.Run("boost score success", func(t *testing.T) {
		oldFactory := boostScoreRunnerFactory
		boostScoreRunnerFactory = mockBoostScoreRunnerFactory(boostScoreOutput{
			scores:   []float32{2.0},
			hasScore: []bool{true},
		})
		defer func() { boostScoreRunnerFactory = oldFactory }()

		before := histogramSampleCount(t, successObserver)
		segDFs := []*chain.DataFrame{
			makeBoostScoreTestDF(t, []int64{1}, []float32{0.5}, []int64{10}, []int64{1}),
		}
		defer func() {
			for _, df := range segDFs {
				df.Release()
			}
		}()

		require.NoError(t, task.applyL0Rerank(segDFs, boostPrepared, []segments.Segment{nil}, nil))
		require.Equal(t, before+1, histogramSampleCount(t, successObserver))
	})

	t.Run("boost score failure", func(t *testing.T) {
		before := histogramSampleCount(t, failObserver)
		err := task.applyL0Rerank(nil, boostPrepared, []segments.Segment{nil}, nil)
		require.Error(t, err)
		require.Equal(t, before+1, histogramSampleCount(t, failObserver))
	})

	t.Run("no rerank does not record", func(t *testing.T) {
		successBefore := histogramSampleCount(t, successObserver)
		failBefore := histogramSampleCount(t, failObserver)
		require.NoError(t, task.applyL0Rerank(nil, &preparedQueryNodeFunctionChains{}, nil, nil))
		require.Equal(t, successBefore, histogramSampleCount(t, successObserver))
		require.Equal(t, failBefore, histogramSampleCount(t, failObserver))
	})
}

func l0FunctionChainForTest(ops ...*schemapb.FunctionChainOp) *schemapb.FunctionChain {
	return &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStageL0Rerank,
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
