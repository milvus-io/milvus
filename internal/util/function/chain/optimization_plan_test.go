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
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testOperator struct {
	BaseOp
	name string
}

func newTestOperator(name string, inputs, outputs []string) *testOperator {
	return &testOperator{
		BaseOp: BaseOp{
			inputs:  inputs,
			outputs: outputs,
		},
		name: name,
	}
}

func (op *testOperator) Name() string { return op.name }

func (op *testOperator) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	return input, nil
}

func (op *testOperator) String() string { return op.name }

type addFloatFunction struct{}

func (f *addFloatFunction) Name() string { return "add_float" }

func (f *addFloatFunction) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

func (f *addFloatFunction) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	left := inputs[0]
	right := inputs[1]
	chunks := make([]arrow.Array, len(left.Chunks()))
	for i := 0; i < len(left.Chunks()); i++ {
		leftChunk := left.Chunk(i).(*array.Float32)
		rightChunk := right.Chunk(i).(*array.Float32)
		builder := array.NewFloat32Builder(ctx.Pool())
		for row := 0; row < leftChunk.Len(); row++ {
			builder.Append(leftChunk.Value(row) + rightChunk.Value(row))
		}
		chunks[i] = builder.NewArray()
		builder.Release()
	}
	result := arrow.NewChunked(arrow.PrimitiveTypes.Float32, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	return []*arrow.Chunked{result}, nil
}

func (f *addFloatFunction) IsRunnable(stage string) bool { return true }

func requireColumnSet(t *testing.T, actual ColumnSet, expected ...string) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for _, col := range expected {
		require.Truef(t, actual.Contains(col), "expected column %q in set %#v", col, actual)
	}
}

func TestOptimizationPlanLivenessLinear(t *testing.T) {
	fc := NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(types.StageL2Rerank)
	fc.Add(newTestOperator("op0", []string{"a"}, []string{"tmp_a"}))
	fc.Add(newTestOperator("op1", []string{"tmp_a", "b"}, []string{"tmp_b"}))
	fc.Add(newTestOperator("op2", []string{"tmp_b"}, []string{types.ScoreFieldName}))

	plan, err := fc.buildOptimizationPlan(ExecuteOptions{
		EnableColumnPruning: true,
		Downstream: DownstreamSpec{
			RequiredColumns: []string{"keep"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, plan)

	requireColumnSet(t, plan.Liveness.LiveAfter[2], "keep")
	requireColumnSet(t, plan.Liveness.LiveBefore[2], "tmp_b", "keep")
	requireColumnSet(t, plan.Liveness.LiveAfter[1], "tmp_b", "keep")
	requireColumnSet(t, plan.Liveness.LiveBefore[1], "tmp_a", "b", "keep")
	requireColumnSet(t, plan.Liveness.LiveAfter[0], "tmp_a", "b", "keep")
	requireColumnSet(t, plan.Liveness.LiveBefore[0], "a", "b", "keep")
}

func TestOptimizationPlanIgnoresSystemColumns(t *testing.T) {
	fc := NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(types.StageL2Rerank)
	fc.Add(newTestOperator("op0", []string{types.IDFieldName, "$seg_offset", "field"}, []string{"tmp"}))
	fc.Add(newTestOperator("op1", []string{"tmp", types.ScoreFieldName}, []string{types.ScoreFieldName}))

	plan, err := fc.buildOptimizationPlan(ExecuteOptions{
		EnableColumnPruning: true,
		Downstream: DownstreamSpec{
			RequiredColumns: []string{types.IDFieldName, types.ScoreFieldName, "field_out"},
		},
	})
	require.NoError(t, err)

	requireColumnSet(t, plan.Liveness.LiveBefore[1], "tmp", "field_out")
	requireColumnSet(t, plan.Liveness.LiveBefore[0], "field", "field_out")
}

func TestPruneDataFrameDropsDeadNonSystemColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df := buildPruningTestDataFrame(t, pool)
	pruned, err := PruneDataFrame(df, NewColumnSet("keep"), SystemColumnPolicy{KeepAllSystemColumns: true})
	require.NoError(t, err)
	require.NotSame(t, df, pruned)

	require.True(t, pruned.HasColumn(types.IDFieldName))
	require.True(t, pruned.HasColumn(types.ScoreFieldName))
	require.True(t, pruned.HasColumn("$seg_offset"))
	require.True(t, pruned.HasColumn("keep"))
	require.False(t, pruned.HasColumn("drop"))
	require.Equal(t, df.ChunkSizes(), pruned.ChunkSizes())

	pruned.Release()
	df.Release()
}

func TestPruneDataFrameNoDropReturnsSameDataFrame(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df := buildPruningTestDataFrame(t, pool)
	pruned, err := PruneDataFrame(df, NewColumnSet("keep", "drop"), SystemColumnPolicy{KeepAllSystemColumns: true})
	require.NoError(t, err)
	require.Same(t, df, pruned)
	df.Release()
}

func TestExecuteWithOptionsPrunesMapTemporaryColumn(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df := buildPruningTestDataFrame(t, pool)
	defer df.Release()

	mapTmp, err := NewMapOp(&addFloatFunction{}, []string{types.ScoreFieldName, "keep"}, []string{"tmp_score"})
	require.NoError(t, err)
	mapScore, err := NewMapOp(&addFloatFunction{}, []string{"tmp_score", types.ScoreFieldName}, []string{types.ScoreFieldName})
	require.NoError(t, err)

	fc := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank)
	fc.Add(mapTmp)
	fc.Add(mapScore)

	result, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{
		EnableColumnPruning: true,
		Downstream: DownstreamSpec{
			RequiredColumns: []string{"keep"},
		},
		SystemColumnPolicy: SystemColumnPolicy{KeepAllSystemColumns: true},
	}, df)
	require.NoError(t, err)
	defer result.Release()

	require.True(t, result.HasColumn(types.IDFieldName))
	require.True(t, result.HasColumn(types.ScoreFieldName))
	require.True(t, result.HasColumn("$seg_offset"))
	require.True(t, result.HasColumn("keep"))
	require.False(t, result.HasColumn("tmp_score"))
	require.False(t, result.HasColumn("drop"))
}

func TestExecuteWithContextDefaultBehaviorUnchanged(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df := buildPruningTestDataFrame(t, pool)
	defer df.Release()

	mapTmp, err := NewMapOp(&addFloatFunction{}, []string{types.ScoreFieldName, "keep"}, []string{"tmp_score"})
	require.NoError(t, err)

	fc := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank)
	fc.Add(mapTmp)

	result, err := fc.ExecuteWithContext(context.Background(), df)
	require.NoError(t, err)
	defer result.Release()

	require.True(t, result.HasColumn("tmp_score"))
	require.True(t, result.HasColumn("drop"))
}

func TestOptimizationPlanDisabledReturnsNil(t *testing.T) {
	fc := NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(types.StageL2Rerank)
	fc.Add(newTestOperator("op", []string{"a"}, []string{"b"}))

	plan, err := fc.buildOptimizationPlan(ExecuteOptions{})
	require.NoError(t, err)
	require.Nil(t, plan)
}

func TestOptimizationPlanParallelNotImplemented(t *testing.T) {
	fc := NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(types.StageL2Rerank)
	fc.Add(newTestOperator("op", []string{"a"}, []string{"b"}))

	plan, err := fc.buildOptimizationPlan(ExecuteOptions{EnableParallel: true})
	require.Error(t, err)
	require.Nil(t, plan)
	require.Contains(t, err.Error(), "parallel execution is not implemented")
}

func TestOptimizationPlanValidateOperatorMetadata(t *testing.T) {
	tests := []struct {
		name       string
		operators  []Operator
		downstream []string
	}{
		{
			name:      "nil operator",
			operators: []Operator{nil},
		},
		{
			name:      "empty input",
			operators: []Operator{newTestOperator("op", []string{""}, []string{"out"})},
		},
		{
			name:      "empty output",
			operators: []Operator{newTestOperator("op", []string{"in"}, []string{""})},
		},
		{
			name:      "duplicate output",
			operators: []Operator{newTestOperator("op", []string{"in"}, []string{"out", "out"})},
		},
		{
			name:       "empty downstream",
			operators:  []Operator{newTestOperator("op", []string{"in"}, []string{"out"})},
			downstream: []string{""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(types.StageL2Rerank)
			for _, op := range tt.operators {
				fc.Add(op)
			}

			plan, err := fc.buildOptimizationPlan(ExecuteOptions{
				EnableColumnPruning: true,
				Downstream: DownstreamSpec{
					RequiredColumns: tt.downstream,
				},
			})
			require.Error(t, err)
			require.Nil(t, plan)
		})
	}
}

func TestOptimizationPlanLivenessInputOutputSameColumn(t *testing.T) {
	fc := NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(types.StageL2Rerank)
	fc.Add(newTestOperator("op", []string{"tmp"}, []string{"tmp"}))

	plan, err := fc.buildOptimizationPlan(ExecuteOptions{
		EnableColumnPruning: true,
		Downstream: DownstreamSpec{
			RequiredColumns: []string{"tmp"},
		},
	})
	require.NoError(t, err)
	requireColumnSet(t, plan.Liveness.LiveAfter[0], "tmp")
	requireColumnSet(t, plan.Liveness.LiveBefore[0], "tmp")
}

func TestOptimizationPlanLivenessEmptyDownstream(t *testing.T) {
	fc := NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(types.StageL2Rerank)
	fc.Add(newTestOperator("op", []string{"a"}, []string{"tmp"}))

	plan, err := fc.buildOptimizationPlan(ExecuteOptions{EnableColumnPruning: true})
	require.NoError(t, err)
	requireColumnSet(t, plan.Liveness.LiveAfter[0])
	requireColumnSet(t, plan.Liveness.LiveBefore[0], "a")
}

func TestPruneDataFrameNil(t *testing.T) {
	pruned, err := PruneDataFrame(nil, NewColumnSet("keep"), SystemColumnPolicy{KeepAllSystemColumns: true})
	require.Error(t, err)
	require.Nil(t, pruned)
}

func TestPruneDataFrameDefaultKeepsSystemColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df := buildPruningTestDataFrame(t, pool)
	pruned, err := PruneDataFrame(df, NewColumnSet("keep"), SystemColumnPolicy{})
	require.NoError(t, err)
	defer pruned.Release()

	require.True(t, pruned.HasColumn(types.IDFieldName))
	require.True(t, pruned.HasColumn(types.ScoreFieldName))
	require.True(t, pruned.HasColumn("$seg_offset"))
	require.True(t, pruned.HasColumn("keep"))
	require.False(t, pruned.HasColumn("drop"))

	df.Release()
}

func TestPruneDataFramePreservesMetadata(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df := buildPruningTestDataFrame(t, pool)
	pruned, err := PruneDataFrame(df, NewColumnSet("keep"), SystemColumnPolicy{KeepAllSystemColumns: true})
	require.NoError(t, err)
	defer pruned.Release()

	fieldType, ok := pruned.FieldType("keep")
	require.True(t, ok)
	require.Equal(t, schemapb.DataType_Float, fieldType)
	fieldID, ok := pruned.FieldID("keep")
	require.True(t, ok)
	require.EqualValues(t, 100, fieldID)
	require.False(t, pruned.fieldNullables["keep"])
	metricType, ok := pruned.MetricType()
	require.True(t, ok)
	require.Equal(t, "IP", metricType)

	df.Release()
}

type assertColumnsOperator struct {
	BaseOp
	expected []string
}

func newAssertColumnsOperator(inputs, outputs, expected []string) *assertColumnsOperator {
	return &assertColumnsOperator{
		BaseOp: BaseOp{
			inputs:  inputs,
			outputs: outputs,
		},
		expected: expected,
	}
}

func (op *assertColumnsOperator) Name() string { return "assert_columns" }

func (op *assertColumnsOperator) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	if !equalStringSet(op.expected, input.ColumnNames()) {
		return nil, merr.WrapErrServiceInternalMsg("expected columns %v, got %v", op.expected, input.ColumnNames())
	}
	return input, nil
}

func equalStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]int, len(left))
	for _, value := range left {
		seen[value]++
	}
	for _, value := range right {
		seen[value]--
		if seen[value] < 0 {
			return false
		}
	}
	return true
}

func (op *assertColumnsOperator) String() string { return op.Name() }

func TestExecuteWithOptionsPrunesBeforeEachOperator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df := buildPruningTestDataFrame(t, pool)
	defer df.Release()

	mapTmp, err := NewMapOp(&addFloatFunction{}, []string{types.ScoreFieldName, "keep"}, []string{"tmp_score"})
	require.NoError(t, err)

	fc := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank)
	fc.Add(mapTmp)
	fc.Add(newAssertColumnsOperator(
		[]string{"tmp_score"},
		nil,
		[]string{types.IDFieldName, types.ScoreFieldName, "$seg_offset", "keep", "tmp_score"},
	))

	result, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{
		EnableColumnPruning: true,
		Downstream: DownstreamSpec{
			RequiredColumns: []string{"keep"},
		},
	}, df)
	require.NoError(t, err)
	defer result.Release()

	require.True(t, result.HasColumn("keep"))
	require.False(t, result.HasColumn("tmp_score"))
	require.False(t, result.HasColumn("drop"))
}

func TestExecuteWithOptionsPrunesAfterMergeOp(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df1 := buildPruningTestDataFrame(t, pool)
	defer df1.Release()
	df2 := buildPruningTestDataFrame(t, pool)
	defer df2.Release()

	fc := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank)
	fc.Add(NewMergeOp(MergeStrategyMax))
	fc.Add(newAssertColumnsOperator(
		[]string{"keep"},
		nil,
		[]string{types.IDFieldName, types.ScoreFieldName, "$seg_offset", "keep"},
	))

	result, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{
		EnableColumnPruning: true,
		Downstream: DownstreamSpec{
			RequiredColumns: []string{"keep"},
		},
	}, df1, df2)
	require.NoError(t, err)
	defer result.Release()

	require.True(t, result.HasColumn(types.IDFieldName))
	require.True(t, result.HasColumn(types.ScoreFieldName))
	require.True(t, result.HasColumn("$seg_offset"))
	require.True(t, result.HasColumn("keep"))
	require.False(t, result.HasColumn("drop"))
}

func TestExecuteWithOptionsEmptyChainDoesNotPrune(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	df := buildPruningTestDataFrame(t, pool)
	defer df.Release()

	fc := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank)
	result, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{
		EnableColumnPruning: true,
		Downstream: DownstreamSpec{
			RequiredColumns: []string{"keep"},
		},
	}, df)
	require.NoError(t, err)
	require.Same(t, df, result)
	require.True(t, result.HasColumn("drop"))
}

func buildPruningTestDataFrame(t *testing.T, pool memory.Allocator) *DataFrame {
	t.Helper()
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes([]int64{3})
	builder.SetMetricType("IP")
	addInt64Column(t, builder, pool, types.IDFieldName, []int64{1, 2, 3}, schemapb.DataType_Int64)
	addFloat32Column(t, builder, pool, types.ScoreFieldName, []float32{0.1, 0.2, 0.3}, schemapb.DataType_Float)
	addInt64Column(t, builder, pool, "$seg_offset", []int64{10, 11, 12}, schemapb.DataType_Int64)
	addFloat32Column(t, builder, pool, "keep", []float32{1, 2, 3}, schemapb.DataType_Float)
	builder.SetFieldID("keep", 100)
	addFloat32Column(t, builder, pool, "drop", []float32{4, 5, 6}, schemapb.DataType_Float)
	return builder.Build()
}

func addInt64Column(t *testing.T, builder *DataFrameBuilder, pool memory.Allocator, name string, values []int64, dataType schemapb.DataType) {
	t.Helper()
	b := array.NewInt64Builder(pool)
	b.AppendValues(values, nil)
	arr := b.NewArray()
	b.Release()
	builder.SetFieldType(name, dataType)
	builder.SetFieldNullable(name, false)
	require.NoError(t, builder.AddColumnFromChunks(name, []arrow.Array{arr}))
}

func addFloat32Column(t *testing.T, builder *DataFrameBuilder, pool memory.Allocator, name string, values []float32, dataType schemapb.DataType) {
	t.Helper()
	b := array.NewFloat32Builder(pool)
	b.AppendValues(values, nil)
	arr := b.NewArray()
	b.Release()
	builder.SetFieldType(name, dataType)
	builder.SetFieldNullable(name, false)
	require.NoError(t, builder.AddColumnFromChunks(name, []arrow.Array{arr}))
}
