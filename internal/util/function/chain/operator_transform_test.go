// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chain

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type operatorTransformExpr struct {
	name    string
	stages  []string
	outputs []arrow.DataType
	execute func(*types.FuncContext, []*arrow.Chunked) ([]*arrow.Chunked, error)
}

func (e *operatorTransformExpr) Name() string                      { return e.name }
func (e *operatorTransformExpr) OutputDataTypes() []arrow.DataType { return e.outputs }
func (e *operatorTransformExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return e.execute(ctx, inputs)
}

func (e *operatorTransformExpr) IsRunnable(stage string) bool {
	for _, allowed := range e.stages {
		if allowed == stage {
			return true
		}
	}
	return false
}

var registerOperatorTransformExpressions sync.Once

func operatorTransformChunkExpr() *operatorTransformExpr {
	structure := arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true})
	return &operatorTransformExpr{
		name: "test_transform_split", stages: []string{types.StageIngestion}, outputs: []arrow.DataType{arrow.ListOf(structure)},
		execute: func(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
			chunks := make([]arrow.Array, len(inputs[0].Chunks()))
			for group, input := range inputs[0].Chunks() {
				texts := input.(*array.String)
				list := array.NewListBuilder(ctx.Pool(), structure)
				child := list.ValueBuilder().(*array.StructBuilder)
				text := child.FieldBuilder(0).(*array.StringBuilder)
				for row := range texts.Len() {
					if texts.IsNull(row) {
						list.AppendNull()
						continue
					}
					list.Append(true)
					if texts.Value(row) == "" {
						continue
					}
					for _, part := range strings.Split(texts.Value(row), "|") {
						child.Append(true)
						text.Append(part)
					}
				}
				chunks[group] = list.NewArray()
				list.Release()
			}
			result := arrow.NewChunked(arrow.ListOf(structure), chunks)
			for _, chunk := range chunks {
				chunk.Release()
			}
			return []*arrow.Chunked{result}, nil
		},
	}
}

func installOperatorTransformExpressions() {
	registerOperatorTransformExpressions.Do(func() {
		types.MustRegisterFunction("test_transform_split", func(types.FunctionBuildContext, types.FunctionConfig) (types.FunctionExpr, error) {
			return operatorTransformChunkExpr(), nil
		})
		types.MustRegisterFunction("test_transform_keep_long", func(types.FunctionBuildContext, types.FunctionConfig) (types.FunctionExpr, error) {
			return &operatorTransformExpr{
				name: "test_transform_keep_long", stages: []string{types.StageIngestion}, outputs: []arrow.DataType{arrow.FixedWidthTypes.Boolean},
				execute: func(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
					chunks := make([]arrow.Array, len(inputs[0].Chunks()))
					for group, input := range inputs[0].Chunks() {
						texts := input.(*array.String)
						builder := array.NewBooleanBuilder(ctx.Pool())
						for row := range texts.Len() {
							builder.Append(!texts.IsNull(row) && len(texts.Value(row)) > 1)
						}
						chunks[group] = builder.NewArray()
						builder.Release()
					}
					result := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, chunks)
					for _, chunk := range chunks {
						chunk.Release()
					}
					return []*arrow.Chunked{result}, nil
				},
			}, nil
		})
	})
}

func operatorTransformInputExpr(name, column string) *schemapb.FunctionChainExpr {
	return &schemapb.FunctionChainExpr{Name: name, Args: []*schemapb.FunctionChainExprArg{{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: column}}}}}
}

func operatorTransformTable(t *testing.T, fields []arrow.Field, columns []arrow.Array, rows int64) arrow.Table {
	t.Helper()
	record := array.NewRecord(arrow.NewSchema(fields, nil), columns, rows)
	table := array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
	record.Release()
	for _, column := range columns {
		column.Release()
	}
	return table
}

func TestTransformExecutorChunkingProjectionFilter(t *testing.T) {
	installOperatorTransformExpressions()
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	ids := array.NewInt64Builder(pool)
	ids.AppendValues([]int64{7, 8, 9}, nil)
	texts := array.NewStringBuilder(pool)
	texts.AppendValues([]string{"a|bb", "", "ccc"}, nil)
	input := operatorTransformTable(t, []arrow.Field{{Name: "doc_id", Type: arrow.PrimitiveTypes.Int64}, {Name: "text", Type: arrow.BinaryTypes.String}}, []arrow.Array{ids.NewArray(), texts.NewArray()}, 3)
	defer input.Release()
	ids.Release()
	texts.Release()
	flatMap, err := transform.FlatMap("chunk", operatorTransformInputExpr("test_transform_split", "text"), transform.ExplodeOptions{Position: "chunk_index"})
	require.NoError(t, err)
	selectOp, err := transform.Select([]string{"doc_id", "chunk.text", "chunk_index"}, []string{"doc_id", "text", "chunk_index"})
	require.NoError(t, err)
	plan, err := transform.PreInsert("documents", flatMap, selectOp, &schemapb.FunctionChainOp{Op: "filter", Expr: operatorTransformInputExpr("test_transform_keep_long", "text")})
	require.NoError(t, err)
	result, err := NewTransformExecutor(pool, types.FunctionBuildContext{}).Execute(context.Background(), plan, transform.Batch{Groups: []arrow.Table{input}})
	require.NoError(t, err)
	defer ReleaseArrowBatch(result.Data)
	require.Equal(t, int64(2), result.Data.Groups[0].NumRows())
	output := result.Data.Groups[0]
	require.Equal(t, []int64{7, 9}, output.Column(0).Data().Chunk(0).(*array.Int64).Int64Values())
	require.Equal(t, "bb", output.Column(1).Data().Chunk(0).(*array.String).Value(0))
	require.Equal(t, "ccc", output.Column(1).Data().Chunk(0).(*array.String).Value(1))
	require.Equal(t, []int64{1, 0}, output.Column(2).Data().Chunk(0).(*array.Int64).Int64Values())
	require.Equal(t, [][]transform.RowLineage{{{{Group: 0, Row: 0}}, {{Group: 0, Row: 2}}}}, result.Lineage)
}

func TestExplodeOuterStructNullsAndSlicedLists(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	structure := arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true})
	list := array.NewListBuilder(pool, structure)
	child := list.ValueBuilder().(*array.StructBuilder)
	texts := child.FieldBuilder(0).(*array.StringBuilder)
	list.Append(true)
	child.Append(true)
	texts.Append("ignored")
	list.Append(true)
	child.Append(true)
	texts.Append("a")
	child.AppendNull()
	list.Append(true) // empty list
	list.AppendNull()
	all := list.NewArray()
	list.Release()
	sliced := array.NewSlice(all, 1, 4)
	all.Release()
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{3}).SetIdentityLineage()
	require.NoError(t, builder.AddColumnFromChunks("chunks", []arrow.Array{sliced}))
	frame := builder.Build()
	defer frame.Release()
	chain := NewFuncChainWithAllocator(pool).SetStage(types.StageIngestion).
		Explode("chunks", "chunk", ExplodeOptions{Outer: true, Position: "position"}).
		SelectAs([]string{"chunk.text", "position"}, []string{"text", "position"})
	result, err := chain.Execute(frame)
	require.NoError(t, err)
	defer result.Release()
	require.Equal(t, []int64{4}, result.ChunkSizes())
	text := result.Column("text").Chunk(0).(*array.String)
	require.Equal(t, "a", text.Value(0))
	require.True(t, text.IsNull(1), "nullable struct parent masks child contents")
	require.True(t, text.IsNull(2), "outer empty list produces a typed null")
	require.True(t, text.IsNull(3), "outer null list produces a typed null")
	position := result.Column("position").Chunk(0).(*array.Int64)
	require.Equal(t, int64(0), position.Value(0))
	require.Equal(t, int64(1), position.Value(1))
	require.True(t, position.IsNull(2))
	require.Equal(t, [][]transform.RowLineage{{{{Group: 0, Row: 0}}, {{Group: 0, Row: 0}}, {{Group: 0, Row: 1}}, {{Group: 0, Row: 2}}}}, result.Lineage())
}

func TestNestedTakePreservesVectorAndListColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	vectors := array.NewFixedSizeListBuilder(pool, 2, arrow.PrimitiveTypes.Float32)
	values := vectors.ValueBuilder().(*array.Float32Builder)
	for _, pair := range [][]float32{{1, 2}, {3, 4}} {
		vectors.Append(true)
		values.AppendValues(pair, nil)
	}
	vector := vectors.NewArray()
	vectors.Release()
	defer vector.Release()
	picked, err := dispatchPickByIndices(pool, vector, []int{1, 0, 1})
	require.NoError(t, err)
	defer picked.Release()
	require.Equal(t, []float32{3, 4, 1, 2, 3, 4}, picked.(*array.FixedSizeList).ListValues().(*array.Float32).Float32Values())
	nulls, err := takeArrayWithNulls(pool, vector, []int{1, -1, 0})
	require.NoError(t, err)
	defer nulls.Release()
	require.True(t, nulls.IsNull(1))
	_, err = takeArrayWithNulls(pool, vector, []int{2})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestTransformStagesAndProjectionScope(t *testing.T) {
	installOperatorTransformExpressions()
	explode, err := transform.Explode("chunks", "chunk", transform.ExplodeOptions{})
	require.NoError(t, err)
	for _, stage := range []schemapb.FunctionChainStage{
		schemapb.FunctionChainStage_FunctionChainStageL0Rerank,
		schemapb.FunctionChainStage_FunctionChainStageL1Rerank,
		schemapb.FunctionChainStage_FunctionChainStageL2Rerank,
		schemapb.FunctionChainStage_FunctionChainStagePostProcess,
	} {
		_, err := ParseFuncChainProto(&schemapb.FunctionChain{Stage: stage, Ops: []*schemapb.FunctionChainOp{explode}}, memory.DefaultAllocator)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
	_, err = ParseFuncChainProto(&schemapb.FunctionChain{Stage: schemapb.FunctionChainStage_FunctionChainStagePreProcess, Ops: []*schemapb.FunctionChainOp{explode}}, memory.DefaultAllocator)
	require.NoError(t, err)
	selectOp, err := transform.Select([]string{"doc_id"}, nil)
	require.NoError(t, err)
	_, err = ProtoChainToRepr(&schemapb.FunctionChain{Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion, Ops: []*schemapb.FunctionChainOp{
		selectOp, {Op: "map", Expr: operatorTransformInputExpr("test_transform_split", "text"), Outputs: []string{"chunks"}},
	}})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	flatMap, err := transform.FlatMap("chunk", operatorTransformInputExpr("test_transform_split", "text"), transform.ExplodeOptions{})
	require.NoError(t, err)
	project, err := transform.Select([]string{"chunk[\"text\"]"}, []string{"text"})
	require.NoError(t, err)
	repr, err := ProtoChainToRepr(&schemapb.FunctionChain{Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion, Ops: []*schemapb.FunctionChainOp{flatMap, project}})
	require.NoError(t, err)
	require.Equal(t, []string{"text"}, repr.Info.RequiredInputs, "a produced struct path is not fetched from collection schema")
	chain := NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(types.StageIngestion).
		Select("doc_id").Map(operatorTransformChunkExpr(), []string{"text"}, []string{"chunks"})
	require.ErrorIs(t, chain.Validate(), merr.ErrParameterInvalid)
}

func TestSelectPreservesHiddenIdentityAndNullableMap(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	ids := array.NewInt64Builder(pool)
	ids.AppendValues([]int64{1, 2}, nil)
	scores := array.NewFloat32Builder(pool)
	scores.AppendValues([]float32{0.8, 0.7}, nil)
	texts := array.NewStringBuilder(pool)
	texts.AppendValues([]string{"A", "B"}, nil)
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{2}).SetIdentityLineage()
	require.NoError(t, builder.AddColumnFromChunks("$id", []arrow.Array{ids.NewArray()}))
	require.NoError(t, builder.AddColumnFromChunks("$score", []arrow.Array{scores.NewArray()}))
	require.NoError(t, builder.AddColumnFromChunks("text", []arrow.Array{texts.NewArray()}))
	ids.Release()
	scores.Release()
	texts.Release()
	input := builder.Build()
	defer input.Release()
	function := &operatorTransformExpr{
		name: "nullable", stages: []string{types.StagePostProcess}, outputs: []arrow.DataType{arrow.BinaryTypes.String},
		execute: func(ctx *types.FuncContext, _ []*arrow.Chunked) ([]*arrow.Chunked, error) {
			builder := array.NewStringBuilder(ctx.Pool())
			builder.AppendNull()
			builder.Append("HL:B")
			values := builder.NewArray()
			builder.Release()
			output := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{values})
			values.Release()
			return []*arrow.Chunked{output}, nil
		},
	}
	chain := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).
		Map(function, []string{"text"}, []string{"text"}).SelectAs([]string{"text"}, []string{"highlight"})
	output, err := chain.Execute(input)
	require.NoError(t, err)
	defer output.Release()
	require.True(t, output.HasColumn("$id"))
	require.True(t, output.HasColumn("$score"))
	require.False(t, output.HasColumn("text"))
	require.True(t, output.Schema().Field(0).Nullable)
	require.Equal(t, input.Lineage(), output.Lineage())
}

func TestMalformedFunctionOutputAndCancellation(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	values := array.NewStringBuilder(pool)
	values.Append("doc")
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{1}).SetIdentityLineage()
	require.NoError(t, builder.AddColumnFromChunks("text", []arrow.Array{values.NewArray()}))
	values.Release()
	input := builder.Build()
	defer input.Release()
	wrong := &operatorTransformExpr{name: "malformed", stages: []string{types.StageIngestion}, execute: func(*types.FuncContext, []*arrow.Chunked) ([]*arrow.Chunked, error) {
		return []*arrow.Chunked{nil}, nil
	}}
	_, err := NewFuncChainWithAllocator(pool).SetStage(types.StageIngestion).Map(wrong, []string{"text"}, []string{"out"}).Execute(input)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	_, err = NewFuncChainWithAllocator(pool).SetStage(types.StageIngestion).Filter(wrong, []string{"text"}).Execute(input)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = NewFuncChainWithAllocator(pool).SetStage(types.StageIngestion).FlatMap(operatorTransformChunkExpr(), []string{"text"}, "chunk").ExecuteWithContext(ctx, input)
	require.ErrorIs(t, err, context.Canceled)
}
