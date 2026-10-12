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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func preQueryFrame(t *testing.T, pool memory.Allocator, texts []string) *DataFrame {
	t.Helper()
	values := array.NewStringBuilder(pool)
	values.AppendValues(texts, nil)
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{int64(len(texts))}).SetIdentityLineage()
	require.NoError(t, builder.AddColumnFromChunks("query", []arrow.Array{values.NewArray()}))
	values.Release()
	return builder.Build()
}

func preQuerySnapshot(texts ...string) *types.QueryContext {
	values := make([][]byte, len(texts))
	for index, text := range texts {
		values[index] = []byte(text)
	}
	query := &commonpb.PlaceholderValue{Type: commonpb.PlaceholderType_VarChar, Tag: "$0", Values: values}
	return &types.QueryContext{OriginalQueries: []*commonpb.PlaceholderValue{query}, Queries: []*commonpb.PlaceholderValue{query}}
}

func TestPreFlatMapComposesQueryOriginsBetweenOperators(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := preQueryFrame(t, pool, []string{"a|b", "", "c"})
	defer input.Release()
	snapshot := preQuerySnapshot("a|b", "", "c")
	split := operatorTransformChunkExpr()
	split.stages = []string{types.StagePreProcess}
	checked := false
	expectedOrigins := []int64{0, 0, 2}
	inspect := &operatorTransformExpr{
		name: "inspect origins", stages: []string{types.StagePreProcess}, outputs: []arrow.DataType{arrow.BinaryTypes.String},
		execute: func(ctx *types.FuncContext, columns []*arrow.Chunked) ([]*arrow.Chunked, error) {
			checked = true
			require.Equal(t, expectedOrigins, ctx.QueryContext().RowQueryOrigins)
			require.Equal(t, snapshot.OriginalQueries, ctx.QueryContext().OriginalQueries)
			require.Equal(t, snapshot.Queries, ctx.QueryContext().Queries, "PRE snapshots retain the entry payload")
			columns[0].Retain()
			return []*arrow.Chunked{columns[0]}, nil
		},
	}
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePreProcess).
		FlatMap(split, []string{"query"}, "child").SelectAs([]string{`child["text"]`}, []string{"query"}).
		Map(inspect, []string{"query"}, []string{"query"})
	output, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{QueryContext: snapshot}, input)
	require.NoError(t, err)
	defer output.Release()
	require.True(t, checked)
	texts := output.Column("query").Chunk(0).(*array.String)
	require.Equal(t, []string{"a", "b", "c"}, []string{texts.Value(0), texts.Value(1), texts.Value(2)})
	require.Equal(t, [][]transform.RowLineage{{{{Group: 0, Row: 0}}, {{Group: 0, Row: 0}}, {{Group: 0, Row: 2}}}}, output.Lineage())
	require.Nil(t, snapshot.RowQueryOrigins, "per-call metadata must not mutate the request snapshot")

	duplicate := &operatorTransformExpr{
		name: "duplicate queries", stages: []string{types.StagePreProcess}, outputs: []arrow.DataType{arrow.ListOf(arrow.BinaryTypes.String)},
		execute: func(ctx *types.FuncContext, columns []*arrow.Chunked) ([]*arrow.Chunked, error) {
			require.Equal(t, []int64{0, 0, 2}, ctx.QueryContext().RowQueryOrigins)
			list := array.NewListBuilder(ctx.Pool(), arrow.BinaryTypes.String)
			defer list.Release()
			values := list.ValueBuilder().(*array.StringBuilder)
			texts := columns[0].Chunk(0).(*array.String)
			for row := range texts.Len() {
				list.Append(true)
				values.AppendValues([]string{texts.Value(row), texts.Value(row)}, nil)
			}
			rows := list.NewArray()
			defer rows.Release()
			return []*arrow.Chunked{arrow.NewChunked(rows.DataType(), []arrow.Array{rows})}, nil
		},
	}
	expectedOrigins = []int64{0, 0, 0, 0, 2, 2}
	fc = NewFuncChainWithAllocator(pool).SetStage(types.StagePreProcess).
		FlatMap(split, []string{"query"}, "child").SelectAs([]string{`child["text"]`}, []string{"query"}).
		FlatMap(duplicate, []string{"query"}, "query").Map(inspect, []string{"query"}, []string{"query"})
	composed, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{QueryContext: snapshot}, input)
	require.NoError(t, err)
	require.Equal(t, int64(6), composed.NumRows())
	composed.Release()
}

func TestPreCancellationAfterFinalFunctionDoesNotPublishOutput(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := preQueryFrame(t, pool, []string{"a"})
	defer input.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	function := &operatorTransformExpr{
		name: "cancel before publish", stages: []string{types.StagePreProcess}, outputs: []arrow.DataType{arrow.BinaryTypes.String},
		execute: func(_ *types.FuncContext, columns []*arrow.Chunked) ([]*arrow.Chunked, error) {
			cancel()
			columns[0].Retain()
			return []*arrow.Chunked{columns[0]}, nil
		},
	}
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePreProcess).Map(function, []string{"query"}, []string{"query"})
	output, err := fc.ExecuteWithContext(ctx, input)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, output)
}

func TestPreExpansionBudgetAndTypedEmptyRows(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	split := operatorTransformChunkExpr()
	split.stages = []string{types.StagePreProcess}
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePreProcess).
		FlatMap(split, []string{"query"}, "child").SelectAs([]string{`child["text"]`}, []string{"query"})
	input := preQueryFrame(t, pool, []string{"a|b|c", "d|e"})
	defer input.Release()
	output, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{QueryContext: preQuerySnapshot("a|b|c", "d|e"), OutputRowLimit: 4}, input)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.Nil(t, output)
	output, err = fc.ExecuteWithOptions(context.Background(), ExecuteOptions{QueryContext: preQuerySnapshot("a|b|c", "d|e"), OutputRowLimit: 5}, input)
	require.NoError(t, err)
	require.Equal(t, int64(5), output.NumRows())
	output.Release()
	output, err = fc.ExecuteWithOptions(context.Background(), ExecuteOptions{OutputRowLimit: -1}, input)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Nil(t, output)

	empty := preQueryFrame(t, pool, []string{"", ""})
	defer empty.Release()
	called := false
	inspect := &operatorTransformExpr{
		name: "typed empty", stages: []string{types.StagePreProcess}, outputs: []arrow.DataType{arrow.BinaryTypes.String},
		execute: func(ctx *types.FuncContext, columns []*arrow.Chunked) ([]*arrow.Chunked, error) {
			called = true
			require.NotNil(t, ctx.QueryContext().RowQueryOrigins)
			require.Empty(t, ctx.QueryContext().RowQueryOrigins)
			require.Equal(t, arrow.BinaryTypes.String, columns[0].DataType())
			columns[0].Retain()
			return []*arrow.Chunked{columns[0]}, nil
		},
	}
	fc.Map(inspect, []string{"query"}, []string{"query"})
	output, err = fc.ExecuteWithOptions(context.Background(), ExecuteOptions{QueryContext: preQuerySnapshot("", ""), OutputRowLimit: 5}, empty)
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, int64(0), output.NumRows())
	output.Release()
}

func TestPreQueryContextRejectsBadLineageBeforeFunction(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	for _, lineage := range [][][]transform.RowLineage{
		{{{{Group: 0, Row: 1}}, {{Group: 0, Row: 0}}}},
		{{{{Group: 0, Row: 0}, {Group: 0, Row: 1}}, {{Group: 0, Row: 1}}}},
		{{{{Group: 0, Row: 0}}, {{Group: 0, Row: 2}}}},
	} {
		input := preQueryFrame(t, pool, []string{"a", "b"})
		input.lineage = lineage
		ctx := types.NewFuncContextWithStage(pool, types.StagePreProcess).WithQueryContext(preQuerySnapshot("a", "b"))
		_, err := preProcessContextForFrame(ctx, input)
		require.ErrorIs(t, err, merr.ErrFunctionFailed)
		input.Release()
	}
	input := preQueryFrame(t, pool, []string{"a", "b"})
	defer input.Release()
	ctx := types.NewFuncContextWithStage(pool, types.StagePreProcess).WithQueryContext(preQuerySnapshot("a", "b"))
	unchanged, err := preProcessContextForFrame(ctx, input)
	require.NoError(t, err)
	require.Same(t, ctx, unchanged, "identity PRE must retain the legacy context")
}
