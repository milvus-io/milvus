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

	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func udfAggregateTestFrame(t *testing.T, alloc memory.Allocator, chunks ...[]int64) *DataFrame {
	t.Helper()
	builder := NewDataFrameBuilder()
	defer builder.Release()
	sizes := make([]int64, len(chunks))
	arrays := make([]arrow.Array, len(chunks))
	for i, values := range chunks {
		b := array.NewInt64Builder(alloc)
		b.AppendValues(values, nil)
		arrays[i] = b.NewArray()
		b.Release()
		sizes[i] = int64(len(values))
	}
	builder.SetChunkSizes(sizes).SetIdentityLineage()
	require.NoError(t, builder.AddColumnFromChunks("key", arrays))
	return builder.Build()
}

func TestUDFAggregateTableGroupingAndEmptySchema(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			t.Run(map[bool]string{true: "keyed", false: "global"}[keyed]+map[bool]string{true: "empty", false: "rows"}[empty], func(t *testing.T) {
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				values := []int64{2, 2, 3}
				if empty {
					values = nil
				}
				input := udfAggregateTestFrame(t, alloc, values, nil)
				defer input.Release()
				calls := 0
				fn := &operatorTransformExpr{name: "test_udf_aggregate", stages: []string{types.StagePostProcess}, execute: func(ctx *types.FuncContext, columns []*arrow.Chunked) ([]*arrow.Chunked, error) {
					calls++
					require.True(t, ctx.Reduction())
					require.True(t, ctx.TableResult())
					chunks := make([]arrow.Array, len(columns[0].Chunks()))
					for i, chunk := range columns[0].Chunks() {
						b := array.NewInt64Builder(ctx.Pool())
						b.Append(int64(chunk.Len()))
						chunks[i] = b.NewArray()
						b.Release()
					}
					result := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
					for _, chunk := range chunks {
						chunk.Release()
					}
					return []*arrow.Chunked{result}, nil
				}}
				op := &AggregateOp{BaseOp: BaseOp{inputs: []string{"key"}, outputs: []string{"count"}}, function: fn}
				if keyed {
					op.keys = []string{"key"}
					op.inputs = []string{"key", "key"}
					op.outputs = []string{"category", "count"}
				}
				ctx := types.NewFuncContextFull(context.Background(), alloc, types.StagePostProcess).WithTableResult(true)
				output, err := op.Execute(ctx, input)
				require.NoError(t, err)
				defer output.Release()
				require.Equal(t, 1, calls)
				require.False(t, ctx.Reduction(), "operator must not mutate the caller context")
				if keyed {
					expected := []int64{2, 0}
					if empty {
						expected = []int64{0, 0}
					}
					require.Equal(t, expected, output.ChunkSizes())
					if !empty {
						require.Equal(t, []int64{2, 1}, output.Column("count").Chunk(0).(*array.Int64).Int64Values())
						require.Equal(t, []int64{2, 3}, output.Column("category").Chunk(0).(*array.Int64).Int64Values())
						require.Equal(t, transform.RowLineage{{Group: 0, Row: 0}, {Group: 0, Row: 1}}, output.Lineage()[0][0])
					}
				} else {
					require.Equal(t, []int64{1, 1}, output.ChunkSizes())
					require.Equal(t, int64(len(values)), output.Column("count").Chunk(0).(*array.Int64).Value(0))
					require.Empty(t, output.Lineage()[1][0])
				}
			})
		}
	}
}

func TestUDFAggregateRejectsMalformedOutputAndCancellation(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	input := udfAggregateTestFrame(t, alloc, []int64{1, 2})
	defer input.Release()
	calls := 0
	fn := &operatorTransformExpr{name: "test_udf_aggregate", stages: []string{types.StagePostProcess}, execute: func(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
		calls++
		inputs[0].Retain()
		return []*arrow.Chunked{inputs[0]}, nil
	}}
	op := &AggregateOp{BaseOp: BaseOp{inputs: []string{"key"}, outputs: []string{"count"}}, function: fn}
	ctx := types.NewFuncContextFull(context.Background(), alloc, types.StagePostProcess).WithTableResult(true)
	result, err := op.Execute(ctx, input)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.Nil(t, result)
	require.Equal(t, 1, calls)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = op.Execute(types.NewFuncContextFull(cancelled, alloc, types.StagePostProcess).WithTableResult(true), input)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}
