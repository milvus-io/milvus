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

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func expansionTestFrame(t *testing.T, pool memory.Allocator) *DataFrame {
	t.Helper()
	b := NewDataFrameBuilder()
	defer b.Release()
	b.SetChunkSizes([]int64{2, 0, 1})
	for _, col := range []struct {
		name   string
		dt     arrow.DataType
		chunks []string
	}{
		{"$id", arrow.PrimitiveTypes.Int64, []string{"[10,20]", "[]", "[30]"}},
		{"$l1_source_index", arrow.PrimitiveTypes.Int64, []string{"[0,1]", "[]", "[0]"}},
		{"value", arrow.PrimitiveTypes.Int64, []string{"[100,null]", "[]", "[300]"}},
		{"detail", arrow.BinaryTypes.String, []string{`["A",null]`, `[]`, `["C"]`}},
		{"unused", arrow.PrimitiveTypes.Int64, []string{"[7,8]", "[]", "[9]"}},
	} {
		require.NoError(t, b.addColumn(col.name, selectionTestColumn(t, pool, col.dt, col.chunks...)))
		b.SetFieldNullable(col.name, col.name == "value" || col.name == "detail")
	}
	return b.Build()
}

// Only a test fixture: no operator registration, protocol or production code.
// Each logical query expands independently: 2->5, 0->0, 1->3.
func newExpansionTestOp(t *testing.T) *selectionTestOp {
	t.Helper()
	return &selectionTestOp{
		BaseOp: BaseOp{outputs: []string{"ordinal"}},
		run: func(ctx *types.FuncContext, input *DataFrame) (*OperatorResult, error) {
			require.Equal(t, []int64{2, 0, 1}, input.ChunkSizes())
			return &OperatorResult{
				Selection: &RowSelection{Chunks: []ChunkSelection{
					RowIndices([]int{1, 0, 1, 1, 0}), RowIndices(nil), RowIndices([]int{0, 0, 0}),
				}},
				Columns: []NamedColumn{{
					Field: arrow.Field{Name: "ordinal", Type: arrow.PrimitiveTypes.Int64},
					Data:  selectionTestColumn(t, ctx.Pool(), arrow.PrimitiveTypes.Int64, "[0,1,2,3,4]", "[]", "[0,1,2]"),
				}},
			}, nil
		},
	}
}

func assertExpansionColumn(t *testing.T, pool memory.Allocator, df *DataFrame, name string, chunks ...string) {
	t.Helper()
	col := df.Column(name)
	require.NotNil(t, col)
	want := selectionTestColumn(t, pool, col.DataType(), chunks...)
	defer want.Release()
	require.Len(t, col.Chunks(), len(chunks))
	for i := range chunks {
		require.True(t, array.Equal(want.Chunk(i), col.Chunk(i)), "column %s chunk %d: want %s, got %s", name, i, want.Chunk(i), col.Chunk(i))
	}
}

func TestSelectionExpansion(t *testing.T) {
	for _, prune := range []bool{false, true} {
		for _, inputFirst := range []bool{false, true} {
			name := map[bool]string{false: "all-columns", true: "pruned"}[prune] + "/" + map[bool]string{false: "output-first", true: "input-first"}[inputFirst]
			t.Run(name, func(t *testing.T) {
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				defer pool.AssertSize(t, 0)
				input := expansionTestFrame(t, pool)
				defer func() {
					if input != nil {
						input.Release()
					}
				}()
				chain := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Add(newExpansionTestOp(t))
				output, err := chain.ExecuteWithOptions(context.Background(), ExecuteOptions{
					EnableColumnPruning: prune,
					Downstream:          DownstreamSpec{RequiredColumns: []string{"ordinal", "value", "detail"}},
				}, input)
				require.NoError(t, err)
				defer func() {
					if output != nil {
						output.Release()
					}
				}()
				if inputFirst {
					input.Release()
					input = nil
				}
				require.Equal(t, []int64{5, 0, 3}, output.ChunkSizes())
				require.NoError(t, validateFrameLayout(output))
				assertExpansionColumn(t, pool, output, "$id", "[20,10,20,20,10]", "[]", "[30,30,30]")
				assertExpansionColumn(t, pool, output, "$l1_source_index", "[1,0,1,1,0]", "[]", "[0,0,0]")
				assertExpansionColumn(t, pool, output, "ordinal", "[0,1,2,3,4]", "[]", "[0,1,2]")
				assertExpansionColumn(t, pool, output, "value", "[null,100,null,null,100]", "[]", "[300,300,300]")
				assertExpansionColumn(t, pool, output, "detail",
					`[null,"A",null,null,"A"]`,
					`[]`, `["C","C","C"]`)
				require.Equal(t, !prune, output.HasColumn("unused"))
				output.Release()
				output = nil
				if !inputFirst {
					assertExpansionColumn(t, pool, input, "$id", "[10,20]", "[]", "[30]")
					assertExpansionColumn(t, pool, input, "value", "[100,null]", "[]", "[300]")
				}
			})
		}
	}
}

// These expressions consume actual expanded values rather than returning a
// fixed mask, so the chain exercises argument binding after row expansion.
type expansionTestExpr struct {
	filter bool
	calls  int
}

func (*expansionTestExpr) Name() string           { return "expansion_test" }
func (*expansionTestExpr) IsRunnable(string) bool { return true }
func (e *expansionTestExpr) OutputDataTypes() []arrow.DataType {
	if e.filter {
		return []arrow.DataType{arrow.FixedWidthTypes.Boolean}
	}
	return []arrow.DataType{arrow.PrimitiveTypes.Int64}
}

func (e *expansionTestExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	e.calls++
	chunks := make([]arrow.Array, len(inputs[0].Chunks()))
	defer releaseArrays(chunks)
	for i, chunk := range inputs[0].Chunks() {
		values := chunk.(*array.Int64)
		if e.filter {
			b := array.NewBooleanBuilder(ctx.Pool())
			for j := 0; j < values.Len(); j++ {
				b.Append(values.Value(j) >= 20)
			}
			chunks[i] = b.NewArray()
			b.Release()
		} else {
			b := array.NewInt64Builder(ctx.Pool())
			for j := 0; j < values.Len(); j++ {
				b.Append(values.Value(j) * 10)
			}
			chunks[i] = b.NewArray()
			b.Release()
		}
	}
	return []*arrow.Chunked{arrow.NewChunked(e.OutputDataTypes()[0], chunks)}, nil
}

func TestSelectionExpansionFollowedByOperators(t *testing.T) {
	for _, prune := range []bool{false, true} {
		t.Run(map[bool]string{false: "all-columns", true: "pruned"}[prune], func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)
			input := expansionTestFrame(t, pool)
			defer input.Release()
			rank := &expansionTestExpr{}
			mask := &expansionTestExpr{filter: true}
			chain := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).
				Add(newExpansionTestOp(t)).Map(rank, []string{"ordinal"}, []string{"rank"})
			// Observe the intermediate result to prove Map processed all expanded
			// rows, not just the final row retained by Limit.
			chain.Add(&selectionTestOp{
				BaseOp: BaseOp{inputs: []string{"rank"}},
				run: func(_ *types.FuncContext, df *DataFrame) (*OperatorResult, error) {
					require.Equal(t, []int64{5, 0, 3}, df.ChunkSizes())
					assertExpansionColumn(t, pool, df, "rank", "[0,10,20,30,40]", "[]", "[0,10,20]")
					return &OperatorResult{}, nil
				},
			})
			chain.Filter(mask, []string{"rank"}).Sort("rank", true, "").LimitWithOffset(1, 1)
			output, err := chain.ExecuteWithOptions(context.Background(), ExecuteOptions{
				EnableColumnPruning: prune,
				Downstream:          DownstreamSpec{RequiredColumns: []string{"ordinal", "rank", "value", "detail"}},
			}, input)
			require.NoError(t, err)
			defer output.Release()
			require.Equal(t, 1, rank.calls)
			require.Equal(t, 1, mask.calls)
			require.Equal(t, []int64{1, 0, 0}, output.ChunkSizes())
			assertExpansionColumn(t, pool, output, "$id", "[20]", "[]", "[]")
			assertExpansionColumn(t, pool, output, "$l1_source_index", "[1]", "[]", "[]")
			assertExpansionColumn(t, pool, output, "ordinal", "[3]", "[]", "[]")
			assertExpansionColumn(t, pool, output, "rank", "[30]", "[]", "[]")
			assertExpansionColumn(t, pool, output, "value", "[null]", "[]", "[]")
			assertExpansionColumn(t, pool, output, "detail", "[null]", "[]", "[]")
		})
	}
}

func TestSelectionExpansionRejectsInvalidResults(t *testing.T) {
	for _, name := range []string{"short-computed-column", "source-in-empty-chunk"} {
		t.Run(name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)
			input := expansionTestFrame(t, pool)
			defer input.Release()
			op := newExpansionTestOp(t)
			run := op.run
			op.run = func(ctx *types.FuncContext, df *DataFrame) (*OperatorResult, error) {
				result, err := run(ctx, df)
				if name == "short-computed-column" {
					result.Columns[0].Data.Release()
					result.Columns[0].Data = selectionTestColumn(t, pool, arrow.PrimitiveTypes.Int64, "[0,1,2,3]", "[]", "[0,1,2]")
				} else {
					result.Selection.Chunks[1] = RowIndices([]int{0})
				}
				return result, err
			}
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Add(op).Execute(input)
			require.Nil(t, output)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			if name == "short-computed-column" {
				require.Contains(t, err.Error(), "output \"ordinal\"")
			} else {
				require.Contains(t, err.Error(), "out of range in chunk 1")
			}
		})
	}
}
