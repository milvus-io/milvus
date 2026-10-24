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
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func selectionTestColumn(t *testing.T, pool memory.Allocator, dt arrow.DataType, chunks ...string) *arrow.Chunked {
	t.Helper()
	arrays := make([]arrow.Array, len(chunks))
	defer func() {
		for _, arr := range arrays {
			if arr != nil {
				arr.Release()
			}
		}
	}()
	for i, chunk := range chunks {
		arr, _, err := array.FromJSON(pool, dt, strings.NewReader(chunk))
		require.NoError(t, err)
		arrays[i] = arr
	}
	return arrow.NewChunked(dt, arrays)
}

// Includes an empty logical query, nullable scalar columns, and a source token
// whose meaning must survive sorting and repeated selections without renumbering.
func selectionTestFrame(t *testing.T, pool memory.Allocator) *DataFrame {
	t.Helper()
	b := NewDataFrameBuilder()
	defer b.Release()
	b.SetChunkSizes([]int64{4, 0, 2}).SetMetadata("query", "test")
	require.NoError(t, b.addColumn("$id", selectionTestColumn(t, pool, arrow.PrimitiveTypes.Int64, "[10,11,12,13]", "[]", "[20,21]")))
	require.NoError(t, b.addColumn("$l1_source_index", selectionTestColumn(t, pool, arrow.PrimitiveTypes.Int64, "[0,1,2,3]", "[]", "[0,1]")))
	require.NoError(t, b.addColumn("keep", selectionTestColumn(t, pool, arrow.FixedWidthTypes.Boolean, "[true,null,false,true]", "[]", "[false,true]")))
	require.NoError(t, b.addColumn("detail", selectionTestColumn(t, pool, arrow.BinaryTypes.String,
		`["a",null,"","d"]`, `[]`, `["e","f"]`)))
	b.SetFieldType("$id", schemapb.DataType_Int64).SetFieldID("$id", 100)
	b.SetFieldNullable("keep", true).SetFieldNullable("detail", true)
	df := b.Build()
	fields := df.schema.Fields()
	fields[3].Metadata = arrow.NewMetadata([]string{"field-key"}, []string{"field-value"})
	md := arrow.NewMetadata([]string{"schema-key"}, []string{"schema-value"})
	df.schema = arrow.NewSchema(fields, &md)
	return df
}

type selectionTestOp struct {
	BaseOp
	run func(*types.FuncContext, *DataFrame) (*OperatorResult, error)
}

func (*selectionTestOp) Name() string   { return "selection_test" }
func (*selectionTestOp) String() string { return "selection_test" }
func (o *selectionTestOp) Execute(ctx *types.FuncContext, df *DataFrame) (*OperatorResult, error) {
	return o.run(ctx, df)
}

type selectionMaskExpr struct {
	dt     arrow.DataType
	chunks []string
	err    error
	cancel context.CancelFunc
}

func (*selectionMaskExpr) Name() string                      { return "selection_mask" }
func (*selectionMaskExpr) OutputDataTypes() []arrow.DataType { return nil }
func (*selectionMaskExpr) IsRunnable(string) bool            { return true }
func (e *selectionMaskExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if e.cancel != nil {
		e.cancel()
	}
	if e.dt == nil {
		return []*arrow.Chunked{nil}, e.err
	}
	var arrays []arrow.Array
	defer func() {
		for _, arr := range arrays {
			arr.Release()
		}
	}()
	for _, chunk := range e.chunks {
		arr, _, err := array.FromJSON(ctx.Pool(), e.dt, strings.NewReader(chunk))
		if err != nil {
			return nil, err
		}
		arrays = append(arrays, arr)
	}
	return []*arrow.Chunked{arrow.NewChunked(e.dt, arrays)}, e.err
}

func TestSelectionSequentialOperators(t *testing.T) {
	for _, prune := range []bool{false, true} {
		t.Run(map[bool]string{false: "all-columns", true: "pruned"}[prune], func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)
			input := selectionTestFrame(t, pool)
			schema := input.schema
			// Add a nullable computed column before Filter, Sort, and Limit. It is
			// already aligned and owns a separate reference to borrowed input.
			op := &selectionTestOp{BaseOp: BaseOp{inputs: []string{"detail"}, outputs: []string{"computed"}}}
			op.run = func(_ *types.FuncContext, df *DataFrame) (*OperatorResult, error) {
				col := df.Column("detail")
				col.Retain()
				f := df.schema.Field(df.nameIndex["detail"])
				f.Name = "computed"
				return &OperatorResult{Columns: []NamedColumn{{Field: f, Data: col}}}, nil
			}
			fc := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Add(op)
			fc.Filter(&selectionMaskExpr{dt: arrow.FixedWidthTypes.Boolean, chunks: []string{"[true,null,false,true]", "[]", "[false,true]"}}, []string{"keep"})
			fc.Sort("$id", true, "").LimitWithOffset(1, 1)
			out, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{EnableColumnPruning: prune, Downstream: DownstreamSpec{RequiredColumns: []string{"detail", "computed"}}}, input)
			require.NoError(t, err)
			input.Release() // verify independently retained storage
			require.Equal(t, []int64{1, 0, 0}, out.ChunkSizes())
			require.Equal(t, int64(10), out.Column("$id").Chunk(0).(*array.Int64).Value(0))
			require.Equal(t, int64(0), out.Column("$l1_source_index").Chunk(0).(*array.Int64).Value(0))
			require.True(t, array.Equal(out.Column("detail").Chunk(0), out.Column("computed").Chunk(0)))
			want := selectionTestColumn(t, pool, out.Column("detail").DataType(), `["a"]`)
			require.True(t, array.Equal(want.Chunk(0), out.Column("detail").Chunk(0)))
			want.Release()
			require.True(t, schema.Metadata().Equal(out.schema.Metadata()))
			require.True(t, schema.Field(3).Equal(out.schema.Field(out.nameIndex["detail"])))
			require.Equal(t, "test", out.metadata["query"])
			require.Equal(t, int64(100), out.fieldIDs["$id"])
			require.False(t, out.HasColumn("$row_id"))
			out.Release()
		})
	}
}

func TestSelectionReorderRepeatAndNewColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	input := selectionTestFrame(t, pool)
	defer input.Release()
	result := &OperatorResult{
		Selection: &RowSelection{Chunks: []ChunkSelection{RowIndices([]int{3, 1, 1}), AllRows(), RowRange(1, 2)}},
		Columns:   []NamedColumn{{Field: arrow.Field{Name: "label", Type: arrow.BinaryTypes.String}, Data: selectionTestColumn(t, pool, arrow.BinaryTypes.String, `["first","second","third"]`, `[]`, `["last"]`)}},
	}
	out, err := assembleOperatorResult(types.NewFuncContext(pool), input, []string{"label"}, result)
	require.NoError(t, err)
	result.Release()
	require.Equal(t, []int64{13, 11, 11}, out.Column("$id").Chunk(0).(*array.Int64).Int64Values())
	require.Equal(t, []int64{3, 1, 1}, out.Column("$l1_source_index").Chunk(0).(*array.Int64).Int64Values())
	require.Equal(t, "first", out.Column("label").Chunk(0).(*array.String).Value(0))
	require.True(t, out.Column("detail").Chunk(0).IsNull(1))
	next := &OperatorResult{Selection: &RowSelection{Chunks: []ChunkSelection{RowIndices([]int{1}), AllRows(), RowIndices(nil)}}}
	last, err := assembleOperatorResult(types.NewFuncContext(pool), out, nil, next)
	require.NoError(t, err)
	out.Release()
	require.Equal(t, int64(11), last.Column("$id").Chunk(0).(*array.Int64).Value(0))
	require.Equal(t, "second", last.Column("label").Chunk(0).(*array.String).Value(0))
	last.Release() // output-first release order, original input remains readable
	require.Equal(t, int64(13), input.Column("$id").Chunk(0).(*array.Int64).Value(3))
}

func TestSelectionInvalidContracts(t *testing.T) {
	cases := []struct {
		name      string
		selection *RowSelection
	}{
		{"missing-chunks", &RowSelection{}},
		{"unset-chunk", &RowSelection{Chunks: []ChunkSelection{{}, AllRows(), AllRows()}}},
		{"negative", &RowSelection{Chunks: []ChunkSelection{RowIndices([]int{-1}), AllRows(), AllRows()}}},
		{"past-end", &RowSelection{Chunks: []ChunkSelection{RowIndices([]int{4}), AllRows(), AllRows()}}},
		{"range", &RowSelection{Chunks: []ChunkSelection{RowRange(2, 1), AllRows(), AllRows()}}},
		{"range-end", &RowSelection{Chunks: []ChunkSelection{RowRange(0, 5), AllRows(), AllRows()}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)
			input := selectionTestFrame(t, pool)
			defer input.Release()
			op := &selectionTestOp{run: func(*types.FuncContext, *DataFrame) (*OperatorResult, error) {
				return &OperatorResult{Selection: tc.selection}, nil
			}}
			fc := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Add(op)
			out, err := fc.Execute(input)
			require.Nil(t, out)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
}

func TestSelectionOutputValidationAndCleanup(t *testing.T) {
	for _, name := range []string{"nil-result", "nil-column", "missing-output", "undeclared", "duplicate", "wrong-shape", "wrong-type", "nonnullable", "error-with-result", "canceled-with-result", "timeout-with-result"} {
		t.Run(name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)
			input := selectionTestFrame(t, pool)
			defer input.Release()
			op := &selectionTestOp{BaseOp: BaseOp{outputs: []string{"new"}}}
			op.run = func(*types.FuncContext, *DataFrame) (*OperatorResult, error) {
				if name == "nil-result" {
					return nil, nil
				}
				r := &OperatorResult{}
				if name == "missing-output" {
					return r, nil
				}
				f := arrow.Field{Name: "new", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
				var col *arrow.Chunked
				if name != "nil-column" {
					col = selectionTestColumn(t, pool, f.Type, "[1,2,null,4]", "[]", "[5,6]")
				}
				r.Columns = []NamedColumn{{Field: f, Data: col}}
				switch name {
				case "undeclared":
					r.Columns[0].Field.Name = "unlisted"
				case "duplicate":
					col.Retain()
					r.Columns = append(r.Columns, r.Columns[0])
				case "wrong-shape":
					r.Selection = &RowSelection{Chunks: []ChunkSelection{RowRange(0, 1), AllRows(), AllRows()}}
				case "wrong-type":
					r.Columns[0].Field.Type = arrow.BinaryTypes.String
				case "nonnullable":
					r.Columns[0].Field.Nullable = false
				case "error-with-result":
					return r, merr.ErrServiceUnavailable
				case "canceled-with-result":
					return r, context.Canceled
				case "timeout-with-result":
					return r, context.DeadlineExceeded
				}
				return r, nil
			}
			out, err := ExecuteOperator(types.NewFuncContext(pool), input, op)
			require.Nil(t, out)
			switch name {
			case "error-with-result":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			case "canceled-with-result":
				require.ErrorIs(t, err, context.Canceled)
			case "timeout-with-result":
				require.ErrorIs(t, err, context.DeadlineExceeded)
			default:
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			}
		})
	}
}

func TestFilterMaskValidationAndErrorPreservation(t *testing.T) {
	for _, name := range []string{"nil", "wrong-type", "few-chunks", "short-chunk", "long-chunk", "error-with-mask", "cancel"} {
		t.Run(name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)
			input := selectionTestFrame(t, pool)
			defer input.Release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			expr := &selectionMaskExpr{dt: arrow.FixedWidthTypes.Boolean, chunks: []string{"[true,false,true,false]", "[]", "[true,false]"}}
			switch name {
			case "nil":
				expr.dt = nil
			case "wrong-type":
				expr.dt = arrow.PrimitiveTypes.Int64
				expr.chunks = []string{"[1,0,1,0]", "[]", "[1,0]"}
			case "few-chunks":
				expr.chunks = expr.chunks[:2]
			case "short-chunk":
				expr.chunks[0] = "[true]"
			case "long-chunk":
				expr.chunks[2] = "[true,true,true]"
			case "error-with-mask":
				expr.err = merr.ErrServiceUnavailable
			case "cancel":
				expr.cancel = cancel
				expr.err = context.Canceled
			}
			op, err := NewFilterOp(expr, []string{"keep"})
			require.NoError(t, err)
			out, err := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Add(op).ExecuteWithContext(ctx, input)
			require.Nil(t, out)
			switch name {
			case "error-with-mask":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Equal(t, merr.Code(merr.ErrServiceUnavailable), merr.Status(err).Code)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			default:
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			}
		})
	}
}

func TestSelectionZeroChunksAndOverflowSafeLimit(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	b := NewDataFrameBuilder()
	require.NoError(t, b.addColumn("value", arrow.NewChunked(arrow.PrimitiveTypes.Int64, nil)))
	empty := b.Build()
	out, err := ExecuteOperator(types.NewFuncContext(pool), empty, NewLimitOp(1, 0))
	require.NoError(t, err)
	empty.Release()
	require.True(t, out.HasColumn("value"))
	require.Zero(t, out.NumChunks())
	out.Release()
	input := selectionTestFrame(t, pool)
	defer input.Release()
	out, err = ExecuteOperator(types.NewFuncContext(pool), input, NewLimitOp(math.MaxInt64, 1))
	require.NoError(t, err)
	require.Equal(t, []int64{3, 0, 1}, out.ChunkSizes())
	out.Release()
}

func TestSelectionReplacementMetadataAndEmptySelection(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	input := selectionTestFrame(t, pool)
	defer input.Release()
	replacement := selectionTestColumn(t, pool, arrow.BinaryTypes.String, `["a","b","c","d"]`, `[]`, `["e","f"]`)
	metadata := arrow.NewMetadata([]string{"computed"}, []string{"yes"})
	r := &OperatorResult{Columns: []NamedColumn{{Field: arrow.Field{Name: "$id", Type: arrow.BinaryTypes.String, Metadata: metadata}, Data: replacement}}}
	output, err := assembleOperatorResult(types.NewFuncContext(pool), input, []string{"$id"}, r)
	require.NoError(t, err)
	r.Release()
	require.Equal(t, input.ColumnNames(), output.ColumnNames())
	require.Equal(t, schemapb.DataType_VarChar, output.fieldTypes["$id"])
	_, hasID := output.fieldIDs["$id"]
	require.False(t, hasID)
	require.True(t, metadata.Equal(output.schema.Field(0).Metadata))
	empty := &OperatorResult{Selection: &RowSelection{Chunks: []ChunkSelection{RowIndices(nil), RowIndices(nil), RowIndices(nil)}}}
	selected, err := assembleOperatorResult(types.NewFuncContext(pool), output, nil, empty)
	require.NoError(t, err)
	output.Release()
	require.Equal(t, []int64{0, 0, 0}, selected.ChunkSizes())
	require.Equal(t, input.ColumnNames(), selected.ColumnNames())
	require.True(t, metadata.Equal(selected.schema.Field(0).Metadata))
	selected.Release()
}

func TestSelectionRejectsInvalidInputBeforeOperator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	input := selectionTestFrame(t, pool)
	defer input.Release()
	input.chunkSizes[0]++
	called := false
	op := &selectionTestOp{run: func(*types.FuncContext, *DataFrame) (*OperatorResult, error) {
		called = true
		return &OperatorResult{}, nil
	}}
	output, err := ExecuteOperator(types.NewFuncContext(pool), input, op)
	require.Nil(t, output)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.False(t, called)
}

func TestGroupBySelectionKeepsScalarColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	input := selectionTestFrame(t, pool)
	defer input.Release()
	scores := selectionTestColumn(t, pool, arrow.PrimitiveTypes.Float32, "[1,2,3,4]", "[]", "[5,6]")
	scoreResult := &OperatorResult{Columns: []NamedColumn{{Field: arrow.Field{Name: "$score", Type: arrow.PrimitiveTypes.Float32}, Data: scores}}}
	withScore, err := assembleOperatorResult(types.NewFuncContext(pool), input, []string{"$score"}, scoreResult)
	require.NoError(t, err)
	scoreResult.Release()
	// true-group chooses id 13, false-group chooses id 12; NULL is its own
	// group. GroupBy then Limit must keep scalar payloads aligned throughout.
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).
		Add(NewGroupByOp("keep", 1, 3, 0)).Limit(1)
	out, err := fc.Execute(withScore)
	require.NoError(t, err)
	large, err := ExecuteOperator(types.NewFuncContext(pool), withScore, NewGroupByOp("keep", 1, math.MaxInt64, 1))
	require.NoError(t, err)
	require.Equal(t, []int64{2, 0, 1}, large.ChunkSizes())
	large.Release()
	withScore.Release()
	require.Equal(t, []int64{1, 0, 1}, out.ChunkSizes())
	require.Equal(t, int64(13), out.Column("$id").Chunk(0).(*array.Int64).Value(0))
	require.Equal(t, int64(3), out.Column("$l1_source_index").Chunk(0).(*array.Int64).Value(0))
	require.Equal(t, float32(4), out.Column("$group_score").Chunk(0).(*array.Float32).Value(0))
	want := selectionTestColumn(t, pool, input.Column("detail").DataType(), `["d"]`)
	require.True(t, array.Equal(want.Chunk(0), out.Column("detail").Chunk(0)))
	want.Release()
	out.Release()
}

// Cancel during allocation of a selected column, after validation and operator
// execution. Assembly completes without polling for cancellation.
type selectionCancelAllocator struct {
	memory.Allocator
	cancel context.CancelFunc
}

func (a *selectionCancelAllocator) Allocate(size int) []byte {
	data := a.Allocator.Allocate(size)
	a.cancel()
	return data
}

func (a *selectionCancelAllocator) Reallocate(size int, data []byte) []byte {
	out := a.Allocator.Reallocate(size, data)
	a.cancel()
	return out
}

func TestSelectionCancellationBeforeExecution(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "timeout"}[timeout], func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)
			input := selectionTestFrame(t, pool)
			defer input.Release()
			var ctx context.Context
			var cancel context.CancelFunc
			if timeout {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			cancel()
			called := false
			op := &selectionTestOp{run: func(*types.FuncContext, *DataFrame) (*OperatorResult, error) {
				called = true
				return &OperatorResult{}, nil
			}}
			output, err := ExecuteOperator(types.NewFuncContextWithContext(ctx, pool), input, op)
			require.Nil(t, output)
			require.ErrorIs(t, err, ctx.Err())
			require.False(t, called)
		})
	}
}

func TestSelectionCancellationBetweenOperators(t *testing.T) {
	for _, phase := range []string{"execution", "assembly"} {
		for _, next := range []bool{false, true} {
			name := phase + map[bool]string{false: "/last-operator", true: "/next-operator"}[next]
			t.Run(name, func(t *testing.T) {
				pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
				defer pool.AssertSize(t, 0)
				input := selectionTestFrame(t, pool)
				defer input.Release()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var allocator memory.Allocator = pool
				if phase == "assembly" {
					allocator = &selectionCancelAllocator{Allocator: pool, cancel: cancel}
				}
				op := &selectionTestOp{run: func(*types.FuncContext, *DataFrame) (*OperatorResult, error) {
					if phase == "execution" {
						cancel()
					}
					return &OperatorResult{Selection: &RowSelection{Chunks: []ChunkSelection{
						RowIndices([]int{3, 1}), AllRows(), RowIndices([]int{0}),
					}}}, nil
				}}
				chain := NewFuncChainWithAllocator(allocator).SetStage(types.StageL2Rerank).Add(op)
				nextCalled := false
				if next {
					chain.Add(&selectionTestOp{run: func(*types.FuncContext, *DataFrame) (*OperatorResult, error) {
						nextCalled = true
						return &OperatorResult{}, nil
					}})
				}
				output, err := chain.ExecuteWithContext(ctx, input)
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.False(t, nextCalled)
				if next {
					require.Nil(t, output)
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, merr.Status(context.Canceled).Code, merr.Status(err).Code)
				} else {
					require.NoError(t, err)
					defer output.Release()
					require.Equal(t, []int64{2, 0, 1}, output.ChunkSizes())
					require.Equal(t, []int64{13, 11}, output.Column("$id").Chunk(0).(*array.Int64).Int64Values())
				}
			})
		}
	}
}

func TestSelectionUnsupportedTypesAndCleanup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		dt     arrow.DataType
		values string
	}{
		{"binary", arrow.BinaryTypes.Binary, `["YQ==","Yg=="]`},
		{"list", arrow.ListOf(arrow.PrimitiveTypes.Int64), `[[1],[2]]`},
		{"struct", arrow.StructOf(arrow.Field{Name: "value", Type: arrow.BinaryTypes.String}), `[{"value":"a"},{"value":"b"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)
			b := NewDataFrameBuilder()
			defer b.Release()
			b.SetChunkSizes([]int64{2})
			// The first column is gathered successfully before the second fails.
			require.NoError(t, b.addColumn("$id", selectionTestColumn(t, pool, arrow.PrimitiveTypes.Int64, "[10,20]")))
			require.NoError(t, b.addColumn("unsupported", selectionTestColumn(t, pool, tc.dt, tc.values)))
			input := b.Build()
			defer input.Release()
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Sort("$id", true, "").Execute(input)
			require.Nil(t, output)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			require.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Status(err).Code)
			require.Contains(t, err.Error(), "unsupported array type")
			require.Contains(t, err.Error(), `select column "unsupported"`)
			require.Equal(t, []int64{10, 20}, input.Column("$id").Chunk(0).(*array.Int64).Int64Values())
		})
	}
}

func TestSelectionComputedColumnsPrecedeRetainedColumns(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	b := NewDataFrameBuilder()
	defer b.Release()
	b.SetChunkSizes([]int64{2})
	for _, name := range []string{"$id", "a", "b"} {
		require.NoError(t, b.addColumn(name, selectionTestColumn(t, pool, arrow.PrimitiveTypes.Int64, "[10,20]")))
		b.SetFieldType(name, schemapb.DataType_Int64)
	}
	require.NoError(t, b.addColumn("$score", selectionTestColumn(t, pool, arrow.PrimitiveTypes.Float32, "[1,2]")))
	input := b.Build()
	defer input.Release()
	result := &OperatorResult{
		Selection: &RowSelection{Chunks: []ChunkSelection{RowIndices([]int{1, 0})}},
		Columns: []NamedColumn{
			{Field: arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64}, Data: selectionTestColumn(t, pool, arrow.PrimitiveTypes.Int64, "[300,400]")},
			{Field: arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Int64}, Data: selectionTestColumn(t, pool, arrow.PrimitiveTypes.Int64, "[500,600]")},
		},
	}
	defer result.Release()
	output, err := assembleOperatorResult(types.NewFuncContext(pool), input, []string{"b", "c"}, result)
	require.NoError(t, err)
	defer output.Release()
	require.Equal(t, []string{"b", "c", "$id", "a", "$score"}, output.ColumnNames())
	require.NoError(t, validateFrameLayout(output))
	require.Equal(t, []int64{300, 400}, output.Column("b").Chunk(0).(*array.Int64).Int64Values())
	require.Equal(t, []int64{20, 10}, output.Column("a").Chunk(0).(*array.Int64).Int64Values())
	exported, err := ToSearchResultData(output)
	require.NoError(t, err)
	require.Len(t, exported.FieldsData, 3)
	require.Equal(t, "b", exported.FieldsData[0].FieldName)
	require.Equal(t, []int64{300, 400}, exported.FieldsData[0].GetScalars().GetLongData().GetData())
	require.Equal(t, "c", exported.FieldsData[1].FieldName)
	require.Equal(t, "a", exported.FieldsData[2].FieldName)
	require.Equal(t, []int64{20, 10}, exported.FieldsData[2].GetScalars().GetLongData().GetData())
}

// Count cumulative Arrow allocation requests, including replacement buffers,
// so benchmarks expose repeated growth copies rather than only retained memory.
type selectionBenchmarkAllocator struct {
	memory.Allocator
	bytes int64
}

func (a *selectionBenchmarkAllocator) Allocate(size int) []byte {
	a.bytes += int64(size)
	return a.Allocator.Allocate(size)
}

func (a *selectionBenchmarkAllocator) Reallocate(size int, data []byte) []byte {
	a.bytes += int64(size)
	return a.Allocator.Reallocate(size, data)
}

func BenchmarkSelectionStringUUID(b *testing.B) {
	for _, rows := range []int{1000, 4000, 16000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			pool := memory.NewGoAllocator()
			builder := array.NewStringBuilder(pool)
			indices := make([]int, rows)
			for i := range indices {
				builder.Append(fmt.Sprintf("%08x-0000-0000-0000-%012x", i, i))
				indices[i] = rows - 1 - i
			}
			values := builder.NewArray()
			builder.Release()
			input := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{values})
			values.Release()
			defer input.Release()
			selection := &RowSelection{Chunks: []ChunkSelection{RowIndices(indices)}}
			allocator := &selectionBenchmarkAllocator{Allocator: pool}
			ctx := types.NewFuncContext(allocator)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				output, err := selectColumn(ctx, input, selection)
				if err != nil {
					b.Fatal(err)
				}
				output.Release()
			}
			b.StopTimer()
			b.ReportMetric(float64(allocator.bytes)/float64(b.N), "arrow-B/op")
		})
	}
}
