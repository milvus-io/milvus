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
	"math"
	"strings"
	"sync/atomic"
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

func postTableFixture(t *testing.T, pool memory.Allocator, empty bool) *DataFrame {
	t.Helper()
	keys := [][]string{{"b", "a", "b", ""}, {}, {"a", "b"}}
	values := [][]int32{{2, 1, 0, 4}, {}, {3, 5}}
	texts := [][]string{{"B0", "A0", "B1", "NULL"}, {}, {"A2", "B2"}}
	if empty {
		keys, values, texts = [][]string{{}, {}}, [][]int32{{}, {}}, [][]string{{}, {}}
	}
	columns := map[string][]arrow.Array{}
	sizes := make([]int64, len(keys))
	for group := range keys {
		key, value, text := array.NewStringBuilder(pool), array.NewInt32Builder(pool), array.NewStringBuilder(pool)
		id, query := array.NewInt64Builder(pool), array.NewInt64Builder(pool)
		for row := range keys[group] {
			if group == 0 && row == 3 {
				key.AppendNull()
			} else {
				key.Append(keys[group][row])
			}
			if group == 0 && row == 2 {
				value.AppendNull()
			} else {
				value.Append(values[group][row])
			}
			text.Append(texts[group][row])
			id.Append(int64(100*group + row))
			query.Append(int64(group))
		}
		columns["key"] = append(columns["key"], key.NewArray())
		columns["value"] = append(columns["value"], value.NewArray())
		columns["text"] = append(columns["text"], text.NewArray())
		columns[types.IDFieldName] = append(columns[types.IDFieldName], id.NewArray())
		columns[types.QueryIndexFieldName] = append(columns[types.QueryIndexFieldName], query.NewArray())
		key.Release()
		value.Release()
		text.Release()
		id.Release()
		query.Release()
		sizes[group] = int64(len(keys[group]))
	}
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(sizes).SetIdentityLineage()
	for _, name := range []string{"key", "value", "text", types.IDFieldName, types.QueryIndexFieldName} {
		require.NoError(t, builder.AddColumnFromChunks(name, columns[name]))
	}
	builder.SetFieldNullable("key", true).SetFieldNullable("value", true)
	return builder.Build()
}

func postTableNativeSpecs() ([]string, []AggregateSpec) {
	return []string{"bucket", "sum", "rows", "valid", "avg", "min", "max", "first", "last", "values"}, []AggregateSpec{
		{Op: "sum", Input: "value"},
		{Op: "count"},
		{Op: "count", Input: "value"},
		{Op: "avg", Input: "value"},
		{Op: "min", Input: "value"},
		{Op: "max", Input: "value"},
		{Op: "first", Input: "text"},
		{Op: "last", Input: "text"},
		{Op: "collect_list", Input: "value"},
	}
}

func TestPostTableConcatAggregateAcrossQueries(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := postTableFixture(t, pool, false)
	defer input.Release()
	outputs, specs := postTableNativeSpecs()
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
		Concat().Aggregate([]string{"key"}, outputs, specs...).ExecuteWithContext(context.Background(), input)
	require.NoError(t, err)
	defer output.Release()
	require.Equal(t, []int64{3}, output.chunkSizes)
	require.Equal(t, outputs, output.ColumnNames())
	require.Equal(t, "b", output.Column("bucket").Chunk(0).(*array.String).Value(0))
	require.Equal(t, "a", output.Column("bucket").Chunk(0).(*array.String).Value(1))
	require.True(t, output.Column("bucket").Chunk(0).IsNull(2))
	require.Equal(t, []int64{7, 4, 4}, output.Column("sum").Chunk(0).(*array.Int64).Int64Values())
	require.Equal(t, []int64{3, 2, 1}, output.Column("rows").Chunk(0).(*array.Int64).Int64Values())
	require.Equal(t, []int64{2, 2, 1}, output.Column("valid").Chunk(0).(*array.Int64).Int64Values())
	require.Equal(t, []float64{3.5, 2, 4}, output.Column("avg").Chunk(0).(*array.Float64).Float64Values())
	require.Equal(t, []int32{2, 1, 4}, output.Column("min").Chunk(0).(*array.Int32).Int32Values())
	require.Equal(t, []int32{5, 3, 4}, output.Column("max").Chunk(0).(*array.Int32).Int32Values())
	require.Equal(t, "B0", output.Column("first").Chunk(0).(*array.String).Value(0))
	require.Equal(t, "B2", output.Column("last").Chunk(0).(*array.String).Value(0))
	list := output.Column("values").Chunk(0).(*array.List)
	start, end := list.ValueOffsets(0)
	require.EqualValues(t, 3, end-start)
	require.True(t, list.ListValues().IsNull(int(start)+1))
	require.Equal(t, int32(5), list.ListValues().(*array.Int32).Value(int(end)-1))
	require.Equal(t, transform.RowLineage{{Group: 0, Row: 0}, {Group: 0, Row: 2}, {Group: 2, Row: 1}}, output.Lineage()[0][0])
	batch, err := ToArrowBatch(input)
	require.NoError(t, err)
	defer ReleaseArrowBatch(batch)
	result, err := ToArrowBatch(output)
	require.NoError(t, err)
	defer ReleaseArrowBatch(result)
	require.NoError(t, transform.ValidateLineage(batch, transform.Result{Data: result, Lineage: output.Lineage()}))
}

func TestPostTableAggregateRetainsCurrentGroupBoundaries(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := postTableFixture(t, pool, false)
	defer input.Release()
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
		Aggregate([]string{"key"}, []string{"key", "rows"}, AggregateSpec{Op: "count"}).Execute(input)
	require.NoError(t, err)
	defer output.Release()
	require.Equal(t, []int64{3, 0, 2}, output.chunkSizes)
	require.Equal(t, int64(2), output.Column("rows").Chunk(0).(*array.Int64).Value(0))
	require.Equal(t, int64(1), output.Column("rows").Chunk(2).(*array.Int64).Value(1))
}

func TestPostTableEmptyAggregateHasTypedSQLValues(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		t.Run(map[bool]string{false: "global", true: "keyed"}[keyed], func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			input := postTableFixture(t, pool, true)
			defer input.Release()
			outputs, specs := postTableNativeSpecs()
			keys := []string{"key"}
			if !keyed {
				keys = nil
				outputs = outputs[1:]
			}
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
				Aggregate(keys, outputs, specs...).Execute(input)
			require.NoError(t, err)
			defer output.Release()
			for group := range output.NumChunks() {
				if keyed {
					require.Zero(t, output.chunkSizes[group])
					continue
				}
				require.EqualValues(t, 1, output.chunkSizes[group])
				require.EqualValues(t, 0, output.Column("rows").Chunk(group).(*array.Int64).Value(0))
				require.EqualValues(t, 0, output.Column("valid").Chunk(group).(*array.Int64).Value(0))
				for _, name := range []string{"sum", "avg", "min", "max", "first", "last"} {
					require.True(t, output.Column(name).Chunk(group).IsNull(0), name)
				}
				list := output.Column("values").Chunk(group).(*array.List)
				start, end := list.ValueOffsets(0)
				require.Equal(t, start, end)
				require.False(t, list.IsNull(0))
				require.Empty(t, output.Lineage()[group][0])
			}
			require.Equal(t, arrow.INT64, output.Column("sum").DataType().ID())
			require.Equal(t, arrow.FLOAT64, output.Column("avg").DataType().ID())
		})
	}
}

func TestPostTableCountStarProtoFactory(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := postTableFixture(t, pool, false)
	defer input.Release()
	plan, err := transform.PostTable("count", &schemapb.FunctionChainOp{
		Op: "aggregate", Inputs: []string{}, Outputs: []string{"hits"}, Params: map[string]*schemapb.FunctionParamValue{
			"keys": {Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{}}},
			"aggregates": {Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{Values: []*schemapb.FunctionParamValue{
				{Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{"op": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "count"}}}}}},
			}}}},
		},
	})
	require.NoError(t, err)
	repr, err := ProtoChainToRepr(plan)
	require.NoError(t, err)
	require.True(t, repr.TableResult)
	fc, err := FuncChainFromRepr(repr, pool)
	require.NoError(t, err)
	output, err := fc.Execute(input)
	require.NoError(t, err)
	defer output.Release()
	require.Equal(t, []int64{1, 1, 1}, output.chunkSizes)
	for group, want := range []int64{4, 0, 2} {
		require.Equal(t, want, output.Column("hits").Chunk(group).(*array.Int64).Value(0))
	}
}

func TestPostTableProjectionScopeAndRowLineage(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := postTableFixture(t, pool, false)
	defer input.Release()
	predicate := &operatorTransformExpr{name: "table_filter", stages: []string{types.StagePostProcess}, outputs: []arrow.DataType{arrow.FixedWidthTypes.Boolean}, execute: func(ctx *types.FuncContext, cols []*arrow.Chunked) ([]*arrow.Chunked, error) {
		chunks := make([]arrow.Array, len(cols[0].Chunks()))
		for group, data := range cols[0].Chunks() {
			b := array.NewBooleanBuilder(ctx.Pool())
			v := data.(*array.Int32)
			for row := range v.Len() {
				b.Append(!v.IsNull(row) && v.Value(row) >= 2)
			}
			chunks[group] = b.NewArray()
			b.Release()
		}
		out := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, chunks)
		releaseAggregateArrays(chunks)
		return []*arrow.Chunked{out}, nil
	}}
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
		Concat().Filter(predicate, []string{"value"}).Sort("value", true, "").LimitWithOffset(2, 1).Select("value").
		ExecuteWithOptions(context.Background(), ExecuteOptions{EnableColumnPruning: true, Downstream: DownstreamSpec{RequiredColumns: []string{"value"}}}, input)
	require.NoError(t, err)
	defer output.Release()
	require.Equal(t, []string{"value"}, output.ColumnNames())
	require.Equal(t, []int32{4, 3}, output.Column("value").Chunk(0).(*array.Int32).Int32Values())
	require.Equal(t, [][]transform.RowLineage{{{{Group: 0, Row: 3}}, {{Group: 2, Row: 0}}}}, output.Lineage())
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
		Aggregate([]string{"key"}, []string{"key", "rows"}, AggregateSpec{Op: "count"}).Select(types.IDFieldName)
	require.Error(t, fc.Validate(), "aggregate must not resurrect dropped system inputs")
	legacy, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).Select("value").Execute(input)
	require.NoError(t, err)
	defer legacy.Release()
	require.True(t, legacy.HasColumn(types.IDFieldName), "HITS retains protected identity")
}

func TestPostTableNativeOperatorsRejectOtherModes(t *testing.T) {
	for _, stage := range []string{types.StageIngestion, types.StagePreProcess, types.StagePostProcess, types.StageL2Rerank} {
		fc := NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(stage).Concat()
		require.Error(t, fc.Validate(), stage)
	}
	_, err := NewAggregateOp([]string{"key"}, []string{"key", types.ScoreFieldName}, []AggregateSpec{{Op: "count"}})
	require.Error(t, err)
	_, err = NewAggregateOp(nil, []string{"rows", " rows "}, []AggregateSpec{{Op: "count"}, {Op: "count"}})
	require.Error(t, err)
}

func TestPostTableAggregateNumericRange(t *testing.T) {
	for _, op := range []string{"sum", "avg"} {
		t.Run(op, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values := array.NewInt64Builder(pool)
			values.AppendValues([]int64{math.MaxInt64, math.MaxInt64}, nil)
			data := values.NewArray()
			values.Release()
			b := NewDataFrameBuilder()
			defer b.Release()
			b.SetChunkSizes([]int64{2}).SetIdentityLineage()
			require.NoError(t, b.AddColumnFromChunks("v", []arrow.Array{data}))
			input := b.Build()
			defer input.Release()
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Aggregate(nil, []string{"out"}, AggregateSpec{Op: op, Input: "v"}).Execute(input)
			if op == "sum" {
				require.Error(t, err)
				require.EqualValues(t, 2400, merr.Code(err))
				return
			}
			require.NoError(t, err)
			defer output.Release()
			require.Equal(t, float64(math.MaxInt64), output.Column("out").Chunk(0).(*array.Float64).Value(0))
		})
	}
}

func TestPostTableAggregatePreservesSlicedNestedValues(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	vectorType := arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)
	structure := arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.String}, arrow.Field{Name: "vector", Type: vectorType})
	list := array.NewListBuilder(pool, structure)
	child := list.ValueBuilder().(*array.StructBuilder)
	text := child.FieldBuilder(0).(*array.StringBuilder)
	vectors := child.FieldBuilder(1).(*array.FixedSizeListBuilder)
	vectorValues := vectors.ValueBuilder().(*array.Float32Builder)
	for _, value := range []string{"ignored", "first", "", "last"} {
		if value == "" {
			list.AppendNull()
			continue
		}
		list.Append(true)
		child.Append(true)
		text.Append(value)
		vectors.Append(true)
		vectorValues.AppendValues([]float32{float32(len(value)), 2}, nil)
	}
	all := list.NewArray()
	list.Release()
	sliced := array.NewSlice(all, 1, 4)
	all.Release()
	empty := array.MakeArrayOfNull(pool, sliced.DataType(), 0)
	b := NewDataFrameBuilder().SetChunkSizes([]int64{3, 0}).SetIdentityLineage()
	defer b.Release()
	require.NoError(t, b.AddColumnFromChunks("nested", []arrow.Array{sliced, empty}))
	b.SetFieldNullable("nested", true)
	input := b.Build()
	defer input.Release()
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
		Aggregate(nil, []string{"first", "last", "values", "count"}, AggregateSpec{Op: "first", Input: "nested"}, AggregateSpec{Op: "last", Input: "nested"}, AggregateSpec{Op: "collect_list", Input: "nested"}, AggregateSpec{Op: "count", Input: "nested"}).Execute(input)
	require.NoError(t, err)
	defer output.Release()
	first, last := array.NewSlice(sliced, 0, 1), array.NewSlice(sliced, 2, 3)
	defer first.Release()
	defer last.Release()
	require.True(t, array.Equal(first, output.Column("first").Chunk(0)))
	require.True(t, array.Equal(last, output.Column("last").Chunk(0)))
	collected := output.Column("values").Chunk(0).(*array.List)
	require.True(t, array.Equal(sliced, collected.ListValues()), "collect preserves sliced List<Struct<fixed-vector>> and null elements")
	require.EqualValues(t, 2, output.Column("count").Chunk(0).(*array.Int64).Value(0))
	require.True(t, output.Column("first").Chunk(1).IsNull(0))
	require.Zero(t, output.Column("values").Chunk(1).(*array.List).ListValues().Len())
}

func TestPostTableAggregateTypeValidationMatchesRuntime(t *testing.T) {
	cases := []struct {
		name             string
		dt               arrow.DataType
		json             string
		numeric, ordered bool
	}{
		{"int8", arrow.PrimitiveTypes.Int8, "[2,null,3]", true, true},
		{"int16", arrow.PrimitiveTypes.Int16, "[2,null,3]", true, true},
		{"int32", arrow.PrimitiveTypes.Int32, "[2,null,3]", true, true},
		{"int64", arrow.PrimitiveTypes.Int64, "[2,null,3]", true, true},
		{"float32", arrow.PrimitiveTypes.Float32, "[2,null,3]", true, true},
		{"float64", arrow.PrimitiveTypes.Float64, "[2,null,3]", true, true},
		{"string", arrow.BinaryTypes.String, `["b",null,"a"]`, false, true},
		{"timestamp", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, "[2,null,3]", false, true},
		{"bool", arrow.FixedWidthTypes.Boolean, "[true,null,false]", false, true},
		{"uint64", arrow.PrimitiveTypes.Uint64, "[2,null,3]", false, true},
		{"list", arrow.ListOf(arrow.PrimitiveTypes.Int32), "[[2],null,[3]]", false, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values, _, err := array.FromJSON(pool, test.dt, strings.NewReader(test.json))
			require.NoError(t, err)
			b := NewDataFrameBuilder().SetChunkSizes([]int64{3}).SetIdentityLineage()
			defer b.Release()
			require.NoError(t, b.AddColumnFromChunks("v", []arrow.Array{values}))
			b.SetFieldNullable("v", true)
			input := b.Build()
			defer input.Release()
			for _, op := range []string{"sum", "avg", "min", "max", "count", "first", "last", "collect_list"} {
				output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
					Aggregate(nil, []string{"out"}, AggregateSpec{Op: op, Input: "v"}).Execute(input)
				allowed := op == "count" || op == "first" || op == "last" || op == "collect_list" || (op == "sum" || op == "avg") && test.numeric || (op == "min" || op == "max") && test.ordered
				if !allowed {
					require.ErrorIs(t, err, merr.ErrParameterInvalid, op)
					require.Nil(t, output)
					continue
				}
				require.NoError(t, err, op)
				require.EqualValues(t, 1, output.NumRows(), op)
				if op == "count" {
					require.EqualValues(t, 2, output.Column("out").Chunk(0).(*array.Int64).Value(0))
				}
				output.Release()
			}
		})
	}
}

type postTableCancelContext struct {
	context.Context
	cancel context.CancelFunc
	calls  atomic.Int32
}

func (ctx *postTableCancelContext) Err() error {
	if ctx.calls.Add(1) >= 4 {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestPostTableGroupingAndLineageObserveCancellationDuringTraversal(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := reduceTestFrame(t, pool, make([]int64, 8192))
	defer input.Release()
	for _, keyed := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		observed := &postTableCancelContext{Context: ctx, cancel: cancel}
		funcCtx := types.NewFuncContextFull(observed, pool, types.StagePostProcess).WithTableResult(true)
		keys := []string(nil)
		if keyed {
			keys = []string{"key"}
		}
		groups, err := groupPostTable(funcCtx, input, keys)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, groups)
		require.EqualValues(t, 4, observed.calls.Load(), "must observe cancellation within a large group")
		cancel()
	}
	groups, err := groupPostTable(types.NewFuncContextFull(context.Background(), pool, types.StagePostProcess).WithTableResult(true), input, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := &postTableCancelContext{Context: ctx, cancel: cancel}
	lineage, err := postTableGroupLineage(input, groups[0][0], types.NewFuncContextFull(observed, pool, types.StagePostProcess).WithTableResult(true))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, lineage)
	require.EqualValues(t, 4, observed.calls.Load())
	// A key group can contain only odd row indexes. Cancellation must be
	// checked by traversal count rather than row index divisibility.
	rows := make([]int, 4096)
	for index := range rows {
		rows[index] = index*2 + 1
	}
	dt, accumulator, err := aggregateOutputType(AggregateSpec{Op: "count"}, input.Column("key"))
	require.NoError(t, err)
	numericContext, numericCancel := context.WithCancel(context.Background())
	defer numericCancel()
	numericObserved := &postTableCancelContext{Context: numericContext, cancel: numericCancel}
	result, err := executeAggregateChunk(types.NewFuncContextFull(numericObserved, pool, types.StagePostProcess).WithTableResult(true), AggregateSpec{Op: "count", Input: "key"}, input.Column("key"), dt, accumulator, 0, []postTableRowGroup{{Chunk: 0, Rows: rows, FirstRow: 1}})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	require.EqualValues(t, 4, numericObserved.calls.Load())
}

func TestPostTableProtoAggregateSortUsesStableOrderWithoutHitIdentity(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := postTableFixture(t, pool, false)
	defer input.Release()
	for _, explicitTie := range []bool{false, true} {
		inputs := []string{"rows"}
		if explicitTie {
			inputs = append(inputs, "text")
		}
		plan, err := transform.PostTable("stable_sort", &schemapb.FunctionChainOp{Op: "concat"}, &schemapb.FunctionChainOp{
			Op: "aggregate", Inputs: []string{"text"}, Outputs: []string{"text", "rows"}, Params: map[string]*schemapb.FunctionParamValue{
				"keys":       arrayParam(stringParam("text")),
				"aggregates": arrayParam(objectParam(map[string]*schemapb.FunctionParamValue{"op": stringParam("count")})),
			},
		}, &schemapb.FunctionChainOp{Op: "sort", Inputs: inputs, Params: map[string]*schemapb.FunctionParamValue{"desc": boolParam(true)}})
		require.NoError(t, err)
		repr, err := ProtoChainToRepr(plan)
		require.NoError(t, err)
		require.Equal(t, []string{"text"}, repr.Info.RequiredInputs)
		fetch, err := CompileDataFrameInputPlan(repr, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar}}})
		require.NoError(t, err)
		require.Equal(t, []string{"text"}, fetch.PhysicalFieldNames())
		fc, err := FuncChainFromRepr(repr, pool)
		require.NoError(t, err, "TABLE factory must not inject an implicit $id dependency")
		require.Equal(t, inputs, fc.operators[2].Inputs())
		output, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{EnableColumnPruning: true, Downstream: DownstreamSpec{RequiredColumns: []string{"text", "rows"}}}, input)
		require.NoError(t, err)
		require.False(t, output.HasColumn(types.IDFieldName))
		want := []string{"B0", "A0", "B1", "NULL", "A2", "B2"}
		if explicitTie {
			want = []string{"A0", "A2", "B0", "B1", "B2", "NULL"}
		}
		for row, text := range want {
			require.Equal(t, text, output.Column("text").Chunk(0).(*array.String).Value(row))
		}
		output.Release()
	}
	legacy, err := NewSortOpFromRepr(&OperatorRepr{Inputs: []string{"value"}})
	require.NoError(t, err)
	require.Equal(t, []string{"value", types.IDFieldName}, legacy.Inputs(), "legacy defaults stay compatible")
}

func TestPostTableFloatSumAndAverageUseIEEEValues(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	values := array.NewFloat64Builder(pool)
	values.AppendValues([]float64{math.Inf(1), math.Inf(-1)}, nil)
	data := values.NewArray()
	values.Release()
	b := NewDataFrameBuilder().SetChunkSizes([]int64{2}).SetIdentityLineage()
	defer b.Release()
	require.NoError(t, b.AddColumnFromChunks("v", []arrow.Array{data}))
	input := b.Build()
	defer input.Release()
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
		Aggregate(nil, []string{"sum", "avg"}, AggregateSpec{Op: "sum", Input: "v"}, AggregateSpec{Op: "avg", Input: "v"}).Execute(input)
	require.NoError(t, err)
	defer output.Release()
	for _, name := range []string{"sum", "avg"} {
		require.True(t, math.IsNaN(output.Column(name).Chunk(0).(*array.Float64).Value(0)), name)
		require.False(t, output.Column(name).Chunk(0).IsNull(0))
	}
}

func TestPostTableOuterExplodeMakesChildSchemaNullable(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	listType := arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: false})
	list := array.NewBuilder(pool, listType).(*array.ListBuilder)
	list.Append(true)
	list.ValueBuilder().(*array.Int32Builder).Append(7)
	list.Append(true) // a valid, non-null empty list still produces an outer null
	data := list.NewArray()
	list.Release()
	b := NewDataFrameBuilder().SetChunkSizes([]int64{2}).SetIdentityLineage()
	defer b.Release()
	require.NoError(t, b.AddColumnFromChunks("items", []arrow.Array{data}))
	b.SetFieldNullable("items", false)
	input := b.Build()
	defer input.Release()
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
		Explode("items", "items", ExplodeOptions{Outer: true, Position: "pos"}).
		SelectAs([]string{"items", "pos"}, []string{"item", "position"}).Execute(input)
	require.NoError(t, err)
	defer output.Release()
	batch, err := ToArrowBatch(output)
	require.NoError(t, err)
	defer ReleaseArrowBatch(batch)
	for column, field := range batch.Groups[0].Schema().Fields() {
		require.True(t, field.Nullable, field.Name)
		require.True(t, batch.Groups[0].Column(column).Data().Chunk(0).IsNull(1), field.Name)
	}
	require.EqualValues(t, 7, output.Column("item").Chunk(0).(*array.Int32).Value(0))
	require.Equal(t, [][]transform.RowLineage{{{{Group: 0, Row: 0}}, {{Group: 0, Row: 1}}}}, output.Lineage())
}

func TestPostTableSortLargeStringValuesAndStableTies(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	data, _, err := array.FromJSON(pool, arrow.BinaryTypes.LargeString, strings.NewReader(`["b","a","b",null]`))
	require.NoError(t, err)
	b := NewDataFrameBuilder().SetChunkSizes([]int64{4}).SetIdentityLineage()
	defer b.Release()
	require.NoError(t, b.AddColumnFromChunks("large", []arrow.Array{data}))
	b.SetFieldNullable("large", true)
	input := b.Build()
	defer input.Release()
	for _, desc := range []bool{false, true} {
		plan, err := transform.PostTable("large_sort", &schemapb.FunctionChainOp{Op: "sort", Inputs: []string{"large"}, Params: map[string]*schemapb.FunctionParamValue{"desc": boolParam(desc)}})
		require.NoError(t, err)
		fc, err := ParseFuncChainProto(plan, pool)
		require.NoError(t, err)
		output, err := fc.Execute(input)
		require.NoError(t, err)
		wantRows := []int{1, 0, 2, 3}
		if desc {
			wantRows = []int{0, 2, 1, 3}
		}
		values := output.Column("large").Chunk(0).(*array.LargeString)
		for row, source := range wantRows {
			require.Equal(t, transform.RowLineage{{Group: 0, Row: int64(source)}}, output.Lineage()[0][row])
			if source == 3 {
				require.True(t, values.IsNull(row), "nulls sort last in both directions")
			} else {
				require.Equal(t, data.(*array.LargeString).Value(source), values.Value(row))
			}
		}
		output.Release()
	}
}
