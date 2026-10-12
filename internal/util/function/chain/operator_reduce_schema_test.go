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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type reduceSchemaExpr struct {
	operatorTransformExpr
	executeWithSchema func(*types.FuncContext, *arrow.Schema, []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error)
}

func (expr *reduceSchemaExpr) ExecuteWithSchema(ctx *types.FuncContext, schema *arrow.Schema, inputs []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error) {
	return expr.executeWithSchema(ctx, schema, inputs)
}

func reduceFirstSchemaExpr(t *testing.T, calls *int) *reduceSchemaExpr {
	t.Helper()
	return &reduceSchemaExpr{operatorTransformExpr: operatorTransformExpr{name: "schema_reducer", stages: []string{types.StagePostProcess}}, executeWithSchema: func(ctx *types.FuncContext, schema *arrow.Schema, inputs []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error) {
		*calls++
		require.True(t, ctx.Reduction())
		require.Equal(t, inputs[0].DataType(), schema.Field(0).Type)
		chunks := make([]arrow.Array, len(inputs[0].Chunks()))
		for group, chunk := range inputs[0].Chunks() {
			index := 0
			if chunk.Len() == 0 {
				index = -1
			}
			var err error
			chunks[group], err = takeArrayWithNulls(ctx.Pool(), chunk, []int{index})
			require.NoError(t, err)
		}
		output := arrow.NewChunked(inputs[0].DataType(), chunks)
		releaseAggregateArrays(chunks)
		field, err := normalizeRuntimeArrowField(schema.Field(0), "return_value", true, true)
		require.NoError(t, err)
		return arrow.NewSchema([]arrow.Field{field}, nil), []*arrow.Chunked{output}, nil
	}}
}

func TestReducePreservesEveryRuntimeTypeAndSchemaMetadata(t *testing.T) {
	for _, spec := range aggregateRuntimeSchemaCases(t) {
		t.Run(spec.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			data, _, err := array.FromJSON(pool, spec.field.Type, strings.NewReader(spec.values))
			require.NoError(t, err)
			input := aggregateSchemaInput(t, pool, spec.field, data)
			defer input.Release()
			calls := 0
			fn := reduceFirstSchemaExpr(t, &calls)
			op := &ReduceOp{BaseOp: BaseOp{inputs: []string{"v"}, outputs: []string{"generated"}}, function: fn}
			output, err := op.Execute(types.NewFuncContextFull(context.Background(), pool, types.StagePostProcess).WithTableResult(true), input)
			require.NoError(t, err)
			defer output.Release()
			require.Equal(t, 1, calls)
			require.Equal(t, []int64{1, 1, 1}, output.ChunkSizes())
			first := array.NewSlice(input.Column("v").Chunk(0), 0, 1)
			defer first.Release()
			require.True(t, array.Equal(first, output.Column("generated").Chunk(0)))
			require.True(t, output.Column("generated").Chunk(1).IsNull(0))
			require.True(t, output.Column("generated").Chunk(2).IsNull(0))
			field, err := output.resolveArrowField("generated")
			require.NoError(t, err)
			require.True(t, field.Nullable)
			if expected, present := spec.field.Metadata.GetValue(types.MetadataKeyMilvusDataType); present {
				actual, present := field.Metadata.GetValue(types.MetadataKeyMilvusDataType)
				require.True(t, present)
				require.Equal(t, expected, actual)
			}
			require.Empty(t, output.Lineage()[1][0])
			require.Equal(t, transform.RowLineage{{Group: 0, Row: 0}, {Group: 0, Row: 1}, {Group: 0, Row: 2}}, output.Lineage()[0][0])
		})
	}
}

func TestReduceValidatesActualReturnedTypeAndNullability(t *testing.T) {
	for _, violation := range []string{"declared_type", "schema_type", "nullable"} {
		t.Run(violation, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			input := reduceTestFrame(t, pool, []int64{1}, nil)
			defer input.Release()
			calls := 0
			fn := reduceFirstSchemaExpr(t, &calls)
			if violation == "declared_type" {
				fn.outputs = []arrow.DataType{arrow.PrimitiveTypes.Float32}
			} else {
				original := fn.executeWithSchema
				fn.executeWithSchema = func(ctx *types.FuncContext, schema *arrow.Schema, columns []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error) {
					returned, outputs, err := original(ctx, schema, columns)
					field := returned.Field(0)
					if violation == "schema_type" {
						field.Type = arrow.PrimitiveTypes.Float32
					} else {
						field.Nullable = false
					}
					return arrow.NewSchema([]arrow.Field{field}, nil), outputs, err
				}
			}
			op := &ReduceOp{BaseOp: BaseOp{inputs: []string{"key"}, outputs: []string{"generated"}}, function: fn}
			output, err := op.Execute(types.NewFuncContextFull(context.Background(), pool, types.StagePostProcess).WithTableResult(true), input)
			require.ErrorIs(t, err, merr.ErrFunctionFailed)
			require.Nil(t, output)
			require.Equal(t, 1, calls)
		})
	}
}

func TestReduceNestedPathsPreserveGroupedSchemaAndParentNulls(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := aggregateNestedSchemaInput(t, pool)
	defer input.Release()
	calls := 0
	fn := reduceFirstSchemaExpr(t, &calls)
	original := fn.executeWithSchema
	fn.executeWithSchema = func(ctx *types.FuncContext, schema *arrow.Schema, inputs []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error) {
		require.Equal(t, 1, schema.NumFields(), "key columns must not become expression args")
		require.Equal(t, `payload['metric']`, schema.Field(0).Name)
		require.True(t, schema.Field(0).Nullable, "null parent masks a nonnullable child")
		return original(ctx, schema, inputs)
	}
	op := &ReduceOp{BaseOp: BaseOp{inputs: []string{"payload.tenant", `payload['metric']`}, outputs: []string{"tenant", "first_metric"}}, keys: []string{"payload.tenant"}, function: fn}
	output, err := op.Execute(types.NewFuncContextFull(context.Background(), pool, types.StagePostProcess).WithTableResult(true), input)
	require.NoError(t, err)
	defer output.Release()
	require.Equal(t, 1, calls)
	require.Equal(t, []int64{3, 0, 1}, output.ChunkSizes())
	require.EqualValues(t, 10, output.Column("first_metric").Chunk(0).(*array.Int64).Value(0))
	require.True(t, output.Column("tenant").Chunk(0).IsNull(1))
	require.True(t, output.Column("first_metric").Chunk(0).IsNull(1))
	require.EqualValues(t, 20, output.Column("first_metric").Chunk(0).(*array.Int64).Value(2))
	require.Equal(t, transform.RowLineage{{Group: 0, Row: 0}, {Group: 0, Row: 3}}, output.Lineage()[0][0])
	field, err := output.resolveArrowField("first_metric")
	require.NoError(t, err)
	actual, ok := field.Metadata.GetValue(types.MetadataKeyMilvusDataType)
	require.True(t, ok)
	require.Equal(t, "5", actual)
}

func TestReducePreservesExplicitRootSchemaMetadata(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	values := array.NewInt64Builder(pool)
	values.Append(1)
	data := values.NewArray()
	values.Release()
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{1}).SetIdentityLineage().SetArrowSchemaMetadata(arrow.NewMetadata([]string{"model_version"}, []string{"input_v1"}))
	defer builder.Release()
	require.NoError(t, builder.AddColumnFromChunks("key", []arrow.Array{data}))
	input := builder.Build()
	defer input.Release()
	calls := 0
	fn := reduceFirstSchemaExpr(t, &calls)
	original := fn.executeWithSchema
	fn.executeWithSchema = func(ctx *types.FuncContext, schema *arrow.Schema, columns []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error) {
		value, ok := schema.Metadata().GetValue("model_version")
		require.True(t, ok)
		require.Equal(t, "input_v1", value)
		returned, outputs, err := original(ctx, schema, columns)
		metadata := arrow.NewMetadata([]string{"model_version"}, []string{"output_v2"})
		return arrow.NewSchema(returned.Fields(), &metadata), outputs, err
	}
	op := &ReduceOp{BaseOp: BaseOp{inputs: []string{"key"}, outputs: []string{"generated"}}, function: fn}
	output, err := op.Execute(types.NewFuncContextFull(context.Background(), pool, types.StagePostProcess).WithTableResult(true), input)
	require.NoError(t, err)
	defer output.Release()
	batch, err := ToArrowBatch(output)
	require.NoError(t, err)
	defer ReleaseArrowBatch(batch)
	actual, ok := batch.Groups[0].Schema().Metadata().GetValue("model_version")
	require.True(t, ok)
	require.Equal(t, "output_v2", actual)
}
