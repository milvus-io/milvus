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
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func mapScoreTestFrame(t *testing.T, pool memory.Allocator) *DataFrame {
	t.Helper()
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes([]int64{2, 0, 1}).SetIdentityLineage()
	for name, dt := range map[string]arrow.DataType{types.IDFieldName: arrow.PrimitiveTypes.Int64, types.ScoreFieldName: arrow.PrimitiveTypes.Float32} {
		chunks := make([]arrow.Array, 0, 3)
		for _, values := range []string{`[1,2]`, `[]`, `[3]`} {
			chunk, _, err := array.FromJSON(pool, dt, strings.NewReader(values))
			require.NoError(t, err)
			chunks = append(chunks, chunk)
		}
		require.NoError(t, builder.AddColumnFromChunks(name, chunks))
	}
	return builder.Build()
}

func mapScoreTestExpr(t *testing.T, dt arrow.DataType, values []string, schema *arrow.Schema) types.FunctionExpr {
	t.Helper()
	fn := operatorTransformExpr{name: "numeric_score", stages: []string{types.StageL2Rerank, types.StagePostProcess}, outputs: []arrow.DataType{dt}, execute: func(ctx *types.FuncContext, _ []*arrow.Chunked) ([]*arrow.Chunked, error) {
		chunks := make([]arrow.Array, 0, len(values))
		for _, group := range values {
			chunk, _, err := array.FromJSON(ctx.Pool(), dt, strings.NewReader(group))
			require.NoError(t, err)
			chunks = append(chunks, chunk)
		}
		output := arrow.NewChunked(dt, chunks)
		releaseAggregateArrays(chunks)
		return []*arrow.Chunked{output}, nil
	}}
	if schema != nil {
		return &schemaAwareTestExpr{operatorTransformExpr: fn, schema: schema}
	}
	return &fn
}

func TestMapNormalizesNumericSearchScoresBeforeSortAndExport(t *testing.T) {
	for _, dt := range []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64, arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64} {
		t.Run(dt.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			input := mapScoreTestFrame(t, pool)
			defer input.Release()
			fn := mapScoreTestExpr(t, dt, []string{`[1,2]`, `[]`, `[3]`}, nil)
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).
				Map(fn, []string{types.ScoreFieldName}, []string{types.ScoreFieldName}).
				Sort(types.ScoreFieldName, true, types.IDFieldName).Execute(input)
			require.NoError(t, err)
			defer output.Release()
			require.Equal(t, arrow.PrimitiveTypes.Float32, output.Column(types.ScoreFieldName).DataType())
			require.Equal(t, []int64{2, 0, 1}, output.ChunkSizes())
			result, err := ToSearchResultData(output)
			require.NoError(t, err)
			require.Equal(t, []float32{2, 1, 3}, result.Scores)
			require.Equal(t, []int64{2, 1, 3}, result.Ids.GetIntId().GetData())
			require.Equal(t, arrow.PrimitiveTypes.Float32, input.Column(types.ScoreFieldName).DataType())
		})
	}
}

func TestMapScoreCastRebindsLogicalSchemaAndKeepsNulls(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := mapScoreTestFrame(t, pool)
	defer input.Release()
	field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "model_score", FieldID: 101, DataType: schemapb.DataType_Double, Nullable: true})
	require.NoError(t, err)
	metadata := field.Metadata.ToMap()
	metadata["model.version"] = "v2"
	field.Metadata = arrow.MetadataFrom(metadata)
	schemaMetadata := arrow.MetadataFrom(map[string]string{"batch.version": "v3"})
	schema := arrow.NewSchema([]arrow.Field{field}, &schemaMetadata)
	fn := mapScoreTestExpr(t, arrow.PrimitiveTypes.Float64, []string{`[0.4,null]`, `[]`, `[0.1]`}, schema)
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Map(fn, []string{types.ScoreFieldName}, []string{types.ScoreFieldName}).Execute(input)
	require.NoError(t, err)
	defer output.Release()
	actual, err := functionInputSchema(output, []string{types.ScoreFieldName})
	require.NoError(t, err)
	require.Equal(t, arrow.PrimitiveTypes.Float32, actual.Field(0).Type)
	require.True(t, actual.Field(0).Nullable)
	require.Equal(t, 1, output.Column(types.ScoreFieldName).NullN())
	require.True(t, output.Column(types.ScoreFieldName).Chunk(0).IsNull(1))
	require.InDelta(t, 0.4, output.Column(types.ScoreFieldName).Chunk(0).(*array.Float32).Value(0), 1e-7)
	logical, err := types.MilvusSchemaFromArrowField(actual.Field(0))
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_Float, logical.GetDataType())
	require.Equal(t, types.ScoreFieldName, logical.GetName())
	require.Zero(t, logical.GetFieldID())
	require.True(t, logical.GetNullable())
	version, ok := actual.Field(0).Metadata.GetValue("model.version")
	require.True(t, ok)
	require.Equal(t, "v2", version)
	version, ok = actual.Metadata().GetValue("batch.version")
	require.True(t, ok)
	require.Equal(t, "v3", version)
	require.NoError(t, types.ValidateRuntimeArrowSchema(actual))
	// The adapter's declared schema is immutable even when Map changes its target.
	require.Equal(t, arrow.PrimitiveTypes.Float64, schema.Field(0).Type)
}

func TestMapScoreCastLeavesTableAndPayloadSchemasIndependent(t *testing.T) {
	for _, spec := range []struct {
		name   string
		table  bool
		target string
	}{{"table_payload", true, "model_score"}, {"ordinary_payload", false, "confidence"}} {
		t.Run(spec.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			input := mapScoreTestFrame(t, pool)
			defer input.Release()
			fn := mapScoreTestExpr(t, arrow.PrimitiveTypes.Float64, []string{`[0.4,0.2]`, `[]`, `[0.1]`}, nil)
			fc := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank)
			if spec.table {
				fc.SetStage(types.StagePostProcess).SetTableResult(true)
			}
			output, err := fc.Map(fn, []string{types.ScoreFieldName}, []string{spec.target}).Execute(input)
			require.NoError(t, err)
			defer output.Release()
			require.Equal(t, arrow.PrimitiveTypes.Float64, output.Column(spec.target).DataType())
			actual, err := functionInputSchema(output, []string{spec.target})
			require.NoError(t, err)
			require.Equal(t, arrow.PrimitiveTypes.Float64, actual.Field(0).Type)
			require.NoError(t, types.ValidateRuntimeArrowSchema(actual))
		})
	}
}

func TestMapRejectsNonNumericSearchScoreWithoutLeaking(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := mapScoreTestFrame(t, pool)
	defer input.Release()
	fn := mapScoreTestExpr(t, arrow.BinaryTypes.String, []string{`["0.4","0.2"]`, `[]`, `["0.1"]`}, nil)
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Map(fn, []string{types.ScoreFieldName}, []string{types.ScoreFieldName}).Execute(input)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.Nil(t, output)
	require.Equal(t, int32(2400), merr.Code(err))
	require.Equal(t, arrow.PrimitiveTypes.Float32, input.Column(types.ScoreFieldName).DataType())
}

func TestMapScoreCastKeepsDeclaredOutputValidation(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := mapScoreTestFrame(t, pool)
	defer input.Release()
	fn := mapScoreTestExpr(t, arrow.PrimitiveTypes.Float64, []string{`[0.4,0.2]`, `[]`, `[0.1]`}, nil).(*operatorTransformExpr)
	fn.outputs = []arrow.DataType{arrow.PrimitiveTypes.Float32}
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Map(fn, []string{types.ScoreFieldName}, []string{types.ScoreFieldName}).Execute(input)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.Nil(t, output)
	require.Contains(t, err.Error(), "differs from declared type")
}

func TestMapScoreCastPreservesIntermediateNonFiniteValues(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := mapScoreTestFrame(t, pool)
	defer input.Release()
	fn := &operatorTransformExpr{name: "nonfinite_score", stages: []string{types.StageL2Rerank}, execute: func(ctx *types.FuncContext, _ []*arrow.Chunked) ([]*arrow.Chunked, error) {
		chunks := make([]arrow.Array, 0, 3)
		for _, group := range [][]float64{{math.NaN(), math.Inf(1)}, {}, {math.Inf(-1)}} {
			b := array.NewFloat64Builder(ctx.Pool())
			b.AppendValues(group, nil)
			chunks = append(chunks, b.NewArray())
			b.Release()
		}
		output := arrow.NewChunked(arrow.PrimitiveTypes.Float64, chunks)
		releaseAggregateArrays(chunks)
		return []*arrow.Chunked{output}, nil
	}}
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Map(fn, []string{types.ScoreFieldName}, []string{types.ScoreFieldName}).Execute(input)
	require.NoError(t, err)
	defer output.Release()
	require.True(t, math.IsNaN(float64(output.Column(types.ScoreFieldName).Chunk(0).(*array.Float32).Value(0))))
	require.True(t, math.IsInf(float64(output.Column(types.ScoreFieldName).Chunk(0).(*array.Float32).Value(1)), 1))
	require.True(t, math.IsInf(float64(output.Column(types.ScoreFieldName).Chunk(2).(*array.Float32).Value(0)), -1))
}

func TestMapScoreCastCancelledReleasesReturnedArrays(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := mapScoreTestFrame(t, pool)
	defer input.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn := mapScoreTestExpr(t, arrow.PrimitiveTypes.Float64, []string{`[0.4,0.2]`, `[]`, `[0.1]`}, nil).(*operatorTransformExpr)
	execute := fn.execute
	fn.execute = func(ctx *types.FuncContext, cols []*arrow.Chunked) ([]*arrow.Chunked, error) {
		outputs, err := execute(ctx, cols)
		cancel()
		return outputs, err
	}
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Map(fn, []string{types.ScoreFieldName}, []string{types.ScoreFieldName}).ExecuteWithContext(ctx, input)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, output)
	require.Equal(t, []int64{2, 0, 1}, input.ChunkSizes())
}
