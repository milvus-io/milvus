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

package expr

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

func TestNewXGBoostExpr(t *testing.T) {
	t.Run("valid_creation", func(t *testing.T) {
		expr, err := NewXGBoostExpr("file:///path/to/model.booster", []string{"f1", "f2"}, arrow.PrimitiveTypes.Float32)
		require.NoError(t, err)
		assert.Equal(t, XGBoostFuncName, expr.Name())
		assert.True(t, expr.IsRunnable(types.StageL2Rerank))
		assert.True(t, expr.IsRunnable(types.StageL1Rerank))
		assert.False(t, expr.IsRunnable(types.StageL0Rerank))
	})

	t.Run("empty_model_uri", func(t *testing.T) {
		_, err := NewXGBoostExpr("", []string{"f1"}, arrow.PrimitiveTypes.Float32)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "model_uri")
	})

	t.Run("empty_features", func(t *testing.T) {
		_, err := NewXGBoostExpr("file:///model.booster", []string{}, arrow.PrimitiveTypes.Float32)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "features")
	})

	t.Run("invalid_output_type", func(t *testing.T) {
		_, err := NewXGBoostExpr("file:///model.booster", []string{"f1"}, arrow.BinaryTypes.String)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "output_type")
	})

	t.Run("default_output_type", func(t *testing.T) {
		expr, err := NewXGBoostExpr("file:///model.booster", []string{"f1"}, nil)
		require.NoError(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Float32, expr.outputType)
	})
}

func TestNewXGBoostExprFromParams(t *testing.T) {
	t.Run("with_all_params", func(t *testing.T) {
		cfg := types.FunctionConfig{
			Name: XGBoostFuncName,
			Params: map[string]*schemapb.FunctionParamValue{
				"model_uri":   &schemapb.FunctionParamValue{Val: &schemapb.FunctionParamValue_StringVal{StringVal: "file:///model.booster"}},
				"features":    &schemapb.FunctionParamValue{Val: &schemapb.FunctionParamValue_StringListVal{StringListVal: &schemapb.StringList{Values: []string{"f1", "f2", "f3"}}}},
				"output_type": &schemapb.FunctionParamValue{Val: &schemapb.FunctionParamValue_StringVal{StringVal: "float32"}},
			},
		}

		expr, err := NewXGBoostExprFromParams(types.FunctionBuildContext{}, cfg)
		require.NoError(t, err)
		assert.Equal(t, XGBoostFuncName, expr.Name())
		assert.Equal(t, arrow.PrimitiveTypes.Float32, expr.(*XGBoostExpr).outputType)
	})

	t.Run("with_int32_output", func(t *testing.T) {
		cfg := types.FunctionConfig{
			Name: XGBoostFuncName,
			Params: map[string]*schemapb.FunctionParamValue{
				"model_uri":   &schemapb.FunctionParamValue{Val: &schemapb.FunctionParamValue_StringVal{StringVal: "file:///model.booster"}},
				"features":    &schemapb.FunctionParamValue{Val: &schemapb.FunctionParamValue_StringListVal{StringListVal: &schemapb.StringList{Values: []string{"f1"}}}},
				"output_type": &schemapb.FunctionParamValue{Val: &schemapb.FunctionParamValue_StringVal{StringVal: "int32"}},
			},
		}

		expr, err := NewXGBoostExprFromParams(types.FunctionBuildContext{}, cfg)
		require.NoError(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Int32, expr.(*XGBoostExpr).outputType)
	})

	t.Run("missing_model_uri", func(t *testing.T) {
		cfg := types.FunctionConfig{
			Name: XGBoostFuncName,
			Params: map[string]*schemapb.FunctionParamValue{
				"features": &schemapb.FunctionParamValue{Val: &schemapb.FunctionParamValue_StringListVal{StringListVal: &schemapb.StringList{Values: []string{"f1"}}}},
			},
		}

		_, err := NewXGBoostExprFromParams(types.FunctionBuildContext{}, cfg)
		require.Error(t, err)
	})

	t.Run("invalid_output_type_param", func(t *testing.T) {
		cfg := types.FunctionConfig{
			Name: XGBoostFuncName,
			Params: map[string]*schemapb.FunctionParamValue{
				"model_uri":   &schemapb.FunctionParamValue{Val: &schemapb.FunctionParamValue_StringVal{StringVal: "file:///model.booster"}},
				"features":    &schemapb.FunctionParamValue{Val: &schemapb.FunctionParamValue_StringListVal{StringListVal: &schemapb.StringList{Values: []string{"f1"}}}},
				"output_type": &schemapb.FunctionParamValue{Val: &schemapb.FunctionParamValue_StringVal{StringVal: "invalid"}},
			},
		}

		_, err := NewXGBoostExprFromParams(types.FunctionBuildContext{}, cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid_type")
	})
}

func TestXGBoostExprOutputDataTypes(t *testing.T) {
	expr, err := NewXGBoostExpr("file:///model.booster", []string{"f1"}, arrow.PrimitiveTypes.Float32)
	require.NoError(t, err)

	outputTypes := expr.OutputDataTypes()
	require.Len(t, outputTypes, 1)
	assert.Equal(t, arrow.PrimitiveTypes.Float32, outputTypes[0])
}

func TestXGBoostExprExecute(t *testing.T) {
	pool := memory.NewGoAllocator()
	defer pool.AssertSize(t, 0)

	t.Run("basic_float32_prediction", func(t *testing.T) {
		expr, err := NewXGBoostExpr("file:///model.booster", []string{"f1", "f2"}, arrow.PrimitiveTypes.Float32)
		require.NoError(t, err)

		// Create feature arrays: [1.0, 2.0], [3.0, 4.0]
		fb1 := array.NewFloat32Builder(pool)
		fb1.AppendValues([]float32{1.0, 2.0}, nil)
		col1 := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{fb1.NewArray()})
		fb1.Release()

		fb2 := array.NewFloat32Builder(pool)
		fb2.AppendValues([]float32{3.0, 4.0}, nil)
		col2 := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{fb2.NewArray()})
		fb2.Release()

		ctx := types.NewFuncContext(pool)
		outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
		require.NoError(t, err)

		col1.Release()
		col2.Release()

		require.Len(t, outputs, 1)
		resultCol := outputs[0]
		defer resultCol.Release()

		assert.Equal(t, arrow.PrimitiveTypes.Float32, resultCol.DataType())
		require.Len(t, resultCol.Chunks(), 1)

		chunk := resultCol.Chunk(0).(*array.Float32)
		assert.Len(t, chunk.Values(), 2)
		// Default behavior: sum of features = 4.0, 6.0
		assert.InDelta(t, float32(4.0), chunk.Value(0), 0.01)
		assert.InDelta(t, float32(6.0), chunk.Value(1), 0.01)
	})

	t.Run("int64_output", func(t *testing.T) {
		expr, err := NewXGBoostExpr("file:///model.booster", []string{"f1"}, arrow.PrimitiveTypes.Int64)
		require.NoError(t, err)

		fb := array.NewFloat32Builder(pool)
		fb.AppendValues([]float32{1.5, 2.5}, nil)
		col := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{fb.NewArray()})
		fb.Release()

		ctx := types.NewFuncContext(pool)
		outputs, err := expr.Execute(ctx, []*arrow.Chunked{col})
		require.NoError(t, err)

		col.Release()

		require.Len(t, outputs, 1)
		resultCol := outputs[0]
		defer resultCol.Release()

		assert.Equal(t, arrow.PrimitiveTypes.Int64, resultCol.DataType())
	})

	t.Run("multiple_chunks", func(t *testing.T) {
		expr, err := NewXGBoostExpr("file:///model.booster", []string{"f1"}, arrow.PrimitiveTypes.Float32)
		require.NoError(t, err)

		// Create two chunks for the same feature column
		fb1 := array.NewFloat32Builder(pool)
		fb1.AppendValues([]float32{1.0, 2.0}, nil)
		chunk1 := fb1.NewArray()
		fb1.Release()

		fb2 := array.NewFloat32Builder(pool)
		fb2.AppendValues([]float32{3.0}, nil)
		chunk2 := fb2.NewArray()
		fb2.Release()

		col := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{chunk1, chunk2})

		ctx := types.NewFuncContext(pool)
		outputs, err := expr.Execute(ctx, []*arrow.Chunked{col})
		require.NoError(t, err)

		col.Release()

		require.Len(t, outputs, 1)
		resultCol := outputs[0]
		defer resultCol.Release()

		assert.Len(t, resultCol.Chunks(), 2)
	})

	t.Run("feature_count_mismatch", func(t *testing.T) {
		expr, err := NewXGBoostExpr("file:///model.booster", []string{"f1", "f2"}, arrow.PrimitiveTypes.Float32)
		require.NoError(t, err)

		fb := array.NewFloat32Builder(pool)
		fb.AppendValues([]float32{1.0}, nil)
		col := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{fb.NewArray()})
		fb.Release()

		ctx := types.NewFuncContext(pool)
		_, err = expr.Execute(ctx, []*arrow.Chunked{col})
		require.Error(t, err)

		col.Release()
	})

	t.Run("invalid_feature_type", func(t *testing.T) {
		expr, err := NewXGBoostExpr("file:///model.booster", []string{"f1"}, arrow.PrimitiveTypes.Float32)
		require.NoError(t, err)

		// Create a string column (invalid)
		sb := array.NewStringBuilder(pool)
		sb.AppendValues([]string{"a", "b"}, nil)
		col := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{sb.NewArray()})
		sb.Release()

		ctx := types.NewFuncContext(pool)
		_, err = expr.Execute(ctx, []*arrow.Chunked{col})
		require.Error(t, err)

		col.Release()
	})

	t.Run("empty_chunk", func(t *testing.T) {
		expr, err := NewXGBoostExpr("file:///model.booster", []string{"f1"}, arrow.PrimitiveTypes.Float32)
		require.NoError(t, err)

		fb := array.NewFloat32Builder(pool)
		col := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{fb.NewArray()})
		fb.Release()

		ctx := types.NewFuncContext(pool)
		outputs, err := expr.Execute(ctx, []*arrow.Chunked{col})
		require.NoError(t, err)

		col.Release()

		require.Len(t, outputs, 1)
		resultCol := outputs[0]
		defer resultCol.Release()

		require.Len(t, resultCol.Chunks(), 1)
		assert.Equal(t, 0, resultCol.Chunk(0).Len())
	})

	t.Run("null_values", func(t *testing.T) {
		expr, err := NewXGBoostExpr("file:///model.booster", []string{"f1"}, arrow.PrimitiveTypes.Float32)
		require.NoError(t, err)

		fb := array.NewFloat32Builder(pool)
		fb.AppendValues([]float32{1.0, 2.0}, []bool{true, false})
		col := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{fb.NewArray()})
		fb.Release()

		ctx := types.NewFuncContext(pool)
		outputs, err := expr.Execute(ctx, []*arrow.Chunked{col})
		require.NoError(t, err)

		col.Release()

		require.Len(t, outputs, 1)
		resultCol := outputs[0]
		defer resultCol.Release()

		chunk := resultCol.Chunk(0).(*array.Float32)
		// First value is 1.0 (sum of features), second null is treated as 0.0
		assert.InDelta(t, float32(1.0), chunk.Value(0), 0.01)
	})
}

func TestXGBoostExprIntegration(t *testing.T) {
	pool := memory.NewGoAllocator()
	defer pool.AssertSize(t, 0)

	// Create expression with 3 features
	expr, err := NewXGBoostExpr("file:///model.booster", []string{"feature1", "feature2", "feature3"}, arrow.PrimitiveTypes.Float32)
	require.NoError(t, err)

	// Create feature columns with different numeric types
	fb1 := array.NewInt32Builder(pool)
	fb1.AppendValues([]int32{1, 2, 3}, nil)
	col1 := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{fb1.NewArray()})
	fb1.Release()

	fb2 := array.NewFloat32Builder(pool)
	fb2.AppendValues([]float32{1.5, 2.5, 3.5}, nil)
	col2 := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{fb2.NewArray()})
	fb2.Release()

	fb3 := array.NewFloat64Builder(pool)
	fb3.AppendValues([]float64{2.0, 3.0, 4.0}, nil)
	col3 := arrow.NewChunked(arrow.PrimitiveTypes.Float64, []arrow.Array{fb3.NewArray()})
	fb3.Release()

	ctx := types.NewFuncContext(pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2, col3})
	require.NoError(t, err)

	col1.Release()
	col2.Release()
	col3.Release()

	require.Len(t, outputs, 1)
	resultCol := outputs[0]
	defer resultCol.Release()

	chunk := resultCol.Chunk(0).(*array.Float32)
	assert.Len(t, chunk.Values(), 3)
	// Predictions are sum of features
	// Row 0: 1 + 1.5 + 2.0 = 4.5
	// Row 1: 2 + 2.5 + 3.0 = 7.5
	// Row 2: 3 + 3.5 + 4.0 = 10.5
	assert.InDelta(t, float32(4.5), chunk.Value(0), 0.01)
	assert.InDelta(t, float32(7.5), chunk.Value(1), 0.01)
	assert.InDelta(t, float32(10.5), chunk.Value(2), 0.01)
}

func TestXGBoostExprIsRunnable(t *testing.T) {
	expr, err := NewXGBoostExpr("file:///model.booster", []string{"f1"}, arrow.PrimitiveTypes.Float32)
	require.NoError(t, err)

	// XGBoost should only run in L1 and L2 rerank stages
	assert.True(t, expr.IsRunnable(types.StageL2Rerank))
	assert.True(t, expr.IsRunnable(types.StageL1Rerank))
	assert.False(t, expr.IsRunnable(types.StageL0Rerank))
	assert.False(t, expr.IsRunnable(types.StageIngestion))
	assert.False(t, expr.IsRunnable(types.StagePreProcess))
	assert.False(t, expr.IsRunnable(types.StagePostProcess))
}

func TestNumericTypeChecks(t *testing.T) {
	t.Run("numeric_types", func(t *testing.T) {
		numericTypes := []arrow.DataType{
			arrow.PrimitiveTypes.Int8,
			arrow.PrimitiveTypes.Int16,
			arrow.PrimitiveTypes.Int32,
			arrow.PrimitiveTypes.Int64,
			arrow.PrimitiveTypes.Uint8,
			arrow.PrimitiveTypes.Uint16,
			arrow.PrimitiveTypes.Uint32,
			arrow.PrimitiveTypes.Uint64,
			arrow.PrimitiveTypes.Float32,
			arrow.PrimitiveTypes.Float64,
		}

		for _, dt := range numericTypes {
			assert.True(t, isNumericType(dt), "expected %s to be numeric", dt)
		}
	})

	t.Run("non_numeric_types", func(t *testing.T) {
		nonNumericTypes := []arrow.DataType{
			arrow.BinaryTypes.String,
			arrow.BinaryTypes.Binary,
			arrow.FixedWidthTypes.Boolean,
		}

		for _, dt := range nonNumericTypes {
			assert.False(t, isNumericType(dt), "expected %s to be non-numeric", dt)
		}
	})
}
