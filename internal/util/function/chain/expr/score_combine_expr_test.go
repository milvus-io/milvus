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
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// =============================================================================
// Test Suite
// =============================================================================

type ScoreCombineExprTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *ScoreCombineExprTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *ScoreCombineExprTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestScoreCombineExprTestSuite(t *testing.T) {
	suite.Run(t, new(ScoreCombineExprTestSuite))
}

// =============================================================================
// Helper Functions
// =============================================================================

func (s *ScoreCombineExprTestSuite) createFloat32ChunkedArray(values []float32) *arrow.Chunked {
	builder := array.NewFloat32Builder(s.pool)
	defer builder.Release()

	for _, v := range values {
		builder.Append(v)
	}

	arr := builder.NewArray()
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{arr})
	arr.Release()

	return chunked
}

func (s *ScoreCombineExprTestSuite) createMultiChunkFloat32Array(chunk1, chunk2 []float32) *arrow.Chunked {
	builder1 := array.NewFloat32Builder(s.pool)
	defer builder1.Release()
	for _, v := range chunk1 {
		builder1.Append(v)
	}
	arr1 := builder1.NewArray()

	builder2 := array.NewFloat32Builder(s.pool)
	defer builder2.Release()
	for _, v := range chunk2 {
		builder2.Append(v)
	}
	arr2 := builder2.NewArray()

	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{arr1, arr2})
	arr1.Release()
	arr2.Release()

	return chunked
}

// =============================================================================
// Constructor Tests
// =============================================================================

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExpr_Valid() {
	expr, err := NewScoreCombineExpr(ModeMultiply, 2, nil)
	s.Require().NoError(err)
	s.NotNil(expr)
	s.Equal("score_combine", expr.Name())
	s.Equal(2, expr.inputCount)
	s.Equal(ModeMultiply, expr.mode)
}

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExpr_AllModes() {
	modes := []string{ModeMultiply, ModeSum, ModeMax, ModeMin, ModeAvg}

	for _, mode := range modes {
		expr, err := NewScoreCombineExpr(mode, 2, nil)
		s.Require().NoError(err, "mode: %s", mode)
		s.NotNil(expr)
		s.Equal(mode, expr.mode)
	}
}

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExpr_WeightedMode() {
	weights := []float64{0.5, 0.3, 0.2}
	expr, err := NewScoreCombineExpr(ModeWeighted, 3, weights)
	s.Require().NoError(err)
	s.NotNil(expr)
	s.Equal(weights, expr.weights)
}

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExpr_TooFewInputs() {
	_, err := NewScoreCombineExpr(ModeMultiply, 1, nil)
	s.Error(err)
	s.Contains(err.Error(), "at least 2 input columns required")
}

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExpr_InvalidMode() {
	_, err := NewScoreCombineExpr("invalid_mode", 2, nil)
	s.Error(err)
	s.Contains(err.Error(), "invalid mode")
}

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExpr_WeightedModeWrongWeightsCount() {
	// Wrong number of weights
	_, err := NewScoreCombineExpr(ModeWeighted, 2, []float64{0.5})
	s.Error(err)
	s.Contains(err.Error(), "weighted mode requires 2 weights")
}

// =============================================================================
// Factory Tests
// =============================================================================

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExprFromParams_Valid() {
	params := map[string]interface{}{
		"input_count": 2,
		"mode":        "multiply",
	}

	expr, err := NewScoreCombineExprFromParams(params)
	s.Require().NoError(err)
	s.NotNil(expr)
	s.Equal("score_combine", expr.Name())
}

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExprFromParams_WithWeights() {
	params := map[string]interface{}{
		"input_count": 2,
		"mode":        "weighted",
		"weights":     []float64{0.6, 0.4},
	}

	expr, err := NewScoreCombineExprFromParams(params)
	s.Require().NoError(err)
	combineExpr := expr.(*ScoreCombineExpr)
	s.Equal([]float64{0.6, 0.4}, combineExpr.weights)
}

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExprFromParams_WeightsAsInterface() {
	// Test with []interface{} for weights
	params := map[string]interface{}{
		"input_count": 2,
		"mode":        "weighted",
		"weights":     []interface{}{0.6, 0.4},
	}

	expr, err := NewScoreCombineExprFromParams(params)
	s.Require().NoError(err)
	s.NotNil(expr)
}

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExprFromParams_DefaultMode() {
	params := map[string]interface{}{
		"input_count": 2,
	}

	expr, err := NewScoreCombineExprFromParams(params)
	s.Require().NoError(err)
	combineExpr := expr.(*ScoreCombineExpr)
	s.Equal(ModeMultiply, combineExpr.mode)
}

func (s *ScoreCombineExprTestSuite) TestNewScoreCombineExprFromParams_MissingInputCount() {
	params := map[string]interface{}{
		"mode": "multiply",
	}

	_, err := NewScoreCombineExprFromParams(params)
	s.Error(err)
	s.Contains(err.Error(), "input_count")
}

// =============================================================================
// Execute Tests
// =============================================================================

func (s *ScoreCombineExprTestSuite) TestExecute_Multiply() {
	col1 := s.createFloat32ChunkedArray([]float32{2.0, 3.0, 4.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{0.5, 0.5, 0.5})
	defer col2.Release()

	expr, err := NewScoreCombineExpr(ModeMultiply, 2, nil)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()

	// Check results: 2.0*0.5=1.0, 3.0*0.5=1.5, 4.0*0.5=2.0
	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(1.0, result.Value(0), 0.001)
	s.InDelta(1.5, result.Value(1), 0.001)
	s.InDelta(2.0, result.Value(2), 0.001)
}

func (s *ScoreCombineExprTestSuite) TestExecute_Sum() {
	col1 := s.createFloat32ChunkedArray([]float32{1.0, 2.0, 3.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{0.5, 0.5, 0.5})
	defer col2.Release()

	expr, err := NewScoreCombineExpr(ModeSum, 2, nil)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	// Check results: 1.0+0.5=1.5, 2.0+0.5=2.5, 3.0+0.5=3.5
	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(1.5, result.Value(0), 0.001)
	s.InDelta(2.5, result.Value(1), 0.001)
	s.InDelta(3.5, result.Value(2), 0.001)
}

func (s *ScoreCombineExprTestSuite) TestExecute_Max() {
	col1 := s.createFloat32ChunkedArray([]float32{1.0, 5.0, 3.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{2.0, 2.0, 4.0})
	defer col2.Release()

	expr, err := NewScoreCombineExpr(ModeMax, 2, nil)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	// Check results: max(1.0, 2.0)=2.0, max(5.0, 2.0)=5.0, max(3.0, 4.0)=4.0
	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(2.0, result.Value(0), 0.001)
	s.InDelta(5.0, result.Value(1), 0.001)
	s.InDelta(4.0, result.Value(2), 0.001)
}

func (s *ScoreCombineExprTestSuite) TestExecute_Min() {
	col1 := s.createFloat32ChunkedArray([]float32{1.0, 5.0, 3.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{2.0, 2.0, 4.0})
	defer col2.Release()

	expr, err := NewScoreCombineExpr(ModeMin, 2, nil)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	// Check results: min(1.0, 2.0)=1.0, min(5.0, 2.0)=2.0, min(3.0, 4.0)=3.0
	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(1.0, result.Value(0), 0.001)
	s.InDelta(2.0, result.Value(1), 0.001)
	s.InDelta(3.0, result.Value(2), 0.001)
}

func (s *ScoreCombineExprTestSuite) TestExecute_Avg() {
	col1 := s.createFloat32ChunkedArray([]float32{2.0, 4.0, 6.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{4.0, 6.0, 8.0})
	defer col2.Release()

	expr, err := NewScoreCombineExpr(ModeAvg, 2, nil)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	// Check results: avg(2.0, 4.0)=3.0, avg(4.0, 6.0)=5.0, avg(6.0, 8.0)=7.0
	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(3.0, result.Value(0), 0.001)
	s.InDelta(5.0, result.Value(1), 0.001)
	s.InDelta(7.0, result.Value(2), 0.001)
}

func (s *ScoreCombineExprTestSuite) TestExecute_Weighted() {
	col1 := s.createFloat32ChunkedArray([]float32{10.0, 20.0, 30.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{1.0, 2.0, 3.0})
	defer col2.Release()

	// weights: 0.8 for col1, 0.2 for col2
	expr, err := NewScoreCombineExpr(ModeWeighted, 2, []float64{0.8, 0.2})
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	// Check results: 10.0*0.8 + 1.0*0.2 = 8.2, 20.0*0.8 + 2.0*0.2 = 16.4, 30.0*0.8 + 3.0*0.2 = 24.6
	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(8.2, result.Value(0), 0.001)
	s.InDelta(16.4, result.Value(1), 0.001)
	s.InDelta(24.6, result.Value(2), 0.001)
}

func (s *ScoreCombineExprTestSuite) TestExecute_MultipleChunks() {
	col1 := s.createMultiChunkFloat32Array([]float32{1.0, 2.0}, []float32{3.0, 4.0})
	defer col1.Release()
	col2 := s.createMultiChunkFloat32Array([]float32{2.0, 2.0}, []float32{2.0, 2.0})
	defer col2.Release()

	expr, err := NewScoreCombineExpr(ModeMultiply, 2, nil)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	// Should have 2 chunks
	s.Equal(2, len(outputs[0].Chunks()))

	// Check first chunk: 1.0*2.0=2.0, 2.0*2.0=4.0
	chunk0 := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(2.0, chunk0.Value(0), 0.001)
	s.InDelta(4.0, chunk0.Value(1), 0.001)

	// Check second chunk: 3.0*2.0=6.0, 4.0*2.0=8.0
	chunk1 := outputs[0].Chunk(1).(*array.Float32)
	s.InDelta(6.0, chunk1.Value(0), 0.001)
	s.InDelta(8.0, chunk1.Value(1), 0.001)
}

func (s *ScoreCombineExprTestSuite) TestExecute_ThreeInputColumns() {
	col1 := s.createFloat32ChunkedArray([]float32{2.0, 3.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{3.0, 4.0})
	defer col2.Release()
	col3 := s.createFloat32ChunkedArray([]float32{1.0, 1.0})
	defer col3.Release()

	expr, err := NewScoreCombineExpr(ModeSum, 3, nil)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2, col3})
	s.Require().NoError(err)
	defer outputs[0].Release()

	// Check results: 2.0+3.0+1.0=6.0, 3.0+4.0+1.0=8.0
	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(6.0, result.Value(0), 0.001)
	s.InDelta(8.0, result.Value(1), 0.001)
}

func (s *ScoreCombineExprTestSuite) TestExecute_TooFewInputs() {
	col1 := s.createFloat32ChunkedArray([]float32{1.0, 2.0})
	defer col1.Release()

	expr, err := NewScoreCombineExpr(ModeMultiply, 2, nil)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	// Execute with only 1 input instead of 2
	_, err = expr.Execute(ctx, []*arrow.Chunked{col1})
	s.Error(err)
	s.Contains(err.Error(), "expected 2 input columns")
}

// =============================================================================
// Interface Method Tests
// =============================================================================

func (s *ScoreCombineExprTestSuite) TestOutputDataTypes() {
	expr, err := NewScoreCombineExpr(ModeMultiply, 2, nil)
	s.Require().NoError(err)

	outputTypes := expr.OutputDataTypes()
	s.Len(outputTypes, 1)
	s.Equal(arrow.PrimitiveTypes.Float32, outputTypes[0])
}

func (s *ScoreCombineExprTestSuite) TestIsRunnable() {
	expr, err := NewScoreCombineExpr(ModeMultiply, 2, nil)
	s.Require().NoError(err)

	// score_combine can run in all stages
	s.True(expr.IsRunnable("L0_rerank"))
	s.True(expr.IsRunnable("L1_rerank"))
	s.True(expr.IsRunnable("L2_rerank"))
}

// =============================================================================
// Integration Tests
// =============================================================================
// Memory Leak Tests
// =============================================================================

func (s *ScoreCombineExprTestSuite) TestMemoryLeak_ScoreCombineExecution() {
	for range 10 {
		col1 := s.createFloat32ChunkedArray([]float32{1.0, 2.0, 3.0})
		col2 := s.createFloat32ChunkedArray([]float32{0.5, 0.5, 0.5})

		expr, err := NewScoreCombineExpr(ModeMultiply, 2, nil)
		s.Require().NoError(err)

		ctx := types.NewFuncContext(s.pool)
		outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
		s.Require().NoError(err)

		outputs[0].Release()
		col1.Release()
		col2.Release()
	}
	// Memory leak check happens in TearDownTest
}

// =============================================================================
// Combine Function Unit Tests
// =============================================================================

func (s *ScoreCombineExprTestSuite) TestCombine_Multiply() {
	expr := &ScoreCombineExpr{mode: ModeMultiply}
	result := expr.combine([]float64{2.0, 3.0, 4.0})
	s.InDelta(24.0, result, 0.001) // 2*3*4 = 24
}

func (s *ScoreCombineExprTestSuite) TestCombine_Sum() {
	expr := &ScoreCombineExpr{mode: ModeSum}
	result := expr.combine([]float64{1.0, 2.0, 3.0})
	s.InDelta(6.0, result, 0.001) // 1+2+3 = 6
}

func (s *ScoreCombineExprTestSuite) TestCombine_Max() {
	expr := &ScoreCombineExpr{mode: ModeMax}
	result := expr.combine([]float64{1.0, 5.0, 3.0})
	s.InDelta(5.0, result, 0.001)
}

func (s *ScoreCombineExprTestSuite) TestCombine_Min() {
	expr := &ScoreCombineExpr{mode: ModeMin}
	result := expr.combine([]float64{1.0, 5.0, 3.0})
	s.InDelta(1.0, result, 0.001)
}

func (s *ScoreCombineExprTestSuite) TestCombine_Avg() {
	expr := &ScoreCombineExpr{mode: ModeAvg}
	result := expr.combine([]float64{2.0, 4.0, 6.0})
	s.InDelta(4.0, result, 0.001) // (2+4+6)/3 = 4
}

func (s *ScoreCombineExprTestSuite) TestCombine_Weighted() {
	expr := &ScoreCombineExpr{
		mode:    ModeWeighted,
		weights: []float64{0.5, 0.3, 0.2},
	}
	result := expr.combine([]float64{10.0, 20.0, 30.0})
	// 10*0.5 + 20*0.3 + 30*0.2 = 5 + 6 + 6 = 17
	s.InDelta(17.0, result, 0.001)
}

// =============================================================================
// Helper Function Tests (base_expr utilities)
// =============================================================================

func (s *ScoreCombineExprTestSuite) TestParseStringSliceParam() {
	const funcName = "test"

	// Test with []string
	params := map[string]interface{}{
		"cols": []string{"a", "b", "c"},
	}
	result, err := ParseStringSliceParam(params, funcName, "cols")
	s.Require().NoError(err)
	s.Equal([]string{"a", "b", "c"}, result)

	// Test with []interface{}
	params = map[string]interface{}{
		"cols": []interface{}{"a", "b", "c"},
	}
	result, err = ParseStringSliceParam(params, funcName, "cols")
	s.Require().NoError(err)
	s.Equal([]string{"a", "b", "c"}, result)

	// Test missing key
	_, err = ParseStringSliceParam(map[string]interface{}{}, funcName, "cols")
	s.Error(err)
	s.Contains(err.Error(), "missing required parameter")
}

func (s *ScoreCombineExprTestSuite) TestParseFloat64SliceParam() {
	const funcName = "test"

	// Test with []float64
	params := map[string]interface{}{
		"weights": []float64{0.5, 0.3, 0.2},
	}
	result, err := ParseFloat64SliceParam(params, funcName, "weights")
	s.Require().NoError(err)
	s.Equal([]float64{0.5, 0.3, 0.2}, result)

	// Test with []interface{}
	params = map[string]interface{}{
		"weights": []interface{}{0.5, 0.3, 0.2},
	}
	result, err = ParseFloat64SliceParam(params, funcName, "weights")
	s.Require().NoError(err)
	s.Equal([]float64{0.5, 0.3, 0.2}, result)

	// Test missing key (should return nil, not error)
	result, err = ParseFloat64SliceParam(map[string]interface{}{}, funcName, "weights")
	s.NoError(err)
	s.Nil(result)
}

func (s *ScoreCombineExprTestSuite) TestGetNumericValue() {
	// Test Float32
	builder32 := array.NewFloat32Builder(s.pool)
	builder32.Append(1.5)
	arr32 := builder32.NewArray()
	defer arr32.Release()
	builder32.Release()

	val, err := GetNumericValue(arr32, 0)
	s.Require().NoError(err)
	s.InDelta(1.5, val, 0.001)

	// Test Float64
	builder64 := array.NewFloat64Builder(s.pool)
	builder64.Append(2.5)
	arr64 := builder64.NewArray()
	defer arr64.Release()
	builder64.Release()

	val, err = GetNumericValue(arr64, 0)
	s.Require().NoError(err)
	s.InDelta(2.5, val, 0.001)

	// Test Int64
	builderInt := array.NewInt64Builder(s.pool)
	builderInt.Append(100)
	arrInt := builderInt.NewArray()
	defer arrInt.Release()
	builderInt.Release()

	val, err = GetNumericValue(arrInt, 0)
	s.Require().NoError(err)
	s.InDelta(100.0, val, 0.001)
}
