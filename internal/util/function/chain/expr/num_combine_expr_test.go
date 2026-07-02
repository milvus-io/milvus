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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// =============================================================================
// Test Suite
// =============================================================================

type NumCombineExprTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *NumCombineExprTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *NumCombineExprTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestNumCombineExprTestSuite(t *testing.T) {
	suite.Run(t, new(NumCombineExprTestSuite))
}

// =============================================================================
// Helper Functions
// =============================================================================

func (s *NumCombineExprTestSuite) createFloat32ChunkedArray(values []float32) *arrow.Chunked {
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

func (s *NumCombineExprTestSuite) createNullableFloat32ChunkedArray(values []float32, valid []bool) *arrow.Chunked {
	builder := array.NewFloat32Builder(s.pool)
	defer builder.Release()

	for i, v := range values {
		if valid[i] {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	}

	arr := builder.NewArray()
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{arr})
	arr.Release()

	return chunked
}

func (s *NumCombineExprTestSuite) createMultiChunkFloat32Array(chunk1, chunk2 []float32) *arrow.Chunked {
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

func (s *NumCombineExprTestSuite) TestNewNumCombineExpr_Valid() {
	expr, err := NewNumCombineExpr(ModeMultiply, nil)
	s.Require().NoError(err)
	s.NotNil(expr)
	s.Equal(NumCombineFuncName, expr.Name())
	s.Equal(ModeMultiply, expr.mode)
}

func (s *NumCombineExprTestSuite) TestNewNumCombineExpr_AllModes() {
	modes := []string{ModeMultiply, ModeSum, ModeMax, ModeMin, ModeAvg}

	for _, mode := range modes {
		expr, err := NewNumCombineExpr(mode, nil)
		s.Require().NoError(err, "mode: %s", mode)
		s.NotNil(expr)
		s.Equal(mode, expr.mode)
	}
}

func (s *NumCombineExprTestSuite) TestNewNumCombineExpr_WeightedMode() {
	weights := []float64{0.5, 0.3, 0.2}
	expr, err := NewNumCombineExpr(ModeWeighted, weights)
	s.Require().NoError(err)
	s.NotNil(expr)
	s.Equal(weights, expr.weights)
}

func (s *NumCombineExprTestSuite) TestNewNumCombineExpr_InvalidMode() {
	_, err := NewNumCombineExpr("invalid_mode", nil)
	s.Error(err)
	s.Contains(err.Error(), "invalid mode")
}

func (s *NumCombineExprTestSuite) TestNewNumCombineExpr_WeightedModeNoWeights() {
	_, err := NewNumCombineExpr(ModeWeighted, nil)
	s.Error(err)
	s.Contains(err.Error(), "weighted mode requires weights")
}

// =============================================================================
// Factory Tests
// =============================================================================

func (s *NumCombineExprTestSuite) TestNewNumCombineExprFromParams_Valid() {
	params := map[string]*schemapb.FunctionParamValue{
		"mode": stringParam("multiply"),
	}

	expr, err := NewNumCombineExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: params})
	s.Require().NoError(err)
	s.NotNil(expr)
	s.Equal(NumCombineFuncName, expr.Name())
}

func (s *NumCombineExprTestSuite) TestNewNumCombineExprFromParams_WithWeights() {
	params := map[string]*schemapb.FunctionParamValue{
		"mode":    stringParam("weighted"),
		"weights": arrayParam(doubleParam(0.6), doubleParam(0.4)),
	}

	expr, err := NewNumCombineExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: params})
	s.Require().NoError(err)
	combineExpr := expr.(*NumCombineExpr)
	s.Equal([]float64{0.6, 0.4}, combineExpr.weights)
}

func (s *NumCombineExprTestSuite) TestNewNumCombineExprFromParams_WeightsAsInts() {
	params := map[string]*schemapb.FunctionParamValue{
		"mode":    stringParam("weighted"),
		"weights": arrayParam(intParam(1), intParam(2)),
	}

	expr, err := NewNumCombineExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: params})
	s.Require().NoError(err)
	s.NotNil(expr)
}

func (s *NumCombineExprTestSuite) TestNewNumCombineExprFromParams_DefaultMode() {
	expr, err := NewNumCombineExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: map[string]*schemapb.FunctionParamValue{}})
	s.Require().NoError(err)
	combineExpr := expr.(*NumCombineExpr)
	s.Equal(ModeMultiply, combineExpr.mode)
}

// =============================================================================
// Execute Tests
// =============================================================================

func (s *NumCombineExprTestSuite) TestExecute_Multiply() {
	col1 := s.createFloat32ChunkedArray([]float32{2.0, 3.0, 4.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{0.5, 0.5, 0.5})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeMultiply, nil)
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

func (s *NumCombineExprTestSuite) TestExecute_Sum() {
	col1 := s.createFloat32ChunkedArray([]float32{1.0, 2.0, 3.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{0.5, 0.5, 0.5})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeSum, nil)
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

func (s *NumCombineExprTestSuite) TestExecute_Max() {
	col1 := s.createFloat32ChunkedArray([]float32{1.0, 5.0, 3.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{2.0, 2.0, 4.0})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeMax, nil)
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

func (s *NumCombineExprTestSuite) TestExecute_Min() {
	col1 := s.createFloat32ChunkedArray([]float32{1.0, 5.0, 3.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{2.0, 2.0, 4.0})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeMin, nil)
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

func (s *NumCombineExprTestSuite) TestExecute_Avg() {
	col1 := s.createFloat32ChunkedArray([]float32{2.0, 4.0, 6.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{4.0, 6.0, 8.0})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeAvg, nil)
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

func (s *NumCombineExprTestSuite) TestExecute_Weighted() {
	col1 := s.createFloat32ChunkedArray([]float32{10.0, 20.0, 30.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{1.0, 2.0, 3.0})
	defer col2.Release()

	// weights: 0.8 for col1, 0.2 for col2
	expr, err := NewNumCombineExpr(ModeWeighted, []float64{0.8, 0.2})
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

func (s *NumCombineExprTestSuite) TestExecute_PropagatesNullInputsByDefault() {
	col1 := s.createNullableFloat32ChunkedArray([]float32{2.0, 0.0, 0.0}, []bool{true, false, false})
	defer col1.Release()
	col2 := s.createNullableFloat32ChunkedArray([]float32{3.0, 4.0, 0.0}, []bool{true, true, false})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeSum, nil)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	result := outputs[0].Chunk(0).(*array.Float32)
	s.False(result.IsNull(0))
	s.InDelta(5.0, result.Value(0), 0.001)
	s.True(result.IsNull(1))
	s.True(result.IsNull(2))
}

func (s *NumCombineExprTestSuite) TestExecute_TreatsNullInputsAsZero() {
	col1 := s.createNullableFloat32ChunkedArray([]float32{2.0, 0.0, 0.0}, []bool{true, false, false})
	defer col1.Release()
	col2 := s.createNullableFloat32ChunkedArray([]float32{3.0, 4.0, 0.0}, []bool{true, true, false})
	defer col2.Release()
	col3 := s.createNullableFloat32ChunkedArray([]float32{0.0, 5.0, 0.0}, []bool{false, true, false})
	defer col3.Release()

	expr, err := NewNumCombineExpr(ModeSum, nil, WithNullPolicy(NumCombineNullAsZero))
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2, col3})
	s.Require().NoError(err)
	defer outputs[0].Release()

	result := outputs[0].Chunk(0).(*array.Float32)
	s.False(result.IsNull(0))
	s.InDelta(5.0, result.Value(0), 0.001)
	s.False(result.IsNull(1))
	s.InDelta(9.0, result.Value(1), 0.001)
	s.False(result.IsNull(2))
	s.InDelta(0.0, result.Value(2), 0.001)
}

func (s *NumCombineExprTestSuite) TestExecute_TreatsNullInputsAsZeroWeighted() {
	col1 := s.createNullableFloat32ChunkedArray([]float32{10.0, 0.0}, []bool{true, false})
	defer col1.Release()
	col2 := s.createNullableFloat32ChunkedArray([]float32{0.0, 20.0}, []bool{false, true})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeWeighted, []float64{0.8, 0.2}, WithNullPolicy(NumCombineNullAsZero))
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(8.0, result.Value(0), 0.001)
	s.InDelta(4.0, result.Value(1), 0.001)
}

func (s *NumCombineExprTestSuite) TestExecute_SkipsNullInputs() {
	col1 := s.createNullableFloat32ChunkedArray([]float32{2.0, 0.0, 0.0}, []bool{true, false, false})
	defer col1.Release()
	col2 := s.createNullableFloat32ChunkedArray([]float32{3.0, 4.0, 0.0}, []bool{true, true, false})
	defer col2.Release()
	col3 := s.createNullableFloat32ChunkedArray([]float32{0.0, 5.0, 0.0}, []bool{false, true, false})
	defer col3.Release()

	expr, err := NewNumCombineExpr(ModeSum, nil, WithNullPolicy(NumCombineNullSkip))
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2, col3})
	s.Require().NoError(err)
	defer outputs[0].Release()

	result := outputs[0].Chunk(0).(*array.Float32)
	s.False(result.IsNull(0))
	s.InDelta(5.0, result.Value(0), 0.001)
	s.False(result.IsNull(1))
	s.InDelta(9.0, result.Value(1), 0.001)
	s.True(result.IsNull(2))
}

func (s *NumCombineExprTestSuite) TestExecute_SkipsNullInputsMultiply() {
	col1 := s.createNullableFloat32ChunkedArray([]float32{2.0, 0.0}, []bool{true, false})
	defer col1.Release()
	col2 := s.createNullableFloat32ChunkedArray([]float32{3.0, 3.0}, []bool{true, true})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeMultiply, nil, WithNullPolicy(NumCombineNullSkip))
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(6.0, result.Value(0), 0.001)
	s.InDelta(3.0, result.Value(1), 0.001)
}

func (s *NumCombineExprTestSuite) TestExecute_TreatsNullInputsAsZeroMultiply() {
	col1 := s.createNullableFloat32ChunkedArray([]float32{2.0, 0.0}, []bool{true, false})
	defer col1.Release()
	col2 := s.createNullableFloat32ChunkedArray([]float32{3.0, 3.0}, []bool{true, true})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeMultiply, nil, WithNullPolicy(NumCombineNullAsZero))
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(6.0, result.Value(0), 0.001)
	s.InDelta(0.0, result.Value(1), 0.001)
}

func (s *NumCombineExprTestSuite) TestExecute_MultipleChunks() {
	col1 := s.createMultiChunkFloat32Array([]float32{1.0, 2.0}, []float32{3.0, 4.0})
	defer col1.Release()
	col2 := s.createMultiChunkFloat32Array([]float32{2.0, 2.0}, []float32{2.0, 2.0})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeMultiply, nil)
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

func (s *NumCombineExprTestSuite) TestExecute_ThreeInputColumns() {
	col1 := s.createFloat32ChunkedArray([]float32{2.0, 3.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{3.0, 4.0})
	defer col2.Release()
	col3 := s.createFloat32ChunkedArray([]float32{1.0, 1.0})
	defer col3.Release()

	expr, err := NewNumCombineExpr(ModeSum, nil)
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

func (s *NumCombineExprTestSuite) TestExecute_TooFewInputs() {
	col1 := s.createFloat32ChunkedArray([]float32{1.0, 2.0})
	defer col1.Release()

	expr, err := NewNumCombineExpr(ModeMultiply, nil)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	// Execute with only 1 input
	_, err = expr.Execute(ctx, []*arrow.Chunked{col1})
	s.Error(err)
	s.ErrorIs(err, merr.ErrParameterInvalid)
	s.Contains(err.Error(), "at least 2 input columns")
}

func (s *NumCombineExprTestSuite) TestExecute_WeightedModeWrongWeightsCount() {
	col1 := s.createFloat32ChunkedArray([]float32{1.0, 2.0})
	defer col1.Release()
	col2 := s.createFloat32ChunkedArray([]float32{3.0, 4.0})
	defer col2.Release()
	col3 := s.createFloat32ChunkedArray([]float32{5.0, 6.0})
	defer col3.Release()

	// Create with 2 weights, but execute with 3 inputs
	expr, err := NewNumCombineExpr(ModeWeighted, []float64{0.5, 0.5})
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	_, err = expr.Execute(ctx, []*arrow.Chunked{col1, col2, col3})
	s.Error(err)
	s.ErrorIs(err, merr.ErrParameterInvalid)
	s.Contains(err.Error(), "weighted mode requires 3 weights, got 2")
}

// =============================================================================
// Interface Method Tests
// =============================================================================

func (s *NumCombineExprTestSuite) TestOutputDataTypes() {
	expr, err := NewNumCombineExpr(ModeMultiply, nil)
	s.Require().NoError(err)

	outputTypes := expr.OutputDataTypes()
	s.Len(outputTypes, 1)
	s.Equal(arrow.PrimitiveTypes.Float32, outputTypes[0])
}

func (s *NumCombineExprTestSuite) TestIsRunnable() {
	expr, err := NewNumCombineExpr(ModeMultiply, nil)
	s.Require().NoError(err)

	// num_combine can run in all stages
	s.True(expr.IsRunnable("L0_rerank"))
	s.True(expr.IsRunnable("L1_rerank"))
	s.True(expr.IsRunnable("L2_rerank"))
}

// =============================================================================
// Integration Tests
// =============================================================================
// Memory Leak Tests
// =============================================================================

func (s *NumCombineExprTestSuite) TestMemoryLeak_NumCombineExecution() {
	for range 10 {
		col1 := s.createFloat32ChunkedArray([]float32{1.0, 2.0, 3.0})
		col2 := s.createFloat32ChunkedArray([]float32{0.5, 0.5, 0.5})

		expr, err := NewNumCombineExpr(ModeMultiply, nil)
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

func (s *NumCombineExprTestSuite) TestCombine_Multiply() {
	expr := &NumCombineExpr{mode: ModeMultiply}
	result := expr.combine([]float64{2.0, 3.0, 4.0}, nil)
	s.InDelta(24.0, result, 0.001) // 2*3*4 = 24
}

func (s *NumCombineExprTestSuite) TestCombine_Sum() {
	expr := &NumCombineExpr{mode: ModeSum}
	result := expr.combine([]float64{1.0, 2.0, 3.0}, nil)
	s.InDelta(6.0, result, 0.001) // 1+2+3 = 6
}

func (s *NumCombineExprTestSuite) TestCombine_Max() {
	expr := &NumCombineExpr{mode: ModeMax}
	result := expr.combine([]float64{1.0, 5.0, 3.0}, nil)
	s.InDelta(5.0, result, 0.001)
}

func (s *NumCombineExprTestSuite) TestCombine_Min() {
	expr := &NumCombineExpr{mode: ModeMin}
	result := expr.combine([]float64{1.0, 5.0, 3.0}, nil)
	s.InDelta(1.0, result, 0.001)
}

func (s *NumCombineExprTestSuite) TestCombine_Avg() {
	expr := &NumCombineExpr{mode: ModeAvg}
	result := expr.combine([]float64{2.0, 4.0, 6.0}, nil)
	s.InDelta(4.0, result, 0.001) // (2+4+6)/3 = 4
}

func (s *NumCombineExprTestSuite) TestCombine_Weighted() {
	expr := &NumCombineExpr{
		mode:    ModeWeighted,
		weights: []float64{0.5, 0.3, 0.2},
	}
	result := expr.combine([]float64{10.0, 20.0, 30.0}, []float64{0.5, 0.3, 0.2})
	// 10*0.5 + 20*0.3 + 30*0.2 = 5 + 6 + 6 = 17
	s.InDelta(17.0, result, 0.001)
}

func (s *NumCombineExprTestSuite) TestGetNumericValue() {
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

func (s *NumCombineExprTestSuite) TestNewNumCombineExprFromParams_Invalid() {
	_, err := NewNumCombineExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: map[string]*schemapb.FunctionParamValue{
		"mode": stringParam("bad-mode"),
	}})
	s.Error(err)
	s.Contains(err.Error(), "invalid mode")

	_, err = NewNumCombineExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: map[string]*schemapb.FunctionParamValue{
		"mode":    stringParam("weighted"),
		"weights": arrayParam(stringParam("bad")),
	}})
	s.Error(err)
	s.Contains(err.Error(), "weights")
}

func (s *NumCombineExprTestSuite) TestExecute_SkipsNullInputsWeighted() {
	col1 := s.createNullableFloat32ChunkedArray([]float32{10.0, 0.0, 0.0}, []bool{true, false, false})
	defer col1.Release()
	col2 := s.createNullableFloat32ChunkedArray([]float32{20.0, 20.0, 0.0}, []bool{true, true, false})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeWeighted, []float64{0.8, 0.2}, WithNullPolicy(NumCombineNullSkip))
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Require().NoError(err)
	defer outputs[0].Release()

	result := outputs[0].Chunk(0).(*array.Float32)
	s.InDelta(12.0, result.Value(0), 0.001)
	s.InDelta(4.0, result.Value(1), 0.001)
	s.True(result.IsNull(2))
}

func (s *NumCombineExprTestSuite) TestExecute_RejectsMismatchedChunksAndUnsupportedTypes() {
	col1 := s.createFloat32ChunkedArray([]float32{1.0, 2.0})
	defer col1.Release()
	col2 := s.createMultiChunkFloat32Array([]float32{1.0}, []float32{2.0})
	defer col2.Release()

	expr, err := NewNumCombineExpr(ModeSum, nil)
	s.Require().NoError(err)
	ctx := types.NewFuncContext(s.pool)

	_, err = expr.Execute(ctx, []*arrow.Chunked{col1, col2})
	s.Error(err)
	s.Contains(err.Error(), "input 0 has 1 chunks")

	shortCol := s.createFloat32ChunkedArray([]float32{1.0})
	defer shortCol.Release()
	_, err = expr.Execute(ctx, []*arrow.Chunked{col1, shortCol})
	s.Error(err)
	s.Contains(err.Error(), "has 2 rows")

	stringBuilder := array.NewStringBuilder(s.pool)
	stringBuilder.AppendValues([]string{"a", "b"}, nil)
	stringArray := stringBuilder.NewArray()
	stringBuilder.Release()
	stringCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{stringArray})
	stringArray.Release()
	defer stringCol.Release()

	_, err = expr.Execute(ctx, []*arrow.Chunked{col1, stringCol})
	s.Error(err)
	s.Contains(err.Error(), "unsupported input column type")
}

func (s *NumCombineExprTestSuite) TestNewNumericReaderAllSupportedTypes() {
	cases := []struct {
		name string
		arr  arrow.Array
		want float64
	}{
		{name: "int8", arr: func() arrow.Array {
			b := array.NewInt8Builder(s.pool)
			b.Append(1)
			arr := b.NewArray()
			b.Release()
			return arr
		}(), want: 1},
		{name: "int16", arr: func() arrow.Array {
			b := array.NewInt16Builder(s.pool)
			b.Append(2)
			arr := b.NewArray()
			b.Release()
			return arr
		}(), want: 2},
		{name: "int32", arr: func() arrow.Array {
			b := array.NewInt32Builder(s.pool)
			b.Append(3)
			arr := b.NewArray()
			b.Release()
			return arr
		}(), want: 3},
		{name: "int64", arr: func() arrow.Array {
			b := array.NewInt64Builder(s.pool)
			b.Append(4)
			arr := b.NewArray()
			b.Release()
			return arr
		}(), want: 4},
		{name: "float32", arr: func() arrow.Array {
			b := array.NewFloat32Builder(s.pool)
			b.Append(5.5)
			arr := b.NewArray()
			b.Release()
			return arr
		}(), want: 5.5},
		{name: "float64", arr: func() arrow.Array {
			b := array.NewFloat64Builder(s.pool)
			b.Append(6.5)
			arr := b.NewArray()
			b.Release()
			return arr
		}(), want: 6.5},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			defer tc.arr.Release()
			reader, ok := newNumericReader(tc.arr)
			s.True(ok)
			s.False(reader.IsNull(0))
			s.InDelta(tc.want, reader.Float64(0), 0.001)
		})
	}

	stringBuilder := array.NewStringBuilder(s.pool)
	stringBuilder.Append("not numeric")
	stringArray := stringBuilder.NewArray()
	stringBuilder.Release()
	defer stringArray.Release()
	_, ok := newNumericReader(stringArray)
	s.False(ok)
}
