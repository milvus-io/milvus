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
	"math"
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

type DecayExprTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *DecayExprTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *DecayExprTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestDecayExprTestSuite(t *testing.T) {
	suite.Run(t, new(DecayExprTestSuite))
}

// =============================================================================
// Helper Functions
// =============================================================================

// createTestInputs creates test input columns for decay function testing.
// Returns (distanceCol, scoreCol) as ChunkedArrays that must be released by caller.
func (s *DecayExprTestSuite) createTestInputsInt64() (*arrow.Chunked, *arrow.Chunked) {
	// Create distance column (Int64) - 2 chunks
	distBuilder1 := array.NewInt64Builder(s.pool)
	distBuilder1.AppendValues([]int64{0, 50, 100, 150, 200}, nil)
	distArr1 := distBuilder1.NewArray()
	distBuilder1.Release()

	distBuilder2 := array.NewInt64Builder(s.pool)
	distBuilder2.AppendValues([]int64{0, 100, 200, 300}, nil)
	distArr2 := distBuilder2.NewArray()
	distBuilder2.Release()

	distCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{distArr1, distArr2})
	distArr1.Release()
	distArr2.Release()

	// Create score column (Float32) - 2 chunks
	scoreBuilder1 := array.NewFloat32Builder(s.pool)
	scoreBuilder1.AppendValues([]float32{1.0, 1.0, 1.0, 1.0, 1.0}, nil)
	scoreArr1 := scoreBuilder1.NewArray()
	scoreBuilder1.Release()

	scoreBuilder2 := array.NewFloat32Builder(s.pool)
	scoreBuilder2.AppendValues([]float32{1.0, 1.0, 1.0, 1.0}, nil)
	scoreArr2 := scoreBuilder2.NewArray()
	scoreBuilder2.Release()

	scoreCol := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{scoreArr1, scoreArr2})
	scoreArr1.Release()
	scoreArr2.Release()

	return distCol, scoreCol
}

func (s *DecayExprTestSuite) createTestInputsFloat32() (*arrow.Chunked, *arrow.Chunked) {
	// Create distance column (Float32) - 2 chunks
	distBuilder1 := array.NewFloat32Builder(s.pool)
	distBuilder1.AppendValues([]float32{0, 50, 100, 150, 200}, nil)
	distArr1 := distBuilder1.NewArray()
	distBuilder1.Release()

	distBuilder2 := array.NewFloat32Builder(s.pool)
	distBuilder2.AppendValues([]float32{0, 100, 200, 300}, nil)
	distArr2 := distBuilder2.NewArray()
	distBuilder2.Release()

	distCol := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{distArr1, distArr2})
	distArr1.Release()
	distArr2.Release()

	// Create score column (Float32) - 2 chunks
	scoreBuilder1 := array.NewFloat32Builder(s.pool)
	scoreBuilder1.AppendValues([]float32{1.0, 1.0, 1.0, 1.0, 1.0}, nil)
	scoreArr1 := scoreBuilder1.NewArray()
	scoreBuilder1.Release()

	scoreBuilder2 := array.NewFloat32Builder(s.pool)
	scoreBuilder2.AppendValues([]float32{1.0, 1.0, 1.0, 1.0}, nil)
	scoreArr2 := scoreBuilder2.NewArray()
	scoreBuilder2.Release()

	scoreCol := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{scoreArr1, scoreArr2})
	scoreArr1.Release()
	scoreArr2.Release()

	return distCol, scoreCol
}

func (s *DecayExprTestSuite) createTestInputsFloat64() (*arrow.Chunked, *arrow.Chunked) {
	// Create distance column (Float64) - 2 chunks
	distBuilder1 := array.NewFloat64Builder(s.pool)
	distBuilder1.AppendValues([]float64{0, 50, 100, 150, 200}, nil)
	distArr1 := distBuilder1.NewArray()
	distBuilder1.Release()

	distBuilder2 := array.NewFloat64Builder(s.pool)
	distBuilder2.AppendValues([]float64{0, 100, 200, 300}, nil)
	distArr2 := distBuilder2.NewArray()
	distBuilder2.Release()

	distCol := arrow.NewChunked(arrow.PrimitiveTypes.Float64, []arrow.Array{distArr1, distArr2})
	distArr1.Release()
	distArr2.Release()

	// Create score column (Float32) - 2 chunks
	scoreBuilder1 := array.NewFloat32Builder(s.pool)
	scoreBuilder1.AppendValues([]float32{1.0, 1.0, 1.0, 1.0, 1.0}, nil)
	scoreArr1 := scoreBuilder1.NewArray()
	scoreBuilder1.Release()

	scoreBuilder2 := array.NewFloat32Builder(s.pool)
	scoreBuilder2.AppendValues([]float32{1.0, 1.0, 1.0, 1.0}, nil)
	scoreArr2 := scoreBuilder2.NewArray()
	scoreBuilder2.Release()

	scoreCol := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{scoreArr1, scoreArr2})
	scoreArr1.Release()
	scoreArr2.Release()

	return distCol, scoreCol
}

func (s *DecayExprTestSuite) createTestInputsInt32() (*arrow.Chunked, *arrow.Chunked) {
	// Create distance column (Int32) - 2 chunks
	distBuilder1 := array.NewInt32Builder(s.pool)
	distBuilder1.AppendValues([]int32{0, 50, 100, 150, 200}, nil)
	distArr1 := distBuilder1.NewArray()
	distBuilder1.Release()

	distBuilder2 := array.NewInt32Builder(s.pool)
	distBuilder2.AppendValues([]int32{0, 100, 200, 300}, nil)
	distArr2 := distBuilder2.NewArray()
	distBuilder2.Release()

	distCol := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{distArr1, distArr2})
	distArr1.Release()
	distArr2.Release()

	// Create score column (Float32) - 2 chunks
	scoreBuilder1 := array.NewFloat32Builder(s.pool)
	scoreBuilder1.AppendValues([]float32{1.0, 1.0, 1.0, 1.0, 1.0}, nil)
	scoreArr1 := scoreBuilder1.NewArray()
	scoreBuilder1.Release()

	scoreBuilder2 := array.NewFloat32Builder(s.pool)
	scoreBuilder2.AppendValues([]float32{1.0, 1.0, 1.0, 1.0}, nil)
	scoreArr2 := scoreBuilder2.NewArray()
	scoreBuilder2.Release()

	scoreCol := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{scoreArr1, scoreArr2})
	scoreArr1.Release()
	scoreArr2.Release()

	return distCol, scoreCol
}

// =============================================================================
// Constructor Tests
// =============================================================================

func (s *DecayExprTestSuite) TestNewDecayExpr_Valid() {
	// Note: inputColumn is no longer part of the function, it's handled by MapOp
	expr, err := NewDecayExpr(GaussFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)
	s.NotNil(expr)
	s.Equal("decay", expr.Name())
	s.Equal(GaussFunction, expr.function)
	s.Equal(100.0, expr.origin)
	s.Equal(50.0, expr.scale)
	s.Equal(0.0, expr.offset)
	s.Equal(0.5, expr.decay)
}

func (s *DecayExprTestSuite) TestNewDecayExpr_AllFunctions() {
	// Gauss
	expr, err := NewDecayExpr(GaussFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)
	s.NotNil(expr.decayFunc)

	// Exp
	expr, err = NewDecayExpr(ExpFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)
	s.NotNil(expr.decayFunc)

	// Linear
	expr, err = NewDecayExpr(LinearFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)
	s.NotNil(expr.decayFunc)
}

func (s *DecayExprTestSuite) TestNewDecayExpr_InvalidFunction() {
	_, err := NewDecayExpr("invalid", 100, 50, 0, 0.5)
	s.Error(err)
	s.Contains(err.Error(), "invalid function")
}

func (s *DecayExprTestSuite) TestNewDecayExpr_InvalidScale() {
	_, err := NewDecayExpr(GaussFunction, 100, 0, 0, 0.5)
	s.Error(err)
	s.Contains(err.Error(), "scale must be > 0")

	_, err = NewDecayExpr(GaussFunction, 100, -10, 0, 0.5)
	s.Error(err)
	s.Contains(err.Error(), "scale must be > 0")
}

func (s *DecayExprTestSuite) TestNewDecayExpr_InvalidOffset() {
	_, err := NewDecayExpr(GaussFunction, 100, 50, -10, 0.5)
	s.Error(err)
	s.Contains(err.Error(), "offset must be >= 0")
}

func (s *DecayExprTestSuite) TestNewDecayExpr_InvalidDecay() {
	_, err := NewDecayExpr(GaussFunction, 100, 50, 0, 0)
	s.Error(err)
	s.Contains(err.Error(), "decay must be 0 < decay < 1")

	_, err = NewDecayExpr(GaussFunction, 100, 50, 0, 1)
	s.Error(err)
	s.Contains(err.Error(), "decay must be 0 < decay < 1")

	_, err = NewDecayExpr(GaussFunction, 100, 50, 0, 1.5)
	s.Error(err)
	s.Contains(err.Error(), "decay must be 0 < decay < 1")
}

func (s *DecayExprTestSuite) TestNewDecayExpr_NumericalStability() {
	// Values too close to 0 should fail
	_, err := NewDecayExpr(GaussFunction, 100, 50, 0, 0.0001)
	s.Error(err)
	s.Contains(err.Error(), "numerical stability")

	// Values too close to 1 should fail
	_, err = NewDecayExpr(GaussFunction, 100, 50, 0, 0.9999)
	s.Error(err)
	s.Contains(err.Error(), "numerical stability")

	// Valid values near boundaries should work
	_, err = NewDecayExpr(GaussFunction, 100, 50, 0, 0.01)
	s.NoError(err)

	_, err = NewDecayExpr(GaussFunction, 100, 50, 0, 0.99)
	s.NoError(err)
}

// =============================================================================
// Factory Tests
// =============================================================================

func (s *DecayExprTestSuite) TestNewDecayExprFromParams_Valid() {
	// Note: input_column is no longer part of function params, it's handled by MapOp
	params := map[string]interface{}{
		"function": "gauss",
		"origin":   100.0,
		"scale":    50.0,
		"offset":   10.0,
		"decay":    0.3,
	}

	expr, err := NewDecayExprFromParams(params)
	s.Require().NoError(err)
	s.NotNil(expr)
	s.Equal("decay", expr.Name())
}

func (s *DecayExprTestSuite) TestNewDecayExprFromParams_DefaultValues() {
	params := map[string]interface{}{
		"function": "gauss",
		"origin":   100.0,
		"scale":    50.0,
	}

	expr, err := NewDecayExprFromParams(params)
	s.Require().NoError(err)

	decayExpr := expr.(*DecayExpr)
	s.Equal(0.0, decayExpr.offset) // default
	s.Equal(0.5, decayExpr.decay)  // default
}

func (s *DecayExprTestSuite) TestNewDecayExprFromParams_MissingRequired() {
	// Missing function
	_, err := NewDecayExprFromParams(map[string]interface{}{
		"origin": 100.0,
		"scale":  50.0,
	})
	s.Error(err)
	s.Contains(err.Error(), "function")

	// Missing origin
	_, err = NewDecayExprFromParams(map[string]interface{}{
		"function": "gauss",
		"scale":    50.0,
	})
	s.Error(err)
	s.Contains(err.Error(), "origin")

	// Missing scale
	_, err = NewDecayExprFromParams(map[string]interface{}{
		"function": "gauss",
		"origin":   100.0,
	})
	s.Error(err)
	s.Contains(err.Error(), "scale")
}

func (s *DecayExprTestSuite) TestNewDecayExprFromParams_WrongTypes() {
	// function is not string
	_, err := NewDecayExprFromParams(map[string]interface{}{
		"function": 123,
		"origin":   100.0,
		"scale":    50.0,
	})
	s.Error(err)
	s.Contains(err.Error(), "must be a string")

	// scale is not number
	_, err = NewDecayExprFromParams(map[string]interface{}{
		"function": "gauss",
		"origin":   100.0,
		"scale":    "fifty",
	})
	s.Error(err)
	s.Contains(err.Error(), "must be a number")
}

func (s *DecayExprTestSuite) TestNewDecayExprFromParams_NumericConversions() {
	// Test int values are converted to float64
	params := map[string]interface{}{
		"function": "gauss",
		"origin":   100, // int
		"scale":    int64(50),
		"offset":   int32(10),
		"decay":    float32(0.3),
	}

	expr, err := NewDecayExprFromParams(params)
	s.Require().NoError(err)
	s.NotNil(expr)
}

// =============================================================================
// Decay Function Tests
// =============================================================================

func (s *DecayExprTestSuite) TestGaussianDecay() {
	// At origin, decay should be 1
	result := gaussianDecay(100, 50, 0.5, 0, 100)
	s.InDelta(1.0, result, 0.001)

	// At scale distance, decay should be the decay parameter
	result = gaussianDecay(100, 50, 0.5, 0, 150)
	s.InDelta(0.5, result, 0.001)

	// Further away, decay should be less
	result1 := gaussianDecay(100, 50, 0.5, 0, 150)
	result2 := gaussianDecay(100, 50, 0.5, 0, 200)
	s.Greater(result1, result2)
}

func (s *DecayExprTestSuite) TestExpDecay() {
	// At origin, decay should be 1
	result := expDecay(100, 50, 0.5, 0, 100)
	s.InDelta(1.0, result, 0.001)

	// At scale distance, decay should be the decay parameter
	result = expDecay(100, 50, 0.5, 0, 150)
	s.InDelta(0.5, result, 0.001)

	// Further away, decay should be less
	result1 := expDecay(100, 50, 0.5, 0, 150)
	result2 := expDecay(100, 50, 0.5, 0, 200)
	s.Greater(result1, result2)
}

func (s *DecayExprTestSuite) TestLinearDecay() {
	// At origin, decay should be 1
	result := linearDecay(100, 50, 0.5, 0, 100)
	s.InDelta(1.0, result, 0.001)

	// At scale distance, decay should be the decay parameter
	result = linearDecay(100, 50, 0.5, 0, 150)
	s.InDelta(0.5, result, 0.001)

	// Linear should never go below decay value
	result = linearDecay(100, 50, 0.5, 0, 1000)
	s.InDelta(0.5, result, 0.001)
}

func (s *DecayExprTestSuite) TestDecayWithOffset() {
	// With offset, decay starts after offset distance
	// At origin + offset, decay should still be 1
	result := gaussianDecay(100, 50, 0.5, 20, 120)
	s.InDelta(1.0, result, 0.001)

	// At origin + offset + scale, decay should be the decay parameter
	result = gaussianDecay(100, 50, 0.5, 20, 170)
	s.InDelta(0.5, result, 0.001)
}

// =============================================================================
// Execute Tests
// =============================================================================

func (s *DecayExprTestSuite) TestExecute_GaussDecay_Int64() {
	distanceCol, scoreCol := s.createTestInputsInt64()
	defer distanceCol.Release()
	defer scoreCol.Release()

	expr, err := NewDecayExpr(GaussFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)

	inputs := []*arrow.Chunked{distanceCol, scoreCol}

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, inputs)
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()

	// Verify output is the new score column
	resultScoreCol := outputs[0]
	s.Equal(int64(9), int64(resultScoreCol.Len())) // 5 + 4 = 9

	// Check first chunk - distances: 0, 50, 100, 150, 200
	// At distance=100 (origin), decay should be 1.0
	// At distance=150 (scale away), decay should be 0.5
	chunk0Scores := make([]float32, 5)
	for i := 0; i < 5; i++ {
		chunk0Scores[i] = resultScoreCol.Chunk(0).(*array.Float32).Value(i)
	}

	// Distance 100 should have highest score (at origin)
	s.Greater(float64(chunk0Scores[2]), float64(chunk0Scores[0])) // 100 > 0
	s.Greater(float64(chunk0Scores[2]), float64(chunk0Scores[4])) // 100 > 200
}

func (s *DecayExprTestSuite) TestExecute_ExpDecay_Float() {
	distanceCol, scoreCol := s.createTestInputsFloat32()
	defer distanceCol.Release()
	defer scoreCol.Release()

	expr, err := NewDecayExpr(ExpFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)

	inputs := []*arrow.Chunked{distanceCol, scoreCol}

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, inputs)
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()

	s.Equal(int64(9), int64(outputs[0].Len()))
}

func (s *DecayExprTestSuite) TestExecute_LinearDecay_Double() {
	distanceCol, scoreCol := s.createTestInputsFloat64()
	defer distanceCol.Release()
	defer scoreCol.Release()

	expr, err := NewDecayExpr(LinearFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)

	inputs := []*arrow.Chunked{distanceCol, scoreCol}

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, inputs)
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()

	s.Equal(int64(9), int64(outputs[0].Len()))
}

func (s *DecayExprTestSuite) TestExecute_Int32Input() {
	distanceCol, scoreCol := s.createTestInputsInt32()
	defer distanceCol.Release()
	defer scoreCol.Release()

	expr, err := NewDecayExpr(GaussFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)

	inputs := []*arrow.Chunked{distanceCol, scoreCol}

	ctx := types.NewFuncContext(s.pool)
	outputs, err := expr.Execute(ctx, inputs)
	s.Require().NoError(err)
	s.Len(outputs, 1)
	defer outputs[0].Release()
}

func (s *DecayExprTestSuite) TestExecute_WrongInputCount() {
	expr, err := NewDecayExpr(GaussFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)

	// Test with wrong number of inputs
	_, err = expr.Execute(ctx, []*arrow.Chunked{})
	s.Error(err)
	s.Contains(err.Error(), "expected 2 input columns")
}

// =============================================================================
// Memory Leak Tests
// =============================================================================

func (s *DecayExprTestSuite) TestMemoryLeak_DecayExecution() {
	for range 10 {
		distanceCol, scoreCol := s.createTestInputsInt64()

		expr, err := NewDecayExpr(GaussFunction, 100, 50, 0, 0.5)
		s.Require().NoError(err)

		inputs := []*arrow.Chunked{distanceCol, scoreCol}

		ctx := types.NewFuncContext(s.pool)
		outputs, err := expr.Execute(ctx, inputs)
		s.Require().NoError(err)

		outputs[0].Release()
		distanceCol.Release()
		scoreCol.Release()
	}
	// Memory leak check happens in TearDownTest
}

// =============================================================================
// Interface Method Tests
// =============================================================================

func (s *DecayExprTestSuite) TestOutputDataTypes() {
	expr, err := NewDecayExpr(GaussFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)

	outputTypes := expr.OutputDataTypes()
	s.Len(outputTypes, 1)
	s.Equal(arrow.PrimitiveTypes.Float32, outputTypes[0])
}

func (s *DecayExprTestSuite) TestIsRunnable() {
	expr, err := NewDecayExpr(GaussFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)

	s.True(expr.IsRunnable(types.StageL2Rerank))
	s.True(expr.IsRunnable(types.StageL1Rerank))
	s.False(expr.IsRunnable("unknown_stage"))
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func (s *DecayExprTestSuite) TestDecayAtExtremeDistances() {
	// Test decay at very large distances
	result := gaussianDecay(0, 100, 0.5, 0, 10000)
	s.True(result >= 0 && result <= 1)
	s.True(!math.IsNaN(result))
	s.True(!math.IsInf(result, 0))

	result = expDecay(0, 100, 0.5, 0, 10000)
	s.True(result >= 0 && result <= 1)
	s.True(!math.IsNaN(result))
	s.True(!math.IsInf(result, 0))

	result = linearDecay(0, 100, 0.5, 0, 10000)
	s.True(result >= 0 && result <= 1)
	s.True(!math.IsNaN(result))
	s.True(!math.IsInf(result, 0))
}

func (s *DecayExprTestSuite) TestDecaySymmetric() {
	// Decay should be symmetric around origin
	r1 := gaussianDecay(100, 50, 0.5, 0, 150)
	r2 := gaussianDecay(100, 50, 0.5, 0, 50)
	s.InDelta(r1, r2, 0.001)

	r1 = expDecay(100, 50, 0.5, 0, 150)
	r2 = expDecay(100, 50, 0.5, 0, 50)
	s.InDelta(r1, r2, 0.001)

	r1 = linearDecay(100, 50, 0.5, 0, 150)
	r2 = linearDecay(100, 50, 0.5, 0, 50)
	s.InDelta(r1, r2, 0.001)
}
