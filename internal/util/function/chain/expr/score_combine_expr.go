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
	"fmt"
	"math"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// =============================================================================
// Constants (use types package constants)
// =============================================================================

const (
	// Parameter keys for ScoreCombineExpr
	ModeKey       = types.ScoreCombineParamMode
	WeightsKey    = types.ScoreCombineParamWeights
	InputCountKey = types.ScoreCombineParamInputCount

	// Mode values
	ModeMultiply = types.ScoreCombineModeMultiply
	ModeSum      = types.ScoreCombineModeSum
	ModeMax      = types.ScoreCombineModeMax
	ModeMin      = types.ScoreCombineModeMin
	ModeAvg      = types.ScoreCombineModeAvg
	ModeWeighted = types.ScoreCombineModeWeighted
)

// =============================================================================
// Types
// =============================================================================

// ScoreCombineExpr implements FunctionExpr for combining multiple score columns into one.
// It supports dynamic input columns to prepare for multi-rerank scenarios.
// Column mapping is handled by MapOp.
//
// Expected inputs (passed from MapOp):
//   - inputs[0..N-1]: N numeric columns to combine
//
// Outputs:
//   - outputs[0]: combined score column
type ScoreCombineExpr struct {
	BaseExpr
	mode       string    // combine mode: multiply, sum, max, min, avg, weighted
	weights    []float64 // weights for weighted mode
	inputCount int       // expected number of input columns
}

// =============================================================================
// Constructor Functions
// =============================================================================

// NewScoreCombineExpr creates a new ScoreCombineExpr with the given parameters.
// Note: Column mapping (which columns to use as input/output) is handled by MapOp,
// not by the function itself.
func NewScoreCombineExpr(mode string, inputCount int, weights []float64) (*ScoreCombineExpr, error) {
	if inputCount < 2 {
		return nil, fmt.Errorf("score_combine: at least 2 input columns required, got %d", inputCount)
	}

	// Default mode
	if mode == "" {
		mode = ModeMultiply
	}

	// Validate mode
	validModes := map[string]bool{
		ModeMultiply: true,
		ModeSum:      true,
		ModeMax:      true,
		ModeMin:      true,
		ModeAvg:      true,
		ModeWeighted: true,
	}
	if !validModes[mode] {
		return nil, fmt.Errorf("score_combine: invalid mode %q, must be one of [%s, %s, %s, %s, %s, %s]",
			mode, ModeMultiply, ModeSum, ModeMax, ModeMin, ModeAvg, ModeWeighted)
	}

	// Weighted mode requires weights
	if mode == ModeWeighted {
		if len(weights) != inputCount {
			return nil, fmt.Errorf("score_combine: weighted mode requires %d weights, got %d",
				inputCount, len(weights))
		}
	}

	// nil supportStages means the function supports all stages
	return &ScoreCombineExpr{
		BaseExpr:   *NewBaseExpr("score_combine", nil),
		mode:       mode,
		weights:    weights,
		inputCount: inputCount,
	}, nil
}

// NewScoreCombineExprFromParams creates a ScoreCombineExpr from a parameter map.
// This is the factory function for the function registry.
// All parameter parsing is handled here, keeping it close to the expr definition.
func NewScoreCombineExprFromParams(params map[string]interface{}) (types.FunctionExpr, error) {
	const funcName = "score_combine"

	// Parse mode (optional, default multiply)
	mode, err := GetStringParam(params, funcName, ModeKey, false)
	if err != nil {
		return nil, err
	}

	// Parse input_count (required)
	inputCountFloat, err := GetFloat64Param(params, funcName, InputCountKey, true, 0)
	if err != nil {
		return nil, err
	}
	inputCount := int(inputCountFloat)
	if float64(inputCount) != inputCountFloat {
		return nil, fmt.Errorf("score_combine: %s must be a whole number, got %f", InputCountKey, inputCountFloat)
	}

	// Parse weights (optional)
	weights, err := ParseFloat64SliceParam(params, funcName, WeightsKey)
	if err != nil {
		return nil, err
	}

	return NewScoreCombineExpr(mode, inputCount, weights)
}

// =============================================================================
// FunctionExpr Interface Implementation
// =============================================================================

// Name() and IsRunnable() are inherited from BaseExpr
// (nil supportStages in BaseExpr means the function supports all stages)

// OutputDataTypes returns the data types of output columns.
// ScoreCombineExpr outputs a single Float32 column (the combined score).
func (s *ScoreCombineExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

// Execute executes the score combine function on input columns and returns output columns.
func (s *ScoreCombineExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if len(inputs) != s.inputCount {
		return nil, fmt.Errorf("score_combine: expected %d input columns, got %d", s.inputCount, len(inputs))
	}

	if len(inputs) < 2 {
		return nil, fmt.Errorf("score_combine: expected at least 2 input columns, got %d", len(inputs))
	}

	numChunks := len(inputs[0].Chunks())
	resultChunks := make([]arrow.Array, numChunks)

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		newChunk, err := s.processChunk(ctx, inputs, chunkIdx)
		if err != nil {
			// Release already created chunks on error
			for i := 0; i < chunkIdx; i++ {
				resultChunks[i].Release()
			}
			return nil, err
		}
		resultChunks[chunkIdx] = newChunk
	}

	// Create ChunkedArray for output
	result := arrow.NewChunked(arrow.PrimitiveTypes.Float32, resultChunks)

	// Release individual arrays after creating chunked (NewChunked retains them)
	for _, chunk := range resultChunks {
		chunk.Release()
	}

	return []*arrow.Chunked{result}, nil
}

// =============================================================================
// Internal Processing Methods
// =============================================================================

// processChunk processes a single chunk, combining scores.
func (s *ScoreCombineExpr) processChunk(ctx *types.FuncContext, inputs []*arrow.Chunked, chunkIdx int) (arrow.Array, error) {
	builder := array.NewFloat32Builder(ctx.Pool())
	defer builder.Release()

	chunkLen := inputs[0].Chunk(chunkIdx).Len()

	for rowIdx := 0; rowIdx < chunkLen; rowIdx++ {
		// Check if any input is null
		hasNull := false
		for _, input := range inputs {
			if input.Chunk(chunkIdx).IsNull(rowIdx) {
				hasNull = true
				break
			}
		}

		if hasNull {
			builder.AppendNull()
			continue
		}

		// Collect values from all input columns using base_expr utility
		values := make([]float64, len(inputs))
		for colIdx, input := range inputs {
			val, err := GetNumericValue(input.Chunk(chunkIdx), rowIdx)
			if err != nil {
				return nil, fmt.Errorf("score_combine: column %d: %w", colIdx, err)
			}
			values[colIdx] = val
		}

		// Combine values based on mode
		result := s.combine(values)
		builder.Append(float32(result))
	}

	return builder.NewArray(), nil
}

// combine combines multiple values based on the mode.
func (s *ScoreCombineExpr) combine(values []float64) float64 {
	switch s.mode {
	case ModeMultiply:
		result := 1.0
		for _, v := range values {
			result *= v
		}
		return result

	case ModeSum:
		result := 0.0
		for _, v := range values {
			result += v
		}
		return result

	case ModeMax:
		result := values[0]
		for _, v := range values[1:] {
			result = math.Max(result, v)
		}
		return result

	case ModeMin:
		result := values[0]
		for _, v := range values[1:] {
			result = math.Min(result, v)
		}
		return result

	case ModeAvg:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))

	case ModeWeighted:
		sum := 0.0
		for i, v := range values {
			sum += v * s.weights[i]
		}
		return sum

	default:
		// This should never happen since the constructor validates modes,
		// but return 0 as a safe fallback.
		return 0
	}
}

// =============================================================================
// Registration
// =============================================================================

func init() {
	types.MustRegisterFunction("score_combine", NewScoreCombineExprFromParams)
}
