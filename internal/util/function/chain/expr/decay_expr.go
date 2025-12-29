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
	// Decay function types
	GaussFunction  = types.DecayFuncGauss
	LinearFunction = types.DecayFuncLinear
	ExpFunction    = types.DecayFuncExp

	// Parameter keys for DecayExpr
	FunctionKey = types.DecayParamFunction
	OriginKey   = types.DecayParamOrigin
	ScaleKey    = types.DecayParamScale
	OffsetKey   = types.DecayParamOffset
	DecayKey    = types.DecayParamDecay
)

// =============================================================================
// Types
// =============================================================================

// decayReScorer is a function type for decay calculation.
type decayReScorer func(origin, scale, decay, offset, distance float64) float64

// DecayExpr implements FunctionExpr for decay scoring.
// It takes a numeric input column and a score column, then outputs a new score
// column with decay applied. Column mapping is handled by MapOp.
//
// Expected inputs (passed from MapOp):
//   - inputs[0]: numeric column to calculate distance from origin
//   - inputs[1]: original score column
//
// Outputs:
//   - outputs[0]: new score column (original_score * decay_factor)
type DecayExpr struct {
	BaseExpr
	function  string        // "gauss", "exp", or "linear"
	origin    float64       // origin point
	scale     float64       // scale parameter (must > 0)
	offset    float64       // offset (default 0, must >= 0)
	decay     float64       // decay factor (default 0.5, 0 < decay < 1)
	decayFunc decayReScorer // selected decay function
}

// =============================================================================
// Decay Calculation Functions
// =============================================================================

// gaussianDecay calculates Gaussian decay.
func gaussianDecay(origin, scale, decay, offset, distance float64) float64 {
	adjustedDist := math.Max(0, math.Abs(distance-origin)-offset)
	sigmaSquare := math.Pow(scale, 2.0) / math.Log(decay)
	exponent := math.Pow(adjustedDist, 2.0) / sigmaSquare
	return math.Exp(exponent)
}

// expDecay calculates exponential decay.
func expDecay(origin, scale, decay, offset, distance float64) float64 {
	adjustedDist := math.Max(0, math.Abs(distance-origin)-offset)
	lambda := math.Log(decay) / scale
	return math.Exp(lambda * adjustedDist)
}

// linearDecay calculates linear decay.
func linearDecay(origin, scale, decay, offset, distance float64) float64 {
	adjustedDist := math.Max(0, math.Abs(distance-origin)-offset)
	slope := (1 - decay) / scale
	return math.Max(decay, 1-slope*adjustedDist)
}

// =============================================================================
// Constructor Functions
// =============================================================================

// minDecayValue is the minimum allowed decay value to ensure numerical stability.
// When decay is too close to 0, log(decay) approaches -∞ causing numerical instability.
const minDecayValue = 0.001

// maxDecayValue is the maximum allowed decay value to ensure numerical stability.
// When decay is too close to 1, log(decay) approaches 0 causing division issues.
const maxDecayValue = 0.999

// NewDecayExpr creates a new DecayExpr with the given parameters.
// Note: Column mapping (which columns to use as input/output) is handled by MapOp,
// not by the function itself.
func NewDecayExpr(function string, origin, scale, offset, decay float64) (*DecayExpr, error) {
	if scale <= 0 {
		return nil, fmt.Errorf("decay: scale must be > 0, got %f", scale)
	}

	if offset < 0 {
		return nil, fmt.Errorf("decay: offset must be >= 0, got %f", offset)
	}

	if decay <= 0 || decay >= 1 {
		return nil, fmt.Errorf("decay: decay must be 0 < decay < 1, got %f", decay)
	}

	// Additional check for numerical stability
	if decay < minDecayValue || decay > maxDecayValue {
		return nil, fmt.Errorf("decay: decay must be between %f and %f for numerical stability, got %f",
			minDecayValue, maxDecayValue, decay)
	}

	expr := &DecayExpr{
		BaseExpr: *NewBaseExpr("decay", types.AllStages),
		function: function,
		origin:   origin,
		scale:    scale,
		offset:   offset,
		decay:    decay,
	}

	// Select decay function
	switch function {
	case GaussFunction:
		expr.decayFunc = gaussianDecay
	case ExpFunction:
		expr.decayFunc = expDecay
	case LinearFunction:
		expr.decayFunc = linearDecay
	default:
		return nil, fmt.Errorf("decay: invalid function %q, must be one of [%s, %s, %s]",
			function, GaussFunction, ExpFunction, LinearFunction)
	}

	return expr, nil
}

// NewDecayExprFromParams creates a DecayExpr from a parameter map.
// This is the factory function for the function registry.
// All parameter parsing is handled here, keeping it close to the expr definition.
func NewDecayExprFromParams(params map[string]interface{}) (types.FunctionExpr, error) {
	const funcName = "decay"

	// Extract function (required)
	function, err := GetStringParam(params, funcName, FunctionKey, true)
	if err != nil {
		return nil, err
	}

	// Extract origin (required)
	origin, err := GetFloat64Param(params, funcName, OriginKey, true, 0)
	if err != nil {
		return nil, err
	}

	// Extract scale (required)
	scale, err := GetFloat64Param(params, funcName, ScaleKey, true, 0)
	if err != nil {
		return nil, err
	}

	// Extract offset (optional, default 0)
	offset, err := GetFloat64Param(params, funcName, OffsetKey, false, 0)
	if err != nil {
		return nil, err
	}

	// Extract decay (optional, default 0.5)
	decayVal, err := GetFloat64Param(params, funcName, DecayKey, false, 0.5)
	if err != nil {
		return nil, err
	}

	return NewDecayExpr(function, origin, scale, offset, decayVal)
}

// =============================================================================
// FunctionExpr Interface Implementation
// =============================================================================

// Name() and IsRunnable() are inherited from BaseExpr

// OutputDataTypes returns the data types of output columns.
// DecayExpr outputs a single Float32 column (the new score).
func (d *DecayExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

// Execute executes the decay function on input columns and returns output columns.
// inputs[0]: the numeric column to calculate decay from
// inputs[1]: the score column
// returns: new score column with decay applied
func (d *DecayExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if len(inputs) != 2 {
		return nil, fmt.Errorf("decay: expected 2 input columns, got %d", len(inputs))
	}

	inputCol := inputs[0] // decay input column
	scoreCol := inputs[1] // score column

	// Process each chunk and build new score arrays
	numChunks := len(inputCol.Chunks())
	newScoreChunks := make([]arrow.Array, numChunks)

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		inputChunk := inputCol.Chunk(chunkIdx)
		scoreChunk, ok := scoreCol.Chunk(chunkIdx).(*array.Float32)
		if !ok {
			return nil, fmt.Errorf("decay: score column chunk %d must be Float32, got %T", chunkIdx, scoreCol.Chunk(chunkIdx))
		}

		newScoreChunk, err := d.processChunk(ctx, inputChunk, scoreChunk)
		if err != nil {
			// Release already created chunks on error
			for i := 0; i < chunkIdx; i++ {
				newScoreChunks[i].Release()
			}
			return nil, err
		}
		newScoreChunks[chunkIdx] = newScoreChunk
	}

	// Create ChunkedArray for output
	result := arrow.NewChunked(arrow.PrimitiveTypes.Float32, newScoreChunks)

	// Release individual arrays after creating chunked (NewChunked retains them)
	for _, chunk := range newScoreChunks {
		chunk.Release()
	}

	return []*arrow.Chunked{result}, nil
}

// =============================================================================
// Internal Processing Methods
// =============================================================================

// processChunk processes a single chunk, calculating new scores.
func (d *DecayExpr) processChunk(ctx *types.FuncContext, inputChunk arrow.Array, scoreChunk *array.Float32) (arrow.Array, error) {
	builder := array.NewFloat32Builder(ctx.Pool())
	defer builder.Release()

	for i := range inputChunk.Len() {
		if inputChunk.IsNull(i) || scoreChunk.IsNull(i) {
			builder.AppendNull()
			continue
		}

		// Get numeric value from input column using base_expr utility
		distance, err := GetNumericValue(inputChunk, i)
		if err != nil {
			return nil, fmt.Errorf("decay: %w", err)
		}

		// Calculate decay score
		decayScore := d.decayFunc(d.origin, d.scale, d.decay, d.offset, distance)

		// Multiply original score by decay score
		originalScore := scoreChunk.Value(i)
		newScore := float32(float64(originalScore) * decayScore)

		builder.Append(newScore)
	}

	return builder.NewArray(), nil
}

// =============================================================================
// Registration
// =============================================================================

func init() {
	types.MustRegisterFunction("decay", NewDecayExprFromParams)
}
