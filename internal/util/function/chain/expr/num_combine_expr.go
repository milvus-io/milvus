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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// =============================================================================
// Constants (use types package constants)
// =============================================================================

const (
	// Parameter keys for NumCombineExpr
	ModeKey    = types.NumCombineParamMode
	WeightsKey = types.NumCombineParamWeights

	// Mode values
	ModeMultiply = types.NumCombineModeMultiply
	ModeSum      = types.NumCombineModeSum
	ModeMax      = types.NumCombineModeMax
	ModeMin      = types.NumCombineModeMin
	ModeAvg      = types.NumCombineModeAvg
	ModeWeighted = types.NumCombineModeWeighted
)

// =============================================================================
// Types
// =============================================================================

const NumCombineFuncName = "num_combine"

// NumCombineExpr implements FunctionExpr for combining multiple numeric columns into one.
// It supports dynamic input columns to prepare for multi-rerank scenarios.
// Column mapping is handled by MapOp.
//
// Expected inputs (passed from MapOp):
//   - inputs[0..N-1]: N numeric columns to combine (at least 2)
//
// Outputs:
//   - outputs[0]: combined numeric column
type NumCombineNullPolicy int

const (
	// NumCombineNullPropagate returns null if any input is null.
	NumCombineNullPropagate NumCombineNullPolicy = iota
	// NumCombineNullAsZero treats null inputs as zero.
	NumCombineNullAsZero
	// NumCombineNullSkip skips null inputs and returns null if all inputs are null.
	NumCombineNullSkip
)

type NumCombineExpr struct {
	BaseExpr
	mode       string               // combine mode: multiply, sum, max, min, avg, weighted
	weights    []float64            // weights for weighted mode
	nullPolicy NumCombineNullPolicy // null handling policy
}

type NumCombineOption func(*NumCombineExpr)

func WithNullPolicy(policy NumCombineNullPolicy) NumCombineOption {
	return func(s *NumCombineExpr) {
		s.nullPolicy = policy
	}
}

// =============================================================================
// Constructor Functions
// =============================================================================

// NewNumCombineExpr creates a new NumCombineExpr with the given parameters.
// Note: Column mapping (which columns to use as input/output) is handled by MapOp,
// not by the function itself.
func NewNumCombineExpr(mode string, weights []float64, opts ...NumCombineOption) (*NumCombineExpr, error) {
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
		return nil, merr.WrapErrParameterInvalidMsg("num_combine: invalid mode %q, must be one of [%s, %s, %s, %s, %s, %s]",
			mode, ModeMultiply, ModeSum, ModeMax, ModeMin, ModeAvg, ModeWeighted)
	}

	// Weighted mode requires weights
	if mode == ModeWeighted && len(weights) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("num_combine: weighted mode requires weights")
	}

	// nil supportStages means the function supports all stages
	expr := &NumCombineExpr{
		BaseExpr:   *NewBaseExpr(NumCombineFuncName, nil),
		mode:       mode,
		weights:    weights,
		nullPolicy: NumCombineNullPropagate,
	}
	for _, opt := range opts {
		opt(expr)
	}
	return expr, nil
}

// NewNumCombineExprFromParams creates a NumCombineExpr from a parameter map.
// This is the factory function for the function registry.
// All parameter parsing is handled here, keeping it close to the expr definition.
func NewNumCombineExprFromParams(_ types.FunctionBuildContext, cfg types.FunctionConfig) (types.FunctionExpr, error) {
	reader := types.NewParamReader(NumCombineFuncName, cfg.Params)

	mode, err := reader.String(ModeKey, false)
	if err != nil {
		return nil, err
	}

	weights, err := reader.Float64Slice(WeightsKey, false)
	if err != nil {
		return nil, err
	}

	return NewNumCombineExpr(mode, weights)
}

// =============================================================================
// FunctionExpr Interface Implementation
// =============================================================================

// Name() and IsRunnable() are inherited from BaseExpr
// (nil supportStages in BaseExpr means the function supports all stages)

// OutputDataTypes returns the data types of output columns.
// NumCombineExpr outputs a single Float32 column (the combined numeric value).
func (s *NumCombineExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

// Execute executes the numeric combine function on input columns and returns output columns.
func (s *NumCombineExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if len(inputs) < 2 {
		return nil, merr.WrapErrParameterInvalidMsg("num_combine: expected at least 2 input columns, got %d", len(inputs))
	}

	if s.mode == ModeWeighted && len(s.weights) != len(inputs) {
		return nil, merr.WrapErrParameterInvalidMsg("num_combine: weighted mode requires %d weights, got %d", len(inputs), len(s.weights))
	}

	numChunks := len(inputs[0].Chunks())
	for idx := 1; idx < len(inputs); idx++ {
		if len(inputs[idx].Chunks()) != numChunks {
			return nil, merr.WrapErrServiceInternalMsg("num_combine: input 0 has %d chunks but input %d has %d chunks", numChunks, idx, len(inputs[idx].Chunks()))
		}
	}
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
func (s *NumCombineExpr) processChunk(ctx *types.FuncContext, inputs []*arrow.Chunked, chunkIdx int) (arrow.Array, error) {
	builder := array.NewFloat32Builder(ctx.Pool())
	defer builder.Release()

	chunkLen := inputs[0].Chunk(chunkIdx).Len()
	readers := make([]numericReader, len(inputs))
	for colIdx, input := range inputs {
		chunk := input.Chunk(chunkIdx)
		if chunk.Len() != chunkLen {
			return nil, merr.WrapErrServiceInternalMsg("num_combine: input 0 chunk %d has %d rows but input %d has %d rows", chunkIdx, chunkLen, colIdx, chunk.Len())
		}
		reader, ok := newNumericReader(chunk)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("num_combine: column %d: unsupported input column type %T, expected numeric type", colIdx, chunk)
		}
		readers[colIdx] = reader
	}

	s.processRows(builder, readers, chunkLen)

	return builder.NewArray(), nil
}

func (s *NumCombineExpr) processRows(builder *array.Float32Builder, readers []numericReader, chunkLen int) {
	values := make([]float64, 0, len(readers))
	weights := make([]float64, 0, len(readers))
	for rowIdx := 0; rowIdx < chunkLen; rowIdx++ {
		rowValues, rowWeights, ok := s.collectRowValues(readers, rowIdx, values, weights)
		if !ok {
			builder.AppendNull()
			continue
		}
		builder.Append(float32(s.combine(rowValues, rowWeights)))
	}
}

func (s *NumCombineExpr) collectRowValues(readers []numericReader, rowIdx int, values []float64, weights []float64) ([]float64, []float64, bool) {
	values = values[:0]
	weights = weights[:0]
	for idx, reader := range readers {
		if reader.IsNull(rowIdx) {
			switch s.nullPolicy {
			case NumCombineNullPropagate:
				return values, weights, false
			case NumCombineNullAsZero:
				values = append(values, 0)
				if s.mode == ModeWeighted {
					weights = append(weights, s.weights[idx])
				}
			case NumCombineNullSkip:
				continue
			default:
				return values, weights, false
			}
			continue
		}

		values = append(values, reader.Float64(rowIdx))
		if s.mode == ModeWeighted {
			weights = append(weights, s.weights[idx])
		}
	}
	return values, weights, len(values) > 0
}

type numericReader interface {
	IsNull(int) bool
	Float64(int) float64
}

type numericValue interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~float32 | ~float64
}

type arrowNumericArray[T numericValue] interface {
	IsNull(int) bool
	Value(int) T
}

type typedNumericReader[T numericValue, A arrowNumericArray[T]] struct {
	arr A
}

func (r typedNumericReader[T, A]) IsNull(idx int) bool {
	return r.arr.IsNull(idx)
}

func (r typedNumericReader[T, A]) Float64(idx int) float64 {
	return float64(r.arr.Value(idx))
}

func newNumericReader(arr arrow.Array) (numericReader, bool) {
	switch a := arr.(type) {
	case *array.Int8:
		return typedNumericReader[int8, *array.Int8]{arr: a}, true
	case *array.Int16:
		return typedNumericReader[int16, *array.Int16]{arr: a}, true
	case *array.Int32:
		return typedNumericReader[int32, *array.Int32]{arr: a}, true
	case *array.Int64:
		return typedNumericReader[int64, *array.Int64]{arr: a}, true
	case *array.Float32:
		return typedNumericReader[float32, *array.Float32]{arr: a}, true
	case *array.Float64:
		return typedNumericReader[float64, *array.Float64]{arr: a}, true
	default:
		return nil, false
	}
}

// combine combines multiple values based on the mode.
func (s *NumCombineExpr) combine(values []float64, weights []float64) float64 {
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
			sum += v * weights[i]
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
	types.MustRegisterFunction(NumCombineFuncName, NewNumCombineExprFromParams)
}
