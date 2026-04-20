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
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const RoundDecimalFuncName = "round_decimal"

// RoundDecimalExpr rounds float32 scores to a specified number of decimal places.
// Formula: floor(score * 10^decimal + 0.5) / 10^decimal
//
// Input:  1 column ($score, Float32)
// Output: 1 column ($score, Float32, rounded)
//
// Note: Not registered in the function registry because it requires a specific
// decimal parameter that is set programmatically from SearchParams.RoundDecimal
// in rerank_builder.go, not from user-facing function configuration.
type RoundDecimalExpr struct {
	BaseExpr
	decimal    int64
	multiplier float64
}

// NewRoundDecimalExpr creates a new RoundDecimalExpr.
// decimal must be >= 0 and <= 6.
func NewRoundDecimalExpr(decimal int64) (*RoundDecimalExpr, error) {
	if decimal < 0 || decimal > 6 {
		return nil, merr.WrapErrParameterInvalidMsg("round_decimal: decimal must be in range [0, 6], got %d", decimal)
	}
	return &RoundDecimalExpr{
		BaseExpr:   *NewBaseExpr(RoundDecimalFuncName, types.AllStages),
		decimal:    decimal,
		multiplier: math.Pow(10.0, float64(decimal)),
	}, nil
}

func (e *RoundDecimalExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

func (e *RoundDecimalExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if len(inputs) != 1 {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("round_decimal: expected 1 input column, got %d", len(inputs)))
	}

	scoreCol := inputs[0]
	numChunks := len(scoreCol.Chunks())
	newChunks := make([]arrow.Array, numChunks)

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		scoreChunk, ok := scoreCol.Chunk(chunkIdx).(*array.Float32)
		if !ok {
			for i := 0; i < chunkIdx; i++ {
				newChunks[i].Release()
			}
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("round_decimal: input chunk %d must be Float32, got %T", chunkIdx, scoreCol.Chunk(chunkIdx)))
		}

		builder := array.NewFloat32Builder(ctx.Pool())
		for i := 0; i < scoreChunk.Len(); i++ {
			if scoreChunk.IsNull(i) {
				builder.AppendNull()
				continue
			}
			score := scoreChunk.Value(i)
			rounded := float32(math.Floor(float64(score)*e.multiplier+0.5) / e.multiplier)
			builder.Append(rounded)
		}
		newChunks[chunkIdx] = builder.NewArray()
		builder.Release()
	}

	result := arrow.NewChunked(arrow.PrimitiveTypes.Float32, newChunks)
	for _, chunk := range newChunks {
		chunk.Release()
	}

	return []*arrow.Chunked{result}, nil
}
