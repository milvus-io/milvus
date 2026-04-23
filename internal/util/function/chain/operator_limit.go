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
	"fmt"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func init() {
	MustRegisterOperator(types.OpTypeLimit, NewLimitOpFromRepr)
}

// LimitOp limits the number of rows in each chunk.
// Note: Limit is applied independently to each chunk (per-query limiting for search results).
type LimitOp struct {
	BaseOp
	limit  int64
	offset int64
}

// NewLimitOp creates a new LimitOp with the given limit and offset.
func NewLimitOp(limit, offset int64) *LimitOp {
	return &LimitOp{
		BaseOp: BaseOp{
			inputs:  []string{}, // Limit works on all columns
			outputs: []string{}, // Limit doesn't produce new columns
		},
		limit:  limit,
		offset: offset,
	}
}

func (o *LimitOp) Name() string { return "Limit" }

// Inputs and Outputs are inherited from BaseOp

func (o *LimitOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	colNames := input.ColumnNames()
	collector := NewChunkCollector(colNames, input.NumChunks())
	defer collector.Release()

	newChunkSizes := make([]int64, input.NumChunks())

	// Process each chunk
	for chunkIdx := range input.NumChunks() {
		chunkSize := input.chunkSizes[chunkIdx]

		// Calculate actual offset and limit for this chunk
		start := min(o.offset, chunkSize)
		end := min(start+o.limit, chunkSize)

		newChunkSizes[chunkIdx] = end - start

		// Slice each column
		for _, colName := range colNames {
			col := input.Column(colName)
			dataChunk := col.Chunk(chunkIdx)
			sliced, err := sliceArray(dataChunk, int(start), int(end))
			if err != nil {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("limit_op: column %s: %v", colName, err))
			}
			collector.Set(colName, chunkIdx, sliced)
		}
	}

	// Create new DataFrame with all chunks
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(newChunkSizes)

	for _, colName := range colNames {
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("limit_op: %v", err))
		}
		builder.CopyFieldMetadata(input, colName)
	}

	return builder.Build(), nil
}

func (o *LimitOp) String() string {
	if o.offset > 0 {
		return fmt.Sprintf("Limit(%d, offset=%d)", o.limit, o.offset)
	}
	return fmt.Sprintf("Limit(%d)", o.limit)
}

// NewLimitOpFromRepr creates a LimitOp from an OperatorRepr.
func NewLimitOpFromRepr(repr *OperatorRepr) (Operator, error) {
	limitVal, ok := repr.Params["limit"]
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("limit_op: limit is required")
	}
	var limit int64
	switch v := limitVal.(type) {
	case int64:
		limit = v
	case int:
		limit = int64(v)
	case float64:
		limit = int64(v)
	default:
		return nil, merr.WrapErrParameterInvalidMsg("limit_op: limit must be a number")
	}
	if limit <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("limit_op: limit must be positive")
	}
	offset := int64(0)
	if offsetVal, ok := repr.Params["offset"]; ok {
		switch v := offsetVal.(type) {
		case int64:
			offset = v
		case int:
			offset = int64(v)
		case float64:
			offset = int64(v)
		}
	}
	if offset < 0 {
		return nil, merr.WrapErrParameterInvalidMsg("limit_op: offset must be non-negative")
	}
	return NewLimitOp(limit, offset), nil
}
