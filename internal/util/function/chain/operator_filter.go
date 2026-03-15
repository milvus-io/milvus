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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func init() {
	MustRegisterOperator(types.OpTypeFilter, NewFilterOpFromRepr)
}

// FilterOp filters the DataFrame based on the boolean result of a FunctionExpr.
// The FunctionExpr must return exactly one boolean column.
type FilterOp struct {
	BaseOp
	function types.FunctionExpr // FunctionExpr that returns bool type
}

// NewFilterOp creates a new FilterOp with the given FunctionExpr and input columns.
// The function must return exactly one boolean column.
func NewFilterOp(function types.FunctionExpr, inputCols []string) (*FilterOp, error) {
	if function == nil {
		return nil, merr.WrapErrServiceInternal("filter_op: function is nil")
	}

	// Validate: function must return exactly one boolean type
	// Skip validation if OutputDataTypes() returns nil (dynamic output types)
	outputTypes := function.OutputDataTypes()
	if outputTypes != nil {
		if len(outputTypes) != 1 {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("filter_op: function must return exactly 1 output, got %d", len(outputTypes)))
		}
		if outputTypes[0].ID() != arrow.BOOL {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("filter_op: function must return boolean type, got %s", outputTypes[0].Name()))
		}
	}

	return &FilterOp{
		BaseOp: BaseOp{
			inputs:  inputCols,
			outputs: []string{}, // Filter doesn't produce new columns
		},
		function: function,
	}, nil
}

func (o *FilterOp) Name() string { return "Filter" }

// Inputs and Outputs are inherited from BaseOp

func (o *FilterOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	// Read input columns from DataFrame
	inputs := make([]*arrow.Chunked, len(o.inputs))
	for i, name := range o.inputs {
		col := input.Column(name)
		if col == nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("filter_op: column %q not found", name))
		}
		inputs[i] = col
	}

	// Execute FunctionExpr to get boolean result
	outputs, err := o.function.Execute(ctx, inputs)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("filter_op: function execution failed: %v", err))
	}

	// Validate output at runtime (especially important for dynamic output types)
	if len(outputs) != 1 {
		for _, out := range outputs {
			if out != nil {
				out.Release()
			}
		}
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("filter_op: function must return exactly 1 output, got %d", len(outputs)))
	}

	filterCol := outputs[0]
	defer filterCol.Release() // Release the temporary boolean column

	// Validate the output is boolean type
	if filterCol.DataType().ID() != arrow.BOOL {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("filter_op: function must return boolean type, got %s", filterCol.DataType().Name()))
	}

	// Create builder for result DataFrame
	builder := NewDataFrameBuilder()
	defer builder.Release()

	// Process each chunk - extract boolean chunks with type safety
	newChunkSizes := make([]int64, 0, input.NumChunks())
	filterChunks := make([]*array.Boolean, input.NumChunks())
	for chunkIdx := range input.NumChunks() {
		boolChunk, ok := filterCol.Chunk(chunkIdx).(*array.Boolean)
		if !ok {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("filter_op: chunk %d is not a boolean array", chunkIdx))
		}
		filterChunks[chunkIdx] = boolChunk

		// Count true values
		trueCount := int64(0)
		for i := range boolChunk.Len() {
			if boolChunk.IsValid(i) && boolChunk.Value(i) {
				trueCount++
			}
		}
		newChunkSizes = append(newChunkSizes, trueCount)
	}

	builder.SetChunkSizes(newChunkSizes)

	// Use ChunkCollector for filtered chunks
	colNames := input.ColumnNames()
	collector := NewChunkCollector(colNames, input.NumChunks())
	defer collector.Release()

	// Filter each column
	for _, colName := range colNames {
		col := input.Column(colName)

		for chunkIdx := range input.NumChunks() {
			dataChunk := col.Chunk(chunkIdx)

			filtered, err := filterArray(ctx.Pool(), dataChunk, filterChunks[chunkIdx])
			if err != nil {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("filter_op: column %s: %v", colName, err))
			}
			collector.Set(colName, chunkIdx, filtered)
		}

		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(input, colName)
	}

	return builder.Build(), nil
}

func (o *FilterOp) String() string {
	return fmt.Sprintf("Filter(%s)", o.function.Name())
}

// filterArray filters an array based on a boolean mask.
func filterArray(pool memory.Allocator, data arrow.Array, mask *array.Boolean) (arrow.Array, error) {
	indices := make([]int, 0, mask.Len())
	for i := range mask.Len() {
		if mask.IsValid(i) && mask.Value(i) {
			indices = append(indices, i)
		}
	}
	return dispatchPickByIndices(pool, data, indices)
}

// NewFilterOpFromRepr creates a FilterOp from an OperatorRepr.
func NewFilterOpFromRepr(repr *OperatorRepr) (Operator, error) {
	if repr.Function == nil {
		return nil, merr.WrapErrParameterInvalidMsg("filter_op: function is required")
	}
	fn, err := FunctionFromRepr(repr.Function)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("filter function: %v", err))
	}
	if len(repr.Inputs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("filter_op: inputs is required")
	}
	return NewFilterOp(fn, repr.Inputs)
}
