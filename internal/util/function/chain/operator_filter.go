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

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// FilterOp is temporarily unavailable in user-submitted function chains:
// Execute currently masks function error codes, including PyUDF errors.
// Keep registration disabled until error propagation is fixed and verified.
// func init() {
// 	MustRegisterOperator(types.OpTypeFilter, NewFilterOpFromReprWithContext)
// }

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
			return nil, merr.WrapErrServiceInternalMsg("filter_op: function must return exactly 1 output, got %d", len(outputTypes))
		}
		if outputTypes[0].ID() != arrow.BOOL {
			return nil, merr.WrapErrServiceInternalMsg("filter_op: function must return boolean type, got %s", outputTypes[0].Name())
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

func (o *FilterOp) Execute(ctx *types.FuncContext, input *DataFrame) (*OperatorResult, error) {
	if o.function == nil {
		return nil, merr.WrapErrServiceInternalMsg("filter_op: function is nil")
	}
	inputs, err := o.ReadInputColumns("filter_op", input)
	if err != nil {
		return nil, err
	}
	outputs, err := o.function.Execute(ctx, inputs)
	defer func() {
		for _, output := range outputs {
			if output != nil {
				output.Release()
			}
		}
	}()
	if err != nil {
		return nil, merr.Wrap(err, "filter_op: function execution failed")
	}
	if len(outputs) != 1 {
		return nil, merr.WrapErrServiceInternalMsg("filter_op: function must return exactly 1 output, got %d", len(outputs))
	}
	mask := outputs[0]
	if err := validateChunkedColumn(mask, input.chunkSizes); err != nil {
		return nil, merr.Wrap(err, "filter_op: mask")
	}
	if mask.DataType().ID() != arrow.BOOL {
		return nil, merr.WrapErrServiceInternalMsg("filter_op: function must return boolean type, got %s", mask.DataType().Name())
	}
	selection := &RowSelection{Chunks: make([]ChunkSelection, input.NumChunks())}
	for chunkIdx, chunk := range mask.Chunks() {
		values := chunk.(*array.Boolean)
		indices := make([]int, 0, values.Len())
		for row := 0; row < values.Len(); row++ {
			// SQL filter semantics: false and NULL both exclude the row.
			if values.IsValid(row) && values.Value(row) {
				indices = append(indices, row)
			}
		}
		selection.Chunks[chunkIdx] = RowIndices(indices)
	}
	return &OperatorResult{Selection: selection}, nil
}

func (o *FilterOp) String() string {
	return fmt.Sprintf("Filter(%s)", o.function.Name())
}

// NewFilterOpFromRepr creates a FilterOp from an OperatorRepr.
func NewFilterOpFromRepr(repr *OperatorRepr) (Operator, error) {
	return NewFilterOpFromReprWithContext(repr, types.FunctionBuildContext{})
}

func NewFilterOpFromReprWithContext(repr *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
	if repr.Function == nil {
		return nil, merr.WrapErrParameterMissingMsg("filter_op: function is required")
	}
	fn, err := FunctionFromReprWithContext(repr.Function, buildCtx)
	if err != nil {
		return nil, merr.Wrap(err, "filter function")
	}
	if len(repr.Inputs) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("filter_op: inputs is required")
	}
	return NewFilterOp(fn, repr.Inputs)
}
