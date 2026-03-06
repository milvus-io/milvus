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

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func init() {
	MustRegisterOperator(types.OpTypeMap, NewMapOpFromRepr)
}

// MapOp applies a function to specified columns of the DataFrame.
// Column mapping is handled at the Operator layer, not the Function layer.
type MapOp struct {
	BaseOp
	function types.FunctionExpr // The function to apply
}

// NewMapOp creates a new MapOp with explicit column mappings.
func NewMapOp(function types.FunctionExpr, inputCols, outputCols []string) (*MapOp, error) {
	if function == nil {
		return nil, merr.WrapErrServiceInternal("map_op: function is nil")
	}

	// Validate: outputCols length must match function.OutputDataTypes() length
	// Skip validation if OutputDataTypes() returns nil (dynamic output types)
	outputTypes := function.OutputDataTypes()
	if outputTypes != nil && len(outputCols) != len(outputTypes) {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("map_op: output columns count (%d) must match function output types count (%d)",
			len(outputCols), len(outputTypes)))
	}

	return &MapOp{
		BaseOp: BaseOp{
			inputs:  inputCols,
			outputs: outputCols,
		},
		function: function,
	}, nil
}

func (o *MapOp) Name() string { return "Map" }

// Inputs and Outputs are inherited from BaseOp

func (o *MapOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	if o.function == nil {
		return nil, merr.WrapErrServiceInternal("map_op: function is nil")
	}

	// 1. Read input columns from DataFrame using inputs
	inputs := make([]*arrow.Chunked, len(o.inputs))
	for i, name := range o.inputs {
		col := input.Column(name)
		if col == nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("map_op: column %q not found", name))
		}
		inputs[i] = col
	}

	// 2. Call FunctionExpr to process columns
	outputs, err := o.function.Execute(ctx, inputs)
	if err != nil {
		return nil, err
	}

	// 3. Validate output count matches expected output columns
	// This is especially important for dynamic output types where validation
	// was skipped at creation time
	if len(outputs) != len(o.outputs) {
		// Release outputs before returning error
		for _, out := range outputs {
			if out != nil {
				out.Release()
			}
		}
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("map_op: function returned %d outputs, expected %d",
			len(outputs), len(o.outputs)))
	}

	// 4. Create builder
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(input.chunkSizes)

	// Build set of output column names (these will replace any existing columns with the same name)
	outputColSet := make(map[string]struct{})
	for _, name := range o.outputs {
		outputColSet[name] = struct{}{}
	}

	// 5. Copy input columns that are not in output columns
	for _, colName := range input.ColumnNames() {
		if _, isOutput := outputColSet[colName]; isOutput {
			continue // Skip, will be replaced by output
		}
		if err := builder.AddColumnFrom(input, colName); err != nil {
			// Release outputs since they haven't been added to builder yet
			for _, out := range outputs {
				if out != nil {
					out.Release()
				}
			}
			return nil, err
		}
	}

	// 6. Add all output columns at once
	if err := builder.AddColumns(o.outputs, outputs); err != nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("map_op: %v", err))
	}

	return builder.Build(), nil
}

func (o *MapOp) String() string {
	if o.function != nil {
		return fmt.Sprintf("Map(%s)", o.function.Name())
	}
	return "Map(nil)"
}

// NewMapOpFromRepr creates a MapOp from an OperatorRepr.
func NewMapOpFromRepr(repr *OperatorRepr) (Operator, error) {
	if repr.Function == nil {
		return nil, merr.WrapErrParameterInvalidMsg("map operator requires function")
	}
	fn, err := FunctionFromRepr(repr.Function)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("map function: %v", err))
	}
	if len(repr.Inputs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("map operator requires inputs")
	}
	if len(repr.Outputs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("map operator requires outputs")
	}
	return NewMapOp(fn, repr.Inputs, repr.Outputs)
}
