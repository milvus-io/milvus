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
	"github.com/apache/arrow/go/v17/arrow/compute"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func init() {
	MustRegisterOperator(types.OpTypeMap, NewMapOpFromReprWithContext)
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
		return nil, merr.WrapErrServiceInternalMsg("map_op: output columns count (%d) must match function output types count (%d)",
			len(outputCols), len(outputTypes))
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

func (o *MapOp) Execute(ctx *types.FuncContext, input *DataFrame) (*OperatorResult, error) {
	if o.function == nil {
		return nil, merr.WrapErrServiceInternal("map_op: function is nil")
	}
	inputs, err := o.ReadInputColumns("map_op", input)
	if err != nil {
		return nil, err
	}
	outputs, err := o.function.Execute(ctx, inputs)
	if err != nil {
		releaseMapOutputs(outputs)
		return nil, err
	}
	if len(outputs) != len(o.outputs) {
		releaseMapOutputs(outputs)
		return nil, merr.WrapErrFunctionFailedMsg("map_op: function returned %d outputs, expected %d", len(outputs), len(o.outputs))
	}
	if err := normalizeMapScoreOutput(ctx, o.outputs, outputs); err != nil {
		releaseMapOutputs(outputs)
		return nil, err
	}
	result := &OperatorResult{Columns: make([]NamedColumn, len(outputs))}
	for i, output := range outputs {
		field := arrow.Field{Name: o.outputs[i], Nullable: true}
		if output != nil {
			field.Type = output.DataType()
		}
		result.Columns[i] = NamedColumn{Field: field, Data: output}
	}
	return result, nil
}

func normalizeMapScoreOutput(ctx *types.FuncContext, outputNames []string, outputs []*arrow.Chunked) error {
	for i, name := range outputNames {
		if name != types.ScoreFieldName {
			continue
		}
		if outputs[i] == nil {
			return merr.WrapErrFunctionFailedMsg("map_op: output %d mapped to %q is nil", i, name)
		}
		if outputs[i].DataType().ID() != arrow.FLOAT32 {
			if !isMapScoreConvertibleType(outputs[i].DataType().ID()) {
				return merr.WrapErrFunctionFailedMsg("map_op: output %d mapped to %q must be numeric, got %s",
					i, name, outputs[i].DataType())
			}

			converted, err := castMapScoreToFloat32(ctx, outputs[i])
			if err != nil {
				return merr.WrapErrFunctionFailed(err, "map_op: convert output %d mapped to %q from %s to Float32",
					i, name, outputs[i].DataType())
			}
			outputs[i].Release()
			outputs[i] = converted
		}
	}
	return nil
}

func isMapScoreConvertibleType(dataType arrow.Type) bool {
	switch dataType {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

func castMapScoreToFloat32(ctx *types.FuncContext, output *arrow.Chunked) (*arrow.Chunked, error) {
	computeCtx := compute.WithAllocator(ctx.Context(), ctx.Pool())
	options := compute.SafeCastOptions(arrow.PrimitiveTypes.Float32)
	options.AllowFloatTruncate = true
	chunks := make([]arrow.Array, 0, len(output.Chunks()))
	defer func() {
		for _, chunk := range chunks {
			chunk.Release()
		}
	}()
	for _, chunk := range output.Chunks() {
		converted, err := compute.CastArray(computeCtx, chunk, options)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, converted)
	}
	return arrow.NewChunked(arrow.PrimitiveTypes.Float32, chunks), nil
}

func releaseMapOutputs(outputs []*arrow.Chunked) {
	for _, output := range outputs {
		if output != nil {
			output.Release()
		}
	}
}

func (o *MapOp) String() string {
	if o.function != nil {
		return fmt.Sprintf("Map(%s)", o.function.Name())
	}
	return "Map(nil)"
}

// NewMapOpFromRepr creates a MapOp from an OperatorRepr.
func NewMapOpFromRepr(repr *OperatorRepr) (Operator, error) {
	return NewMapOpFromReprWithContext(repr, types.FunctionBuildContext{})
}

func NewMapOpFromReprWithContext(repr *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
	if repr.Function == nil {
		return nil, merr.WrapErrParameterInvalidMsg("map operator requires function")
	}
	fn, err := FunctionFromReprWithContext(repr.Function, buildCtx)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("map function: %v", err)
	}
	if len(repr.Inputs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("map operator requires inputs")
	}
	if len(repr.Outputs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("map operator requires outputs")
	}
	return NewMapOp(fn, repr.Inputs, repr.Outputs)
}
