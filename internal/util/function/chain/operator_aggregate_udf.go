// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chain

import (
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Custom aggregation uses the same public operator as native specifications.
// Its UDF returns one row per key group; native code owns keys and lineage.
func newUDFAggregateOpFromRepr(repr *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
	if repr == nil || repr.Function == nil {
		return nil, merr.WrapErrParameterInvalidMsg("custom aggregate requires an expression")
	}
	for key := range repr.Params {
		if key != "keys" {
			return nil, merr.WrapErrParameterInvalidMsg("custom aggregate: unsupported parameter %q", key)
		}
	}
	keys, err := types.NewParamReader("aggregate", repr.Params).StringSlice("keys", false)
	if err != nil {
		return nil, err
	}
	if len(repr.Inputs) <= len(keys) || len(repr.Outputs) <= len(keys) {
		return nil, merr.WrapErrParameterInvalidMsg("custom aggregate requires expression inputs and output aliases after group keys")
	}
	seen := make(map[string]bool)
	for i, key := range keys {
		if strings.TrimSpace(key) == "" || seen[key] || repr.Inputs[i] != key {
			return nil, merr.WrapErrParameterInvalidMsg("aggregate keys must be unique and align with input columns")
		}
		seen[key] = true
	}
	seen = make(map[string]bool)
	for i, name := range repr.Outputs {
		if strings.TrimSpace(name) == "" || seen[name] {
			return nil, merr.WrapErrParameterInvalidMsg("aggregate outputs must be non-empty and unique")
		}
		if strings.HasPrefix(name, "$") && (i >= len(keys) || name != keys[i]) {
			return nil, merr.WrapErrParameterInvalidMsg("aggregate cannot write reserved output %q", name)
		}
		seen[name] = true
	}
	function, err := FunctionFromReprWithContext(repr.Function, buildCtx)
	if err != nil {
		return nil, err
	}
	capability, ok := function.(types.ReductionExpr)
	if !ok || !capability.SupportsReduction() {
		return nil, merr.WrapErrParameterInvalidMsg("function %q does not support reduction", function.Name())
	}
	return &AggregateOp{BaseOp: BaseOp{inputs: append([]string(nil), repr.Inputs...), outputs: append([]string(nil), repr.Outputs...)}, keys: keys, function: function}, nil
}

func (o *AggregateOp) executeUDF(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	groups, err := groupPostTable(ctx, input, o.keys)
	if err != nil {
		return nil, err
	}
	flat := make([]postTableRowGroup, 0)
	sizes := make([]int64, len(groups))
	lineage := make([][]transform.RowLineage, len(groups))
	for chunk, values := range groups {
		sizes[chunk] = int64(len(values))
		lineage[chunk] = make([]transform.RowLineage, len(values))
		for i, group := range values {
			lineage[chunk][i], err = postTableGroupLineage(input, group, ctx)
			if err != nil {
				return nil, err
			}
			flat = append(flat, group)
		}
	}
	// A dynamic UDF determines output types at execution. If there are no key
	// groups, invoke it once with typed empty inputs, then retain only its schema.
	// There is no retry or extra invocation when real groups exist.
	if len(flat) == 0 {
		if input.NumChunks() == 0 {
			return nil, merr.WrapErrServiceInternal("aggregate requires at least one typed input group")
		}
		flat = append(flat, postTableRowGroup{Chunk: 0, FirstRow: -1})
	}
	columns, err := o.readResolvedColumns("aggregate", input, ctx.Pool())
	if err != nil {
		return nil, err
	}
	defer releaseColumns(columns)
	inputSchema, err := functionInputSchema(input, o.inputs[len(o.keys):])
	if err != nil {
		return nil, err
	}
	for index, column := range columns {
		if err := types.ValidateRuntimeArrowDataType(column.DataType()); err != nil {
			return nil, merr.Wrapf(err, "aggregate input %q", o.inputs[index])
		}
	}
	arguments := make([]*arrow.Chunked, 0, len(columns)-len(o.keys))
	defer func() { releaseColumns(arguments) }()
	for _, column := range columns[len(o.keys):] {
		chunks := make([]arrow.Array, 0, len(flat))
		for _, group := range flat {
			if err := ctx.Context().Err(); err != nil {
				for _, chunk := range chunks {
					chunk.Release()
				}
				return nil, err
			}
			chunk, err := takeArrayWithNulls(ctx.Pool(), column.Chunk(group.Chunk), group.Rows)
			if err != nil {
				for _, chunk := range chunks {
					chunk.Release()
				}
				return nil, err
			}
			chunks = append(chunks, chunk)
		}
		arguments = append(arguments, arrow.NewChunked(column.DataType(), chunks))
		for _, chunk := range chunks {
			chunk.Release()
		}
	}
	outputSchema, outputs, err := executeFunction(ctx.WithReduction(true), o.function, inputSchema, arguments)
	if err != nil {
		return nil, err
	}
	defer releaseColumns(outputs)
	if len(outputs) != len(o.outputs)-len(o.keys) {
		return nil, merr.WrapErrFunctionFailedMsg("aggregate returned %d columns, expected %d", len(outputs), len(o.outputs)-len(o.keys))
	}
	expected := make([]int64, len(flat))
	for i := range expected {
		expected[i] = 1
	}
	if err := validateFunctionOutputShape(outputs, expected); err != nil {
		return nil, err
	}
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(sizes).SetLineage(lineage)
	if _, aware := o.function.(types.SchemaAwareFunctionExpr); aware && outputSchema != nil {
		builder.SetArrowSchemaMetadata(outputSchema.Metadata())
	}
	for column, name := range o.outputs {
		chunks := make([]arrow.Array, 0, len(groups))
		position := 0
		for chunk, group := range groups {
			var values arrow.Array
			if column < len(o.keys) {
				indices := make([]int, len(group))
				for i, item := range group {
					indices[i] = item.FirstRow
				}
				values, err = takeArrayWithNulls(ctx.Pool(), columns[column].Chunk(chunk), indices)
			} else {
				output := outputs[column-len(o.keys)]
				if len(group) == 0 {
					values = array.MakeArrayOfNull(ctx.Pool(), output.DataType(), 0)
				} else {
					values, err = array.Concatenate(output.Chunks()[position:position+len(group)], ctx.Pool())
				}
			}
			if err != nil {
				for _, value := range chunks {
					value.Release()
				}
				return nil, merr.Wrap(err, "assemble aggregate outputs")
			}
			position += len(group)
			chunks = append(chunks, values)
		}
		if err := builder.AddColumnFromChunks(name, chunks); err != nil {
			return nil, err
		}
		var field arrow.Field
		if column < len(o.keys) {
			field, err = postTableSourceField(input, o.keys[column])
			field.Nullable = true
		} else {
			field = outputSchema.Field(column - len(o.keys))
		}
		if err != nil {
			return nil, err
		}
		if err := setFunctionOutputField(builder, name, field); err != nil {
			return nil, err
		}
	}
	return builder.Build(), nil
}
