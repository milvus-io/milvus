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

// ReduceOp invokes a UDF once with all key groups. The UDF returns one row
// per group; native code owns key projection and the many-source lineage.
type ReduceOp struct {
	BaseOp
	keys     []string
	function types.FunctionExpr
}

func init() { MustRegisterOperator("reduce", NewReduceOpFromReprWithContext) }

func (o *ReduceOp) Name() string                   { return "Reduce" }
func (o *ReduceOp) String() string                 { return "Reduce(" + o.function.Name() + ")" }
func (o *ReduceOp) Expression() types.FunctionExpr { return o.function }

func NewReduceOpFromReprWithContext(repr *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
	if repr.Function == nil {
		return nil, merr.WrapErrParameterInvalidMsg("reduce requires an expression")
	}
	for key := range repr.Params {
		if key != "keys" {
			return nil, merr.WrapErrParameterInvalidMsg("reduce: unsupported parameter %q", key)
		}
	}
	keys, err := types.NewParamReader("reduce", repr.Params).StringSlice("keys", false)
	if err != nil {
		return nil, err
	}
	if len(repr.Inputs) <= len(keys) || len(repr.Outputs) <= len(keys) {
		return nil, merr.WrapErrParameterInvalidMsg("reduce requires expression inputs and output aliases after group keys")
	}
	seen := make(map[string]bool)
	for i, key := range keys {
		if strings.TrimSpace(key) == "" || seen[key] || repr.Inputs[i] != key {
			return nil, merr.WrapErrParameterInvalidMsg("reduce keys must be unique and align with input columns")
		}
		seen[key] = true
	}
	seen = make(map[string]bool)
	for i, name := range repr.Outputs {
		if strings.TrimSpace(name) == "" || seen[name] {
			return nil, merr.WrapErrParameterInvalidMsg("reduce outputs must be non-empty and unique")
		}
		if strings.HasPrefix(name, "$") && (i >= len(keys) || name != keys[i]) {
			return nil, merr.WrapErrParameterInvalidMsg("reduce cannot write reserved output %q", name)
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
	return &ReduceOp{BaseOp: BaseOp{inputs: repr.Inputs, outputs: repr.Outputs}, keys: keys, function: function}, nil
}

func (o *ReduceOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	if !ctx.TableResult() || ctx.Stage() != types.StagePostProcess {
		return nil, merr.WrapErrParameterInvalidMsg("reduce requires POST TABLE")
	}
	if err := ctx.Context().Err(); err != nil {
		return nil, err
	}
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
			return nil, merr.WrapErrServiceInternal("reduce requires at least one typed input group")
		}
		flat = append(flat, postTableRowGroup{Chunk: 0, FirstRow: -1})
	}
	columns, err := o.readResolvedColumns("reduce", input, ctx.Pool())
	if err != nil {
		return nil, err
	}
	defer releaseColumns(columns)
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
	outputs, err := o.function.Execute(ctx.WithReduction(true), arguments)
	if err != nil {
		return nil, err
	}
	defer releaseColumns(outputs)
	if len(outputs) != len(o.outputs)-len(o.keys) {
		return nil, merr.WrapErrFunctionFailedMsg("reduce returned %d columns, expected %d", len(outputs), len(o.outputs)-len(o.keys))
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
				return nil, merr.Wrap(err, "assemble reduce outputs")
			}
			position += len(group)
			chunks = append(chunks, values)
		}
		if err := builder.AddColumnFromChunks(name, chunks); err != nil {
			return nil, err
		}
		builder.SetFieldNullable(name, true)
	}
	return builder.Build(), nil
}
