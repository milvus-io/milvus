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
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type ExplodeOptions = transform.ExplodeOptions

type ExplodeOp struct {
	BaseOp
	options ExplodeOptions
}

func init() {
	MustRegisterOperator(types.OpTypeExplode, statelessOperatorFactory(NewExplodeOpFromRepr))
}

func NewExplodeOp(input, output string, options ExplodeOptions) (*ExplodeOp, error) {
	op, err := transform.Explode(input, output, options)
	if err != nil {
		return nil, err
	}
	return &ExplodeOp{BaseOp: BaseOp{inputs: op.Inputs, outputs: op.Outputs}, options: options}, nil
}

func (o *ExplodeOp) Name() string { return "Explode" }
func (o *ExplodeOp) String() string {
	return fmt.Sprintf("Explode(%s -> %s)", o.inputs[0], o.outputs[0])
}

type listArray interface {
	arrow.Array
	ListValues() arrow.Array
	ValueOffsets(int) (int64, int64)
}

func (o *ExplodeOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	if ctx.Stage() != types.StageIngestion {
		return nil, merr.WrapErrParameterInvalidMsg("explode cannot expand rows in stage %q", ctx.Stage())
	}
	column, err := input.ResolveColumn(o.inputs[0], ctx.Pool())
	if err != nil {
		return nil, err
	}
	defer column.Release()
	if column.DataType().ID() != arrow.LIST && column.DataType().ID() != arrow.LARGE_LIST {
		return nil, merr.WrapErrFunctionFailedMsg("explode input %q must be List, got %s", o.inputs[0], column.DataType())
	}
	parentIndices := make([][]int, input.NumChunks())
	newSizes := make([]int64, input.NumChunks())
	colNames := make([]string, 0, input.NumColumns()+2)
	for _, name := range input.ColumnNames() {
		if name != o.outputs[0] && name != o.options.Position {
			colNames = append(colNames, name)
		}
	}
	colNames = append(colNames, o.outputs[0])
	if o.options.Position != "" {
		colNames = append(colNames, o.options.Position)
	}
	collector := NewChunkCollector(colNames, input.NumChunks())
	defer collector.Release()
	for group := range input.NumChunks() {
		if err := ctx.Context().Err(); err != nil {
			return nil, err
		}
		list, ok := column.Chunk(group).(listArray)
		if !ok || int64(list.Len()) != input.chunkSizes[group] {
			return nil, merr.WrapErrFunctionFailedMsg("explode list chunk %d does not match input shape", group)
		}
		parents := make([]int, 0)
		elements := make([]int, 0)
		position := array.NewInt64Builder(ctx.Pool())
		for row := range list.Len() {
			if row%1024 == 0 {
				if err := ctx.Context().Err(); err != nil {
					position.Release()
					return nil, err
				}
			}
			start, end := list.ValueOffsets(row)
			if start < 0 || end < start || end > int64(list.ListValues().Len()) {
				position.Release()
				return nil, merr.WrapErrFunctionFailedMsg("explode list offsets are malformed in chunk %d row %d", group, row)
			}
			if list.IsNull(row) || start == end {
				if o.options.Outer {
					parents = append(parents, row)
					elements = append(elements, -1)
					position.AppendNull()
				}
				continue
			}
			for element := start; element < end; element++ {
				parents = append(parents, row)
				elements = append(elements, int(element))
				position.Append(element - start)
			}
		}
		parentIndices[group] = parents
		newSizes[group] = int64(len(parents))
		values, err := takeArrayWithNulls(ctx.Pool(), list.ListValues(), elements)
		if err != nil {
			position.Release()
			return nil, err
		}
		collector.Set(o.outputs[0], group, values)
		if o.options.Position != "" {
			collector.Set(o.options.Position, group, position.NewArray())
		}
		position.Release()
		for _, name := range input.ColumnNames() {
			if name == o.outputs[0] || name == o.options.Position {
				continue
			}
			copied, err := dispatchPickByIndices(ctx.Pool(), input.Column(name).Chunk(group), parents)
			if err != nil {
				return nil, merr.Wrapf(err, "explode parent column %q", name)
			}
			collector.Set(name, group, copied)
		}
	}
	lineage, err := pickLineage(input, parentIndices)
	if err != nil {
		return nil, err
	}
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(newSizes).CopyAllMetadata(input).SetLineage(lineage)
	for _, name := range colNames {
		if err := builder.AddColumnFromChunks(name, collector.Consume(name)); err != nil {
			return nil, err
		}
		if name != o.outputs[0] && name != o.options.Position {
			builder.CopyFieldMetadata(input, name)
		} else {
			builder.SetFieldNullable(name, true)
		}
	}
	return builder.Build(), nil
}

func parseExplodeOptions(repr *OperatorRepr) (ExplodeOptions, error) {
	if len(repr.Outputs) < 1 || len(repr.Outputs) > 2 {
		return ExplodeOptions{}, merr.WrapErrParameterInvalidMsg("explode requires output and optional position output")
	}
	options := ExplodeOptions{}
	if len(repr.Outputs) == 2 {
		options.Position = repr.Outputs[1]
	}
	for key, value := range repr.Params {
		if key != "outer" || value == nil {
			return options, merr.WrapErrParameterInvalidMsg("explode: unknown or nil parameter %q", key)
		}
		boolean, ok := value.Value.(*schemapb.FunctionParamValue_BoolValue)
		if !ok || boolean == nil {
			return options, merr.WrapErrParameterInvalidMsg("explode: outer must be bool")
		}
		options.Outer = boolean.BoolValue
	}
	return options, nil
}

func NewExplodeOpFromRepr(repr *OperatorRepr) (Operator, error) {
	if repr.Function != nil || len(repr.Inputs) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("explode requires one input and no expression")
	}
	options, err := parseExplodeOptions(repr)
	if err != nil {
		return nil, err
	}
	return NewExplodeOp(repr.Inputs[0], repr.Outputs[0], options)
}

func (fc *FuncChain) Explode(input, output string, options ...ExplodeOptions) *FuncChain {
	if len(options) > 1 {
		return fc.addWithError(nil, merr.WrapErrParameterInvalidMsg("explode accepts at most one options value"))
	}
	opts := ExplodeOptions{}
	if len(options) == 1 {
		opts = options[0]
	}
	op, err := NewExplodeOp(input, output, opts)
	return fc.addWithError(op, err)
}
