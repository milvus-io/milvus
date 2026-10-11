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

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// FlatMapOp lowers to an outer-cardinality-preserving map followed by explode.
// List offsets identify the original parent of every output element.
type FlatMapOp struct {
	BaseOp
	mapping *MapOp
	explode *ExplodeOp
}

func init() {
	MustRegisterOperator(types.OpTypeFlatMap, NewFlatMapOpFromReprWithContext)
}

func NewFlatMapOp(function types.FunctionExpr, inputs []string, output string, options ExplodeOptions) (*FlatMapOp, error) {
	mapping, err := NewMapOp(function, inputs, []string{output})
	if err != nil {
		return nil, err
	}
	explode, err := NewExplodeOp(output, output, options)
	if err != nil {
		return nil, err
	}
	return &FlatMapOp{BaseOp: BaseOp{inputs: append([]string(nil), inputs...), outputs: explode.outputs}, mapping: mapping, explode: explode}, nil
}

func (o *FlatMapOp) Name() string                   { return "FlatMap" }
func (o *FlatMapOp) String() string                 { return fmt.Sprintf("FlatMap(%s)", o.mapping.function.Name()) }
func (o *FlatMapOp) Expression() types.FunctionExpr { return o.mapping.function }

func (o *FlatMapOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	return o.executeWithProjection(ctx, input, nil, SystemColumnPolicy{})
}

// executeWithProjection drops dead parent payloads after evaluating the map,
// before explode duplicates each remaining parent column. A nil keep set
// preserves the standalone operator's full-column behavior.
func (o *FlatMapOp) executeWithProjection(ctx *types.FuncContext, input *DataFrame, keep ColumnSet, policy SystemColumnPolicy) (*DataFrame, error) {
	if ctx.Stage() != types.StageIngestion && !(ctx.Stage() == types.StagePostProcess && ctx.TableResult()) {
		return nil, merr.WrapErrParameterInvalidMsg("flat_map cannot expand rows in stage %q", ctx.Stage())
	}
	mapped, err := o.mapping.Execute(ctx, input)
	if err != nil {
		return nil, err
	}
	defer mapped.Release()
	if keep != nil {
		needed := keep.Clone()
		needed.Add(o.explode.inputs[0])
		projected, err := PruneDataFrame(mapped, needed, policy)
		if err != nil {
			return nil, err
		}
		if projected != mapped {
			defer projected.Release()
		}
		mapped = projected
	}
	return o.explode.Execute(ctx, mapped)
}

func NewFlatMapOpFromReprWithContext(repr *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
	if repr.Function == nil || len(repr.Inputs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("flat_map requires an expression with input columns")
	}
	options, err := parseExplodeOptions(repr)
	if err != nil {
		return nil, err
	}
	function, err := FunctionFromReprWithContext(repr.Function, buildCtx)
	if err != nil {
		return nil, merr.Wrap(err, "flat_map function")
	}
	return NewFlatMapOp(function, repr.Inputs, repr.Outputs[0], options)
}

func (fc *FuncChain) FlatMap(function types.FunctionExpr, inputs []string, output string, options ...ExplodeOptions) *FuncChain {
	if len(options) > 1 {
		return fc.addWithError(nil, merr.WrapErrParameterInvalidMsg("flat_map accepts at most one options value"))
	}
	opts := ExplodeOptions{}
	if len(options) == 1 {
		opts = options[0]
	}
	op, err := NewFlatMapOp(function, inputs, output, opts)
	return fc.addWithError(op, err)
}
