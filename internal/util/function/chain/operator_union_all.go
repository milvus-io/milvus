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
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// UnionAllOp appends logical groups in their existing order, without deduplication.
// Original source coordinates survive the change to one output group.
type UnionAllOp struct{ BaseOp }

func init() {
	MustRegisterOperator(types.OpTypeUnionAll, statelessOperatorFactory(NewUnionAllOpFromRepr))
}

func NewUnionAllOp() *UnionAllOp     { return &UnionAllOp{} }
func (o *UnionAllOp) Name() string   { return "UnionAll" }
func (o *UnionAllOp) String() string { return "UnionAll()" }

func (o *UnionAllOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	if err := requirePostTable(ctx, o.Name()); err != nil {
		return nil, err
	}
	if err := validateFrameShape(input); err != nil {
		return nil, err
	}
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes([]int64{input.NumRows()}).CopyAllMetadata(input)
	if input.lineage != nil {
		rows := make([]transform.RowLineage, 0, int(input.NumRows()))
		for _, group := range input.lineage {
			rows = append(rows, group...)
		}
		builder.SetLineage([][]transform.RowLineage{rows})
	}
	for _, name := range input.ColumnNames() {
		if err := ctx.Context().Err(); err != nil {
			return nil, err
		}
		column := input.Column(name)
		var data arrow.Array
		var err error
		if len(column.Chunks()) == 0 {
			data = array.MakeArrayOfNull(ctx.Pool(), column.DataType(), 0)
		} else {
			data, err = array.Concatenate(column.Chunks(), ctx.Pool())
		}
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "union_all: Arrow concatenation failed")
		}
		if err := builder.AddColumnFromChunks(name, []arrow.Array{data}); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(input, name)
	}
	return builder.Build(), nil
}

func NewUnionAllOpFromRepr(repr *OperatorRepr) (Operator, error) {
	if repr == nil || repr.Function != nil || len(repr.Inputs) != 0 || len(repr.Outputs) != 0 || len(repr.Params) != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("union_all does not accept inputs, outputs, expression or parameters")
	}
	return NewUnionAllOp(), nil
}

func (fc *FuncChain) UnionAll() *FuncChain { return fc.Add(NewUnionAllOp()) }

func requirePostTable(ctx *types.FuncContext, name string) error {
	if ctx == nil {
		return merr.WrapErrServiceInternal("POST table operator context is nil")
	}
	if ctx.Stage() != types.StagePostProcess || !ctx.TableResult() {
		return merr.WrapErrParameterInvalidMsg("%s requires POST TABLE mode", name)
	}
	return ctx.Context().Err()
}
