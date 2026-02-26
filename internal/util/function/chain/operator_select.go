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
	MustRegisterOperator(types.OpTypeSelect, NewSelectOpFromRepr)
}

// SelectOp selects specific columns from the DataFrame.
// Note: Uses BaseOp.inputs as the column names to select.
// BaseOp.outputs is set to the same as inputs since selected columns are output.
type SelectOp struct {
	BaseOp
}

// NewSelectOp creates a new SelectOp with the given columns.
func NewSelectOp(columns []string) *SelectOp {
	return &SelectOp{
		BaseOp: BaseOp{
			inputs:  columns,
			outputs: columns, // Selected columns are the outputs
		},
	}
}

func (o *SelectOp) Name() string { return "Select" }

// Inputs and Outputs are inherited from BaseOp

func (o *SelectOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(input.chunkSizes)

	// Use ChunkCollector for selected chunks
	collector := NewChunkCollector(o.inputs, input.NumChunks())
	defer collector.Release()

	for _, colName := range o.inputs {
		col := input.Column(colName)
		if col == nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("select_op: column %q not found", colName))
		}

		// Copy chunks with retain
		for i, chunk := range col.Chunks() {
			chunk.Retain()
			collector.Set(colName, i, chunk)
		}

		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(input, colName)
	}

	return builder.Build(), nil
}

func (o *SelectOp) String() string {
	return fmt.Sprintf("Select(%v)", o.inputs)
}

// NewSelectOpFromRepr creates a SelectOp from an OperatorRepr.
func NewSelectOpFromRepr(repr *OperatorRepr) (Operator, error) {
	columnsInterface, ok := repr.Params["columns"]
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("select_op: columns is required")
	}
	columns, ok := columnsInterface.([]interface{})
	if !ok {
		// Try []string
		if colsStr, ok := columnsInterface.([]string); ok {
			return NewSelectOp(colsStr), nil
		}
		return nil, merr.WrapErrParameterInvalidMsg("select_op: columns must be a list")
	}
	if len(columns) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("select_op: columns is required")
	}
	colsStr := make([]string, len(columns))
	for i, col := range columns {
		if colStr, ok := col.(string); ok {
			colsStr[i] = colStr
		} else {
			return nil, merr.WrapErrParameterInvalidMsg("select_op: column[%d] must be a string", i)
		}
	}
	return NewSelectOp(colsStr), nil
}
