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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func init() {
	MustRegisterOperator(types.OpTypeLimit, statelessOperatorFactory(NewLimitOpFromRepr))
}

// LimitOp limits the number of rows in each chunk.
// Note: Limit is applied independently to each chunk (per-query limiting for search results).
type LimitOp struct {
	BaseOp
	limit  int64
	offset int64
}

// NewLimitOp creates a new LimitOp with the given limit and offset.
func NewLimitOp(limit, offset int64) *LimitOp {
	return &LimitOp{
		BaseOp: BaseOp{
			inputs:  []string{}, // Limit works on all columns
			outputs: []string{}, // Limit doesn't produce new columns
		},
		limit:  limit,
		offset: offset,
	}
}

func (o *LimitOp) Name() string { return "Limit" }

// Inputs and Outputs are inherited from BaseOp

func (o *LimitOp) Execute(ctx *types.FuncContext, input *DataFrame) (*OperatorResult, error) {
	if o.offset < 0 || o.limit < 0 {
		return nil, merr.WrapErrServiceInternalMsg("negative internal limit or offset")
	}
	selection := &RowSelection{Chunks: make([]ChunkSelection, input.NumChunks())}
	for i, size := range input.chunkSizes {
		start := min(o.offset, size)
		end := start + min(o.limit, size-start)
		selection.Chunks[i] = RowRange(start, end)
	}
	return &OperatorResult{Selection: selection}, nil
}

func (o *LimitOp) String() string {
	if o.offset > 0 {
		return fmt.Sprintf("Limit(%d, offset=%d)", o.limit, o.offset)
	}
	return fmt.Sprintf("Limit(%d)", o.limit)
}

// NewLimitOpFromRepr creates a LimitOp from an OperatorRepr.
func NewLimitOpFromRepr(repr *OperatorRepr) (Operator, error) {
	reader := types.NewParamReader("limit_op", repr.Params)
	limit, err := reader.Int64("limit", true, 0)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("limit_op: limit must be positive")
	}
	offset, err := reader.Int64("offset", false, 0)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, merr.WrapErrParameterInvalidMsg("limit_op: offset must be non-negative")
	}
	return NewLimitOp(limit, offset), nil
}
