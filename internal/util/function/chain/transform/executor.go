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

package transform

import (
	"context"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Batch contains logical groups, independent of Arrow's physical chunking.
// An ingestion group represents one request batch; query adapters define their
// original query groups. All groups have the same Arrow schema and columns with
// exactly NumRows elements. Multiple record batches may back one Table, without
// introducing additional logical groups.
type Batch struct {
	Groups []arrow.Table
	// QueryContext is borrowed, immutable metadata, outside payload projection.
	QueryContext *types.QueryContext
	// OutputRowLimit is an adapter-owned expansion budget. Zero is unlimited.
	OutputRowLimit int64
}

// SourceRow references an original Executor input row, not an intermediate row.
type SourceRow struct {
	Group int
	Row   int64
}

// RowLineage lists the input rows that contributed to one output row. Expansion
// copies a singleton source; many sources can represent a future aggregation.
// It is not a user column and cannot be dropped or rewritten by select/map.
type RowLineage []SourceRow

// Result contains owned output tables and lineage[group][output row]. An
// executor must retain any input table reused as output; the caller releases
// each output table once. Lineage retains no Arrow resources.
type Result struct {
	Data    Batch
	Lineage [][]RowLineage
}

// Executor is the stage adapter boundary. The parent chain package implements it.
// Inputs are borrowed. On success the caller owns Result.Data; on error an
// implementation must release partial output. Executors must honor cancellation
// and preserve typed error codes. Adapters must validate plans and results and
// must not treat an unimplemented stage as successful pass-through execution.
type Executor interface {
	Execute(ctx context.Context, plan *schemapb.FunctionChain, input Batch) (Result, error)
}

// ValidateLineage checks result shapes and source bounds, including the
// multi-source representation used by aggregate. It does not grant
// an operation or stage permission to use that representation.
func ValidateLineage(input Batch, result Result) error {
	return validateLineage(input, result, false)
}

func validateLineage(input Batch, result Result, allowEmptySources bool) error {
	if err := validateBatch(input); err != nil {
		return merr.Wrap(err, "transform input")
	}
	if err := validateBatch(result.Data); err != nil {
		return merr.Wrap(err, "transform output")
	}
	if len(result.Lineage) != len(result.Data.Groups) {
		return merr.WrapErrFunctionFailedMsg("transform: lineage group count does not match output group count")
	}
	for group, table := range result.Data.Groups {
		if int64(len(result.Lineage[group])) != table.NumRows() {
			return merr.WrapErrFunctionFailedMsg("transform: lineage row count does not match output group %d", group)
		}
		for row, sources := range result.Lineage[group] {
			if len(sources) == 0 && !allowEmptySources {
				return merr.WrapErrFunctionFailedMsg("transform: output group %d row %d has no source", group, row)
			}
			for _, source := range sources {
				if source.Group < 0 || source.Group >= len(input.Groups) || source.Row < 0 || source.Row >= input.Groups[source.Group].NumRows() {
					return merr.WrapErrFunctionFailedMsg("transform: source (%d, %d) is out of bounds at output group %d row %d", source.Group, source.Row, group, row)
				}
			}
		}
	}
	return nil
}

// ValidateResultForPlan permits arbitrary table shape only when explicitly
// requested. Every source reference still belongs to the original input.
func ValidateResultForPlan(plan *schemapb.FunctionChain, input Batch, result Result) error {
	if err := ValidatePlan(plan); err != nil {
		return err
	}
	if IsTablePlan(plan) {
		return validateLineage(input, result, true)
	}
	if err := ValidateResult(plan.GetStage(), input, result); err != nil {
		return err
	}
	if input.OutputRowLimit < 0 {
		return merr.WrapErrServiceInternal("transform: output row limit is negative")
	}
	if input.OutputRowLimit > 0 {
		remaining := input.OutputRowLimit
		for _, table := range result.Data.Groups {
			if table.NumRows() > remaining {
				return merr.WrapErrFunctionFailedMsg("transform: output exceeds query row limit %d", input.OutputRowLimit)
			}
			remaining -= table.NumRows()
		}
	}
	return nil
}

// ValidateResult adds the v1 stage policy to ValidateLineage. All stages preserve
// logical group boundaries. Ingestion and PRE allow single-parent subset/expansion
// in parent row order; PostProcess HITS preserves each row in its position. Multiple
// contributing source rows are representable but not executable in v1 plans.
func ValidateResult(stage schemapb.FunctionChainStage, input Batch, result Result) error {
	if err := validateStage(stage); err != nil {
		return err
	}
	if err := ValidateLineage(input, result); err != nil {
		return err
	}
	if len(input.Groups) != len(result.Data.Groups) {
		return merr.WrapErrFunctionFailedMsg("transform: logical group count changed")
	}
	preserveRows := stage == schemapb.FunctionChainStage_FunctionChainStagePostProcess
	for group, table := range result.Data.Groups {
		if preserveRows && table.NumRows() != input.Groups[group].NumRows() {
			return merr.WrapErrFunctionFailedMsg("transform: row count changed in stage %s group %d", stage, group)
		}
		previousSource := int64(-1)
		for row, sources := range result.Lineage[group] {
			if len(sources) != 1 || sources[0].Group != group {
				return merr.WrapErrFunctionFailedMsg("transform: v1 output group %d row %d must have one source in the same group", group, row)
			}
			if preserveRows && sources[0].Row != int64(row) {
				return merr.WrapErrFunctionFailedMsg("transform: row identity/order changed in stage %s group %d row %d", stage, group, row)
			}
			if sources[0].Row < previousSource {
				return merr.WrapErrFunctionFailedMsg("transform: parent row order changed in group %d row %d", group, row)
			}
			previousSource = sources[0].Row
		}
	}
	return nil
}

func validateBatch(batch Batch) error {
	var schema *arrow.Schema
	for group, table := range batch.Groups {
		if isNilPointer(table) || table.Schema() == nil || table.NumRows() < 0 || table.NumCols() != int64(len(table.Schema().Fields())) {
			return merr.WrapErrServiceInternalMsg("transform: malformed Arrow table in group %d", group)
		}
		if schema != nil && !schema.Equal(table.Schema()) {
			return merr.WrapErrServiceInternalMsg("transform: inconsistent Arrow schema in group %d", group)
		}
		schema = table.Schema()
		for column := 0; column < int(table.NumCols()); column++ {
			data := table.Column(column)
			if data == nil || data.Data() == nil || int64(data.Len()) != table.NumRows() {
				return merr.WrapErrServiceInternalMsg("transform: column length does not match group %d row count", group)
			}
		}
	}
	return nil
}
