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
	"sort"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func init() {
	MustRegisterOperator(types.OpTypeSort, NewSortOpFromRepr)
}

// SortOp sorts the DataFrame by a column.
// Note: Uses BaseOp.inputs[0] as the sort column name.
// Each chunk is sorted independently (per-query sorting for search results).
// When tieBreakCol is set, ties on the primary sort column are broken by that
// column in ascending order (e.g., sort by $score DESC, then $id ASC).
type SortOp struct {
	BaseOp
	desc        bool
	tieBreakCol string // optional: column name for tie-breaking (ascending)
}

// NewSortOp creates a new SortOp with the given column and sort direction.
func NewSortOp(column string, desc bool) *SortOp {
	return &SortOp{
		BaseOp: BaseOp{
			inputs:  []string{column},
			outputs: []string{}, // Sort doesn't produce new columns
		},
		desc: desc,
	}
}

// NewSortOpWithTieBreak creates a new SortOp that breaks ties using
// the given column in ascending order.
func NewSortOpWithTieBreak(column string, desc bool, tieBreakCol string) *SortOp {
	op := NewSortOp(column, desc)
	op.tieBreakCol = tieBreakCol
	return op
}

// Column returns the sort column name.
func (o *SortOp) Column() string {
	if len(o.inputs) > 0 {
		return o.inputs[0]
	}
	return ""
}

func (o *SortOp) Name() string { return "Sort" }

// Inputs and Outputs are inherited from BaseOp

func (o *SortOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	column := o.Column()
	sortCol := input.Column(column)
	if sortCol == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("sort_op: column %q not found", column))
	}

	// Validate sort column type is comparable
	if !isComparableType(sortCol.DataType()) {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("sort_op: column %s has non-comparable type %s", column, sortCol.DataType().Name()))
	}

	// Resolve optional tie-break column
	var tieBreakCol *arrow.Chunked
	if o.tieBreakCol != "" {
		tieBreakCol = input.Column(o.tieBreakCol)
		if tieBreakCol != nil && !isComparableType(tieBreakCol.DataType()) {
			tieBreakCol = nil // ignore non-comparable tie-break column
		}
	}

	colNames := input.ColumnNames()
	collector := NewChunkCollector(colNames, input.NumChunks())
	defer collector.Release()

	newChunkSizes := make([]int64, input.NumChunks())

	// Process each chunk independently
	for chunkIdx := range input.NumChunks() {
		sortChunk := sortCol.Chunk(chunkIdx)
		chunkLen := sortChunk.Len()

		// Resolve tie-break chunk for this chunk index
		var tbChunk arrow.Array
		if tieBreakCol != nil {
			tbChunk = tieBreakCol.Chunk(chunkIdx)
		}

		// Build sort indices
		indices := make([]int, chunkLen)
		for i := range chunkLen {
			indices[i] = i
		}

		// Sort indices based on values, with tie-breaking by ID ascending.
		// Nulls always sort to the end regardless of sort direction.
		sort.SliceStable(indices, func(i, j int) bool {
			vi := indices[i]
			vj := indices[j]

			iNull := sortChunk.IsNull(vi)
			jNull := sortChunk.IsNull(vj)
			if iNull && jNull {
				// Both null — use tie-break if available
				if tbChunk != nil {
					return compareArrayValues(tbChunk, vi, vj) < 0
				}
				return false
			}
			if iNull {
				return false // null always goes after non-null
			}
			if jNull {
				return true // non-null always goes before null
			}

			cmp := compareArrayValues(sortChunk, vi, vj)
			if cmp != 0 {
				if o.desc {
					return cmp > 0
				}
				return cmp < 0
			}
			// Tie-break: sort by tie-break column ascending
			if tbChunk != nil {
				return compareArrayValues(tbChunk, vi, vj) < 0
			}
			return false
		})

		newChunkSizes[chunkIdx] = int64(chunkLen)

		// Reorder each column
		for _, colName := range colNames {
			col := input.Column(colName)
			dataChunk := col.Chunk(chunkIdx)
			reordered, err := reorderArray(ctx.Pool(), dataChunk, indices)
			if err != nil {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("sort_op: column %s: %v", colName, err))
			}
			collector.Set(colName, chunkIdx, reordered)
		}
	}

	// Create new DataFrame with all chunks
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(newChunkSizes)

	for _, colName := range colNames {
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(input, colName)
	}

	return builder.Build(), nil
}

// isComparableType checks if an Arrow data type is comparable for sorting.
func isComparableType(dt arrow.DataType) bool {
	switch dt.ID() {
	// Note: LARGE_STRING is declared here for completeness but compareArrayValues
	// does not handle *array.LargeString yet. In practice Milvus VARCHAR fields
	// map to arrow.STRING, so LARGE_STRING columns are not expected.
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64,
		arrow.STRING, arrow.LARGE_STRING:
		return true
	default:
		return false
	}
}

func (o *SortOp) String() string {
	order := "ASC"
	if o.desc {
		order = "DESC"
	}
	if o.tieBreakCol != "" {
		return fmt.Sprintf("Sort(%s %s, %s ASC)", o.Column(), order, o.tieBreakCol)
	}
	return fmt.Sprintf("Sort(%s %s)", o.Column(), order)
}

// compareArrayValues compares two values in an array.
func compareArrayValues(arr arrow.Array, i, j int) int {
	// Handle nulls
	if arr.IsNull(i) && arr.IsNull(j) {
		return 0
	}
	if arr.IsNull(i) {
		return -1
	}
	if arr.IsNull(j) {
		return 1
	}

	switch a := arr.(type) {
	case *array.Int8:
		return compareTyped(a, i, j)
	case *array.Int16:
		return compareTyped(a, i, j)
	case *array.Int32:
		return compareTyped(a, i, j)
	case *array.Int64:
		return compareTyped(a, i, j)
	case *array.Uint8:
		return compareTyped(a, i, j)
	case *array.Uint16:
		return compareTyped(a, i, j)
	case *array.Uint32:
		return compareTyped(a, i, j)
	case *array.Uint64:
		return compareTyped(a, i, j)
	case *array.Float32:
		return compareTyped(a, i, j)
	case *array.Float64:
		return compareTyped(a, i, j)
	case *array.String:
		return compareTyped(a, i, j)
	default:
		return 0
	}
}

// reorderArray reorders an array based on indices.
func reorderArray(pool memory.Allocator, data arrow.Array, indices []int) (arrow.Array, error) {
	return dispatchPickByIndices(pool, data, indices)
}

// NewSortOpFromRepr creates a SortOp from an OperatorRepr.
func NewSortOpFromRepr(repr *OperatorRepr) (Operator, error) {
	column, ok := repr.Params["column"].(string)
	if !ok || column == "" {
		return nil, merr.WrapErrParameterInvalidMsg("sort_op: column is required")
	}
	desc := false
	if descVal, ok := repr.Params["desc"]; ok {
		if descBool, ok := descVal.(bool); ok {
			desc = descBool
		}
	}
	tieBreakCol := ""
	if tbVal, ok := repr.Params["tie_break_col"].(string); ok {
		tieBreakCol = tbVal
	}
	if tieBreakCol != "" {
		return NewSortOpWithTieBreak(column, desc, tieBreakCol), nil
	}
	return NewSortOp(column, desc), nil
}
