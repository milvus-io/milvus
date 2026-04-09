// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queryutil

import (
	"container/heap"
	"context"
	"fmt"
	"sort"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// OrderByLimitOperator sorts a single RetrieveResult by ORDER BY fields
// and truncates to topK results using heap-based partial sort O(N log K).
//
// Field position modes:
//   - Auto-resolve (fieldPositions == nil at construction): positions are resolved
//     at Run time by matching FieldID in FieldsData. Used for plain PK-sort
//     where field layout is not predictable.
//   - Explicit (fieldPositions != nil): ORDER BY field i is at
//     FieldsData[fieldPositions[i]]. Used for GROUP BY + ORDER BY where
//     the layout is [group_cols, agg_cols].
type OrderByLimitOperator struct {
	orderByFields  []*orderby.OrderByField
	fieldPositions []int // nil = default i+1; non-nil = explicit positions
	topK           int64 // max results to keep (offset+limit); 0 = unlimited
}

// NewOrderByLimitOperator creates an operator that auto-resolves field positions
// at Run time by matching FieldID in FieldsData.
func NewOrderByLimitOperator(orderByFields []*orderby.OrderByField, topK int64) *OrderByLimitOperator {
	return &OrderByLimitOperator{
		orderByFields: orderByFields,
		topK:          topK,
	}
}

// NewOrderByLimitOperatorWithPositions creates an operator with explicit field positions.
// fieldPositions[i] specifies the FieldsData index for ORDER BY field i.
func NewOrderByLimitOperatorWithPositions(orderByFields []*orderby.OrderByField, fieldPositions []int, topK int64) *OrderByLimitOperator {
	return &OrderByLimitOperator{
		orderByFields:  orderByFields,
		fieldPositions: fieldPositions,
		topK:           topK,
	}
}

func (op *OrderByLimitOperator) Name() string {
	return OpOrderByLimit
}

// Run sorts a single RetrieveResult by ORDER BY fields and truncates to topK.
// Input[0]: *internalpb.RetrieveResults (single unsorted result)
// Output[0]: *internalpb.RetrieveResults (sorted + truncated result)
func (op *OrderByLimitOperator) Run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "OrderByLimitOperator")
	defer sp.End()

	result := inputs[0].(*internalpb.RetrieveResults)

	if result == nil || len(result.GetFieldsData()) == 0 {
		return []any{result}, nil
	}

	// Resolve field positions by FieldID if not explicitly set.
	// This handles plain query (OrderBy PK) where PK can be at any position.
	if op.fieldPositions == nil {
		op.fieldPositions = make([]int, len(op.orderByFields))
		for i, f := range op.orderByFields {
			op.fieldPositions[i] = -1
			for j, fd := range result.GetFieldsData() {
				if fd.GetFieldId() == f.FieldID {
					op.fieldPositions[i] = j
					break
				}
			}
			if op.fieldPositions[i] < 0 {
				return nil, fmt.Errorf("ORDER BY field '%s' (ID=%d) not found in result FieldsData", f.FieldName, f.FieldID)
			}
		}
	}

	rowCount := getRowCount(result)
	if rowCount <= 1 {
		return []any{result}, nil
	}

	k := int(op.topK)
	if k > 0 && k < rowCount {
		// Partial sort: use max-heap to select top-K indices, then sort them.
		// O(N log K) instead of O(N log N).
		indices := op.partialSort(result, rowCount, k)
		sorted := op.reorderResult(result, indices)
		return []any{sorted}, nil
	}

	// Full sort fallback: K >= N or no limit specified.
	// SliceStable preserves original order for equal elements.
	indices := make([]int, rowCount)
	for i := range indices {
		indices[i] = i
	}
	sort.SliceStable(indices, func(i, j int) bool {
		return op.compareRowsAt(result, indices[i], indices[j]) < 0
	})

	sorted := op.reorderResult(result, indices)
	return []any{sorted}, nil
}

// partialSort selects the top-K smallest row indices using a max-heap,
// then sorts them. Returns a sorted slice of K indices.
//
// Algorithm:
//  1. Build a max-heap of size K (worst-on-top).
//  2. Scan remaining rows: if a row is better than the heap top, replace it.
//  3. Extract all K indices from the heap and sort them.
//
// Complexity: O(N log K) for selection + O(K log K) for final sort.
func (op *OrderByLimitOperator) partialSort(result *internalpb.RetrieveResults, rowCount, k int) []int {
	h := &maxIndexHeap{
		indices: make([]int, 0, k),
		less: func(a, b int) bool {
			return op.compareRowsAt(result, a, b) < 0
		},
	}

	for i := 0; i < rowCount; i++ {
		if h.Len() < k {
			heap.Push(h, i)
		} else if op.compareRowsAt(result, i, h.indices[0]) < 0 {
			// Current row is better (smaller) than the worst in heap; replace.
			h.indices[0] = i
			heap.Fix(h, 0)
		}
	}

	indices := h.indices
	sort.SliceStable(indices, func(i, j int) bool {
		return op.compareRowsAt(result, indices[i], indices[j]) < 0
	})

	return indices
}

// maxIndexHeap is a max-heap of row indices. The "worst" element (largest
// in sort order) sits at the top, so it can be efficiently evicted when
// a better candidate is found during the scan.
type maxIndexHeap struct {
	indices []int
	less    func(a, b int) bool // returns true if row a < row b in sort order
}

func (h *maxIndexHeap) Len() int { return len(h.indices) }

// Less is inverted: heap top should be the max (worst) element.
func (h *maxIndexHeap) Less(i, j int) bool {
	return h.less(h.indices[j], h.indices[i])
}

func (h *maxIndexHeap) Swap(i, j int) {
	h.indices[i], h.indices[j] = h.indices[j], h.indices[i]
}

func (h *maxIndexHeap) Push(x any) {
	h.indices = append(h.indices, x.(int))
}

func (h *maxIndexHeap) Pop() any {
	old := h.indices
	n := len(old)
	x := old[n-1]
	h.indices = old[:n-1]
	return x
}

// compareRowsAt compares two rows at given indices.
// Uses fieldPositions if set, otherwise defaults to positional layout i+1.
func (op *OrderByLimitOperator) compareRowsAt(result *internalpb.RetrieveResults, idx1, idx2 int) int {
	fieldsData := result.GetFieldsData()
	for i, field := range op.orderByFields {
		fd := fieldsData[op.fieldPositions[i]]
		cmp := op.compareFieldValuesAt(fd, idx1, idx2, field)
		if cmp != 0 {
			return cmp
		}
	}

	// Tie-breaker: use PK to guarantee deterministic order across requests.
	// Without this, paginated queries (offset/limit) on fields with duplicate
	// values would produce inconsistent pages because segment merge order
	// varies between requests.
	if ids := result.GetIds(); ids != nil {
		pk1 := typeutil.GetPK(ids, int64(idx1))
		pk2 := typeutil.GetPK(ids, int64(idx2))
		return comparePK(pk1, pk2)
	}

	return 0
}

// compareFieldValuesAt compares two values in the same field at different indices.
func (op *OrderByLimitOperator) compareFieldValuesAt(fd *schemapb.FieldData, idx1, idx2 int, field *orderby.OrderByField) int {
	val1, null1 := getFieldValue(fd, idx1)
	val2, null2 := getFieldValue(fd, idx2)

	// Handle nulls
	if null1 && null2 {
		return 0
	}
	if null1 {
		if field.NullsFirst {
			return -1
		}
		return 1
	}
	if null2 {
		if field.NullsFirst {
			return 1
		}
		return -1
	}

	// Compare actual values
	cmp := compareValues(val1, val2, field.DataType)

	// Apply ascending/descending
	if !field.Ascending {
		cmp = -cmp
	}

	return cmp
}

// reorderResult reorders result rows by given indices.
func (op *OrderByLimitOperator) reorderResult(result *internalpb.RetrieveResults, indices []int) *internalpb.RetrieveResults {
	newResult := &internalpb.RetrieveResults{
		FieldsData: make([]*schemapb.FieldData, len(result.GetFieldsData())),
	}

	if result.GetIds() != nil {
		newResult.Ids = sliceIDs(result.GetIds(), indices)
	}

	for i, fd := range result.GetFieldsData() {
		newResult.FieldsData[i] = sliceFieldData(fd, indices)
	}

	// Propagate element-level metadata (defensive: ORDER BY and element-level
	// are currently mutually exclusive, but this prevents silent data loss
	// if the two features overlap in the future).
	newResult.ElementLevel = result.GetElementLevel()
	if len(result.GetElementIndices()) > 0 {
		newElemIndices := make([]*internalpb.ElementIndices, len(indices))
		for i, idx := range indices {
			newElemIndices[i] = result.GetElementIndices()[idx]
		}
		newResult.ElementIndices = newElemIndices
	}

	return newResult
}
