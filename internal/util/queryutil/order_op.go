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
	"sort"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// OrderOperator sorts a single RetrieveResult by ORDER BY fields.
// Unlike MergeOperator which merges multiple pre-sorted streams,
// this operator sorts a single unsorted result.
//
// When limit is set (> 0), uses heap-based partial sort O(N log K) where
// K = offset + limit, instead of full sort O(N log N). This is significantly
// faster when K << N.
type OrderOperator struct {
	orderByFields []*orderby.OrderByField
	limit         int64
	offset        int64
}

// NewOrderOperator creates a new order operator.
// orderByFields: the fields to sort by (in order of priority)
// limit: maximum number of results needed (-1 or 0 for unlimited, triggers full sort)
// offset: number of results to skip before limit
func NewOrderOperator(orderByFields []*orderby.OrderByField, limit, offset int64) *OrderOperator {
	return &OrderOperator{
		orderByFields: orderByFields,
		limit:         limit,
		offset:        offset,
	}
}

func (op *OrderOperator) Name() string {
	return OpOrderBy
}

// Run sorts a single RetrieveResult by ORDER BY fields.
// Input[0]: *internalpb.RetrieveResults (single unsorted result)
// Output[0]: *internalpb.RetrieveResults (sorted result)
//
// Segcore returns FieldsData in positional layout: [pk, orderby_1, ..., orderby_N, output...].
// ORDER BY field at index i corresponds to FieldsData position i+1 (position 0 is PK).
func (op *OrderOperator) Run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "OrderOperator")
	defer sp.End()

	result := inputs[0].(*internalpb.RetrieveResults)

	if result == nil || len(result.GetFieldsData()) == 0 {
		return []any{result}, nil
	}

	rowCount := getRowCount(result)
	if rowCount <= 1 {
		return []any{result}, nil
	}

	k := op.topK()
	if k > 0 && k < rowCount {
		// Partial sort: use max-heap to select top-K indices, then sort them.
		// O(N log K) instead of O(N log N).
		indices := op.partialSort(result, rowCount, k)
		sorted := op.reorderResult(result, indices)
		return []any{sorted}, nil
	}

	// Full sort fallback: K >= N or no limit specified.
	indices := make([]int, rowCount)
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return op.compareRowsAt(result, indices[i], indices[j]) < 0
	})

	sorted := op.reorderResult(result, indices)
	return []any{sorted}, nil
}

// topK returns the number of top results needed (offset + limit),
// or 0 if no limit is set.
func (op *OrderOperator) topK() int {
	if op.limit <= 0 {
		return 0
	}
	return int(op.offset + op.limit)
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
func (op *OrderOperator) partialSort(result *internalpb.RetrieveResults, rowCount, k int) []int {
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
	sort.Slice(indices, func(i, j int) bool {
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
// Uses positional layout: orderByFields[i] is at FieldsData position i+1.
func (op *OrderOperator) compareRowsAt(result *internalpb.RetrieveResults, idx1, idx2 int) int {
	fieldsData := result.GetFieldsData()
	for i, field := range op.orderByFields {
		// Positional contract: ORDER BY field i is at position i+1
		fieldPos := i + 1
		if fieldPos >= len(fieldsData) {
			continue
		}

		fd := fieldsData[fieldPos]
		cmp := op.compareFieldValuesAt(fd, idx1, idx2, field)
		if cmp != 0 {
			return cmp
		}
	}

	// Stable sort: if equal, preserve original order
	return idx1 - idx2
}

// compareFieldValuesAt compares two values in the same field at different indices.
func (op *OrderOperator) compareFieldValuesAt(fd *schemapb.FieldData, idx1, idx2 int, field *orderby.OrderByField) int {
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
func (op *OrderOperator) reorderResult(result *internalpb.RetrieveResults, indices []int) *internalpb.RetrieveResults {
	newResult := &internalpb.RetrieveResults{
		FieldsData: make([]*schemapb.FieldData, len(result.GetFieldsData())),
	}

	if result.GetIds() != nil {
		newResult.Ids = op.reorderIDs(result.GetIds(), indices)
	}

	for i, fd := range result.GetFieldsData() {
		newResult.FieldsData[i] = sliceFieldData(fd, indices)
	}

	return newResult
}

// reorderIDs reorders IDs by given indices.
func (op *OrderOperator) reorderIDs(ids *schemapb.IDs, indices []int) *schemapb.IDs {
	return sliceIDs(ids, indices)
}
