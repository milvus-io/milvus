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
// Use this for sorting aggregated results or single-source results.
type OrderOperator struct {
	orderByFields []*orderby.OrderByField
}

// NewOrderOperator creates a new order operator.
// orderByFields: the fields to sort by (in order of priority)
func NewOrderOperator(orderByFields []*orderby.OrderByField) *OrderOperator {
	return &OrderOperator{
		orderByFields: orderByFields,
	}
}

func (op *OrderOperator) Name() string {
	return OpOrder
}

// Run sorts a single RetrieveResult by ORDER BY fields.
// Input[0]: *internalpb.RetrieveResults (single unsorted result)
// Output[0]: *internalpb.RetrieveResults (sorted result)
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

	// Build field index map
	fieldIndexMap := make(map[int64]int)
	for i, fd := range result.GetFieldsData() {
		fieldIndexMap[fd.GetFieldId()] = i
	}

	// Create indices and sort them
	indices := make([]int, rowCount)
	for i := range indices {
		indices[i] = i
	}

	// Sort indices based on ORDER BY fields
	sort.Slice(indices, func(i, j int) bool {
		return op.compareRowsAt(result, indices[i], indices[j], fieldIndexMap) < 0
	})

	// Reorder result by sorted indices
	sorted := op.reorderResult(result, indices)
	return []any{sorted}, nil
}

// compareRowsAt compares two rows at given indices.
func (op *OrderOperator) compareRowsAt(result *internalpb.RetrieveResults, idx1, idx2 int, fieldIndexMap map[int64]int) int {
	for _, field := range op.orderByFields {
		fieldIdx, ok := fieldIndexMap[field.FieldID]
		if !ok {
			continue
		}

		fd := result.GetFieldsData()[fieldIdx]
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
