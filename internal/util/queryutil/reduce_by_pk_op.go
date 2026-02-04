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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ReduceByPKOperator merges multiple RetrieveResults by PK with deduplication.
// This is the default reduce operator for non-aggregation queries.
// It performs k-way merge by PK order and deduplicates repeated PKs.
// Mutually exclusive with ReduceByGroupsOperator.
//
// Key semantics (matching existing reducer in result.go):
//   - Uses SelectMinPK for k-way merge selection
//   - Handles drainResult: when a result is exhausted but has HasMoreResult=true,
//     stops early if reduceType requires it (IReduceInOrder or IReduceInOrderForBest)
//   - Deduplicates by PK: keeps only one entry per PK
type ReduceByPKOperator struct {
	reduceType reduce.IReduceType
}

// NewReduceByPKOperator creates a new reduce-by-PK operator.
// Uses IReduceNoOrder by default (no early stopping on drain).
func NewReduceByPKOperator() *ReduceByPKOperator {
	return &ReduceByPKOperator{
		reduceType: reduce.IReduceNoOrder,
	}
}

// NewReduceByPKOperatorWithType creates a reduce-by-PK operator with specific reduce type.
// Use IReduceInOrder or IReduceInOrderForBest for streaming scenarios where
// early stopping on drain is needed.
func NewReduceByPKOperatorWithType(reduceType reduce.IReduceType) *ReduceByPKOperator {
	return &ReduceByPKOperator{
		reduceType: reduceType,
	}
}

func (op *ReduceByPKOperator) Name() string {
	return OpReduceByPK
}

// Run merges multiple RetrieveResults into one by PK order.
// Input[0]: []*internalpb.RetrieveResults
// Output[0]: *internalpb.RetrieveResults
func (op *ReduceByPKOperator) Run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "ReduceByPKOperator")
	defer sp.End()

	results := inputs[0].([]*internalpb.RetrieveResults)

	// Filter out empty results and collect valid ones
	validResults := make([]*internalpb.RetrieveResults, 0, len(results))
	hasMoreResult := false
	for _, r := range results {
		if r == nil || len(r.GetFieldsData()) == 0 || typeutil.GetSizeOfIDs(r.GetIds()) == 0 {
			continue
		}
		validResults = append(validResults, r)
		hasMoreResult = hasMoreResult || r.GetHasMoreResult()
	}

	if len(validResults) == 0 {
		return []any{&internalpb.RetrieveResults{}}, nil
	}

	// If only one result, return as-is
	if len(validResults) == 1 {
		return []any{validResults[0]}, nil
	}

	// Merge multiple results by PK
	merged, err := op.mergeByPK(validResults, hasMoreResult)
	if err != nil {
		return nil, err
	}

	return []any{merged}, nil
}

// mergeByPK merges results by selecting minimum PK and handling duplicates.
// This matches the semantics of MergeInternalRetrieveResult in result.go.
func (op *ReduceByPKOperator) mergeByPK(results []*internalpb.RetrieveResults, hasMoreResult bool) (*internalpb.RetrieveResults, error) {
	// Initialize cursors for each result
	cursors := make([]int64, len(results))

	// Calculate total row count for capacity hint
	totalRows := 0
	for _, r := range results {
		totalRows += typeutil.GetSizeOfIDs(r.GetIds())
	}

	// Track seen PKs for deduplication
	seenPKs := make(map[any]struct{}, totalRows)

	// Collect selected rows in PK order with deduplication
	var selectedRows []rowRef

	for {
		// Use typeutil.SelectMinPK which properly handles drainResult
		sel, drainOneResult := typeutil.SelectMinPK(results, cursors)

		// Check stopping conditions
		if sel == -1 {
			break
		}
		if reduce.ShouldStopWhenDrained(op.reduceType) && drainOneResult {
			// One result drained but has more - stop early for ordered reduce
			break
		}

		// Get PK at current cursor position
		pk := typeutil.GetPK(results[sel].GetIds(), cursors[sel])

		// Deduplicate by PK
		if _, exists := seenPKs[pk]; !exists {
			seenPKs[pk] = struct{}{}
			selectedRows = append(selectedRows, rowRef{resultIdx: sel, rowIdx: cursors[sel]})
		}

		// Advance cursor
		cursors[sel]++
	}

	if len(selectedRows) == 0 {
		return &internalpb.RetrieveResults{HasMoreResult: hasMoreResult}, nil
	}

	// Build merged result
	merged, err := buildMergedRetrieveResults(results, selectedRows)
	if err != nil {
		return nil, err
	}

	// Propagate HasMoreResult flag
	merged.HasMoreResult = hasMoreResult

	return merged, nil
}

// ReduceByPKWithTimestampOperator merges results with timestamp-based deduplication.
// Use this at delegator/worker level where timestamp comparison is needed.
// When duplicate PKs are found, keeps the version with higher timestamp.
type ReduceByPKWithTimestampOperator struct {
	reduceType reduce.IReduceType
}

// NewReduceByPKWithTimestampOperator creates an operator with timestamp-based deduplication.
func NewReduceByPKWithTimestampOperator(reduceType reduce.IReduceType) *ReduceByPKWithTimestampOperator {
	return &ReduceByPKWithTimestampOperator{
		reduceType: reduceType,
	}
}

func (op *ReduceByPKWithTimestampOperator) Name() string {
	return OpReduceByPKTS
}

// Run merges multiple RetrieveResults with timestamp-based deduplication.
// Input[0]: []*internalpb.RetrieveResults (must contain timestamp field)
// Output[0]: *internalpb.RetrieveResults
func (op *ReduceByPKWithTimestampOperator) Run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "ReduceByPKWithTimestampOperator")
	defer sp.End()

	results := inputs[0].([]*internalpb.RetrieveResults)

	// Filter and wrap results with timestamp extraction
	validResults := make([]*timestampedResult, 0, len(results))
	hasMoreResult := false
	for _, r := range results {
		if r == nil || len(r.GetFieldsData()) == 0 || typeutil.GetSizeOfIDs(r.GetIds()) == 0 {
			continue
		}
		tr, err := newTimestampedResult(r)
		if err != nil {
			// If no timestamp field, skip timestamp handling
			validResults = append(validResults, &timestampedResult{result: r, timestamps: nil})
		} else {
			validResults = append(validResults, tr)
		}
		hasMoreResult = hasMoreResult || r.GetHasMoreResult()
	}

	if len(validResults) == 0 {
		return []any{&internalpb.RetrieveResults{}}, nil
	}

	if len(validResults) == 1 {
		return []any{validResults[0].result}, nil
	}

	merged, err := op.mergeByPKWithTimestamp(validResults, hasMoreResult)
	if err != nil {
		return nil, err
	}

	return []any{merged}, nil
}

// mergeByPKWithTimestamp merges with timestamp-based deduplication.
// When duplicate PK found with higher timestamp, replaces the previous entry.
func (op *ReduceByPKWithTimestampOperator) mergeByPKWithTimestamp(results []*timestampedResult, hasMoreResult bool) (*internalpb.RetrieveResults, error) {
	cursors := make([]int64, len(results))

	// Track PK -> (selectedRowIndex, timestamp) for replacement on higher timestamp
	type pkEntry struct {
		rowIndex int // index in selectedRows
		ts       int64
	}
	pkTsMap := make(map[any]pkEntry)

	var selectedRows []rowRef

	for {
		sel, drainOneResult := selectMinPKWithTimestamp(results, cursors)

		if sel == -1 {
			break
		}
		if reduce.ShouldStopWhenDrained(op.reduceType) && drainOneResult {
			break
		}

		pk := typeutil.GetPK(results[sel].result.GetIds(), cursors[sel])
		ts := results[sel].getTimestamp(cursors[sel])

		if entry, exists := pkTsMap[pk]; !exists {
			// New PK - add it
			pkTsMap[pk] = pkEntry{rowIndex: len(selectedRows), ts: ts}
			selectedRows = append(selectedRows, rowRef{resultIdx: sel, rowIdx: cursors[sel]})
		} else {
			// Duplicate PK - keep the one with higher timestamp
			if ts != 0 && ts > entry.ts {
				// Replace existing entry
				pkTsMap[pk] = pkEntry{rowIndex: entry.rowIndex, ts: ts}
				selectedRows[entry.rowIndex] = rowRef{resultIdx: sel, rowIdx: cursors[sel]}
			}
			// Otherwise keep existing (lower or equal timestamp)
		}

		cursors[sel]++
	}

	if len(selectedRows) == 0 {
		return &internalpb.RetrieveResults{HasMoreResult: hasMoreResult}, nil
	}

	// Build merged result from original results
	origResults := make([]*internalpb.RetrieveResults, len(results))
	for i, tr := range results {
		origResults[i] = tr.result
	}

	merged, err := buildMergedRetrieveResults(origResults, selectedRows)
	if err != nil {
		return nil, err
	}

	merged.HasMoreResult = hasMoreResult
	return merged, nil
}

// timestampedResult wraps a RetrieveResult with extracted timestamps
type timestampedResult struct {
	result     *internalpb.RetrieveResults
	timestamps []int64
}

func (r *timestampedResult) GetIds() *schemapb.IDs {
	return r.result.GetIds()
}

func (r *timestampedResult) GetHasMoreResult() bool {
	return r.result.GetHasMoreResult()
}

func (r *timestampedResult) getTimestamp(idx int64) int64 {
	if r.timestamps == nil || int(idx) >= len(r.timestamps) {
		return 0
	}
	return r.timestamps[idx]
}

// newTimestampedResult extracts timestamps from the result's field data
func newTimestampedResult(r *internalpb.RetrieveResults) (*timestampedResult, error) {
	const timestampFieldID int64 = 1 // common.TimeStampField

	for _, fd := range r.GetFieldsData() {
		if fd.GetFieldId() == timestampFieldID {
			timestamps := fd.GetScalars().GetLongData().GetData()
			return &timestampedResult{
				result:     r,
				timestamps: timestamps,
			}, nil
		}
	}

	// No timestamp field found
	return nil, errNoTimestampField
}

var errNoTimestampField = errorString("RetrieveResult does not have timestamp field")

type errorString string

func (e errorString) Error() string { return string(e) }

// selectMinPKWithTimestamp selects the result with minimum PK, preferring higher timestamp on ties.
// Returns (selectedIndex, drainResult)
func selectMinPKWithTimestamp(results []*timestampedResult, cursors []int64) (int, bool) {
	sel := -1
	drainResult := false
	var maxTimestamp int64 = 0
	var minIntPK int64 = 1<<63 - 1 // MaxInt64

	firstStr := true
	firstInt := true
	var minStrPK string

	for i, cursor := range cursors {
		size := typeutil.GetSizeOfIDs(results[i].result.GetIds())

		// Handle drain result
		if int(cursor) >= size && results[i].result.GetHasMoreResult() {
			drainResult = true
			continue
		}

		if int(cursor) >= size {
			continue
		}

		pkInterface := typeutil.GetPK(results[i].result.GetIds(), cursor)
		ts := results[i].getTimestamp(cursor)

		switch pk := pkInterface.(type) {
		case string:
			if firstStr || pk < minStrPK || (pk == minStrPK && ts > maxTimestamp) {
				firstStr = false
				minStrPK = pk
				sel = i
				maxTimestamp = ts
			}
		case int64:
			if firstInt || pk < minIntPK || (pk == minIntPK && ts > maxTimestamp) {
				firstInt = false
				minIntPK = pk
				sel = i
				maxTimestamp = ts
			}
		}
	}

	return sel, drainResult
}
