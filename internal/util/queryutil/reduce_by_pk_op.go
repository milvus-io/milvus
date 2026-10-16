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

	"github.com/apache/arrow/go/v17/arrow"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ReduceByPKOperator merges multiple RetrieveResults by PK with deduplication.
// It performs k-way merge by PK order. Duplicate PKs across shards indicate
// data corruption and cause an error.
//
// Used at proxy level where each shard/delegator has already deduplicated
// internally. Cross-shard PK overlap should not occur.
//
// reduceType controls iterator stop-on-drain behavior (ShouldStopWhenDrained).
// When a source is exhausted but has HasMoreResult=true, iterator queries
// (IReduceInOrder/IReduceInOrderForBest) stop early to maintain page boundaries.
type ReduceByPKOperator struct {
	reduceType reduce.IReduceType
	schema     *schemapb.CollectionSchema
}

// NewSortAndCheckPKOperator creates a reduce-by-PK operator for proxy level.
// It sorts by PK ASC and returns an error if duplicate PKs are detected
// across shards (indicating a data integrity issue).
// schema is used to determine per-field nullable flags for correct merge semantics.
// Pass nil to treat all fields as non-nullable (QN-side / schema-unaware callers).
func NewSortAndCheckPKOperator(reduceType reduce.IReduceType, schema *schemapb.CollectionSchema) *ReduceByPKOperator {
	return &ReduceByPKOperator{reduceType: reduceType, schema: schema}
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

// mergeByPK performs k-way merge by PK order with deduplication.
func (op *ReduceByPKOperator) mergeByPK(results []*internalpb.RetrieveResults, hasMoreResult bool) (*internalpb.RetrieveResults, error) {
	// Calculate total row count for capacity hint
	totalRows := 0
	for _, r := range results {
		totalRows += typeutil.GetSizeOfIDs(r.GetIds())
	}

	// Track seen PKs for deduplication
	seenPKs := make(map[any]struct{}, totalRows)

	// Collect selected rows with deduplication via k-way merge by PK order.
	var selectedRows []rowRef
	cursors := make([]int64, len(results))
	for {
		sel, drainOneResult := typeutil.SelectMinPK(results, cursors)
		if sel == -1 {
			break
		}
		if reduce.ShouldStopWhenDrained(op.reduceType) && drainOneResult {
			break
		}

		pk := typeutil.GetPK(results[sel].GetIds(), cursors[sel])
		if _, exists := seenPKs[pk]; !exists {
			seenPKs[pk] = struct{}{}
			selectedRows = append(selectedRows, rowRef{resultIdx: sel, rowIdx: cursors[sel]})
		} else {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate PK %v found across shards", pk)
		}
		cursors[sel]++
	}

	if len(selectedRows) == 0 {
		return &internalpb.RetrieveResults{HasMoreResult: hasMoreResult}, nil
	}

	// Build merged result
	merged, err := buildMergedRetrieveResults(results, selectedRows, op.schema)
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
//
// maxOutputSize guards against OOM when merging many segments: the merge loop
// estimates accumulated output size and stops with an error when the limit is
// exceeded.  Pass <= 0 to disable (e.g., in tests).
//
// limit: maximum rows (or elements for element-level queries) to keep.
// <= 0 means unlimited. For element-level queries, counting is by elements
// (sum of ElementIndices per row), not by rows.
type ReduceByPKWithTimestampOperator struct {
	reduceType    reduce.IReduceType
	maxOutputSize int64
	limit         int64
	schema        *schemapb.CollectionSchema

	// arrowRecords holds the per-result user output columns when the Arrow
	// transport was used; nil otherwise. It is indexed by the SAME position as
	// the operator's input results, so whoever builds both must filter them in
	// lockstep -- a mismatch silently misattributes every column.
	//
	// The operator neither owns nor reads these records: the reduce decides
	// which rows win from PKs and timestamps alone, which live in the protobuf
	// header. They are passed through into arrowOut so the caller can
	// materialize the chosen rows later, directly into FieldData.
	arrowRecords []arrow.Record

	// arrowOut receives the unmaterialized selection. Supplied by the caller
	// because the pipeline's channels carry the protobuf result only.
	arrowOut *ArrowSelection
}

// NewReduceByPKWithTimestampOperator creates an operator with timestamp-based deduplication.
// maxOutputSize: maximum allowed output size in bytes; <= 0 disables the check.
// limit: maximum rows/elements to keep; <= 0 means unlimited.
// schema is used to determine per-field nullable flags. Pass nil for QN-side callers.
func NewReduceByPKWithTimestampOperator(reduceType reduce.IReduceType, maxOutputSize int64, limit int64, schema *schemapb.CollectionSchema) *ReduceByPKWithTimestampOperator {
	return &ReduceByPKWithTimestampOperator{
		reduceType:    reduceType,
		maxOutputSize: maxOutputSize,
		limit:         limit,
		schema:        schema,
	}
}

// withArrowRecords supplies the per-result Arrow columns, positionally aligned
// with the operator's input results, and the struct that will receive the
// selection the reduce makes over them.
// See ReduceByPKWithTimestampOperator.arrowRecords.
func (op *ReduceByPKWithTimestampOperator) withArrowRecords(records []arrow.Record, out *ArrowSelection) *ReduceByPKWithTimestampOperator {
	op.arrowRecords = records
	op.arrowOut = out
	return op
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

	// Positional agreement is a precondition, not something to tolerate. A
	// short arrowRecords slice would leave later results with a nil record
	// while they are still selectable, and the failure mode of that is every
	// column after the gap attributed to the wrong field -- silently.
	if len(op.arrowRecords) != 0 && len(op.arrowRecords) != len(results) {
		return nil, merr.WrapErrServiceInternalMsg(
			"arrow records (%d) are not positionally aligned with results (%d)",
			len(op.arrowRecords), len(results))
	}

	// Filter and wrap results with timestamp extraction. The record travels ON
	// the wrapper, so a filtered-out result takes its record with it and the
	// two cannot desynchronize -- which matters because selectedRows addresses
	// rows by index into validResults and a mismatch misattributes every
	// column after it, with no error.
	validResults := make([]*timestampedResult, 0, len(results))
	hasMoreResult := false
	for i, r := range results {
		if r == nil || len(r.GetFieldsData()) == 0 || typeutil.GetSizeOfIDs(r.GetIds()) == 0 {
			continue
		}
		tr, err := newTimestampedResult(r)
		if err != nil {
			// If no timestamp field, skip timestamp handling
			tr = &timestampedResult{result: r, timestamps: nil}
		}
		if len(op.arrowRecords) != 0 {
			tr.record = op.arrowRecords[i]
		}
		validResults = append(validResults, tr)
		hasMoreResult = hasMoreResult || r.GetHasMoreResult()
	}

	if len(validResults) == 0 {
		return []any{&internalpb.RetrieveResults{}}, nil
	}

	// No single-result shortcut: even a single result may contain duplicate PKs
	// (e.g., same PK inserted multiple times into the same segment before compaction).
	// Must always run merge+dedup.

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
	rowSizeCalculators := make([]*rowSizeCalculator, len(results))
	for i, result := range results {
		// withArrowRecord is what keeps the maxOutputSize guard honest on the
		// Arrow path, where the user columns are in the record rather than in
		// FieldsData. See rowSizeCalculator.
		rowSizeCalculators[i] = newRowSizeCalculator(result.result).
			withArrowRecord(result.record)
	}

	// Track PK -> (selectedRowIndex, timestamp) for replacement on higher timestamp
	type pkEntry struct {
		rowIndex int // index in selectedRows
		ts       int64
	}
	pkTsMap := make(map[any]pkEntry)

	var retSize int64
	var selectedRows []rowRef
	var availableCount int64 // row count for doc-level, element count for element-level

	// Detect element-level from first result
	isElementLevel := len(results) > 0 && results[0].result.GetElementLevel()

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
		rowSize := rowSizeCalculators[sel].rowSize(cursors[sel])

		// Compute element count for this row
		var elemCount int64 = 1
		if isElementLevel {
			elemIndices := results[sel].result.GetElementIndices()
			if int(cursors[sel]) < len(elemIndices) {
				elemCount = int64(len(elemIndices[cursors[sel]].GetIndices()))
			}
		}

		if entry, exists := pkTsMap[pk]; !exists {
			// New PK - add it
			pkTsMap[pk] = pkEntry{rowIndex: len(selectedRows), ts: ts}
			selectedRows = append(selectedRows, rowRef{resultIdx: sel, rowIdx: cursors[sel]})
			retSize += rowSize
			availableCount += elemCount
		} else {
			// Duplicate PK - keep the one with higher timestamp
			if ts != 0 && ts > entry.ts {
				// Replace existing entry — swap row sizes and element counts
				oldRef := selectedRows[entry.rowIndex]
				oldSize := rowSizeCalculators[oldRef.resultIdx].rowSize(oldRef.rowIdx)
				retSize = retSize - oldSize + rowSize
				// Adjust element count for replacement
				if isElementLevel {
					oldElemIndices := results[oldRef.resultIdx].result.GetElementIndices()
					if int(oldRef.rowIdx) < len(oldElemIndices) {
						availableCount -= int64(len(oldElemIndices[oldRef.rowIdx].GetIndices()))
					}
					availableCount += elemCount
				}
				pkTsMap[pk] = pkEntry{rowIndex: entry.rowIndex, ts: ts}
				selectedRows[entry.rowIndex] = rowRef{resultIdx: sel, rowIdx: cursors[sel]}
			}
		}

		if op.maxOutputSize > 0 && retSize > op.maxOutputSize {
			return nil, merr.WrapErrParameterInvalidMsg("query results exceed the maxOutputSize Limit %d", op.maxOutputSize)
		}

		// Early termination when limit reached
		if op.limit > 0 && availableCount >= op.limit {
			break
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

	// buildMergedRetrieveResults merges whatever FieldsData the inputs carry. On
	// the Arrow path that is the SYSTEM columns only -- the export leaves them in
	// the protobuf header and puts user columns in the record -- so the two
	// halves are merged by the same selectedRows and reassembled by the reader
	// from milvus.field_order.
	merged, err := buildMergedRetrieveResults(origResults, selectedRows, op.schema)
	if err != nil {
		return nil, err
	}

	// Hand the selection out unmaterialized. Nothing between here and the
	// response assembly reads the user columns -- the Arrow-routed pipeline is
	// this operator and nothing else -- so gathering them into a merged record
	// here would only copy the whole payload into an intermediate that the
	// FieldData build then copies out of again.
	if op.arrowOut != nil {
		records := make([]arrow.Record, len(results))
		any := false
		for i, r := range results {
			records[i] = r.record
			any = any || r.record != nil
		}
		if any {
			op.arrowOut.Records = records
			op.arrowOut.Rows = selectedRows
		}
	}

	merged.HasMoreResult = hasMoreResult
	return merged, nil
}

// timestampedResult wraps a RetrieveResult with extracted timestamps
type timestampedResult struct {
	result     *internalpb.RetrieveResults
	timestamps []int64
	// record holds this result's user output columns when the Arrow transport
	// was used, nil otherwise.
	//
	// It lives HERE, on the wrapper that already pairs up everything about one
	// result, rather than in a parallel slice. A parallel slice has to be
	// filtered in lockstep with the results wherever either is filtered, and
	// the failure mode of getting that wrong is not an error -- it is every
	// column after the mismatch attributed to the wrong field.
	record arrow.Record
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
