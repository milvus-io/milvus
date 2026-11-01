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

	validResults, hasMoreResult := filterPKResults(results)
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

// filterPKResults preserves the proxy reducer's definition of a usable input.
func filterPKResults(results []*internalpb.RetrieveResults) ([]*internalpb.RetrieveResults, bool) {
	validResults := make([]*internalpb.RetrieveResults, 0, len(results))
	hasMoreResult := false
	for _, r := range results {
		if r == nil || len(r.GetFieldsData()) == 0 || typeutil.GetSizeOfIDs(r.GetIds()) == 0 {
			continue
		}
		validResults = append(validResults, r)
		hasMoreResult = hasMoreResult || r.GetHasMoreResult()
	}
	return validResults, hasMoreResult
}

// mergeByPK performs k-way merge by PK order with deduplication.
func (op *ReduceByPKOperator) mergeByPK(results []*internalpb.RetrieveResults, hasMoreResult bool) (*internalpb.RetrieveResults, error) {
	selectedRows, err := op.selectRowsByPK(results)
	if err != nil {
		return nil, err
	}
	if len(selectedRows) == 0 {
		return &internalpb.RetrieveResults{HasMoreResult: hasMoreResult}, nil
	}

	merged, err := buildMergedRetrieveResults(results, selectedRows, op.schema)
	if err != nil {
		return nil, err
	}
	merged.HasMoreResult = hasMoreResult
	return merged, nil
}

// selectRowsByPK completes PK validation before any page is materialized.
// In particular, duplicate PKs outside the requested page must still fail.
func (op *ReduceByPKOperator) selectRowsByPK(results []*internalpb.RetrieveResults) ([]rowRef, error) {
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

	return selectedRows, nil
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
	selectedRows, err := op.selectRowsByPKWithTimestamp(results)
	if err != nil {
		return nil, err
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

// selectRowsByPKWithTimestamp picks the rows the merge keeps, in output order.
//
// Every input is normally sorted ascending by PK (segcore and the worker-side
// reduce both emit PK order), and then the k-way merge emits all copies of a
// PK back to back, so dedup is a comparison with the last emitted row rather
// than a map lookup. The fast path reads the PK and timestamp slices directly
// instead of boxing every candidate into `any`, and it checks sortedness as it
// consumes each row; the first out-of-order row abandons it for the map path,
// which is correct for any order. Until that row is consumed the emitted
// sequence is non-decreasing, so whatever the fast path decided before
// noticing is exactly what the map path decides -- but the restart is cheaper
// to reason about than a hand-over, and unsorted input is not a hot case.
func (op *ReduceByPKWithTimestampOperator) selectRowsByPKWithTimestamp(results []*timestampedResult) ([]rowRef, error) {
	rowSizeCalculators := make([]*rowSizeCalculator, len(results))
	for i, result := range results {
		// withArrowRecord is what keeps the maxOutputSize guard honest on the
		// Arrow path, where the user columns are in the record rather than in
		// FieldsData. See rowSizeCalculator.
		rowSizeCalculators[i] = newRowSizeCalculator(result.result).
			withArrowRecord(result.record)
	}

	var (
		rows []rowRef
		ok   bool
		err  error
	)
	if intPKs, strPKs := typedPKs(results); intPKs != nil {
		rows, ok, err = selectSortedByPK(op, results, intPKs, rowSizeCalculators)
	} else if strPKs != nil {
		rows, ok, err = selectSortedByPK(op, results, strPKs, rowSizeCalculators)
	}
	if ok {
		return rows, err
	}
	return op.selectRowsByPKWithTimestampMap(results, rowSizeCalculators)
}

// typedPKs returns every result's PK column as a typed slice when all of them
// share one PK type, and nil, nil otherwise (mixed or unknown ID kinds go to
// the map path).
func typedPKs(results []*timestampedResult) ([][]int64, [][]string) {
	if len(results) == 0 {
		return nil, nil
	}
	switch results[0].result.GetIds().GetIdField().(type) {
	case *schemapb.IDs_IntId:
		pks := make([][]int64, len(results))
		for i, r := range results {
			ids, ok := r.result.GetIds().GetIdField().(*schemapb.IDs_IntId)
			if !ok {
				return nil, nil
			}
			pks[i] = ids.IntId.GetData()
		}
		return pks, nil
	case *schemapb.IDs_StrId:
		pks := make([][]string, len(results))
		for i, r := range results {
			ids, ok := r.result.GetIds().GetIdField().(*schemapb.IDs_StrId)
			if !ok {
				return nil, nil
			}
			pks[i] = ids.StrId.GetData()
		}
		return nil, pks
	default:
		return nil, nil
	}
}

// selectSortedByPK is the fast path of selectRowsByPKWithTimestamp. It returns
// ok=false, with no rows and no error, as soon as it consumes a row whose PK is
// below its predecessor in the same input; the caller then reruns the map path
// from scratch. Everything else -- tie-breaking, replacement, the size guard,
// the limit, the drain stop and element-level counting -- is the map path's
// behavior, row for row; TestSelectRowsByPKWithTimestamp_Differential holds the
// two together.
func selectSortedByPK[K int64 | string](
	op *ReduceByPKWithTimestampOperator,
	results []*timestampedResult,
	pks [][]K,
	rowSizeCalculators []*rowSizeCalculator,
) ([]rowRef, bool, error) {
	n := len(results)
	cursors := make([]int, n)
	hasMore := make([]bool, n)
	elemIndices := make([][]*internalpb.ElementIndices, n)
	isElementLevel := results[0].result.GetElementLevel()
	stopWhenDrained := reduce.ShouldStopWhenDrained(op.reduceType)
	var drained bool
	for i, r := range results {
		hasMore[i] = r.result.GetHasMoreResult()
		drained = drained || (len(pks[i]) == 0 && hasMore[i])
		if isElementLevel {
			elemIndices[i] = r.result.GetElementIndices()
		}
	}
	// getTimestamp's contract: a missing or short timestamp column reads as 0.
	tsAt := func(i, row int) int64 {
		if ts := results[i].timestamps; row < len(ts) {
			return ts[row]
		}
		return 0
	}
	elemCountAt := func(i, row int) int64 {
		if !isElementLevel {
			return 1
		}
		if row < len(elemIndices[i]) {
			return int64(len(elemIndices[i][row].GetIndices()))
		}
		return 1
	}

	var (
		selectedRows   []rowRef
		retSize        int64
		availableCount int64
		lastPK         K
		lastTs         int64
	)
	for {
		// Same choice as selectMinPKWithTimestamp: the smallest PK, a strictly
		// newer timestamp on an equal PK, and otherwise the lowest input index.
		sel := -1
		var minPK K
		var maxTs int64
		for i := 0; i < n; i++ {
			c := cursors[i]
			if c >= len(pks[i]) {
				continue
			}
			pk := pks[i][c]
			if sel == -1 || pk < minPK {
				sel, minPK, maxTs = i, pk, tsAt(i, c)
			} else if pk == minPK {
				if ts := tsAt(i, c); ts > maxTs {
					sel, maxTs = i, ts
				}
			}
		}
		if sel == -1 {
			break
		}
		// drained mirrors selectMinPKWithTimestamp's drainResult: some input
		// with more results behind it has run out. Cursors only grow, so it is
		// tracked where a cursor reaches the end rather than recomputed.
		if stopWhenDrained && drained {
			break
		}

		c := cursors[sel]
		if c > 0 && minPK < pks[sel][c-1] {
			return nil, false, nil
		}
		ts := maxTs
		rowSize := rowSizeCalculators[sel].rowSize(int64(c))
		elemCount := elemCountAt(sel, c)

		if len(selectedRows) == 0 || minPK != lastPK {
			selectedRows = append(selectedRows, rowRef{resultIdx: sel, rowIdx: int64(c)})
			lastPK, lastTs = minPK, ts
			retSize += rowSize
			availableCount += elemCount
		} else if ts != 0 && ts > lastTs {
			// Duplicate PK: all copies are adjacent, so the entry to compare
			// with is the last one emitted.
			last := len(selectedRows) - 1
			oldRef := selectedRows[last]
			retSize = retSize - rowSizeCalculators[oldRef.resultIdx].rowSize(oldRef.rowIdx) + rowSize
			if isElementLevel {
				if old := int(oldRef.rowIdx); old < len(elemIndices[oldRef.resultIdx]) {
					availableCount -= int64(len(elemIndices[oldRef.resultIdx][old].GetIndices()))
				}
				availableCount += elemCount
			}
			selectedRows[last] = rowRef{resultIdx: sel, rowIdx: int64(c)}
			lastTs = ts
		}

		if op.maxOutputSize > 0 && retSize > op.maxOutputSize {
			return nil, true, merr.WrapErrParameterInvalidMsg("query results exceed the maxOutputSize Limit %d", op.maxOutputSize)
		}
		if op.limit > 0 && availableCount >= op.limit {
			break
		}

		cursors[sel]++
		if cursors[sel] >= len(pks[sel]) && hasMore[sel] {
			drained = true
		}
	}
	return selectedRows, true, nil
}

// selectRowsByPKWithTimestampMap is the general selection: it dedups through a
// map keyed by the boxed PK, so it is correct for inputs in any order. It is
// the fallback of selectRowsByPKWithTimestamp and the reference the fast path
// is differentially tested against.
func (op *ReduceByPKWithTimestampOperator) selectRowsByPKWithTimestampMap(results []*timestampedResult, rowSizeCalculators []*rowSizeCalculator) ([]rowRef, error) {
	cursors := make([]int64, len(results))

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

	return selectedRows, nil
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
