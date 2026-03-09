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
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// DeduplicatePKOperator concatenates multiple RetrieveResults and deduplicates
// by PK using a hash set with timestamp-based replacement. Unlike ReduceByPK,
// it does NOT assume PK-sorted inputs and does NOT sort the output by PK.
// This preserves the original ordering from each source (e.g., ORDER BY field
// ordering from segcore).
//
// When duplicate PKs are found, keeps the version with the highest timestamp
// (same semantics as the old MergeInternalRetrieveResult). This handles the
// case where the same PK exists in both growing and sealed segments.
//
// Used at QN/Delegator level for ORDER BY queries where segcore returns results
// sorted by ORDER BY fields, not by PK.
//
// Input[0]: []*internalpb.RetrieveResults
// Output[0]: *internalpb.RetrieveResults (concatenated + deduplicated)
type DeduplicatePKOperator struct {
	maxOutputSize int64
}

func NewDeduplicatePKOperator(maxOutputSize int64) *DeduplicatePKOperator {
	return &DeduplicatePKOperator{maxOutputSize: maxOutputSize}
}

func (op *DeduplicatePKOperator) Name() string {
	return OpDeduplicatePK
}

func (op *DeduplicatePKOperator) Run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "DeduplicatePKOperator")
	defer sp.End()

	results := inputs[0].([]*internalpb.RetrieveResults)

	// Filter out empty results and extract timestamps
	type validEntry struct {
		result     *internalpb.RetrieveResults
		timestamps []int64 // nil if no timestamp field
	}
	validResults := make([]validEntry, 0, len(results))
	hasMoreResult := false
	for _, r := range results {
		if r == nil || len(r.GetFieldsData()) == 0 || typeutil.GetSizeOfIDs(r.GetIds()) == 0 {
			continue
		}
		ts := extractTimestamps(r)
		validResults = append(validResults, validEntry{result: r, timestamps: ts})
		hasMoreResult = hasMoreResult || r.GetHasMoreResult()
	}

	if len(validResults) == 0 {
		return []any{&internalpb.RetrieveResults{}}, nil
	}

	// No single-result shortcut: even a single result may contain duplicate PKs.
	// Must always run dedup.

	// Concatenate all results, dedup by PK hash set with timestamp replacement
	type pkEntry struct {
		rowIndex int   // index in selectedRows
		ts       int64 // timestamp of the selected row
	}
	pkMap := make(map[any]pkEntry)
	var selectedRows []rowRef
	var retSize int64

	origResults := make([]*internalpb.RetrieveResults, len(validResults))
	for i, v := range validResults {
		origResults[i] = v.result
	}

	for i, v := range validResults {
		idCount := typeutil.GetSizeOfIDs(v.result.GetIds())
		for j := int64(0); j < int64(idCount); j++ {
			pk := typeutil.GetPK(v.result.GetIds(), j)
			var ts int64
			if v.timestamps != nil && int(j) < len(v.timestamps) {
				ts = v.timestamps[j]
			}
			rowSize := calcRowSize(v.result, j)

			if entry, exists := pkMap[pk]; !exists {
				pkMap[pk] = pkEntry{rowIndex: len(selectedRows), ts: ts}
				selectedRows = append(selectedRows, rowRef{resultIdx: i, rowIdx: j})
				retSize += rowSize
			} else if ts != 0 && ts > entry.ts {
				// Duplicate PK with higher timestamp — replace, swap sizes
				oldRef := selectedRows[entry.rowIndex]
				oldSize := calcRowSize(origResults[oldRef.resultIdx], oldRef.rowIdx)
				retSize = retSize - oldSize + rowSize
				pkMap[pk] = pkEntry{rowIndex: entry.rowIndex, ts: ts}
				selectedRows[entry.rowIndex] = rowRef{resultIdx: i, rowIdx: j}
			}

			if op.maxOutputSize > 0 && retSize > op.maxOutputSize {
				return nil, fmt.Errorf("query results exceed the maxOutputSize Limit %d", op.maxOutputSize)
			}
		}
	}

	if len(selectedRows) == 0 {
		return []any{&internalpb.RetrieveResults{HasMoreResult: hasMoreResult}}, nil
	}

	merged, err := buildMergedRetrieveResults(origResults, selectedRows)
	if err != nil {
		return nil, err
	}
	merged.HasMoreResult = hasMoreResult

	return []any{merged}, nil
}

// extractTimestamps extracts the timestamp field (FieldID=1) from a RetrieveResult.
// Returns nil if no timestamp field is found.
func extractTimestamps(r *internalpb.RetrieveResults) []int64 {
	const timestampFieldID int64 = 1 // common.TimeStampField
	for _, fd := range r.GetFieldsData() {
		if fd.GetFieldId() == timestampFieldID {
			return fd.GetScalars().GetLongData().GetData()
		}
	}
	return nil
}

// ConcatAndCheckPKOperator concatenates multiple RetrieveResults into one.
// It checks for duplicate PKs across results and returns an error if found,
// indicating a data integrity issue (cross-shard PK duplication).
//
// Used at Proxy level for ORDER BY queries where each shard/delegator returns
// deduplicated results. Cross-shard PK overlap should not occur.
//
// Input[0]: []*internalpb.RetrieveResults
// Output[0]: *internalpb.RetrieveResults (concatenated)
type ConcatAndCheckPKOperator struct{}

func NewConcatAndCheckPKOperator() *ConcatAndCheckPKOperator {
	return &ConcatAndCheckPKOperator{}
}

func (op *ConcatAndCheckPKOperator) Name() string {
	return OpConcatAndCheckPK
}

func (op *ConcatAndCheckPKOperator) Run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "ConcatAndCheckPKOperator")
	defer sp.End()

	results := inputs[0].([]*internalpb.RetrieveResults)

	// Filter out empty results, collecting HasMoreResult from all inputs including empty ones.
	validResults := make([]*internalpb.RetrieveResults, 0, len(results))
	hasMoreResult := false
	for _, r := range results {
		if r != nil {
			hasMoreResult = hasMoreResult || r.GetHasMoreResult()
		}
		if r == nil || len(r.GetFieldsData()) == 0 || typeutil.GetSizeOfIDs(r.GetIds()) == 0 {
			continue
		}
		validResults = append(validResults, r)
	}

	if len(validResults) == 0 {
		return []any{&internalpb.RetrieveResults{HasMoreResult: hasMoreResult}}, nil
	}

	if len(validResults) == 1 {
		// Single result already carries its own HasMoreResult; OR in any flag from
		// empty results that were filtered out above.
		r := validResults[0]
		r.HasMoreResult = r.GetHasMoreResult() || hasMoreResult
		return []any{r}, nil
	}

	// Concatenate all results, fail on duplicate PKs
	seenPKs := make(map[any]struct{})
	var selectedRows []rowRef

	for i, r := range validResults {
		idCount := typeutil.GetSizeOfIDs(r.GetIds())
		for j := int64(0); j < int64(idCount); j++ {
			pk := typeutil.GetPK(r.GetIds(), j)
			if _, exists := seenPKs[pk]; exists {
				return nil, fmt.Errorf("duplicate PK %v found across shards, possible data integrity issue", pk)
			}
			seenPKs[pk] = struct{}{}
			selectedRows = append(selectedRows, rowRef{resultIdx: i, rowIdx: j})
		}
	}

	merged, err := buildMergedRetrieveResults(validResults, selectedRows)
	if err != nil {
		return nil, err
	}
	merged.HasMoreResult = hasMoreResult

	return []any{merged}, nil
}
