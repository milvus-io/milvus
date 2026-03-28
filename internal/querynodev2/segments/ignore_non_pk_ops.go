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

package segments

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// OffsetSelection tracks a row's origin segment and offset for later retrieval.
type OffsetSelection struct {
	SegmentIndex   int                       // index into validSegments array
	Offset         int64                     // segcore offset for RetrieveByOffsets
	ElementIndices *segcorepb.ElementIndices // element indices for element-level query (nil for doc-level)
}

// MergedResultWithOffsets carries PK merge results + offset mappings
// for the FetchFieldsData operator to retrieve full field data.
type MergedResultWithOffsets struct {
	IDs          *schemapb.IDs
	Selections   []OffsetSelection
	ElementLevel bool // true if results are element-level
}

// NewMergeByPKWithOffsetsOperator creates an operator that performs PK-ordered
// merge with timestamp-based deduplication and topK limit, tracking segment
// offsets for later field data retrieval.
//
// For element-level queries, the operator also tracks ElementIndices per row
// and counts available results by element count (not doc count).
//
// Input[0]: []*segcorepb.RetrieveResults (valid results with offsets and timestamps)
// Output[0]: *MergedResultWithOffsets (PKs + offset selections, PK-sorted)
func NewMergeByPKWithOffsetsOperator(
	topK int64,
	reduceType reduce.IReduceType,
) queryutil.Operator {
	return queryutil.NewLambdaOperator(queryutil.OpMergeByPKOffsets, func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		results := inputs[0].([]*segcorepb.RetrieveResults)

		// Wrap results with timestamps for SelectMinPKWithTimestamp
		validResults := make([]*TimestampedRetrieveResult[*segcorepb.RetrieveResults], 0, len(results))
		for _, r := range results {
			tr, err := NewTimestampedRetrieveResult(r)
			if err != nil {
				return nil, fmt.Errorf("failed to create timestamped result: %w", err)
			}
			validResults = append(validResults, tr)
		}

		if len(validResults) == 0 {
			return []any{&MergedResultWithOffsets{IDs: &schemapb.IDs{}}}, nil
		}

		// Detect element-level query
		isElementLevel := validResults[0].Result.GetElementLevel()
		if isElementLevel {
			for i, r := range validResults {
				if r.Result.GetElementLevel() != isElementLevel {
					return nil, fmt.Errorf("inconsistent element-level flag: result[0]=%v, result[%d]=%v",
						isElementLevel, i, r.Result.GetElementLevel())
				}
				size := typeutil.GetSizeOfIDs(r.GetIds())
				if len(r.Result.GetElementIndices()) != size {
					return nil, fmt.Errorf("element_indices length (%d) does not match ids length (%d)",
						len(r.Result.GetElementIndices()), size)
				}
			}
		}

		// Calculate loop bound
		loopEnd := 0
		for _, r := range validResults {
			loopEnd += typeutil.GetSizeOfIDs(r.GetIds())
		}

		limit := -1
		if topK != typeutil.Unlimited && reduce.ShouldUseInputLimit(reduceType) {
			limit = int(topK)
		}

		type pkEntry struct {
			selIdx int // index in selections slice
			ts     int64
		}
		cursors := make([]int64, len(validResults))
		pkMap := make(map[any]pkEntry)
		ids := &schemapb.IDs{}
		var selections []OffsetSelection
		var availableCount int
		var skipDupCnt int64

		for j := 0; j < loopEnd && (limit == -1 || availableCount < limit); j++ {
			sel, drainOneResult := typeutil.SelectMinPKWithTimestamp(validResults, cursors)
			if sel == -1 || (reduce.ShouldStopWhenDrained(reduceType) && drainOneResult) {
				break
			}

			pk := typeutil.GetPK(validResults[sel].GetIds(), cursors[sel])
			ts := validResults[sel].Timestamps[cursors[sel]]
			offset := validResults[sel].Result.GetOffset()[cursors[sel]]

			// Get element indices and count for element-level query
			var elemIndices *segcorepb.ElementIndices
			elemCount := 1
			if isElementLevel {
				elemIndicesList := validResults[sel].Result.GetElementIndices()
				if int(cursors[sel]) < len(elemIndicesList) {
					elemIndices = elemIndicesList[cursors[sel]]
					elemCount = len(elemIndices.GetIndices())
				}
			}

			if entry, ok := pkMap[pk]; !ok {
				pkMap[pk] = pkEntry{selIdx: len(selections), ts: ts}
				typeutil.AppendPKs(ids, pk)
				selections = append(selections, OffsetSelection{
					SegmentIndex:   sel,
					Offset:         offset,
					ElementIndices: elemIndices,
				})
				availableCount += elemCount
			} else {
				skipDupCnt++
				// Duplicate PK: keep the one with higher timestamp
				if ts != 0 && ts > entry.ts {
					// Adjust element count for element-level
					if isElementLevel {
						oldElemCount := len(selections[entry.selIdx].ElementIndices.GetIndices())
						availableCount = availableCount - oldElemCount + elemCount
					}
					pkMap[pk] = pkEntry{selIdx: entry.selIdx, ts: ts}
					selections[entry.selIdx] = OffsetSelection{
						SegmentIndex:   sel,
						Offset:         offset,
						ElementIndices: elemIndices,
					}
				}
			}

			cursors[sel]++
		}

		if skipDupCnt > 0 {
			log.Ctx(ctx).Debug("skip duplicated PKs during IgnoreNonPk merge",
				zap.Int64("dupCount", skipDupCnt))
		}

		return []any{&MergedResultWithOffsets{
			IDs:          ids,
			Selections:   selections,
			ElementLevel: isElementLevel,
		}}, nil
	})
}

// NewFetchFieldsDataOperator creates an operator that retrieves full field data
// from segments using offset-based retrieval. This is the second stage of the
// IgnoreNonPk pipeline: after PK merge + dedup + topK, fetch actual field data
// only for the selected rows.
//
// Input[0]: *MergedResultWithOffsets
// Output[0]: *segcorepb.RetrieveResults (with IDs and full FieldsData)
func NewFetchFieldsDataOperator(
	validSegments []Segment,
	manager *Manager,
	retrievePlan *segcore.RetrievePlan,
) queryutil.Operator {
	return queryutil.NewLambdaOperator(queryutil.OpFetchFields, func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		merged := inputs[0].(*MergedResultWithOffsets)

		ret := &segcorepb.RetrieveResults{
			Ids:          merged.IDs,
			ElementLevel: merged.ElementLevel,
		}

		if len(merged.Selections) == 0 {
			return []any{ret}, nil
		}

		// Group selections by segment index
		groups := lo.GroupBy(merged.Selections, func(sel OffsetSelection) int {
			return sel.SegmentIndex
		})

		// Parallel RetrieveByOffsets per segment
		segmentResults := make([]*segcorepb.RetrieveResults, len(validSegments))
		futures := make([]*conc.Future[any], 0, len(groups))
		for segIdx, sels := range groups {
			idx := segIdx
			offsets := lo.Map(sels, func(sel OffsetSelection, _ int) int64 { return sel.Offset })
			future := GetSQPool().Submit(func() (any, error) {
				var r *segcorepb.RetrieveResults
				var err error
				if err := doOnSegment(ctx, manager, validSegments[idx], func(ctx context.Context, segment Segment) error {
					r, err = segment.RetrieveByOffsets(ctx, &segcore.RetrievePlanWithOffsets{
						RetrievePlan: retrievePlan,
						Offsets:      offsets,
					})
					return err
				}); err != nil {
					return nil, err
				}
				segmentResults[idx] = r
				return nil, nil
			})
			futures = append(futures, future)
		}

		// Must be BlockOnAll: if we fast-fail, cgo struct like `plan` could be used after free.
		if err := conc.BlockOnAll(futures...); err != nil {
			return nil, err
		}

		// Find a non-empty result to initialize FieldsData layout
		for _, r := range segmentResults {
			if r != nil && len(r.GetFieldsData()) != 0 {
				ret.FieldsData = typeutil.PrepareResultFieldData(r.GetFieldsData(), int64(len(merged.Selections)))
				break
			}
		}

		if ret.FieldsData == nil {
			return []any{ret}, nil
		}

		// Build FieldsData in PK-sorted order (matching selections order)
		idxComputers := make([]*typeutil.FieldDataIdxComputer, len(segmentResults))
		for i, r := range segmentResults {
			if r != nil {
				idxComputers[i] = typeutil.NewFieldDataIdxComputer(r.GetFieldsData())
			}
		}

		// Track consumption position per segment's RetrieveByOffsets result.
		// RetrieveByOffsets returns results compacted: row 0, 1, 2, ...
		segmentResOffset := make([]int64, len(segmentResults))
		maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
		var retSize int64

		for _, sel := range merged.Selections {
			r := segmentResults[sel.SegmentIndex]
			if r == nil {
				continue
			}
			fieldsData := r.GetFieldsData()
			fieldIdxs := idxComputers[sel.SegmentIndex].Compute(segmentResOffset[sel.SegmentIndex])
			retSize += typeutil.AppendFieldData(ret.FieldsData, fieldsData, segmentResOffset[sel.SegmentIndex], fieldIdxs...)
			segmentResOffset[sel.SegmentIndex]++

			if retSize > maxOutputSize {
				return nil, fmt.Errorf("query results exceed the maxOutputSize Limit %d", maxOutputSize)
			}
		}

		// Fill ElementIndices for element-level query
		if merged.ElementLevel {
			for _, sel := range merged.Selections {
				ret.ElementIndices = append(ret.ElementIndices, sel.ElementIndices)
			}
		}

		return []any{ret}, nil
	})
}
