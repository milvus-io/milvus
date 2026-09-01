/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tasks

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type reduceRange struct {
	NQOffset   int
	NQCount    int
	ReduceTopK int64
	OutputTopK int64
}

type reduceLayout struct {
	NQ               int
	GroupSize        int64
	PerRequestReduce bool
	Ranges           []reduceRange
}

func hasMixedTopK(topks []int64) bool {
	if len(topks) <= 1 {
		return false
	}
	first := topks[0]
	for _, topk := range topks[1:] {
		if topk != first {
			return true
		}
	}
	return false
}

func requiresPerRequestReduce(groupByOpts *groupByOptions, topks []int64, hasL1 bool) bool {
	if !hasMixedTopK(topks) {
		return false
	}
	if hasL1 {
		return true
	}
	return groupByOpts != nil && groupByOpts.GroupSize > 1
}

// buildReduceLayout describes the request-level NQ ranges in a merged task.
// Group-by reduction with group_size > 1 cannot share a max-TopK reduce when
// the merged requests have different TopKs: group acceptance depends on the
// request's own TopK, so those ranges must be reduced independently. L1 also
// requires independent ranges for mixed TopKs so each request sees its own
// candidate window before reranking.
func (t *SearchTask) buildReduceLayout(groupByOpts *groupByOptions, hasL1 bool) (*reduceLayout, error) {
	if len(t.originNqs) != len(t.originTopks) {
		return nil, merr.WrapErrServiceInternalMsg(
			"reduce layout: origin NQ count %d does not match origin TopK count %d",
			len(t.originNqs), len(t.originTopks))
	}

	layout := &reduceLayout{
		NQ:               int(t.nq),
		GroupSize:        1,
		PerRequestReduce: requiresPerRequestReduce(groupByOpts, t.originTopks, hasL1),
		Ranges:           make([]reduceRange, len(t.originNqs)),
	}
	if int64(layout.NQ) != t.nq || t.nq < 0 {
		return nil, merr.WrapErrServiceInternalMsg("reduce layout: invalid merged NQ %d", t.nq)
	}
	if groupByOpts != nil && groupByOpts.GroupSize > 1 {
		layout.GroupSize = groupByOpts.GroupSize
	}

	nqOffset := 0
	for i, originNQ := range t.originNqs {
		nqCount := int(originNQ)
		if originNQ < 0 || int64(nqCount) != originNQ {
			return nil, merr.WrapErrServiceInternalMsg("reduce layout: invalid origin NQ %d at index %d", originNQ, i)
		}
		if t.originTopks[i] < 0 {
			return nil, merr.WrapErrServiceInternalMsg("reduce layout: invalid origin TopK %d at index %d", t.originTopks[i], i)
		}
		reduceTopK := t.topk
		if layout.PerRequestReduce {
			reduceTopK = t.originTopks[i]
		}
		if reduceTopK < 0 {
			return nil, merr.WrapErrServiceInternalMsg("reduce layout: invalid reduce TopK %d at index %d", reduceTopK, i)
		}
		layout.Ranges[i] = reduceRange{
			NQOffset:   nqOffset,
			NQCount:    nqCount,
			ReduceTopK: reduceTopK,
			OutputTopK: t.originTopks[i],
		}
		nqOffset += nqCount
	}
	if nqOffset != layout.NQ {
		return nil, merr.WrapErrServiceInternalMsg(
			"reduce layout: origin NQ sum %d does not match merged NQ %d",
			nqOffset, layout.NQ)
	}
	return layout, nil
}

// exportSearchResultsAsArrow exports per-segment SearchResults as Arrow DataFrames
// via the Arrow C Stream Interface (one RecordBatch per NQ).
// Each DataFrame contains $id, $score, $seg_offset columns, optional $group_by
// and $element_indices columns, plus any extra fields, with one chunk per NQ
// query. Arrow field metadata is preserved so group-by and extra fields keep
// their Milvus field id, logical type, and nullability.
// extraFieldIDs specifies additional fields to export (e.g., fields needed by L0 rerank).
// The caller is responsible for releasing the returned DataFrames.
func (t *SearchTask) exportSearchResultsAsArrow(
	results []*segments.SearchResult,
	plan *segcore.SearchPlan,
	extraFieldIDs []int64,
) (segDFs []*chain.DataFrame, retErr error) {
	segDFs = make([]*chain.DataFrame, len(results))
	defer func() {
		if retErr != nil {
			for _, df := range segDFs {
				if df != nil {
					df.Release()
				}
			}
		}
	}()

	exportOne := func(ctx context.Context, idx int, result *segments.SearchResult) error {
		record, chunkSizes, err := segcore.ExportSearchResultAsArrowRecordBatch(ctx, result, plan, extraFieldIDs)
		if err != nil {
			mlog.Warn(ctx, "failed to export search result as Arrow", mlog.Err(err))
			return err
		}
		defer record.Release()

		df, err := dataFrameFromArrowRecordBatch(record, chunkSizes)
		if err != nil {
			return err
		}
		segDFs[idx] = df
		return nil
	}

	if len(results) == 1 {
		if err := exportOne(t.ctx, 0, results[0]); err != nil {
			return nil, err
		}
		return segDFs, nil
	}

	errGroup, groupCtx := errgroup.WithContext(t.ctx)
	for i, res := range results {
		idx := i
		result := res
		errGroup.Go(func() error {
			return exportOne(groupCtx, idx, result)
		})
	}
	if err := errGroup.Wait(); err != nil {
		return segDFs, err
	}
	return segDFs, nil
}

func resolveGroupByOptions(segDFs []*chain.DataFrame, results []*segments.SearchResult) *groupByOptions {
	if len(segDFs) == 0 || len(results) == 0 {
		return nil
	}
	groupByColumns := groupByColumnNames(segDFs[0])
	if len(groupByColumns) == 0 {
		return nil
	}
	return &groupByOptions{
		GroupSize: resolveGroupSizeFromSearchResults(results),
		Columns:   groupByColumns,
	}
}

// executeGoReduce only performs cross-segment reduction. L1 rerank, late
// materialization, and result encoding are separate stages in SearchTask.Execute.
func (t *SearchTask) executeGoReduce(
	segDFs []*chain.DataFrame,
	topK int64,
	groupByOpts *groupByOptions,
	nqOffset int,
	nq int,
) (*mergeResult, error) {
	result, err := heapMergeReduceRange(defaultAllocator, segDFs, topK, groupByOpts, nqOffset, nq)
	if err != nil {
		mlog.Warn(t.ctx, "failed to heapMergeReduce", mlog.Err(err))
		return nil, err
	}
	return result, nil
}

func (t *SearchTask) applyL1RerankResult(
	reduced *mergeResult,
	results []*segments.SearchResult,
	plan *segcore.SearchPlan,
	prepared *preparedL1FunctionChain,
) (*mergeResult, error) {
	if prepared == nil {
		return reduced, nil
	}

	reranked, err := t.applyL1Rerank(reduced, results, plan, prepared)
	if err != nil {
		return reduced, err
	}
	if reranked != reduced && reduced.DF != nil {
		reduced.DF.Release()
	}
	return reranked, nil
}

func (t *SearchTask) processReducedSlice(
	i int,
	reduced *mergeResult,
	results []*segments.SearchResult,
	plan *segcore.SearchPlan,
	prepared *preparedL1FunctionChain,
	metricType string,
	tr *timerecord.TimeRecorder,
	relatedDataSize int64,
	allSearchCount int64,
) error {
	if reduced == nil {
		return merr.WrapErrServiceInternalMsg("process reduced slice %d: result is nil", i)
	}
	defer func() {
		if reduced.DF != nil {
			reduced.DF.Release()
		}
	}()

	var err error
	reduced, err = t.applyL1RerankResult(reduced, results, plan, prepared)
	if err != nil {
		return err
	}
	return t.materializeAndAssignResult(
		i,
		reduced,
		results,
		plan,
		metricType,
		tr,
		relatedDataSize,
		allSearchCount,
	)
}

func resolveGroupSizeFromSearchResults(results []*segments.SearchResult) int64 {
	metadata := make([]segcore.SearchResultMetadata, 0, len(results))
	for _, result := range results {
		if result == nil {
			continue
		}
		metadata = append(metadata, result.GetMetadata())
	}
	return resolveGroupSizeFromMetadata(metadata)
}

func resolveGroupSizeFromMetadata(metadata []segcore.SearchResultMetadata) int64 {
	for _, md := range metadata {
		if md.GroupSize > 0 {
			return md.GroupSize
		}
	}
	return 1
}

// attributeStorageCost splits the total storage cost across sub-tasks
// proportionally to NQ. Must run AFTER every slice's Late Mat finishes —
// FillOutputFieldsOrdered accumulates bytes on the C++ SearchResult, so
// GetMetadata().StorageCost is only final after late mat completes.
func (t *SearchTask) attributeStorageCost(results []*segments.SearchResult) {
	var totalNq int64
	for _, n := range t.originNqs {
		totalNq += n
	}
	if totalNq == 0 {
		return
	}
	var totalCost segcore.StorageCost
	for _, r := range results {
		c := r.GetMetadata().StorageCost
		totalCost.ScannedRemoteBytes += c.ScannedRemoteBytes
		totalCost.ScannedTotalBytes += c.ScannedTotalBytes
	}
	for i, sliceNq := range t.originNqs {
		task := t.subTaskAt(i)
		ratio := float64(sliceNq) / float64(totalNq)
		task.result.ScannedRemoteBytes = int64(float64(totalCost.ScannedRemoteBytes) * ratio)
		task.result.ScannedTotalBytes = int64(float64(totalCost.ScannedTotalBytes) * ratio)
	}
}

func (t *SearchTask) marshalReducedResult(
	i int,
	reduced *mergeResult,
	allSearchCount int64,
) (*schemapb.SearchResultData, error) {
	searchResultData, err := marshalReduceResult(reduced)
	if err != nil {
		return nil, err
	}
	// Force SearchResultData.TopK to the requested topK. The chain converter
	// derives TopK from the max chunk size, which is 0 on empty results and
	// would be < originTopks[i] whenever a sub-task exhausts fewer rows than
	// requested. Proxy.checkSearchResultData compares against the requested
	// topK — match the legacy C++ reduce contract (Reduce.cpp
	// set_top_k(slice_topKs_[slice_index])).
	searchResultData.TopK = t.originTopks[i]
	searchResultData.AllSearchCount = allSearchCount
	return searchResultData, nil
}

func (t *SearchTask) materializeAndAssignResult(
	i int,
	reduced *mergeResult,
	results []*segments.SearchResult,
	plan *segcore.SearchPlan,
	metricType string,
	tr *timerecord.TimeRecorder,
	relatedDataSize int64,
	allSearchCount int64,
) error {
	searchResultData, err := t.marshalReducedResult(i, reduced, allSearchCount)
	if err != nil {
		return err
	}
	takeForOutputAllowed := false
	if i < len(t.takeForOutputAllowed) {
		takeForOutputAllowed = t.takeForOutputAllowed[i]
	}
	plan.SetTakeForOutputAllowed(takeForOutputAllowed)
	if err := lateMaterializeOutputFields(t.ctx, results, plan, reduced.Sources, searchResultData); err != nil {
		return err
	}
	return t.encodeAndAssignReducedResult(i, searchResultData, metricType, tr, relatedDataSize)
}

func (t *SearchTask) encodeAndAssignReducedResult(
	i int,
	searchResultData *schemapb.SearchResultData,
	metricType string,
	tr *timerecord.TimeRecorder,
	relatedDataSize int64,
) error {
	searchResults, err := segments.EncodeSearchResultData(
		t.ctx,
		searchResultData,
		t.originNqs[i],
		t.originTopks[i],
		metricType,
	)
	if err != nil {
		return err
	}
	searchResults.Base = &commonpb.MsgBase{
		SourceID: t.GetNodeID(),
	}
	searchResults.SlicedOffset = 1
	searchResults.SlicedNumCount = 1
	searchResults.CostAggregation = &internalpb.CostAggregation{
		ServiceTime:          tr.ElapseSpan().Milliseconds(),
		TotalRelatedDataSize: relatedDataSize,
	}

	task := t.subTaskAt(i)
	task.result = searchResults
	return nil
}

// lateMaterializeOutputFields reads output fields from C++ segments in a single
// CGO call and assembles them into the final SearchResultData. C++ does the
// per-segment FillTargetEntry + MergeDataArray scatter + serialize.
func lateMaterializeOutputFields(
	ctx context.Context,
	results []*segments.SearchResult,
	plan *segcore.SearchPlan,
	sources [][]segmentSource,
	searchResultData *schemapb.SearchResultData,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !plan.HasTargetEntries() {
		return nil
	}

	totalRows := 0
	for _, chunk := range sources {
		totalRows += len(chunk)
	}

	segIndices := make([]int32, totalRows)
	segOffsets := make([]int64, totalRows)
	pos := 0
	for _, chunk := range sources {
		for _, src := range chunk {
			segIndices[pos] = int32(src.InputIdx)
			segOffsets[pos] = src.SegOffset
			pos++
		}
	}

	protoBytes, err := segcore.FillOutputFieldsOrdered(ctx, results, plan, segIndices, segOffsets)
	if err != nil {
		return err
	}
	if len(protoBytes) == 0 {
		return nil
	}

	var fieldResult schemapb.SearchResultData
	// fastpb: wire-equivalent fast decoder for the late-materialize output-fields
	// hot path (~2x varchar / ~6x vector vs proto.Unmarshal).
	if err := fastpb.UnmarshalSearchResultData(protoBytes, &fieldResult); err != nil {
		return err
	}
	searchResultData.FieldsData = fieldResult.FieldsData
	return nil
}

// extractSlice extracts a sub-range of NQ chunks from a mergeResult and
// enforces the per-slice row limit: each NQ chunk is truncated to at most
// maxRowsPerNQ rows. It is valid for standard topK and for group-by with
// groupSize == 1; mixed-topK group-by with groupSize > 1 must use per-slice
// reduce because a max-topK group reduce cannot be row-truncated safely.
func extractSlice(result *mergeResult, nqOffset, nqCount int, maxRowsPerNQ int64) (*mergeResult, error) {
	if nqCount == 0 {
		return &mergeResult{
			DF:      emptyDF(),
			Sources: nil,
		}, nil
	}

	totalChunks := result.DF.NumChunks()
	if nqOffset+nqCount > totalChunks {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("extractSlice: nqOffset(%d)+nqCount(%d) > totalChunks(%d)",
				nqOffset, nqCount, totalChunks))
	}

	allChunkSizes := result.DF.ChunkSizes()
	needTruncate := false
	for j := 0; j < nqCount; j++ {
		if allChunkSizes[nqOffset+j] > maxRowsPerNQ {
			needTruncate = true
			break
		}
	}

	if !needTruncate && nqOffset == 0 && nqCount == totalChunks {
		return result, nil
	}

	sliceChunkSizes := make([]int64, nqCount)
	for j := 0; j < nqCount; j++ {
		sliceChunkSizes[j] = min(allChunkSizes[nqOffset+j], maxRowsPerNQ)
	}

	builder := chain.NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(sliceChunkSizes)

	for _, colName := range result.DF.ColumnNames() {
		col := result.DF.Column(colName)
		chunks := col.Chunks()

		if needTruncate {
			newChunks := make([]arrow.Array, nqCount)
			for j := 0; j < nqCount; j++ {
				src := chunks[nqOffset+j]
				want := sliceChunkSizes[j]
				if int64(src.Len()) > want {
					newChunks[j] = array.NewSlice(src, 0, want)
				} else {
					src.Retain()
					newChunks[j] = src
				}
			}
			if err := builder.AddColumnFromChunks(colName, newChunks); err != nil {
				return nil, err
			}
		} else {
			sliceChunks := chunks[nqOffset : nqOffset+nqCount]
			for _, chunk := range sliceChunks {
				chunk.Retain()
			}
			if err := builder.AddColumnFromChunks(colName, sliceChunks); err != nil {
				return nil, err
			}
		}
		builder.CopyFieldMetadata(result.DF, colName)
	}
	builder.CopyAllMetadata(result.DF)

	var sliceSources [][]segmentSource
	if needTruncate {
		sliceSources = make([][]segmentSource, nqCount)
		for j := 0; j < nqCount; j++ {
			src := result.Sources[nqOffset+j]
			want := int(sliceChunkSizes[j])
			if len(src) > want {
				sliceSources[j] = src[:want]
			} else {
				sliceSources[j] = src
			}
		}
	} else {
		sliceSources = result.Sources[nqOffset : nqOffset+nqCount]
	}

	return &mergeResult{
		DF:      builder.Build(),
		Sources: sliceSources,
	}, nil
}

// emptyDF creates an empty DataFrame for empty slices.
func emptyDF() *chain.DataFrame {
	return chain.NewDataFrameBuilder().Build()
}
