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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

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

// executeGoReduce performs the search reduce pipeline entirely in Go:
//  1. heapMergeReduce (k-way merge with PK dedup, optionally GroupBy-aware)
//  2. Late Materialization (read output fields from segments)
//  3. Marshal to SearchResultData proto
//
// segDFs are the per-segment DataFrames from exportSearchResultsAsArrow.
func (t *SearchTask) executeGoReduce(
	segDFs []*chain.DataFrame,
	results []*segments.SearchResult,
	searchReq *segcore.SearchRequest,
	metricType string,
	tr *timerecord.TimeRecorder,
	relatedDataSize int64,
	allSearchCount int64,
) error {
	plan := searchReq.Plan()

	// Group-by is enabled iff the C++ Arrow exporter emitted one or more
	// $group_by_<fieldID> columns.
	var groupByOpts *groupByOptions
	if len(segDFs) > 0 && len(results) > 0 {
		groupByColumns := groupByColumnNames(segDFs[0])
		if len(groupByColumns) > 0 {
			groupByOpts = &groupByOptions{
				GroupSize: resolveGroupSizeFromSearchResults(results),
				Columns:   groupByColumns,
			}
		}
	}

	if !requiresPerSliceReduce(groupByOpts, t.originTopks) {
		return t.executeGoReduceFastPath(segDFs, results, plan, metricType, tr, relatedDataSize, allSearchCount, groupByOpts)
	}

	nqOffset := 0
	for i := range t.originNqs {
		nq := int(t.originNqs[i])
		reduceResult, err := heapMergeReduceRange(defaultAllocator, segDFs, t.originTopks[i], groupByOpts, nqOffset, nq)
		if err != nil {
			mlog.Warn(t.ctx, "failed to heapMergeReduce", mlog.Err(err))
			return err
		}

		err = t.buildReducedResult(i, reduceResult, results, plan, metricType, tr, relatedDataSize, allSearchCount)
		if reduceResult.DF != nil {
			reduceResult.DF.Release()
		}
		if err != nil {
			return err
		}
		nqOffset += nq
	}

	t.attributeStorageCost(results)
	return nil
}

func (t *SearchTask) executeGoReduceFastPath(
	segDFs []*chain.DataFrame,
	results []*segments.SearchResult,
	plan *segcore.SearchPlan,
	metricType string,
	tr *timerecord.TimeRecorder,
	relatedDataSize int64,
	allSearchCount int64,
	groupByOpts *groupByOptions,
) error {
	// plan.GetTopK() may be reduced by the delegator optimizer. t.topk is the
	// task's unity topK across merged slices, preserving the worker reduce
	// contract based on the original request topK.
	reduceResult, err := heapMergeReduce(defaultAllocator, segDFs, t.topk, groupByOpts)
	if err != nil {
		mlog.Warn(t.ctx, "failed to heapMergeReduce", mlog.Err(err))
		return err
	}
	defer reduceResult.DF.Release()

	var groupSize int64 = 1
	if groupByOpts != nil && groupByOpts.GroupSize > 1 {
		groupSize = groupByOpts.GroupSize
	}

	nqOffset := 0
	for i := range t.originNqs {
		nq := int(t.originNqs[i])
		if err := t.buildSlicedResult(i, nqOffset, nq, groupSize, reduceResult, results, plan, metricType, tr, relatedDataSize, allSearchCount); err != nil {
			return err
		}
		nqOffset += nq
	}

	t.attributeStorageCost(results)
	return nil
}

func requiresPerSliceReduce(groupByOpts *groupByOptions, topks []int64) bool {
	if groupByOpts == nil || groupByOpts.GroupSize <= 1 || len(topks) <= 1 {
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

func (t *SearchTask) buildReducedResult(
	i int,
	reduceResult *mergeResult,
	results []*segments.SearchResult,
	plan *segcore.SearchPlan,
	metricType string,
	tr *timerecord.TimeRecorder,
	relatedDataSize int64,
	allSearchCount int64,
) error {
	searchResultData, err := marshalReduceResult(reduceResult)
	if err != nil {
		return err
	}
	// Force SearchResultData.TopK to the requested topK. The chain converter
	// derives TopK from the max chunk size, which is 0 on empty results and
	// would be < originTopks[i] whenever a sub-task exhausts fewer rows than
	// requested. Proxy.checkSearchResultData compares against the requested
	// topK — match the legacy C++ reduce contract (Reduce.cpp
	// set_top_k(slice_topKs_[slice_index])).
	searchResultData.TopK = t.originTopks[i]
	searchResultData.AllSearchCount = allSearchCount

	if err := lateMaterializeOutputFields(t.ctx, results, plan, reduceResult.Sources, searchResultData); err != nil {
		return err
	}

	searchResults, err := segments.EncodeSearchResultData(t.ctx, searchResultData, t.originNqs[i], t.originTopks[i], metricType)
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

// buildSlicedResult extracts the i-th sub-task's slice from the merged reduce
// result, runs Late Materialization, and assigns the serialized blob to that
// task. This is the common fast path: one max-topK reduce, then per-slice
// slicing. For mixed-topK group-by with groupSize > 1, executeGoReduce uses
// per-slice reduce instead because row truncation is not group-aware.
func (t *SearchTask) buildSlicedResult(
	i, nqOffset, nq int,
	groupSize int64,
	reduceResult *mergeResult,
	results []*segments.SearchResult,
	plan *segcore.SearchPlan,
	metricType string,
	tr *timerecord.TimeRecorder,
	relatedDataSize int64,
	allSearchCount int64,
) error {
	rowLimit := t.originTopks[i] * groupSize
	sliceResult, err := extractSlice(reduceResult, nqOffset, nq, rowLimit)
	if err != nil {
		return err
	}
	if sliceResult != reduceResult && sliceResult.DF != nil {
		defer sliceResult.DF.Release()
	}
	return t.buildReducedResult(i, sliceResult, results, plan, metricType, tr, relatedDataSize, allSearchCount)
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
	if err := proto.Unmarshal(protoBytes, &fieldResult); err != nil {
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
