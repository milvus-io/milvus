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
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/log"
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
	segDFs = make([]*chain.DataFrame, 0, len(results))
	defer func() {
		if retErr != nil {
			for _, df := range segDFs {
				df.Release()
			}
		}
	}()

	for _, res := range results {
		reader, err := segcore.ExportSearchResultAsArrowStream(res, plan, extraFieldIDs)
		if err != nil {
			log.Ctx(t.ctx).Warn("failed to export search result as Arrow", zap.Error(err))
			return nil, err
		}

		df, err := dataFrameFromArrowReader(reader)
		if err != nil {
			return nil, err
		}
		segDFs = append(segDFs, df)
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
	topK := plan.GetTopK()

	// Group-by is enabled iff the C++ Arrow exporter emitted one or more
	// $group_by_<fieldID> columns.
	var groupByOpts *groupByOptions
	if len(segDFs) > 0 && len(results) > 0 {
		groupByColumns := groupByColumnNames(segDFs[0])
		if len(groupByColumns) > 0 {
			groupByOpts = &groupByOptions{
				GroupSize: results[0].GetMetadata().GroupSize,
				Columns:   groupByColumns,
			}
		}
	}

	reduceResult, err := heapMergeReduce(defaultAllocator, segDFs, topK, groupByOpts)
	if err != nil {
		log.Ctx(t.ctx).Warn("failed to heapMergeReduce", zap.Error(err))
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

// buildSlicedResult extracts the i-th sub-task's slice from the merged reduce result,
// runs Late Materialization, and assigns the serialized blob to that task.
// The sliced DataFrame is released here so peak memory stays at one slice.
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
	// Per-NQ row limit: topK rows in standard mode, topK × groupSize in GroupBy
	// mode (each group may contribute up to groupSize results).
	rowLimit := t.originTopks[i] * groupSize
	sliceResult, err := extractSlice(reduceResult, nqOffset, nq, rowLimit)
	if err != nil {
		return err
	}
	if sliceResult != reduceResult && sliceResult.DF != nil {
		defer sliceResult.DF.Release()
	}

	searchResultData, err := marshalReduceResult(sliceResult)
	if err != nil {
		return err
	}
	fixupGroupByFieldType(searchResultData, t.collection.Schema())
	// Force SearchResultData.TopK to the requested topK. The chain converter
	// derives TopK from the max chunk size, which is 0 on empty results and
	// would be < originTopks[i] whenever a sub-task exhausts fewer rows than
	// requested. Proxy.checkSearchResultData compares against the requested
	// topK — match the legacy C++ reduce contract (Reduce.cpp
	// set_top_k(slice_topKs_[slice_index])).
	searchResultData.TopK = t.originTopks[i]
	searchResultData.AllSearchCount = allSearchCount

	if err := lateMaterializeOutputFields(results, plan, sliceResult.Sources, searchResultData); err != nil {
		return err
	}

	slicedBlob, err := proto.Marshal(searchResultData)
	if err != nil {
		return err
	}

	task := t.subTaskAt(i)
	task.result = &internalpb.SearchResults{
		Base: &commonpb.MsgBase{
			SourceID: t.GetNodeID(),
		},
		Status:         merr.Success(),
		MetricType:     metricType,
		NumQueries:     t.originNqs[i],
		TopK:           t.originTopks[i],
		SlicedBlob:     slicedBlob,
		SlicedOffset:   1,
		SlicedNumCount: 1,
		CostAggregation: &internalpb.CostAggregation{
			ServiceTime:          tr.ElapseSpan().Milliseconds(),
			TotalRelatedDataSize: relatedDataSize,
		},
	}
	return nil
}

// lateMaterializeOutputFields reads output fields from C++ segments in a single
// CGO call and assembles them into the final SearchResultData. C++ does the
// per-segment FillTargetEntry + MergeDataArray scatter + serialize.
func lateMaterializeOutputFields(
	results []*segments.SearchResult,
	plan *segcore.SearchPlan,
	sources [][]segmentSource,
	searchResultData *schemapb.SearchResultData,
) error {
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

	protoBytes, err := segcore.FillOutputFieldsOrdered(results, plan, segIndices, segOffsets)
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

// fixupGroupByFieldType corrects GroupByFieldValues protos when the field id is
// available but the value was exported as a physical Arrow string. New Arrow
// streams carry the logical group-by field type as metadata, so this is only a
// fallback for streams that still preserve FieldId but lose the logical type.
func fixupGroupByFieldType(data *schemapb.SearchResultData, schema *schemapb.CollectionSchema) {
	if len(data.GetGroupByFieldValues()) == 0 {
		return
	}
	fieldsByID := make(map[int64]*schemapb.FieldSchema, len(schema.GetFields()))
	for _, f := range schema.GetFields() {
		fieldsByID[f.GetFieldID()] = f
	}
	for _, gbv := range data.GetGroupByFieldValues() {
		if gbv == nil || gbv.GetFieldId() == 0 {
			continue
		}
		f := fieldsByID[gbv.GetFieldId()]
		if f == nil || f.GetDataType() != schemapb.DataType_Geometry {
			continue
		}
		gbv.Type = schemapb.DataType_Geometry
		if sd := gbv.GetScalars().GetStringData(); sd != nil {
			byteSlices := make([][]byte, len(sd.Data))
			for i, s := range sd.Data {
				byteSlices[i] = []byte(s)
			}
			gbv.GetScalars().Data = &schemapb.ScalarField_GeometryData{
				GeometryData: &schemapb.GeometryArray{Data: byteSlices},
			}
		}
	}
}

// extractSlice extracts a sub-range of NQ chunks from a mergeResult and
// enforces the per-slice row limit: each NQ chunk is truncated to at most
// maxRowsPerNQ rows. heapMergeReduce runs with max(originTopks) so a
// small-topK sub-task would otherwise inherit too many rows per NQ. This
// matches the legacy C++ Reduce.cpp slice_topKs_[slice_index] semantics.
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

	// Compute per-chunk sizes; allocate only after the fast path is ruled out.
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
