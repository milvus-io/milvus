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
	"math"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ typeutil.ResultWithID = &internalpb.RetrieveResults{}

var _ typeutil.ResultWithID = &segcorepb.RetrieveResults{}

func ReduceSearchResults(ctx context.Context, results []*internalpb.SearchResults, info *ReduceInfo) (*internalpb.SearchResults, error) {
	results = lo.Filter(results, func(result *internalpb.SearchResults, _ int) bool {
		return result != nil && result.GetSlicedBlob() != nil
	})

	if len(results) == 1 {
		return results[0], nil
	}

	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "ReduceSearchResults")
	defer sp.End()

	channelsMvcc := make(map[string]uint64)
	for _, r := range results {
		for ch, ts := range r.GetChannelsMvcc() {
			channelsMvcc[ch] = ts
		}
		// shouldn't let new SearchResults.MetricType to be empty, though the req.MetricType is empty
		if info.metricType == "" {
			info.metricType = r.MetricType
		}
	}
	log := log.Ctx(ctx)

	searchResultData, err := DecodeSearchResults(ctx, results)
	if err != nil {
		log.Warn("shard leader decode search results errors", zap.Error(err))
		return nil, err
	}
	log.Debug("shard leader get valid search results", zap.Int("numbers", len(searchResultData)))

	for i, sData := range searchResultData {
		log.Debug("reduceSearchResultData",
			zap.Int("result No.", i),
			zap.Int64("nq", sData.NumQueries),
			zap.Int64("topk", sData.TopK))
	}

	searchReduce := InitSearchReducer(info)
	reducedResultData, err := searchReduce.ReduceSearchResultData(ctx, searchResultData, info)
	if err != nil {
		log.Warn("shard leader reduce errors", zap.Error(err))
		return nil, err
	}
	searchResults, err := EncodeSearchResultData(ctx, reducedResultData, info.nq, info.topK, info.metricType)
	if err != nil {
		log.Warn("shard leader encode search result errors", zap.Error(err))
		return nil, err
	}

	requestCosts := lo.FilterMap(results, func(result *internalpb.SearchResults, _ int) (*internalpb.CostAggregation, bool) {
		if paramtable.Get().QueryNodeCfg.EnableWorkerSQCostMetrics.GetAsBool() {
			return result.GetCostAggregation(), true
		}

		if result.GetBase().GetSourceID() == paramtable.GetNodeID() {
			return result.GetCostAggregation(), true
		}

		return nil, false
	})
	searchResults.CostAggregation = mergeRequestCost(requestCosts)
	if searchResults.CostAggregation == nil {
		searchResults.CostAggregation = &internalpb.CostAggregation{}
	}
	relatedDataSize := lo.Reduce(results, func(acc int64, result *internalpb.SearchResults, _ int) int64 {
		return acc + result.GetCostAggregation().GetTotalRelatedDataSize()
	}, 0)
	searchResults.CostAggregation.TotalRelatedDataSize = relatedDataSize
	searchResults.ChannelsMvcc = channelsMvcc
	return searchResults, nil
}

func ReduceAdvancedSearchResults(ctx context.Context, results []*internalpb.SearchResults, nq int64) (*internalpb.SearchResults, error) {
	_, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "ReduceAdvancedSearchResults")
	defer sp.End()

	if len(results) == 1 {
		return results[0], nil
	}

	channelsMvcc := make(map[string]uint64)
	relatedDataSize := int64(0)
	searchResults := &internalpb.SearchResults{
		IsAdvanced: true,
	}

	for _, result := range results {
		relatedDataSize += result.GetCostAggregation().GetTotalRelatedDataSize()
		for ch, ts := range result.GetChannelsMvcc() {
			channelsMvcc[ch] = ts
		}
		if !result.GetIsAdvanced() {
			continue
		}
		// we just append here, no need to split subResult and reduce
		// defer this reduce to proxy
		searchResults.SubResults = append(searchResults.SubResults, result.GetSubResults()...)
		searchResults.NumQueries = result.GetNumQueries()
	}
	searchResults.ChannelsMvcc = channelsMvcc
	requestCosts := lo.FilterMap(results, func(result *internalpb.SearchResults, _ int) (*internalpb.CostAggregation, bool) {
		if paramtable.Get().QueryNodeCfg.EnableWorkerSQCostMetrics.GetAsBool() {
			return result.GetCostAggregation(), true
		}

		if result.GetBase().GetSourceID() == paramtable.GetNodeID() {
			return result.GetCostAggregation(), true
		}

		return nil, false
	})
	searchResults.CostAggregation = mergeRequestCost(requestCosts)
	if searchResults.CostAggregation == nil {
		searchResults.CostAggregation = &internalpb.CostAggregation{}
	}
	searchResults.CostAggregation.TotalRelatedDataSize = relatedDataSize
	return searchResults, nil
}

func MergeToAdvancedResults(ctx context.Context, results []*internalpb.SearchResults) (*internalpb.SearchResults, error) {
	searchResults := &internalpb.SearchResults{
		IsAdvanced: true,
	}

	channelsMvcc := make(map[string]uint64)
	relatedDataSize := int64(0)
	for index, result := range results {
		relatedDataSize += result.GetCostAggregation().GetTotalRelatedDataSize()
		for ch, ts := range result.GetChannelsMvcc() {
			channelsMvcc[ch] = ts
		}
		// we just append here, no need to split subResult and reduce
		// defer this reduce to proxy
		subResult := &internalpb.SubSearchResults{
			MetricType:     result.GetMetricType(),
			NumQueries:     result.GetNumQueries(),
			TopK:           result.GetTopK(),
			SlicedBlob:     result.GetSlicedBlob(),
			SlicedNumCount: result.GetSlicedNumCount(),
			SlicedOffset:   result.GetSlicedOffset(),
			ReqIndex:       int64(index),
		}
		searchResults.NumQueries = result.GetNumQueries()
		searchResults.SubResults = append(searchResults.SubResults, subResult)
	}
	searchResults.ChannelsMvcc = channelsMvcc
	requestCosts := lo.FilterMap(results, func(result *internalpb.SearchResults, _ int) (*internalpb.CostAggregation, bool) {
		if paramtable.Get().QueryNodeCfg.EnableWorkerSQCostMetrics.GetAsBool() {
			return result.GetCostAggregation(), true
		}

		if result.GetBase().GetSourceID() == paramtable.GetNodeID() {
			return result.GetCostAggregation(), true
		}

		return nil, false
	})
	searchResults.CostAggregation = mergeRequestCost(requestCosts)
	if searchResults.CostAggregation == nil {
		searchResults.CostAggregation = &internalpb.CostAggregation{}
	}
	searchResults.CostAggregation.TotalRelatedDataSize = relatedDataSize
	return searchResults, nil
}

func SelectSearchResultData(dataArray []*schemapb.SearchResultData, resultOffsets [][]int64, offsets []int64, qi int64) int {
	var (
		sel                 = -1
		maxDistance         = -float32(math.MaxFloat32)
		resultDataIdx int64 = -1
	)
	for i, offset := range offsets { // query num, the number of ways to merge
		if offset >= dataArray[i].Topks[qi] {
			continue
		}

		idx := resultOffsets[i][qi] + offset
		distance := dataArray[i].Scores[idx]

		if distance > maxDistance {
			sel = i
			maxDistance = distance
			resultDataIdx = idx
		} else if distance == maxDistance {
			if sel == -1 {
				// A bad case happens where knowhere returns distance == +/-maxFloat32
				// by mistake.
				log.Warn("a bad distance is found, something is wrong here!", zap.Float32("score", distance))
			} else if typeutil.ComparePK(
				typeutil.GetPK(dataArray[i].GetIds(), idx),
				typeutil.GetPK(dataArray[sel].GetIds(), resultDataIdx)) {
				sel = i
				maxDistance = distance
				resultDataIdx = idx
			}
		}
	}
	return sel
}

func DecodeSearchResults(ctx context.Context, searchResults []*internalpb.SearchResults) ([]*schemapb.SearchResultData, error) {
	_, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "DecodeSearchResults")
	defer sp.End()

	results := make([]*schemapb.SearchResultData, 0)
	for _, partialSearchResult := range searchResults {
		if partialSearchResult.SlicedBlob == nil {
			continue
		}

		var partialResultData schemapb.SearchResultData
		err := proto.Unmarshal(partialSearchResult.SlicedBlob, &partialResultData)
		if err != nil {
			return nil, err
		}

		results = append(results, &partialResultData)
	}
	return results, nil
}

func EncodeSearchResultData(ctx context.Context, searchResultData *schemapb.SearchResultData, nq int64, topk int64, metricType string) (searchResults *internalpb.SearchResults, err error) {
	_, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "EncodeSearchResultData")
	defer sp.End()

	searchResults = &internalpb.SearchResults{
		Status:     merr.Success(),
		NumQueries: nq,
		TopK:       topk,
		MetricType: metricType,
		SlicedBlob: nil,
	}
	slicedBlob, err := proto.Marshal(searchResultData)
	if err != nil {
		return nil, err
	}
	if searchResultData != nil && searchResultData.Ids != nil && typeutil.GetSizeOfIDs(searchResultData.Ids) != 0 {
		searchResults.SlicedBlob = slicedBlob
	}
	return
}

func MergeInternalRetrieveResult(ctx context.Context, retrieveResults []*internalpb.RetrieveResults, param *mergeParam) (*internalpb.RetrieveResults, error) {
	log.Ctx(ctx).Debug("mergeInternelRetrieveResults",
		zap.Int64("limit", param.limit),
		zap.Int("resultNum", len(retrieveResults)),
	)
	if len(retrieveResults) == 1 {
		return retrieveResults[0], nil
	}
	var (
		ret = &internalpb.RetrieveResults{
			Status: merr.Success(),
			Ids:    &schemapb.IDs{},
		}
		skipDupCnt int64
		loopEnd    int
	)

	_, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "MergeInternalRetrieveResult")
	defer sp.End()

	validRetrieveResults := []*TimestampedRetrieveResult[*internalpb.RetrieveResults]{}
	relatedDataSize := int64(0)
	hasMoreResult := false
	for _, r := range retrieveResults {
		ret.AllRetrieveCount += r.GetAllRetrieveCount()
		relatedDataSize += r.GetCostAggregation().GetTotalRelatedDataSize()
		size := typeutil.GetSizeOfIDs(r.GetIds())
		if r == nil || len(r.GetFieldsData()) == 0 || size == 0 {
			continue
		}
		tr, err := NewTimestampedRetrieveResult(r)
		if err != nil {
			return nil, err
		}
		validRetrieveResults = append(validRetrieveResults, tr)
		loopEnd += size
		hasMoreResult = hasMoreResult || r.GetHasMoreResult()
	}
	ret.HasMoreResult = hasMoreResult

	if len(validRetrieveResults) == 0 {
		return ret, nil
	}

	if param.limit != typeutil.Unlimited && !param.mergeStopForBest {
		loopEnd = int(param.limit)
	}

	ret.FieldsData = typeutil.PrepareResultFieldData(validRetrieveResults[0].Result.GetFieldsData(), int64(loopEnd))
	idTsMap := make(map[interface{}]int64)
	cursors := make([]int64, len(validRetrieveResults))

	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for j := 0; j < loopEnd; {
		sel, drainOneResult := typeutil.SelectMinPKWithTimestamp(validRetrieveResults, cursors)
		if sel == -1 || (param.mergeStopForBest && drainOneResult) {
			break
		}

		pk := typeutil.GetPK(validRetrieveResults[sel].GetIds(), cursors[sel])
		ts := validRetrieveResults[sel].Timestamps[cursors[sel]]
		if _, ok := idTsMap[pk]; !ok {
			typeutil.AppendPKs(ret.Ids, pk)
			retSize += typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].Result.GetFieldsData(), cursors[sel])
			idTsMap[pk] = ts
			j++
		} else {
			// primary keys duplicate
			skipDupCnt++
			if ts != 0 && ts > idTsMap[pk] {
				idTsMap[pk] = ts
				typeutil.DeleteFieldData(ret.FieldsData)
				retSize += typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].Result.GetFieldsData(), cursors[sel])
			}
		}

		// limit retrieve result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("query results exceed the maxOutputSize Limit %d", maxOutputSize)
		}

		cursors[sel]++
	}

	if skipDupCnt > 0 {
		log.Debug("skip duplicated query result while reducing internal.RetrieveResults", zap.Int64("dupCount", skipDupCnt))
	}

	requestCosts := lo.FilterMap(retrieveResults, func(result *internalpb.RetrieveResults, _ int) (*internalpb.CostAggregation, bool) {
		if paramtable.Get().QueryNodeCfg.EnableWorkerSQCostMetrics.GetAsBool() {
			return result.GetCostAggregation(), true
		}

		if result.GetBase().GetSourceID() == paramtable.GetNodeID() {
			return result.GetCostAggregation(), true
		}

		return nil, false
	})
	ret.CostAggregation = mergeRequestCost(requestCosts)
	if ret.CostAggregation == nil {
		ret.CostAggregation = &internalpb.CostAggregation{}
	}
	ret.CostAggregation.TotalRelatedDataSize = relatedDataSize
	return ret, nil
}

func getTS(i *internalpb.RetrieveResults, idx int64) uint64 {
	if i.FieldsData == nil {
		return 0
	}
	for _, fieldData := range i.FieldsData {
		fieldID := fieldData.FieldId
		if fieldID == common.TimeStampField {
			res := fieldData.GetScalars().GetLongData().Data
			return uint64(res[idx])
		}
	}
	return 0
}

func MergeSegcoreRetrieveResults(ctx context.Context, retrieveResults []*segcorepb.RetrieveResults, param *mergeParam, segments []Segment, plan *RetrievePlan, manager *Manager) (*segcorepb.RetrieveResults, error) {
	ctx, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "MergeSegcoreResults")
	defer span.End()

	log.Ctx(ctx).Debug("mergeSegcoreRetrieveResults",
		zap.Int64("limit", param.limit),
		zap.Int("resultNum", len(retrieveResults)),
	)
	var (
		ret = &segcorepb.RetrieveResults{
			Ids: &schemapb.IDs{},
		}

		skipDupCnt int64
		loopEnd    int
	)

	validRetrieveResults := []*TimestampedRetrieveResult[*segcorepb.RetrieveResults]{}
	validSegments := make([]Segment, 0, len(segments))
	selectedOffsets := make([][]int64, 0, len(retrieveResults))
	selectedIndexes := make([][]int64, 0, len(retrieveResults))
	hasMoreResult := false
	for i, r := range retrieveResults {
		size := typeutil.GetSizeOfIDs(r.GetIds())
		ret.AllRetrieveCount += r.GetAllRetrieveCount()
		if r == nil || len(r.GetOffset()) == 0 || size == 0 {
			log.Debug("filter out invalid retrieve result")
			continue
		}
		tr, err := NewTimestampedRetrieveResult(r)
		if err != nil {
			return nil, err
		}
		validRetrieveResults = append(validRetrieveResults, tr)
		if plan.ignoreNonPk {
			validSegments = append(validSegments, segments[i])
		}
		selectedOffsets = append(selectedOffsets, make([]int64, 0, len(r.GetOffset())))
		selectedIndexes = append(selectedIndexes, make([]int64, 0, len(r.GetOffset())))
		loopEnd += size
		hasMoreResult = r.GetHasMoreResult() || hasMoreResult
	}
	ret.HasMoreResult = hasMoreResult

	if len(validRetrieveResults) == 0 {
		return ret, nil
	}

	selected := make([]int, 0, ret.GetAllRetrieveCount())

	var limit int = -1
	if param.limit != typeutil.Unlimited && !param.mergeStopForBest {
		limit = int(param.limit)
	}

	ret.FieldsData = typeutil.PrepareResultFieldData(validRetrieveResults[0].Result.GetFieldsData(), int64(loopEnd))
	cursors := make([]int64, len(validRetrieveResults))
	idTsMap := make(map[any]int64, limit*len(validRetrieveResults))

	var availableCount int
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for j := 0; j < loopEnd && (limit == -1 || availableCount < limit); j++ {
		sel, drainOneResult := typeutil.SelectMinPKWithTimestamp(validRetrieveResults, cursors)
		if sel == -1 || (param.mergeStopForBest && drainOneResult) {
			break
		}

		pk := typeutil.GetPK(validRetrieveResults[sel].GetIds(), cursors[sel])
		ts := validRetrieveResults[sel].Timestamps[cursors[sel]]
		if _, ok := idTsMap[pk]; !ok {
			typeutil.AppendPKs(ret.Ids, pk)
			selected = append(selected, sel)
			selectedOffsets[sel] = append(selectedOffsets[sel], validRetrieveResults[sel].Result.GetOffset()[cursors[sel]])
			selectedIndexes[sel] = append(selectedIndexes[sel], cursors[sel])
			idTsMap[pk] = ts
			availableCount++
		} else {
			// primary keys duplicate
			skipDupCnt++
			if ts != 0 && ts > idTsMap[pk] {
				idTsMap[pk] = ts
				selectedOffsets[sel][len(selectedOffsets[sel])-1] = validRetrieveResults[sel].Result.GetOffset()[cursors[sel]]
				selectedIndexes[sel][len(selectedIndexes[sel])-1] = cursors[sel]
			}
		}

		cursors[sel]++
	}

	if skipDupCnt > 0 {
		log.Debug("skip duplicated query result while reducing segcore.RetrieveResults", zap.Int64("dupCount", skipDupCnt))
	}

	if !plan.ignoreNonPk {
		// target entry already retrieved, don't do this after AppendPKs for better performance. Save the cost everytime
		// judge the `!plan.ignoreNonPk` condition.
		_, span2 := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "MergeSegcoreResults-AppendFieldData")
		defer span2.End()
		ret.FieldsData = typeutil.PrepareResultFieldData(validRetrieveResults[0].Result.GetFieldsData(), int64(len(selected)))
		cursors = make([]int64, len(validRetrieveResults))
		for _, sel := range selected {
			// cannot use `cursors[sel]` directly, since some of them may be skipped.
			retSize += typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].Result.GetFieldsData(), selectedIndexes[sel][cursors[sel]])

			// limit retrieve result to avoid oom
			if retSize > maxOutputSize {
				return nil, fmt.Errorf("query results exceed the maxOutputSize Limit %d", maxOutputSize)
			}

			cursors[sel]++
		}
	} else {
		// target entry not retrieved.
		ctx, span2 := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "MergeSegcoreResults-RetrieveByOffsets-AppendFieldData")
		defer span2.End()
		segmentResults := make([]*segcorepb.RetrieveResults, len(validRetrieveResults))
		futures := make([]*conc.Future[any], 0, len(validRetrieveResults))
		for i, offsets := range selectedOffsets {
			if len(offsets) == 0 {
				log.Ctx(ctx).Debug("skip empty retrieve results", zap.Int64("segment", validSegments[i].ID()))
				continue
			}
			idx, theOffsets := i, offsets
			future := GetSQPool().Submit(func() (any, error) {
				var r *segcorepb.RetrieveResults
				var err error
				if err := doOnSegment(ctx, manager, validSegments[idx], func(ctx context.Context, segment Segment) error {
					r, err = segment.RetrieveByOffsets(ctx, plan, theOffsets)
					return err
				}); err != nil {
					return nil, err
				}
				segmentResults[idx] = r
				return nil, nil
			})
			futures = append(futures, future)
		}
		if err := conc.AwaitAll(futures...); err != nil {
			return nil, err
		}

		for _, r := range segmentResults {
			if len(r.GetFieldsData()) != 0 {
				ret.FieldsData = typeutil.PrepareResultFieldData(r.GetFieldsData(), int64(len(selected)))
				break
			}
		}

		_, span3 := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "MergeSegcoreResults-AppendFieldData")
		defer span3.End()
		cursors = make([]int64, len(segmentResults))
		for _, sel := range selected {
			retSize += typeutil.AppendFieldData(ret.FieldsData, segmentResults[sel].GetFieldsData(), cursors[sel])

			// limit retrieve result to avoid oom
			if retSize > maxOutputSize {
				return nil, fmt.Errorf("query results exceed the maxOutputSize Limit %d", maxOutputSize)
			}

			cursors[sel]++
		}
	}

	return ret, nil
}

func mergeInternalRetrieveResultsAndFillIfEmpty(
	ctx context.Context,
	retrieveResults []*internalpb.RetrieveResults,
	param *mergeParam,
) (*internalpb.RetrieveResults, error) {
	mergedResult, err := MergeInternalRetrieveResult(ctx, retrieveResults, param)
	if err != nil {
		return nil, err
	}

	if err := typeutil2.FillRetrieveResultIfEmpty(typeutil2.NewInternalResult(mergedResult), param.outputFieldsId, param.schema); err != nil {
		return nil, fmt.Errorf("failed to fill internal retrieve results: %s", err.Error())
	}

	return mergedResult, nil
}

func mergeSegcoreRetrieveResultsAndFillIfEmpty(
	ctx context.Context,
	retrieveResults []*segcorepb.RetrieveResults,
	param *mergeParam,
	segments []Segment,
	plan *RetrievePlan,
	manager *Manager,
) (*segcorepb.RetrieveResults, error) {
	mergedResult, err := MergeSegcoreRetrieveResults(ctx, retrieveResults, param, segments, plan, manager)
	if err != nil {
		return nil, err
	}

	if err := typeutil2.FillRetrieveResultIfEmpty(typeutil2.NewSegcoreResults(mergedResult), param.outputFieldsId, param.schema); err != nil {
		return nil, fmt.Errorf("failed to fill segcore retrieve results: %s", err.Error())
	}

	return mergedResult, nil
}
