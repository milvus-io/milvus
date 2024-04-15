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

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ typeutil.ResultWithID = &internalpb.RetrieveResults{}

var _ typeutil.ResultWithID = &segcorepb.RetrieveResults{}

func ReduceSearchResults(ctx context.Context, results []*internalpb.SearchResults, nq int64, topk int64, metricType string) (*internalpb.SearchResults, error) {
	results = lo.Filter(results, func(result *internalpb.SearchResults, _ int) bool {
		return result != nil && result.GetSlicedBlob() != nil
	})

	if len(results) == 1 {
		return results[0], nil
	}

	channelsMvcc := make(map[string]uint64)
	for _, r := range results {
		for ch, ts := range r.GetChannelsMvcc() {
			channelsMvcc[ch] = ts
		}
		// shouldn't let new SearchResults.MetricType to be empty, though the req.MetricType is empty
		if metricType == "" {
			metricType = r.MetricType
		}
	}
	log := log.Ctx(ctx)

	searchResultData, err := DecodeSearchResults(results)
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

	reducedResultData, err := ReduceSearchResultData(ctx, searchResultData, nq, topk)
	if err != nil {
		log.Warn("shard leader reduce errors", zap.Error(err))
		return nil, err
	}
	searchResults, err := EncodeSearchResultData(reducedResultData, nq, topk, metricType)
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

func ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, nq int64, topk int64) (*schemapb.SearchResultData, error) {
	log := log.Ctx(ctx)

	if len(searchResultData) == 0 {
		return &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topk,
			FieldsData: make([]*schemapb.FieldData, 0),
			Scores:     make([]float32, 0),
			Ids:        &schemapb.IDs{},
			Topks:      make([]int64, 0),
		}, nil
	}
	ret := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topk,
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0),
	}

	resultOffsets := make([][]int64, len(searchResultData))
	for i := 0; i < len(searchResultData); i++ {
		resultOffsets[i] = make([]int64, len(searchResultData[i].Topks))
		for j := int64(1); j < nq; j++ {
			resultOffsets[i][j] = resultOffsets[i][j-1] + searchResultData[i].Topks[j-1]
		}
		ret.AllSearchCount += searchResultData[i].GetAllSearchCount()
	}

	var skipDupCnt int64
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for i := int64(0); i < nq; i++ {
		offsets := make([]int64, len(searchResultData))

		idSet := make(map[interface{}]struct{})
		groupByValueSet := make(map[interface{}]struct{})
		var j int64
		for j = 0; j < topk; {
			sel := SelectSearchResultData(searchResultData, resultOffsets, offsets, i)
			if sel == -1 {
				break
			}
			idx := resultOffsets[sel][i] + offsets[sel]

			id := typeutil.GetPK(searchResultData[sel].GetIds(), idx)
			groupByVal := typeutil.GetData(searchResultData[sel].GetGroupByFieldValue(), int(idx))
			score := searchResultData[sel].Scores[idx]

			// remove duplicates
			if _, ok := idSet[id]; !ok {
				groupByValExist := false
				if groupByVal != nil {
					_, groupByValExist = groupByValueSet[groupByVal]
				}
				if !groupByValExist {
					retSize += typeutil.AppendFieldData(ret.FieldsData, searchResultData[sel].FieldsData, idx)
					typeutil.AppendPKs(ret.Ids, id)
					ret.Scores = append(ret.Scores, score)
					if groupByVal != nil {
						groupByValueSet[groupByVal] = struct{}{}
						if err := typeutil.AppendGroupByValue(ret, groupByVal, searchResultData[sel].GetGroupByFieldValue().GetType()); err != nil {
							log.Error("Failed to append groupByValues", zap.Error(err))
							return ret, err
						}
					}
					idSet[id] = struct{}{}
					j++
				}
			} else {
				// skip entity with same id
				skipDupCnt++
			}
			offsets[sel]++
		}

		// if realTopK != -1 && realTopK != j {
		// 	log.Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
		// 	// return nil, errors.New("the length (topk) between all result of query is different")
		// }
		ret.Topks = append(ret.Topks, j)

		// limit search result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}
	log.Debug("skip duplicated search result", zap.Int64("count", skipDupCnt))
	return ret, nil
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

func DecodeSearchResults(searchResults []*internalpb.SearchResults) ([]*schemapb.SearchResultData, error) {
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

func EncodeSearchResultData(searchResultData *schemapb.SearchResultData, nq int64, topk int64, metricType string) (searchResults *internalpb.SearchResults, err error) {
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

	validRetrieveResults := []*internalpb.RetrieveResults{}
	relatedDataSize := int64(0)
	for _, r := range retrieveResults {
		ret.AllRetrieveCount += r.GetAllRetrieveCount()
		relatedDataSize += r.GetCostAggregation().GetTotalRelatedDataSize()
		size := typeutil.GetSizeOfIDs(r.GetIds())
		if r == nil || len(r.GetFieldsData()) == 0 || size == 0 {
			continue
		}
		validRetrieveResults = append(validRetrieveResults, r)
		loopEnd += size
	}

	if len(validRetrieveResults) == 0 {
		return ret, nil
	}

	if param.limit != typeutil.Unlimited && !param.mergeStopForBest {
		loopEnd = int(param.limit)
	}

	ret.FieldsData = make([]*schemapb.FieldData, len(validRetrieveResults[0].GetFieldsData()))
	idTsMap := make(map[interface{}]uint64)
	cursors := make([]int64, len(validRetrieveResults))

	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for j := 0; j < loopEnd; {
		sel, drainOneResult := typeutil.SelectMinPK(validRetrieveResults, cursors)
		if sel == -1 || (param.mergeStopForBest && drainOneResult) {
			break
		}

		pk := typeutil.GetPK(validRetrieveResults[sel].GetIds(), cursors[sel])
		ts := getTS(validRetrieveResults[sel], cursors[sel])
		if _, ok := idTsMap[pk]; !ok {
			typeutil.AppendPKs(ret.Ids, pk)
			retSize += typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].GetFieldsData(), cursors[sel])
			idTsMap[pk] = ts
			j++
		} else {
			// primary keys duplicate
			skipDupCnt++
			if ts != 0 && ts > idTsMap[pk] {
				idTsMap[pk] = ts
				typeutil.DeleteFieldData(ret.FieldsData)
				retSize += typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].GetFieldsData(), cursors[sel])
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

func MergeSegcoreRetrieveResults(ctx context.Context, retrieveResults []*segcorepb.RetrieveResults, param *mergeParam) (*segcorepb.RetrieveResults, error) {
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

	validRetrieveResults := []*segcorepb.RetrieveResults{}
	for _, r := range retrieveResults {
		size := typeutil.GetSizeOfIDs(r.GetIds())
		ret.AllRetrieveCount += r.GetAllRetrieveCount()
		if r == nil || len(r.GetOffset()) == 0 || size == 0 {
			log.Debug("filter out invalid retrieve result")
			continue
		}
		validRetrieveResults = append(validRetrieveResults, r)
		loopEnd += size
	}

	if len(validRetrieveResults) == 0 {
		return ret, nil
	}

	var limit int = -1
	if param.limit != typeutil.Unlimited && !param.mergeStopForBest {
		limit = int(param.limit)
	}

	ret.FieldsData = make([]*schemapb.FieldData, len(validRetrieveResults[0].GetFieldsData()))
	idSet := make(map[interface{}]struct{})
	cursors := make([]int64, len(validRetrieveResults))

	var availableCount int
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for j := 0; j < loopEnd && (limit == -1 || availableCount < limit); j++ {
		sel, drainOneResult := typeutil.SelectMinPK(validRetrieveResults, cursors)
		if sel == -1 || (param.mergeStopForBest && drainOneResult) {
			break
		}

		pk := typeutil.GetPK(validRetrieveResults[sel].GetIds(), cursors[sel])
		if _, ok := idSet[pk]; !ok {
			typeutil.AppendPKs(ret.Ids, pk)
			retSize += typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].GetFieldsData(), cursors[sel])
			idSet[pk] = struct{}{}
			availableCount++
		} else {
			// primary keys duplicate
			skipDupCnt++
		}

		// limit retrieve result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("query results exceed the maxOutputSize Limit %d", maxOutputSize)
		}

		cursors[sel]++
	}

	if skipDupCnt > 0 {
		log.Debug("skip duplicated query result while reducing segcore.RetrieveResults", zap.Int64("dupCount", skipDupCnt))
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
) (*segcorepb.RetrieveResults, error) {
	mergedResult, err := MergeSegcoreRetrieveResults(ctx, retrieveResults, param)
	if err != nil {
		return nil, err
	}

	if err := typeutil2.FillRetrieveResultIfEmpty(typeutil2.NewSegcoreResults(mergedResult), param.outputFieldsId, param.schema); err != nil {
		return nil, fmt.Errorf("failed to fill segcore retrieve results: %s", err.Error())
	}

	return mergedResult, nil
}
