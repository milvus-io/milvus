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
	"math"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ typeutil.ResultWithID = &internalpb.RetrieveResults{}

var _ typeutil.ResultWithID = &segcorepb.RetrieveResults{}

func ReduceSearchOnQueryNode(ctx context.Context, results []*internalpb.SearchResults, info *reduce.ResultInfo) (*internalpb.SearchResults, error) {
	if info.GetIsAdvance() {
		return ReduceAdvancedSearchResults(ctx, results)
	}
	return ReduceSearchResults(ctx, results, info)
}

func ReduceSearchResults(ctx context.Context, results []*internalpb.SearchResults, info *reduce.ResultInfo) (*internalpb.SearchResults, error) {
	results = lo.Filter(results, func(result *internalpb.SearchResults, _ int) bool {
		return result != nil && (result.GetSlicedBlob() != nil || result.GetResultData() != nil)
	})

	if len(results) == 1 {
		log.Debug("Shortcut return ReduceSearchResults", zap.Any("result info", info))
		return results[0], nil
	}

	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "ReduceSearchResults")
	defer sp.End()

	channelsMvcc := make(map[string]uint64)
	isTopkReduce := false
	isRecallEvaluation := false
	for _, r := range results {
		for ch, ts := range r.GetChannelsMvcc() {
			channelsMvcc[ch] = ts
		}
		if r.GetIsTopkReduce() {
			isTopkReduce = true
		}
		if r.GetIsRecallEvaluation() {
			isRecallEvaluation = true
		}
		// shouldn't let new SearchResults.MetricType to be empty, though the req.MetricType is empty
		if info.GetMetricType() == "" {
			info.SetMetricType(r.MetricType)
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
			zap.Int64("topk", sData.TopK),
			zap.Int("ids.len", typeutil.GetSizeOfIDs(sData.Ids)),
			zap.Int("fieldsData.len", len(sData.FieldsData)))
	}

	searchReduce := InitSearchReducer(info)
	reducedResultData, err := searchReduce.ReduceSearchResultData(ctx, searchResultData, info)
	if err != nil {
		log.Warn("shard leader reduce errors", zap.Error(err))
		return nil, err
	}
	searchResults, err := EncodeSearchResultData(ctx, reducedResultData, info.GetNq(), info.GetTopK(), info.GetMetricType())
	if err != nil {
		log.Warn("shard leader encode search result errors", zap.Error(err))
		return nil, err
	}

	requestCosts := lo.FilterMap(results, func(result *internalpb.SearchResults, _ int) (*internalpb.CostAggregation, bool) {
		// delegator node won't be used to load sealed segment if stream node is enabled
		// and if growing segment doesn't exists, delegator won't produce any cost metrics
		// so we deprecate the EnableWorkerSQCostMetrics param
		return result.GetCostAggregation(), true
	})
	searchResults.CostAggregation = mergeRequestCost(requestCosts)
	if searchResults.CostAggregation == nil {
		searchResults.CostAggregation = &internalpb.CostAggregation{}
	}
	relatedDataSize := lo.Reduce(results, func(acc int64, result *internalpb.SearchResults, _ int) int64 {
		return acc + result.GetCostAggregation().GetTotalRelatedDataSize()
	}, 0)
	storageCost := lo.Reduce(results, func(acc segcore.StorageCost, result *internalpb.SearchResults, _ int) segcore.StorageCost {
		acc.ScannedRemoteBytes += result.GetScannedRemoteBytes()
		acc.ScannedTotalBytes += result.GetScannedTotalBytes()
		return acc
	}, segcore.StorageCost{})
	searchResults.CostAggregation.TotalRelatedDataSize = relatedDataSize
	searchResults.ChannelsMvcc = channelsMvcc
	searchResults.IsTopkReduce = isTopkReduce
	searchResults.IsRecallEvaluation = isRecallEvaluation
	searchResults.ScannedRemoteBytes = storageCost.ScannedRemoteBytes
	searchResults.ScannedTotalBytes = storageCost.ScannedTotalBytes
	return searchResults, nil
}

func ReduceAdvancedSearchResults(ctx context.Context, results []*internalpb.SearchResults) (*internalpb.SearchResults, error) {
	_, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "ReduceAdvancedSearchResults")
	defer sp.End()

	channelsMvcc := make(map[string]uint64)
	relatedDataSize := int64(0)
	isTopkReduce := false
	searchResults := &internalpb.SearchResults{
		IsAdvanced: true,
	}
	storageCost := segcore.StorageCost{}
	for index, result := range results {
		if result.GetIsTopkReduce() {
			isTopkReduce = true
		}
		relatedDataSize += result.GetCostAggregation().GetTotalRelatedDataSize()
		storageCost.ScannedRemoteBytes += result.GetScannedRemoteBytes()
		storageCost.ScannedTotalBytes += result.GetScannedTotalBytes()
		for ch, ts := range result.GetChannelsMvcc() {
			channelsMvcc[ch] = ts
		}
		searchResults.NumQueries = result.GetNumQueries()
		// we just append here, no need to split subResult and reduce
		// defer this reduction to proxy
		subResult := &internalpb.SubSearchResults{
			MetricType:     result.GetMetricType(),
			NumQueries:     result.GetNumQueries(),
			TopK:           result.GetTopK(),
			SlicedBlob:     result.GetSlicedBlob(),
			ResultData:     result.GetResultData(),
			SlicedNumCount: result.GetSlicedNumCount(),
			SlicedOffset:   result.GetSlicedOffset(),
			ReqIndex:       int64(index),
		}
		searchResults.SubResults = append(searchResults.SubResults, subResult)
	}
	searchResults.ChannelsMvcc = channelsMvcc
	requestCosts := lo.FilterMap(results, func(result *internalpb.SearchResults, _ int) (*internalpb.CostAggregation, bool) {
		// delegator node won't be used to load sealed segment if stream node is enabled
		// and if growing segment doesn't exists, delegator won't produce any cost metrics
		// so we deprecate the EnableWorkerSQCostMetrics param
		return result.GetCostAggregation(), true
	})
	searchResults.CostAggregation = mergeRequestCost(requestCosts)
	if searchResults.CostAggregation == nil {
		searchResults.CostAggregation = &internalpb.CostAggregation{}
	}
	searchResults.CostAggregation.TotalRelatedDataSize = relatedDataSize
	searchResults.IsTopkReduce = isTopkReduce
	searchResults.ScannedRemoteBytes = storageCost.ScannedRemoteBytes
	searchResults.ScannedTotalBytes = storageCost.ScannedTotalBytes
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
		if partialSearchResult.ResultData != nil {
			// Pre-decoded by delegator — use directly, no unmarshal needed.
			results = append(results, partialSearchResult.ResultData)
		} else if partialSearchResult.SlicedBlob != nil {
			var partialResultData schemapb.SearchResultData
			err := proto.Unmarshal(partialSearchResult.SlicedBlob, &partialResultData)
			if err != nil {
				return nil, err
			}
			results = append(results, &partialResultData)
		}
	}
	return results, nil
}

func EncodeSearchResultData(ctx context.Context, searchResultData *schemapb.SearchResultData,
	nq int64, topk int64, metricType string,
) (searchResults *internalpb.SearchResults, err error) {
	_, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "EncodeSearchResultData")
	defer sp.End()

	searchResults = &internalpb.SearchResults{
		Status:     merr.Success(),
		NumQueries: nq,
		TopK:       topk,
		MetricType: metricType,
	}

	hasData := searchResultData != nil && searchResultData.Ids != nil && typeutil.GetSizeOfIDs(searchResultData.Ids) != 0
	if hasData && paramtable.Get().QueryNodeCfg.EnableResultZeroCopy.GetAsBool() {
		// New path: embed struct directly, skip marshal
		searchResults.ResultData = searchResultData
	} else if hasData {
		// Legacy path: marshal to SlicedBlob
		slicedBlob, err := proto.Marshal(searchResultData)
		if err != nil {
			return nil, err
		}
		searchResults.SlicedBlob = slicedBlob
	}
	return
}
