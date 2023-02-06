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

package querynodev2

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func loadGrowingSegments(ctx context.Context, delegator delegator.ShardDelegator, req *querypb.WatchDmChannelsRequest) error {
	// load growing segments
	growingSegments := make([]*querypb.SegmentLoadInfo, 0, len(req.Infos))
	for _, info := range req.Infos {
		for _, segmentID := range info.GetUnflushedSegmentIds() {
			// unFlushed segment may not have binLogs, skip loading
			segmentInfo := req.GetSegmentInfos()[segmentID]
			if segmentInfo == nil {
				log.Warn("an unflushed segment is not found in segment infos", zap.Int64("segment ID", segmentID))
				continue
			}
			if len(segmentInfo.GetBinlogs()) > 0 {
				growingSegments = append(growingSegments, &querypb.SegmentLoadInfo{
					SegmentID:     segmentInfo.ID,
					PartitionID:   segmentInfo.PartitionID,
					CollectionID:  segmentInfo.CollectionID,
					BinlogPaths:   segmentInfo.Binlogs,
					NumOfRows:     segmentInfo.NumOfRows,
					Statslogs:     segmentInfo.Statslogs,
					Deltalogs:     segmentInfo.Deltalogs,
					InsertChannel: segmentInfo.InsertChannel,
				})
			} else {
				log.Info("skip segment which binlog is empty", zap.Int64("segmentID", segmentInfo.ID))
			}
		}
	}

	return delegator.LoadGrowing(ctx, growingSegments, req.GetVersion())
}

func (node *QueryNode) loadDeltaLogs(ctx context.Context, req *querypb.LoadSegmentsRequest) *commonpb.Status {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	loadedSegments := make([]int64, 0, len(req.GetInfos()))
	var finalErr error
	for _, info := range req.GetInfos() {
		segment := node.manager.Segment.GetSealed(info.GetSegmentID())
		if segment == nil {
			continue
		}

		local := segment.(*segments.LocalSegment)
		err := node.loader.LoadDeltaLogs(ctx, local, info.GetDeltalogs())
		if err != nil {
			if finalErr == nil {
				finalErr = err
			}
			continue
		}

		loadedSegments = append(loadedSegments, info.GetSegmentID())
	}

	if finalErr != nil {
		log.Warn("failed to load delta logs", zap.Error(finalErr))
		return util.WrapStatus(commonpb.ErrorCode_UnexpectedError, "failed to load delta logs", finalErr)
	}

	return util.SuccessStatus()
}

func (node *QueryNode) queryChannel(ctx context.Context, req *querypb.QueryRequest, channel string) (*internalpb.RetrieveResults, error) {
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.TotalLabel).Inc()
	failRet := WrapRetrieveResult(commonpb.ErrorCode_UnexpectedError, "")
	msgID := req.Req.Base.GetMsgID()
	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", msgID),
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)

	defer func() {
		if failRet.Status.ErrorCode != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.FailLabel).Inc()
		}
	}()

	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		failRet.Status.Reason = msgQueryNodeIsUnhealthy(paramtable.GetNodeID())
		return failRet, nil
	}
	defer node.lifetime.Done()

	log.Debug("start do query with channel",
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)
	// add cancel when error occurs
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	//TODO From Shard Delegator
	if req.FromShardLeader {
		tr := timerecord.NewTimeRecorder("queryChannel")
		results, err := node.querySegments(queryCtx, req)
		if err != nil {
			log.Warn("failed to query channel", zap.Error(err))
			failRet.Status.Reason = err.Error()
			return failRet, nil
		}

		tr.CtxElapse(ctx, fmt.Sprintf("do query done, traceID = %s, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
			traceID,
			req.GetFromShardLeader(),
			channel,
			req.GetSegmentIDs(),
		))

		failRet.Status.ErrorCode = commonpb.ErrorCode_Success
		//TODO QueryNodeSQLatencyInQueue QueryNodeReduceLatency
		latency := tr.ElapseSpan()
		metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
		metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel).Inc()
		return results, nil
	}
	//From Proxy
	tr := timerecord.NewTimeRecorder("queryDelegator")
	//get delegator
	sd, ok := node.delegators.Get(channel)
	if !ok {
		log.Warn("Query failed, failed to get query shard delegator", zap.Error(ErrGetDelegatorFailed))
		failRet.Status.Reason = ErrGetDelegatorFailed.Error()
		return failRet, nil
	}

	// do query
	results, err := sd.Query(queryCtx, req)
	if err != nil {
		log.Warn("failed to query on delegator", zap.Error(err))
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	// reduce result
	tr.CtxElapse(ctx, fmt.Sprintf("start reduce query result, traceID = %s, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		traceID,
		req.GetFromShardLeader(),
		channel,
		req.GetSegmentIDs(),
	))

	collection := node.manager.Collection.Get(req.Req.GetCollectionID())
	if collection == nil {
		log.Warn("Query failed, failed to get collection")
		failRet.Status.Reason = segments.WrapCollectionNotFound(req.Req.CollectionID).Error()
		return failRet, nil
	}

	ret, err := segments.MergeInternalRetrieveResultsAndFillIfEmpty(ctx, results, req.Req.GetLimit(), req.GetReq().GetOutputFieldsId(), collection.Schema())
	if err != nil {
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do query with channel done , vChannel = %s, segmentIDs = %v",
		channel,
		req.GetSegmentIDs(),
	))

	//
	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.Leader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel).Inc()
	return ret, nil
}

func (node *QueryNode) querySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	collection := node.manager.Collection.Get(req.Req.GetCollectionID())
	if collection == nil {
		return nil, segments.ErrCollectionNotFound
	}
	//build plan
	retrievePlan, err := segments.NewRetrievePlan(
		collection,
		req.Req.GetSerializedExprPlan(),
		req.Req.GetTravelTimestamp(),
		req.Req.Base.GetMsgID(),
	)
	if err != nil {
		return nil, err
	}
	defer retrievePlan.Delete()

	var results []*segcorepb.RetrieveResults
	if req.GetScope() == querypb.DataScope_Historical {
		results, _, _, err = segments.RetrieveHistorical(ctx, node.manager, retrievePlan, req.Req.CollectionID, nil, req.GetSegmentIDs(), node.cacheChunkManager)
	} else {
		results, _, _, err = segments.RetrieveStreaming(ctx, node.manager, retrievePlan, req.Req.CollectionID, nil, req.GetSegmentIDs(), node.cacheChunkManager)
	}
	if err != nil {
		return nil, err
	}

	reducedResult, err := segments.MergeSegcoreRetrieveResultsAndFillIfEmpty(ctx, results, req.Req.GetLimit(), req.Req.GetOutputFieldsId(), collection.Schema())
	if err != nil {
		return nil, err
	}

	return &internalpb.RetrieveResults{
		Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Ids:        reducedResult.Ids,
		FieldsData: reducedResult.FieldsData,
	}, nil
}

func (node *QueryNode) searchChannel(ctx context.Context, req *querypb.SearchRequest, channel string) (*internalpb.SearchResults, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Int64("collectionID", req.Req.GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)
	failRet := &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()

	//update metric to prometheus
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.TotalLabel).Inc()
	defer func() {
		if failRet.Status.ErrorCode != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.FailLabel).Inc()
		}
	}()

	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		failRet.Status.Reason = msgQueryNodeIsUnhealthy(paramtable.GetNodeID())
		return failRet, nil
	}
	defer node.lifetime.Done()

	log.Debug("start to search channel",
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)
	searchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	//TODO From Shard Delegator
	if req.GetFromShardLeader() {
		tr := timerecord.NewTimeRecorder("searchChannel")
		log.Debug("search channel...")

		results, err := node.searchSegments(searchCtx, req)
		if err != nil {
			log.Warn("failed to search channel", zap.Error(err))
			failRet.Status.Reason = err.Error()
			return failRet, nil
		}

		tr.CtxElapse(ctx, fmt.Sprintf("search channel done, channel = %s, segmentIDs = %v",
			channel,
			req.GetSegmentIDs(),
		))

		failRet.Status.ErrorCode = commonpb.ErrorCode_Success
		//TODO QueryNodeSQLatencyInQueue QueryNodeReduceLatency
		latency := tr.ElapseSpan()
		metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
		metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.SuccessLabel).Inc()
		return results, nil
	}

	//From Proxy
	tr := timerecord.NewTimeRecorder("searchDelegator")
	//get delegator
	sd, ok := node.delegators.Get(channel)
	if !ok {
		log.Warn("Query failed, failed to get query shard delegator", zap.Error(ErrGetDelegatorFailed))
		failRet.Status.Reason = ErrGetDelegatorFailed.Error()
		return failRet, nil
	}
	//do search
	results, err := sd.Search(searchCtx, req)
	if err != nil {
		log.Warn("failed to search on delegator", zap.Error(err))
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	// reduce result
	tr.CtxElapse(ctx, fmt.Sprintf("start reduce query result, traceID = %s, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		traceID,
		req.GetFromShardLeader(),
		channel,
		req.GetSegmentIDs(),
	))

	ret, err := segments.ReduceSearchResults(ctx, results, req.Req.GetNq(), req.Req.GetTopk(), req.Req.GetMetricType())
	if err != nil {
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do search with channel done , vChannel = %s, segmentIDs = %v",
		channel,
		req.GetSegmentIDs(),
	))

	//update metric to prometheus
	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.Leader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.SuccessLabel).Inc()
	metrics.QueryNodeSearchNQ.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(req.Req.GetNq()))
	metrics.QueryNodeSearchTopK.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(req.Req.GetTopk()))

	return ret, nil
}

func (node *QueryNode) searchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	log := log.Ctx(ctx)

	collection := node.manager.Collection.Get(req.Req.GetCollectionID())
	if collection == nil {
		return nil, segments.ErrCollectionNotFound
	}

	searchReq, err := segments.NewSearchRequest(collection, req, req.Req.GetPlaceholderGroup())
	if err != nil {
		return nil, err
	}
	defer searchReq.Delete()

	var results []*segments.SearchResult
	if req.GetScope() == querypb.DataScope_Historical {
		results, _, _, err = segments.SearchHistorical(ctx, node.manager, searchReq, req.Req.GetCollectionID(), nil, req.GetSegmentIDs())
	} else if req.GetScope() == querypb.DataScope_Streaming {
		results, _, _, err = segments.SearchStreaming(ctx, node.manager, searchReq, req.Req.GetCollectionID(), nil, req.GetSegmentIDs())
	}
	if err != nil {
		return nil, err
	}
	defer segments.DeleteSearchResults(results)

	if len(results) == 0 {
		return &internalpb.SearchResults{
			Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			MetricType:     req.GetReq().GetMetricType(),
			NumQueries:     req.GetReq().GetNq(),
			TopK:           req.GetReq().GetTopk(),
			SlicedBlob:     nil,
			SlicedOffset:   1,
			SlicedNumCount: 1,
		}, nil
	}

	blobs, err := segments.ReduceSearchResultsAndFillData(
		searchReq.Plan(),
		results,
		int64(len(results)),
		[]int64{req.GetReq().GetNq()},
		[]int64{req.GetReq().GetTopk()},
	)
	if err != nil {
		log.Warn("failed to reduce search results", zap.Error(err))
		return nil, err
	}
	defer segments.DeleteSearchResultDataBlobs(blobs)

	blob, err := segments.GetSearchResultDataBlob(blobs, 0)
	if err != nil {
		return nil, err
	}

	//tips:blob is unsafe because get from C
	bs := make([]byte, len(blob))
	copy(bs, blob)

	return &internalpb.SearchResults{
		Status:         util.WrapStatus(commonpb.ErrorCode_Success, ""),
		MetricType:     req.GetReq().GetMetricType(),
		NumQueries:     req.GetReq().GetNq(),
		TopK:           req.GetReq().GetTopk(),
		SlicedBlob:     bs,
		SlicedOffset:   1,
		SlicedNumCount: 1,
	}, nil
}

func (node *QueryNode) getChannelStatistics(ctx context.Context, req *querypb.GetStatisticsRequest, channel string) (*internalpb.GetStatisticsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.Req.GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)
	failRet := &internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if req.GetFromShardLeader() {
		var results []segments.SegmentStats
		var err error

		switch req.GetScope() {
		case querypb.DataScope_Historical:
			results, _, _, err = segments.StatisticsHistorical(ctx, node.manager, req.Req.GetCollectionID(), req.Req.GetPartitionIDs(), req.GetSegmentIDs())
		case querypb.DataScope_Streaming:
			results, _, _, err = segments.StatisticStreaming(ctx, node.manager, req.Req.GetCollectionID(), req.Req.GetPartitionIDs(), req.GetSegmentIDs())
		}

		if err != nil {
			log.Warn("get segments statistics failed", zap.Error(err))
			return nil, err
		}
		return segmentStatsResponse(results), nil
	}

	sd, ok := node.delegators.Get(channel)
	if !ok {
		log.Warn("GetStatistics failed, failed to get query shard delegator")
		return failRet, nil
	}

	results, err := sd.GetStatistics(ctx, req)
	if err != nil {
		log.Warn("failed to get statistics from delegator", zap.Error(err))
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}
	ret, err := reduceStatisticResponse(results)
	if err != nil {
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	return ret, nil
}

func segmentStatsResponse(segStats []segments.SegmentStats) *internalpb.GetStatisticsResponse {
	var totalRowNum int64
	for _, stats := range segStats {
		totalRowNum += stats.RowCount
	}

	resultMap := make(map[string]string)
	resultMap["row_count"] = strconv.FormatInt(totalRowNum, 10)

	ret := &internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Stats:  funcutil.Map2KeyValuePair(resultMap),
	}
	return ret
}

func reduceStatisticResponse(results []*internalpb.GetStatisticsResponse) (*internalpb.GetStatisticsResponse, error) {
	mergedResults := map[string]interface{}{
		"row_count": int64(0),
	}
	fieldMethod := map[string]func(string) error{
		"row_count": func(str string) error {
			count, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			mergedResults["row_count"] = mergedResults["row_count"].(int64) + count
			return nil
		},
	}

	for _, partialResult := range results {
		for _, pair := range partialResult.Stats {
			fn, ok := fieldMethod[pair.Key]
			if !ok {
				return nil, fmt.Errorf("unknown statistic field: %s", pair.Key)
			}
			if err := fn(pair.Value); err != nil {
				return nil, err
			}
		}
	}

	stringMap := make(map[string]string)
	for k, v := range mergedResults {
		stringMap[k] = fmt.Sprint(v)
	}

	ret := &internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Stats:  funcutil.Map2KeyValuePair(stringMap),
	}
	return ret, nil
}
