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

package querynode

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// GetComponentStates returns information about whether the node is healthy
func (node *QueryNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	stats := &internalpb.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	code, ok := node.stateCode.Load().(internalpb.StateCode)
	if !ok {
		errMsg := "unexpected error in type assertion"
		stats.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    errMsg,
		}
		return stats, nil
	}
	nodeID := common.NotRegisteredID
	if node.session != nil && node.session.Registered() {
		nodeID = node.session.ServerID
	}
	info := &internalpb.ComponentInfo{
		NodeID:    nodeID,
		Role:      typeutil.QueryNodeRole,
		StateCode: code,
	}
	stats.State = info
	log.Debug("Get QueryNode component state done", zap.Any("stateCode", info.StateCode))
	return stats, nil
}

// GetTimeTickChannel returns the time tick channel
// TimeTickChannel contains many time tick messages, which will be sent by query nodes
func (node *QueryNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.CommonCfg.QueryCoordTimeTick,
	}, nil
}

// GetStatisticsChannel returns the statistics channel
// Statistics channel contains statistics infos of query nodes, such as segment infos, memory infos
func (node *QueryNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (node *QueryNode) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	log.Debug("received GetStatisticsRequest",
		zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Strings("vChannels", req.GetDmlChannels()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))

	failRet := &internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	toReduceResults := make([]*internalpb.GetStatisticsResponse, 0)
	runningGp, runningCtx := errgroup.WithContext(ctx)
	mu := &sync.Mutex{}
	for _, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.GetStatisticsRequest{
			Req:             req.Req,
			DmlChannels:     []string{ch},
			SegmentIDs:      req.SegmentIDs,
			FromShardLeader: req.FromShardLeader,
			Scope:           req.Scope,
		}
		runningGp.Go(func() error {
			ret, err := node.getStatisticsWithDmlChannel(runningCtx, req, ch)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failRet.Status.Reason = err.Error()
				failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
				return err
			}
			if ret.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				failRet.Status.Reason = ret.Status.Reason
				failRet.Status.ErrorCode = ret.Status.ErrorCode
				return fmt.Errorf("%s", ret.Status.Reason)
			}
			toReduceResults = append(toReduceResults, ret)
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		return failRet, nil
	}
	ret, err := reduceStatisticResponse(toReduceResults)
	if err != nil {
		failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}
	return ret, nil
}

func (node *QueryNode) getStatisticsWithDmlChannel(ctx context.Context, req *queryPb.GetStatisticsRequest, dmlChannel string) (*internalpb.GetStatisticsResponse, error) {
	failRet := &internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if !node.isHealthy() {
		failRet.Status.Reason = msgQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())
		return failRet, nil
	}

	msgID := req.GetReq().GetBase().GetMsgID()
	log.Debug("received GetStatisticRequest",
		zap.Int64("msgID", msgID),
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.String("vChannel", dmlChannel),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))

	if node.queryShardService == nil {
		failRet.Status.Reason = "queryShardService is nil"
		return failRet, nil
	}

	qs, err := node.queryShardService.getQueryShard(dmlChannel)
	if err != nil {
		log.Warn("get statistics failed, failed to get query shard",
			zap.Int64("msgID", msgID),
			zap.String("dml channel", dmlChannel),
			zap.Error(err))
		failRet.Status.ErrorCode = commonpb.ErrorCode_NotShardLeader
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	log.Debug("start do statistics",
		zap.Int64("msgID", msgID),
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.String("vChannel", dmlChannel),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()))
	tr := timerecord.NewTimeRecorder("")

	waitCanDo := func(ctx context.Context) error {
		l := node.tSafeReplica.WatchChannel(dmlChannel)
		defer l.Unregister()
		for {
			select {
			case <-l.On():
				serviceTime, err := qs.getServiceableTime(dmlChannel)
				if err != nil {
					return err
				}
				guaranteeTs := req.GetReq().GetGuaranteeTimestamp()
				if guaranteeTs <= serviceTime {
					return nil
				}
			case <-ctx.Done():
				return errors.New("get statistics context timeout")
			}
		}
	}

	if req.FromShardLeader {
		historicalTask := newStatistics(ctx, req, querypb.DataScope_Historical, qs, waitCanDo)
		err := historicalTask.Execute(ctx)
		if err != nil {
			failRet.Status.Reason = err.Error()
			return failRet, nil
		}

		tr.Elapse(fmt.Sprintf("do statistics done, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
			msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))
		failRet.Status.ErrorCode = commonpb.ErrorCode_Success
		return historicalTask.Ret, nil
	}

	// from Proxy

	cluster, ok := qs.clusterService.getShardCluster(dmlChannel)
	if !ok {
		failRet.Status.ErrorCode = commonpb.ErrorCode_NotShardLeader
		failRet.Status.Reason = fmt.Sprintf("channel %s leader is not here", dmlChannel)
		return failRet, nil
	}

	statisticCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var results []*internalpb.GetStatisticsResponse
	var streamingResult *internalpb.GetStatisticsResponse
	var errCluster error

	withStreaming := func(ctx context.Context) error {
		streamingTask := newStatistics(ctx, req, querypb.DataScope_Streaming, qs, waitCanDo)
		err := streamingTask.Execute(ctx)
		if err != nil {
			return err
		}
		streamingResult = streamingTask.Ret
		return nil
	}

	// shard leader dispatches request to its shard cluster
	results, errCluster = cluster.GetStatistics(statisticCtx, req, withStreaming)
	if errCluster != nil {
		log.Warn("get statistics on cluster failed", zap.Int64("msgID", msgID), zap.Int64("collectionID", req.Req.GetCollectionID()), zap.Error(errCluster))
		failRet.Status.Reason = errCluster.Error()
		return failRet, nil
	}

	tr.Elapse(fmt.Sprintf("start reduce statistic result, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	results = append(results, streamingResult)
	ret, err := reduceStatisticResponse(results)
	if err != nil {
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}
	log.Debug("reduce statistic result done", zap.Int64("msgID", msgID), zap.Any("results", ret))

	tr.Elapse(fmt.Sprintf("do statistics done, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	return ret, nil
}

// WatchDmChannels create consumers on dmChannels to receive Incremental data，which is the important part of real-time query
func (node *QueryNode) WatchDmChannels(ctx context.Context, in *queryPb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		err := fmt.Errorf("query node %d is not ready", Params.QueryNodeCfg.GetNodeID())
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	dct := &watchDmChannelsTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:  in,
		node: node,
	}

	err := node.scheduler.queue.Enqueue(dct)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		log.Warn(err.Error())
		return status, nil
	}
	log.Info("watchDmChannelsTask Enqueue done", zap.Int64("collectionID", in.CollectionID), zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()), zap.Int64("replicaID", in.GetReplicaID()))
	waitFunc := func() (*commonpb.Status, error) {
		err = dct.WaitToFinish()
		if err != nil {
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			}
			log.Warn(err.Error())
			return status, nil
		}
		log.Info("watchDmChannelsTask WaitToFinish done", zap.Int64("collectionID", in.CollectionID), zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	return waitFunc()
}

// LoadSegments load historical data into query node, historical data can be vector data or index
func (node *QueryNode) LoadSegments(ctx context.Context, in *queryPb.LoadSegmentsRequest) (*commonpb.Status, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		err := fmt.Errorf("query node %d is not ready", Params.QueryNodeCfg.GetNodeID())
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	dct := &loadSegmentsTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:  in,
		node: node,
	}

	segmentIDs := make([]UniqueID, 0, len(in.GetInfos()))
	for _, info := range in.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	err := node.scheduler.queue.Enqueue(dct)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		log.Warn(err.Error())
		return status, nil
	}

	log.Info("loadSegmentsTask Enqueue done", zap.Int64("collectionID", in.CollectionID), zap.Int64s("segmentIDs", segmentIDs), zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()))

	waitFunc := func() (*commonpb.Status, error) {
		err = dct.WaitToFinish()
		if err != nil {
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			}
			log.Warn(err.Error())
			return status, nil
		}
		log.Info("loadSegmentsTask WaitToFinish done", zap.Int64("collectionID", in.CollectionID), zap.Int64s("segmentIDs", segmentIDs), zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	return waitFunc()
}

// ReleaseCollection clears all data related to this collection on the querynode
func (node *QueryNode) ReleaseCollection(ctx context.Context, in *queryPb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		err := fmt.Errorf("query node %d is not ready", Params.QueryNodeCfg.GetNodeID())
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	dct := &releaseCollectionTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:  in,
		node: node,
	}

	err := node.scheduler.queue.Enqueue(dct)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		log.Warn(err.Error())
		return status, nil
	}
	log.Info("releaseCollectionTask Enqueue done", zap.Int64("collectionID", in.CollectionID))

	func() {
		err = dct.WaitToFinish()
		if err != nil {
			log.Warn(err.Error())
			return
		}
		log.Info("releaseCollectionTask WaitToFinish done", zap.Int64("collectionID", in.CollectionID))
	}()

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
}

// ReleasePartitions clears all data related to this partition on the querynode
func (node *QueryNode) ReleasePartitions(ctx context.Context, in *queryPb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		err := fmt.Errorf("query node %d is not ready", Params.QueryNodeCfg.GetNodeID())
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	dct := &releasePartitionsTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:  in,
		node: node,
	}

	err := node.scheduler.queue.Enqueue(dct)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		log.Warn(err.Error())
		return status, nil
	}
	log.Info("releasePartitionsTask Enqueue done", zap.Int64("collectionID", in.CollectionID), zap.Int64s("partitionIDs", in.PartitionIDs))

	func() {
		err = dct.WaitToFinish()
		if err != nil {
			log.Warn(err.Error())
			return
		}
		log.Info("releasePartitionsTask WaitToFinish done", zap.Int64("collectionID", in.CollectionID), zap.Int64s("partitionIDs", in.PartitionIDs))
	}()

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
}

// ReleaseSegments remove the specified segments from query node according segmentIDs, partitionIDs, and collectionID
func (node *QueryNode) ReleaseSegments(ctx context.Context, in *queryPb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		err := fmt.Errorf("query node %d is not ready", Params.QueryNodeCfg.GetNodeID())
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}

	collection, err := node.metaReplica.getCollectionByID(in.CollectionID)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    fmt.Sprintf("cannot find collection %d when ReleaseSegments", in.CollectionID),
		}
		return status, nil
	}

	collection.Lock()
	defer collection.Unlock()
	for _, id := range in.SegmentIDs {
		switch in.GetScope() {
		case queryPb.DataScope_Streaming:
			node.metaReplica.removeSegment(id, segmentTypeGrowing)
		case queryPb.DataScope_Historical:
			node.metaReplica.removeSegment(id, segmentTypeSealed)
		case queryPb.DataScope_All:
			node.metaReplica.removeSegment(id, segmentTypeSealed)
			node.metaReplica.removeSegment(id, segmentTypeGrowing)
		}
	}

	log.Info("release segments done", zap.Int64("collectionID", in.CollectionID), zap.Int64s("segmentIDs", in.SegmentIDs), zap.String("Scope", in.GetScope().String()))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// GetSegmentInfo returns segment information of the collection on the queryNode, and the information includes memSize, numRow, indexName, indexID ...
func (node *QueryNode) GetSegmentInfo(ctx context.Context, in *queryPb.GetSegmentInfoRequest) (*queryPb.GetSegmentInfoResponse, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		err := fmt.Errorf("query node %d is not ready", Params.QueryNodeCfg.GetNodeID())
		res := &queryPb.GetSegmentInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}
		return res, nil
	}
	var segmentInfos []*queryPb.SegmentInfo

	segmentIDs := make(map[int64]struct{})
	for _, segmentID := range in.GetSegmentIDs() {
		segmentIDs[segmentID] = struct{}{}
	}

	infos := node.metaReplica.getSegmentInfosByColID(in.CollectionID)
	segmentInfos = append(segmentInfos, filterSegmentInfo(infos, segmentIDs)...)

	return &queryPb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Infos: segmentInfos,
	}, nil
}

// filterSegmentInfo returns segment info which segment id in segmentIDs map
func filterSegmentInfo(segmentInfos []*queryPb.SegmentInfo, segmentIDs map[int64]struct{}) []*queryPb.SegmentInfo {
	if len(segmentIDs) == 0 {
		return segmentInfos
	}
	filtered := make([]*queryPb.SegmentInfo, 0, len(segmentIDs))
	for _, info := range segmentInfos {
		_, ok := segmentIDs[info.GetSegmentID()]
		if !ok {
			continue
		}
		filtered = append(filtered, info)
	}
	return filtered
}

// isHealthy checks if QueryNode is healthy
func (node *QueryNode) isHealthy() bool {
	code := node.stateCode.Load().(internalpb.StateCode)
	return code == internalpb.StateCode_Healthy
}

// Search performs replica search tasks.
func (node *QueryNode) Search(ctx context.Context, req *queryPb.SearchRequest) (*internalpb.SearchResults, error) {
	log.Debug("Received SearchRequest",
		zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Strings("vChannels", req.GetDmlChannels()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))

	failRet := &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	toReduceResults := make([]*internalpb.SearchResults, 0)
	runningGp, runningCtx := errgroup.WithContext(ctx)
	mu := &sync.Mutex{}
	for _, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.SearchRequest{
			Req:             req.Req,
			DmlChannels:     []string{ch},
			SegmentIDs:      req.SegmentIDs,
			FromShardLeader: req.FromShardLeader,
			Scope:           req.Scope,
		}
		runningGp.Go(func() error {
			ret, err := node.searchWithDmlChannel(runningCtx, req, ch)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failRet.Status.Reason = err.Error()
				failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
				return err
			}
			if ret.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				failRet.Status.Reason = ret.Status.Reason
				failRet.Status.ErrorCode = ret.Status.ErrorCode
				return fmt.Errorf("%s", ret.Status.Reason)
			}
			toReduceResults = append(toReduceResults, ret)
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		return failRet, nil
	}
	ret, err := reduceSearchResults(toReduceResults, req.Req.GetNq(), req.Req.GetTopk(), req.Req.GetMetricType())
	if err != nil {
		failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}
	return ret, nil
}

func (node *QueryNode) searchWithDmlChannel(ctx context.Context, req *queryPb.SearchRequest, dmlChannel string) (*internalpb.SearchResults, error) {
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.TotalLabel).Inc()
	failRet := &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	defer func() {
		if failRet.Status.ErrorCode != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.FailLabel).Inc()
		}
	}()
	if !node.isHealthy() {
		failRet.Status.Reason = msgQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())
		return failRet, nil
	}

	msgID := req.GetReq().GetBase().GetMsgID()
	log.Debug("Received SearchRequest",
		zap.Int64("msgID", msgID),
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.String("vChannel", dmlChannel),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))

	if node.queryShardService == nil {
		failRet.Status.Reason = "queryShardService is nil"
		return failRet, nil
	}

	qs, err := node.queryShardService.getQueryShard(dmlChannel)
	if err != nil {
		log.Warn("Search failed, failed to get query shard",
			zap.Int64("msgID", msgID),
			zap.String("dml channel", dmlChannel),
			zap.Error(err))
		failRet.Status.ErrorCode = commonpb.ErrorCode_NotShardLeader
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	log.Debug("start do search",
		zap.Int64("msgID", msgID),
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.String("vChannel", dmlChannel),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()))
	tr := timerecord.NewTimeRecorder("")

	if req.FromShardLeader {
		historicalTask, err2 := newSearchTask(ctx, req)
		if err2 != nil {
			failRet.Status.Reason = err2.Error()
			return failRet, nil
		}
		historicalTask.QS = qs
		historicalTask.DataScope = querypb.DataScope_Historical
		err2 = node.scheduler.AddReadTask(ctx, historicalTask)
		if err2 != nil {
			failRet.Status.Reason = err2.Error()
			return failRet, nil
		}

		err2 = historicalTask.WaitToFinish()
		if err2 != nil {
			failRet.Status.Reason = err2.Error()
			return failRet, nil
		}

		tr.Elapse(fmt.Sprintf("do search done, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
			msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

		failRet.Status.ErrorCode = commonpb.ErrorCode_Success
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.SearchLabel).Observe(float64(historicalTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.SearchLabel).Observe(float64(historicalTask.reduceDur.Milliseconds()))
		latency := tr.ElapseSpan()
		metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel).Observe(float64(latency.Milliseconds()))
		metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.SuccessLabel).Inc()
		return historicalTask.Ret, nil
	}

	//from Proxy
	cluster, ok := qs.clusterService.getShardCluster(dmlChannel)
	if !ok {
		failRet.Status.ErrorCode = commonpb.ErrorCode_NotShardLeader
		failRet.Status.Reason = fmt.Sprintf("channel %s leader is not here", dmlChannel)
		return failRet, nil
	}

	searchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var results []*internalpb.SearchResults
	var streamingResult *internalpb.SearchResults
	var errCluster error

	withStreaming := func(ctx context.Context) error {
		streamingTask, err := newSearchTask(searchCtx, req)
		if err != nil {
			return err
		}
		streamingTask.QS = qs
		streamingTask.DataScope = querypb.DataScope_Streaming
		err = node.scheduler.AddReadTask(searchCtx, streamingTask)
		if err != nil {
			return err
		}
		err = streamingTask.WaitToFinish()
		if err != nil {
			return err
		}
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.SearchLabel).Observe(float64(streamingTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.SearchLabel).Observe(float64(streamingTask.reduceDur.Milliseconds()))
		streamingResult = streamingTask.Ret
		return nil
	}

	// shard leader dispatches request to its shard cluster
	results, errCluster = cluster.Search(searchCtx, req, withStreaming)
	if errCluster != nil {
		log.Warn("search cluster failed", zap.Int64("msgID", msgID), zap.Int64("collectionID", req.Req.GetCollectionID()), zap.Error(errCluster))
		failRet.Status.Reason = errCluster.Error()
		return failRet, nil
	}

	tr.Elapse(fmt.Sprintf("start reduce search result, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	results = append(results, streamingResult)
	ret, err2 := reduceSearchResults(results, req.Req.GetNq(), req.Req.GetTopk(), req.Req.GetMetricType())
	if err2 != nil {
		failRet.Status.Reason = err2.Error()
		return failRet, nil
	}

	tr.Elapse(fmt.Sprintf("do search done, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.SuccessLabel).Inc()
	metrics.QueryNodeSearchNQ.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Observe(float64(req.Req.GetNq()))
	metrics.QueryNodeSearchTopK.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Observe(float64(req.Req.GetTopk()))
	return ret, nil
}

func (node *QueryNode) queryWithDmlChannel(ctx context.Context, req *queryPb.QueryRequest, dmlChannel string) (*internalpb.RetrieveResults, error) {
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel, metrics.TotalLabel).Inc()
	failRet := &internalpb.RetrieveResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	defer func() {
		if failRet.Status.ErrorCode != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.FailLabel).Inc()
		}
	}()
	if !node.isHealthy() {
		failRet.Status.Reason = msgQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())
		return failRet, nil
	}

	msgID := req.GetReq().GetBase().GetMsgID()
	log.Debug("Received QueryRequest",
		zap.Int64("msgID", msgID),
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.String("vChannel", dmlChannel),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))

	if node.queryShardService == nil {
		failRet.Status.Reason = "queryShardService is nil"
		return failRet, nil
	}

	qs, err := node.queryShardService.getQueryShard(dmlChannel)
	if err != nil {
		log.Warn("Query failed, failed to get query shard", zap.Int64("msgID", msgID), zap.String("dml channel", dmlChannel), zap.Error(err))
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	log.Debug("start do query",
		zap.Int64("msgID", msgID),
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.String("vChannel", dmlChannel),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()))
	tr := timerecord.NewTimeRecorder("")

	if req.FromShardLeader {
		// construct a queryTask
		queryTask := newQueryTask(ctx, req)
		queryTask.QS = qs
		queryTask.DataScope = querypb.DataScope_Historical
		err2 := node.scheduler.AddReadTask(ctx, queryTask)
		if err2 != nil {
			failRet.Status.Reason = err2.Error()
			return failRet, nil
		}

		err2 = queryTask.WaitToFinish()
		if err2 != nil {
			failRet.Status.Reason = err2.Error()
			return failRet, nil
		}

		tr.Elapse(fmt.Sprintf("do query done, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
			msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

		failRet.Status.ErrorCode = commonpb.ErrorCode_Success
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.QueryLabel).Observe(float64(queryTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.QueryLabel).Observe(float64(queryTask.reduceDur.Milliseconds()))
		latency := tr.ElapseSpan()
		metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel).Observe(float64(latency.Milliseconds()))
		metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel).Inc()
		return queryTask.Ret, nil
	}

	cluster, ok := qs.clusterService.getShardCluster(dmlChannel)
	if !ok {
		failRet.Status.ErrorCode = commonpb.ErrorCode_NotShardLeader
		failRet.Status.Reason = fmt.Sprintf("channel %s leader is not here", dmlChannel)
		return failRet, nil
	}

	// add cancel when error occurs
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var results []*internalpb.RetrieveResults
	var streamingResult *internalpb.RetrieveResults

	withStreaming := func(ctx context.Context) error {
		streamingTask := newQueryTask(queryCtx, req)
		streamingTask.DataScope = querypb.DataScope_Streaming
		streamingTask.QS = qs
		err := node.scheduler.AddReadTask(queryCtx, streamingTask)

		if err != nil {
			return err
		}
		err = streamingTask.WaitToFinish()
		if err != nil {
			return err
		}
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.QueryLabel).Observe(float64(streamingTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.QueryLabel).Observe(float64(streamingTask.reduceDur.Milliseconds()))
		streamingResult = streamingTask.Ret
		return nil
	}

	var errCluster error
	// shard leader dispatches request to its shard cluster
	results, errCluster = cluster.Query(queryCtx, req, withStreaming)
	if errCluster != nil {
		log.Warn("failed to query cluster", zap.Int64("msgID", msgID), zap.Int64("collectionID", req.Req.GetCollectionID()), zap.Error(errCluster))
		failRet.Status.Reason = errCluster.Error()
		return failRet, nil
	}

	tr.Elapse(fmt.Sprintf("start reduce query result, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	results = append(results, streamingResult)
	ret, err2 := mergeInternalRetrieveResults(results)
	if err2 != nil {
		failRet.Status.Reason = err2.Error()
		return failRet, nil
	}

	tr.Elapse(fmt.Sprintf("do query done, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel).Inc()
	return ret, nil
}

// Query performs replica query tasks.
func (node *QueryNode) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	log.Debug("Received QueryRequest", zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Strings("vChannels", req.GetDmlChannels()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.Req.GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))

	failRet := &internalpb.RetrieveResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}

	toMergeResults := make([]*internalpb.RetrieveResults, 0)
	runningGp, runningCtx := errgroup.WithContext(ctx)
	mu := &sync.Mutex{}

	for _, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.QueryRequest{
			Req:             req.Req,
			DmlChannels:     []string{ch},
			SegmentIDs:      req.SegmentIDs,
			FromShardLeader: req.FromShardLeader,
			Scope:           req.Scope,
		}
		runningGp.Go(func() error {
			ret, err := node.queryWithDmlChannel(runningCtx, req, ch)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failRet.Status.Reason = err.Error()
				failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
				return err
			}
			if ret.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				failRet.Status.Reason = ret.Status.Reason
				failRet.Status.ErrorCode = ret.Status.ErrorCode
				return fmt.Errorf("%s", ret.Status.Reason)
			}
			toMergeResults = append(toMergeResults, ret)
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		return failRet, nil
	}
	ret, err := mergeInternalRetrieveResults(toMergeResults)
	if err != nil {
		failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}
	return ret, nil
}

// SyncReplicaSegments syncs replica node & segments states
func (node *QueryNode) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	if !node.isHealthy() {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    msgQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID()),
		}, nil
	}

	log.Debug("Received SyncReplicaSegments request", zap.String("vchannelName", req.GetVchannelName()))

	err := node.ShardClusterService.SyncReplicaSegments(req.GetVchannelName(), req.GetReplicaSegments())
	if err != nil {
		log.Warn("failed to sync replica semgents,", zap.String("vchannel", req.GetVchannelName()), zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("SyncReplicaSegments Done", zap.String("vchannel", req.GetVchannelName()))

	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

//ShowConfigurations returns the configurations of queryNode matching req.Pattern
func (node *QueryNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if !node.isHealthy() {
		log.Warn("QueryNode.ShowConfigurations failed",
			zap.Int64("nodeId", Params.QueryNodeCfg.GetNodeID()),
			zap.String("req", req.Pattern),
			zap.Error(errQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())))

		return &internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID()),
			},
			Configuations: nil,
		}, nil
	}

	return getComponentConfigurations(ctx, req), nil
}

// GetMetrics return system infos of the query node, such as total memory, memory usage, cpu usage ...
// TODO(dragondriver): cache the Metrics and set a retention to the cache
func (node *QueryNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if !node.isHealthy() {
		log.Warn("QueryNode.GetMetrics failed",
			zap.Int64("nodeId", Params.QueryNodeCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(errQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID()),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("QueryNode.GetMetrics failed to parse metric type",
			zap.Int64("nodeId", Params.QueryNodeCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response: "",
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := getSystemInfoMetrics(ctx, req, node)
		if err != nil {
			log.Warn("QueryNode.GetMetrics failed",
				zap.Int64("nodeId", Params.QueryNodeCfg.GetNodeID()),
				zap.String("req", req.Request),
				zap.String("metricType", metricType),
				zap.Error(err))
		}

		return metrics, nil
	}

	log.Debug("QueryNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("nodeId", Params.QueryNodeCfg.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}
