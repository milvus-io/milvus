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
		Value: Params.CommonCfg.QueryNodeStats,
	}, nil
}

// AddQueryChannel watch queryChannel of the collection to receive query message
func (node *QueryNode) AddQueryChannel(ctx context.Context, in *queryPb.AddQueryChannelRequest) (*commonpb.Status, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		err := fmt.Errorf("query node %d is not ready", Params.QueryNodeCfg.GetNodeID())
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	dct := &addQueryChannelTask{
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
	log.Info("addQueryChannelTask Enqueue done",
		zap.Int64("collectionID", in.CollectionID),
		zap.String("queryChannel", in.QueryChannel),
		zap.String("queryResultChannel", in.QueryResultChannel),
	)

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
		log.Info("addQueryChannelTask WaitToFinish done",
			zap.Int64("collectionID", in.CollectionID),
			zap.String("queryChannel", in.QueryChannel),
			zap.String("queryResultChannel", in.QueryResultChannel),
		)

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	return waitFunc()
}

// RemoveQueryChannel remove queryChannel of the collection to stop receiving query message
func (node *QueryNode) RemoveQueryChannel(ctx context.Context, in *queryPb.RemoveQueryChannelRequest) (*commonpb.Status, error) {
	// if node.searchService == nil || node.searchService.searchMsgStream == nil {
	// 	errMsg := "null search service or null search result message stream"
	// 	status := &commonpb.Status{
	// 		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	// 		Reason:    errMsg,
	// 	}

	// 	return status, errors.New(errMsg)
	// }

	// searchStream, ok := node.searchService.searchMsgStream.(*pulsarms.PulsarMsgStream)
	// if !ok {
	// 	errMsg := "type assertion failed for search message stream"
	// 	status := &commonpb.Status{
	// 		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	// 		Reason:    errMsg,
	// 	}

	// 	return status, errors.New(errMsg)
	// }

	// resultStream, ok := node.searchService.searchResultMsgStream.(*pulsarms.PulsarMsgStream)
	// if !ok {
	// 	errMsg := "type assertion failed for search result message stream"
	// 	status := &commonpb.Status{
	// 		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	// 		Reason:    errMsg,
	// 	}

	// 	return status, errors.New(errMsg)
	// }

	// // remove request channel
	// consumeChannels := []string{in.RequestChannelID}
	// consumeSubName := Params.MsgChannelSubName
	// // TODO: searchStream.RemovePulsarConsumers(producerChannels)
	// searchStream.AsConsumer(consumeChannels, consumeSubName)

	// // remove result channel
	// producerChannels := []string{in.ResultChannelID}
	// // TODO: resultStream.RemovePulsarProducer(producerChannels)
	// resultStream.AsProducer(producerChannels)

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
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

// WatchDeltaChannels create consumers on dmChannels to receive Incremental data，which is the important part of real-time query
func (node *QueryNode) WatchDeltaChannels(ctx context.Context, in *queryPb.WatchDeltaChannelsRequest) (*commonpb.Status, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		err := fmt.Errorf("query node %d is not ready", Params.QueryNodeCfg.GetNodeID())
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	dct := &watchDeltaChannelsTask{
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

	log.Info("watchDeltaChannelsTask Enqueue done", zap.Int64("collectionID", in.CollectionID), zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()))

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

		log.Info("watchDeltaChannelsTask WaitToFinish done", zap.Int64("collectionID", in.CollectionID), zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()))
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

	err := node.scheduler.queue.Enqueue(dct)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		log.Warn(err.Error())
		return status, nil
	}
	segmentIDs := make([]UniqueID, 0)
	for _, info := range in.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
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
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	// collection lock is not needed since we guarantee not query/search will be dispatch from leader
	for _, id := range in.SegmentIDs {
		err := node.historical.removeSegment(id)
		if err != nil {
			// not return, try to release all segments
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
		}
		err = node.streaming.removeSegment(id)
		if err != nil {
			// not return, try to release all segments
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
		}
	}

	log.Info("release segments done", zap.Int64("collectionID", in.CollectionID), zap.Int64s("segmentIDs", in.SegmentIDs))
	return status, nil
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

	// get info from historical
	historicalSegmentInfos, err := node.historical.getSegmentInfosByColID(in.CollectionID)
	if err != nil {
		log.Warn("GetSegmentInfo: get historical segmentInfo failed", zap.Int64("collectionID", in.CollectionID), zap.Error(err))
		res := &queryPb.GetSegmentInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}
		return res, nil
	}
	segmentInfos = append(segmentInfos, filterSegmentInfo(historicalSegmentInfos, segmentIDs)...)

	// get info from streaming
	streamingSegmentInfos, err := node.streaming.getSegmentInfosByColID(in.CollectionID)
	if err != nil {
		log.Warn("GetSegmentInfo: get streaming segmentInfo failed", zap.Int64("collectionID", in.CollectionID), zap.Error(err))
		res := &queryPb.GetSegmentInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}
		return res, nil
	}
	segmentInfos = append(segmentInfos, filterSegmentInfo(streamingSegmentInfos, segmentIDs)...)

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
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.TotalLabel).Inc()
	failRet := &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	defer func() {
		if failRet.Status.ErrorCode != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel, metrics.FailLabel).Inc()
		}
	}()
	if !node.isHealthy() {
		failRet.Status.Reason = msgQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())
		return failRet, nil
	}

	log.Debug("Received SearchRequest", zap.String("vchannel", req.GetDmlChannel()), zap.Int64s("segmentIDs", req.GetSegmentIDs()))

	if node.queryShardService == nil {
		failRet.Status.Reason = "queryShardService is nil"
		return failRet, nil
	}

	qs, err := node.queryShardService.getQueryShard(req.GetDmlChannel())
	if err != nil {
		log.Warn("Search failed, failed to get query shard", zap.String("dml channel", req.GetDmlChannel()), zap.Error(err))
		failRet.Status.ErrorCode = commonpb.ErrorCode_NotShardLeader
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("search %d", req.Req.CollectionID))

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
	cluster, ok := qs.clusterService.getShardCluster(req.GetDmlChannel())
	if !ok {
		failRet.Status.Reason = fmt.Sprintf("channel %s leader is not here", req.GetDmlChannel())
		return failRet, nil
	}

	searchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var results []*internalpb.SearchResults
	var streamingResult *internalpb.SearchResults

	var wg sync.WaitGroup
	var errCluster error

	wg.Add(1) // search cluster
	go func() {
		defer wg.Done()
		// shard leader dispatches request to its shard cluster
		oResults, cErr := cluster.Search(searchCtx, req)
		if cErr != nil {
			log.Warn("search cluster failed", zap.Int64("collectionID", req.Req.GetCollectionID()), zap.Error(cErr))
			cancel()
			errCluster = cErr
			return
		}
		results = oResults
	}()

	var errStreaming error
	wg.Add(1) // search streaming
	go func() {
		defer func() {
			if errStreaming != nil {
				cancel()
			}
		}()

		defer wg.Done()
		streamingTask, err2 := newSearchTask(searchCtx, req)
		if err2 != nil {
			errStreaming = err2
		}
		streamingTask.QS = qs
		streamingTask.DataScope = querypb.DataScope_Streaming
		err2 = node.scheduler.AddReadTask(searchCtx, streamingTask)
		if err2 != nil {
			errStreaming = err2
			return
		}
		err2 = streamingTask.WaitToFinish()
		if err2 != nil {
			errStreaming = err2
			return
		}
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.SearchLabel).Observe(float64(streamingTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.SearchLabel).Observe(float64(streamingTask.reduceDur.Milliseconds()))
		streamingResult = streamingTask.Ret
	}()
	wg.Wait()

	var mainErr error
	if errCluster != nil {
		mainErr = errCluster
		if errors.Is(errCluster, context.Canceled) {
			if errStreaming != nil {
				mainErr = errStreaming
			}
		}
	} else if errStreaming != nil {
		mainErr = errStreaming
	}

	if mainErr != nil {
		failRet.Status.Reason = mainErr.Error()
		return failRet, nil
	}
	results = append(results, streamingResult)
	ret, err2 := reduceSearchResults(results, req.Req.GetNq(), req.Req.GetTopk(), req.Req.GetMetricType())
	if err2 != nil {
		failRet.Status.Reason = err2.Error()
		return failRet, nil
	}

	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.SuccessLabel).Inc()
	return ret, nil
}

// Query performs replica query tasks.
func (node *QueryNode) Query(ctx context.Context, req *queryPb.QueryRequest) (*internalpb.RetrieveResults, error) {
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
	log.Debug("Received QueryRequest", zap.String("vchannel", req.GetDmlChannel()), zap.Int64s("segmentIDs", req.GetSegmentIDs()))

	if node.queryShardService == nil {
		failRet.Status.Reason = "queryShardService is nil"
		return failRet, nil
	}

	qs, err := node.queryShardService.getQueryShard(req.GetDmlChannel())
	if err != nil {
		log.Warn("Query failed, failed to get query shard", zap.String("dml channel", req.GetDmlChannel()), zap.Error(err))
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("retrieve %d", req.Req.CollectionID))
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

	cluster, ok := qs.clusterService.getShardCluster(req.GetDmlChannel())
	if !ok {
		failRet.Status.Reason = fmt.Sprintf("channel %s leader is not here", req.GetDmlChannel())
		return failRet, nil
	}

	// add cancel when error occurs
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var results []*internalpb.RetrieveResults
	var streamingResult *internalpb.RetrieveResults
	var wg sync.WaitGroup

	var errCluster error
	wg.Add(1)
	go func() {
		defer wg.Done()
		// shard leader dispatches request to its shard cluster
		oResults, cErr := cluster.Query(queryCtx, req)
		if cErr != nil {
			log.Warn("failed to query cluster", zap.Int64("collectionID", req.Req.GetCollectionID()), zap.Error(cErr))
			log.Info("czs_query_cluster_cancel", zap.Error(cErr))
			errCluster = cErr
			cancel()
			return
		}
		results = oResults
	}()

	var errStreaming error
	wg.Add(1)
	go func() {
		defer wg.Done()
		streamingTask := newQueryTask(queryCtx, req)
		streamingTask.DataScope = querypb.DataScope_Streaming
		streamingTask.QS = qs
		err2 := node.scheduler.AddReadTask(queryCtx, streamingTask)
		defer func() {
			errStreaming = err2
			if err2 != nil {
				cancel()
			}
		}()
		if err2 != nil {
			return
		}
		err2 = streamingTask.WaitToFinish()
		if err2 != nil {
			return
		}
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.QueryLabel).Observe(float64(streamingTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.QueryLabel).Observe(float64(streamingTask.reduceDur.Milliseconds()))
		streamingResult = streamingTask.Ret
	}()
	wg.Wait()

	var mainErr error
	if errCluster != nil {
		mainErr = errCluster
		if errors.Is(errCluster, context.Canceled) {
			if errStreaming != nil {
				mainErr = errStreaming
			}
		}
	} else if errStreaming != nil {
		mainErr = errStreaming
	}

	if mainErr != nil {
		failRet.Status.Reason = mainErr.Error()
		return failRet, nil
	}
	results = append(results, streamingResult)
	ret, err2 := mergeInternalRetrieveResults(results)
	if err2 != nil {
		failRet.Status.Reason = err2.Error()
		return failRet, nil
	}
	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel).Inc()
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

// GetMetrics return system infos of the query node, such as total memory, memory usage, cpu usage ...
// TODO(dragondriver): cache the Metrics and set a retention to the cache
func (node *QueryNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if !node.isHealthy() {
		log.Warn("QueryNode.GetMetrics failed",
			zap.Int64("node_id", Params.QueryNodeCfg.GetNodeID()),
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
			zap.Int64("node_id", Params.QueryNodeCfg.GetNodeID()),
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
				zap.Int64("node_id", Params.QueryNodeCfg.GetNodeID()),
				zap.String("req", req.Request),
				zap.String("metric_type", metricType),
				zap.Error(err))
		}

		return metrics, nil
	}

	log.Debug("QueryNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", Params.QueryNodeCfg.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metric_type", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}
