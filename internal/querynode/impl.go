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
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/contextutil"
	"github.com/milvus-io/milvus/internal/util/errorutil"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func isUnavailableCode[T any](node *QueryNode, failResp T, failStatus *commonpb.Status, fn func()) (T, bool) {
	isUnavailableCode := func() bool {
		if !commonpbutil.IsHealthy(node.stateCode) {
			node.NotReadyServeResp(failStatus)
			if fn != nil {
				fn()
			}
			return true
		}
		return false
	}

	if isUnavailableCode() {
		return failResp, true
	}
	node.rpcLock.RLock()
	defer node.rpcLock.RUnlock()
	if isUnavailableCode() {
		return failResp, true
	}
	return failResp, false
}

func (node *QueryNode) GetStateCode() commonpb.StateCode {
	code := node.stateCode.Load()
	if code == nil {
		return commonpb.StateCode_Abnormal
	}
	return code.(commonpb.StateCode)
}

func (node *QueryNode) NotReadyServeResp(status *commonpb.Status) {
	status.ErrorCode = commonpb.ErrorCode_NotReadyServe
	status.Reason = errorutil.NotServingReason(typeutil.QueryNodeRole, Params.QueryNodeCfg.GetNodeID(), node.GetStateCode().String())
}

// GetComponentStates returns information about whether the node is healthy
func (node *QueryNode) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	stats := &milvuspb.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	code, ok := node.stateCode.Load().(commonpb.StateCode)
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
		nodeID = Params.QueryNodeCfg.GetNodeID()
	}
	info := &milvuspb.ComponentInfo{
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

func (node *QueryNode) getStatisticsWithDmlChannel(ctx context.Context, req *querypb.GetStatisticsRequest, dmlChannel string) (*internalpb.GetStatisticsResponse, error) {
	failRet := &internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	failRet, isUnavailable := isUnavailableCode(node, failRet, failRet.Status, nil)
	if isUnavailable {
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
	var errCluster error

	withStreaming := func(ctx context.Context) (error, *internalpb.GetStatisticsResponse) {
		streamingTask := newStatistics(ctx, req, querypb.DataScope_Streaming, qs, waitCanDo)
		err := streamingTask.Execute(ctx)
		if err != nil {
			return err, nil
		}
		return nil, streamingTask.Ret
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
func (node *QueryNode) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	failStatus := &commonpb.Status{}
	failStatus, isUnavailable := isUnavailableCode(node, failStatus, failStatus, nil)
	if isUnavailable {
		return failStatus, nil
	}

	// check target matches
	if in.GetBase().GetTargetID() != Params.QueryNodeCfg.GetNodeID() {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(in.GetBase().GetTargetID(), Params.QueryNodeCfg.GetNodeID()),
		}
		return status, nil
	}

	log := log.With(
		zap.Int64("collectionID", in.GetCollectionID()),
		zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()),
		zap.Strings("channels", lo.Map(in.GetInfos(), func(info *datapb.VchannelInfo, _ int) string {
			return info.GetChannelName()
		})),
	)

	task := &watchDmChannelsTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:  in,
		node: node,
	}

	startTs := time.Now()
	log.Info("watchDmChannels init")
	// currently we only support load one channel as a time
	future := node.taskPool.Submit(func() (interface{}, error) {
		log.Info("watchDmChannels start ",
			zap.Duration("timeInQueue", time.Since(startTs)))
		err := task.PreExecute(ctx)
		if err != nil {
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			}
			log.Warn("failed to subscribe channel on preExecute ", zap.Error(err))
			return status, nil
		}

		err = task.Execute(ctx)
		if err != nil {
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			}
			log.Warn("failed to subscribe channel", zap.Error(err))
			return status, nil
		}

		err = task.PostExecute(ctx)
		if err != nil {
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			}
			log.Warn("failed to unsubscribe channel on postExecute ", zap.Error(err))
			return status, nil
		}

		log.Info("successfully watchDmChannelsTask")
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	})
	ret, err := future.Await()
	if status, ok := ret.(*commonpb.Status); ok {
		return status, nil
	}
	log.Warn("fail to convert the *commonpb.Status", zap.Any("ret", ret), zap.Error(err))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}, nil
}

func (node *QueryNode) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channel", req.GetChannelName()),
	)

	failStatus := &commonpb.Status{}
	failStatus, isUnavailable := isUnavailableCode(node, failStatus, failStatus, nil)
	if isUnavailable {
		return failStatus, nil
	}

	// check target matches
	if req.GetBase().GetTargetID() != Params.QueryNodeCfg.GetNodeID() {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), Params.QueryNodeCfg.GetNodeID()),
		}
		return status, nil
	}

	log.Info("unsubscribe channel request received")

	unsubTask := &unsubDmChannelTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		node:         node,
		collectionID: req.GetCollectionID(),
		channel:      req.GetChannelName(),
	}

	err := node.scheduler.queue.Enqueue(unsubTask)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		log.Warn("failed to enqueue subscribe channel task", zap.Error(err))
		return status, nil
	}
	log.Info("unsubDmChannelTask enqueue done")

	err = unsubTask.WaitToFinish()
	if err != nil {
		log.Warn("failed to do subscribe channel task successfully", zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Info("unsubDmChannelTask WaitToFinish done")
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// LoadSegments load historical data into query node, historical data can be vector data or index
func (node *QueryNode) LoadSegments(ctx context.Context, in *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	// check node healthy
	failStatus := &commonpb.Status{}
	failStatus, isUnavailable := isUnavailableCode(node, failStatus, failStatus, nil)
	if isUnavailable {
		return failStatus, nil
	}

	// check target matches
	if in.GetBase().GetTargetID() != Params.QueryNodeCfg.GetNodeID() {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(in.GetBase().GetTargetID(), Params.QueryNodeCfg.GetNodeID()),
		}
		return status, nil
	}

	if in.GetNeedTransfer() {
		return node.TransferLoad(ctx, in)
	}

	alreadyLoaded := true
	for _, info := range in.Infos {
		has, _ := node.metaReplica.hasSegment(info.SegmentID, segmentTypeSealed)
		if !has {
			alreadyLoaded = false
		}
	}

	// if all segment has been loaded, just return success
	if alreadyLoaded {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	task := &loadSegmentsTask{
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
	sort.SliceStable(segmentIDs, func(i, j int) bool {
		return segmentIDs[i] < segmentIDs[j]
	})

	startTs := time.Now()
	log.Info("loadSegmentsTask init", zap.Int64("collectionID", in.CollectionID),
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()))

	// TODO remove concurrent load segment for now, unless we solve the memory issue
	log.Info("loadSegmentsTask start ", zap.Int64("collectionID", in.CollectionID),
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Duration("timeInQueue", time.Since(startTs)))
	err := node.scheduler.queue.Enqueue(task)
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
		err = task.WaitToFinish()
		if err != nil {
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			}
			if errors.Is(err, ErrInsufficientMemory) {
				status.ErrorCode = commonpb.ErrorCode_InsufficientMemoryToLoad
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
func (node *QueryNode) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	failStatus := &commonpb.Status{}
	failStatus, isUnavailable := isUnavailableCode(node, failStatus, failStatus, nil)
	if isUnavailable {
		return failStatus, nil
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
func (node *QueryNode) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	failStatus := &commonpb.Status{}
	failStatus, isUnavailable := isUnavailableCode(node, failStatus, failStatus, nil)
	if isUnavailable {
		return failStatus, nil
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
func (node *QueryNode) ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	failStatus := &commonpb.Status{}
	failStatus, isUnavailable := isUnavailableCode(node, failStatus, failStatus, nil)
	if isUnavailable {
		return failStatus, nil
	}

	// check target matches
	if in.GetBase().GetTargetID() != Params.QueryNodeCfg.GetNodeID() {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(in.GetBase().GetTargetID(), Params.QueryNodeCfg.GetNodeID()),
		}
		return status, nil
	}

	if in.GetNeedTransfer() {
		return node.TransferRelease(ctx, in)
	}

	log.Info("start to release segments", zap.Int64("collectionID", in.CollectionID), zap.Int64s("segmentIDs", in.SegmentIDs))

	releaseSealed := int64(0)
	for _, id := range in.SegmentIDs {
		switch in.GetScope() {
		case querypb.DataScope_Streaming:
			node.metaReplica.removeSegment(id, segmentTypeGrowing)
		case querypb.DataScope_Historical:
			releaseSealed += node.metaReplica.removeSegment(id, segmentTypeSealed)
		case querypb.DataScope_All:
			node.metaReplica.removeSegment(id, segmentTypeGrowing)
			releaseSealed += node.metaReplica.removeSegment(id, segmentTypeSealed)
		}
	}

	segments, err := node.metaReplica.getSegmentIDsByVChannel(nil, in.GetShard(), segmentTypeSealed)
	if err != nil {
		// unreachable path
		log.Warn("failed to check segments with VChannel", zap.Error(err))
		return failStatus, nil
	}

	// only remove query shard service in worker when all sealed segment has been removed.
	// `releaseSealed > 0` make sure that releaseSegment happens in worker.
	// `len(segments) == 0` make sure that all sealed segment in worker has been removed.
	if releaseSealed > 0 && len(segments) == 0 {
		node.queryShardService.removeQueryShard(in.GetShard(), 1)
	}

	// note that argument is dmlchannel name
	node.dataSyncService.removeEmptyFlowGraphByChannel(in.GetCollectionID(), in.GetShard())

	log.Info("release segments done", zap.Int64("collectionID", in.CollectionID), zap.Int64s("segmentIDs", in.SegmentIDs), zap.String("Scope", in.GetScope().String()))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// GetSegmentInfo returns segment information of the collection on the queryNode, and the information includes memSize, numRow, indexName, indexID ...
func (node *QueryNode) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	failRes := &querypb.GetSegmentInfoResponse{
		Status: &commonpb.Status{},
	}

	failRes, isUnavailable := isUnavailableCode(node, failRes, failRes.Status, nil)
	if isUnavailable {
		return failRes, nil
	}

	var segmentInfos []*querypb.SegmentInfo

	segmentIDs := make(map[int64]struct{})
	for _, segmentID := range in.GetSegmentIDs() {
		segmentIDs[segmentID] = struct{}{}
	}

	infos := node.metaReplica.getSegmentInfosByColID(in.CollectionID)
	segmentInfos = append(segmentInfos, filterSegmentInfo(infos, segmentIDs)...)

	return &querypb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Infos: segmentInfos,
	}, nil
}

// filterSegmentInfo returns segment info which segment id in segmentIDs map
func filterSegmentInfo(segmentInfos []*querypb.SegmentInfo, segmentIDs map[int64]struct{}) []*querypb.SegmentInfo {
	if len(segmentIDs) == 0 {
		return segmentInfos
	}
	filtered := make([]*querypb.SegmentInfo, 0, len(segmentIDs))
	for _, info := range segmentInfos {
		_, ok := segmentIDs[info.GetSegmentID()]
		if !ok {
			continue
		}
		filtered = append(filtered, info)
	}
	return filtered
}

// Search performs replica search tasks.
func (node *QueryNode) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	if !node.IsStandAlone && req.GetReq().GetBase().GetTargetID() != Params.QueryNodeCfg.GetNodeID() {
		return &internalpb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
				Reason: fmt.Sprintf("QueryNode %d can't serve, recovering: %s",
					Params.QueryNodeCfg.GetNodeID(),
					common.WrapNodeIDNotMatchMsg(req.GetReq().GetBase().GetTargetID(), Params.QueryNodeCfg.GetNodeID())),
			},
		}, nil
	}

	failRet := &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}

	tr := timerecord.NewTimeRecorder("Search")
	if !req.GetFromShardLeader() {
		log.Ctx(ctx).Debug("Received SearchRequest",
			zap.Strings("vChannels", req.GetDmlChannels()),
			zap.Int64s("segmentIDs", req.GetSegmentIDs()),
			zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
			zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))
	}

	collection, err := node.metaReplica.getCollectionByID(req.GetReq().GetCollectionID())
	if err != nil {
		failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	// Check if the metric type specified in search params matches the metric type in the index info.
	if !req.GetFromShardLeader() && req.GetReq().GetMetricType() != "" {
		if req.GetReq().GetMetricType() != collection.getMetricType() {
			failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			failRet.Status.Reason = fmt.Sprintf("collection:%d, metric type not match: expected=%s, actual=%s: invalid parameter",
				collection.ID(), collection.getMetricType(), req.GetReq().GetMetricType())
			return failRet, nil
		}
	}

	// Define the metric type when it has not been explicitly assigned by the user.
	if !req.GetFromShardLeader() && req.GetReq().GetMetricType() == "" {
		req.Req.MetricType = collection.getMetricType()
	}

	toReduceResults := make([]*internalpb.SearchResults, 0)
	runningGp, runningCtx := errgroup.WithContext(ctx)
	mu := &sync.Mutex{}
	for _, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.SearchRequest{
			Req:             typeutil.Clone(req.Req),
			DmlChannels:     []string{ch},
			SegmentIDs:      req.SegmentIDs,
			FromShardLeader: req.FromShardLeader,
			Scope:           req.Scope,
			TotalChannelNum: req.TotalChannelNum,
		}
		runningGp.Go(func() error {
			ret, err := node.searchWithDmlChannel(runningCtx, req, ch)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
				return err
			}
			if ret.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				failRet.Status.ErrorCode = ret.Status.ErrorCode
				return fmt.Errorf("%s", ret.Status.Reason)
			}
			toReduceResults = append(toReduceResults, ret)
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		// make fail reason comes from first err
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	ret, err := reduceSearchResults(ctx, toReduceResults, req.Req.GetNq(), req.Req.GetTopk(), req.Req.GetMetricType())
	if err != nil {
		failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	if !req.FromShardLeader {
		tr.CtxElapse(ctx, "search done in all shards")
		rateCol.Add(metricsinfo.NQPerSecond, float64(req.GetReq().GetNq()))
		rateCol.Add(metricsinfo.SearchThroughput, float64(proto.Size(req)))
		metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(Params.QueryNodeCfg.GetNodeID(), 10), metrics.SearchLabel).Add(float64(proto.Size(req)))
	}

	if ret.SlicedBlob == nil {
		log.Ctx(ctx).Debug("search result is empty",
			zap.Strings("vChannels", req.GetDmlChannels()),
			zap.Int64s("segmentIDs", req.GetSegmentIDs()),
			zap.Int64("collection", req.Req.CollectionID),
			zap.Strings("shard", req.DmlChannels),
			zap.Bool("from shard leader", req.FromShardLeader),
			zap.String("dsl", req.Req.Dsl))
	}
	return ret, nil
}

func (node *QueryNode) searchWithDmlChannel(ctx context.Context, req *querypb.SearchRequest, dmlChannel string) (*internalpb.SearchResults, error) {
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

	failRet, isUnavailable := isUnavailableCode(node, failRet, failRet.Status, nil)
	if isUnavailable {
		return failRet, nil
	}

	if node.queryShardService == nil {
		failRet.Status.Reason = "queryShardService is nil"
		return failRet, nil
	}

	qs, err := node.queryShardService.getQueryShard(dmlChannel)
	if err != nil {
		log.Ctx(ctx).Warn("Search failed, failed to get query shard",
			zap.String("vChannel", dmlChannel),
			zap.Error(err))
		failRet.Status.ErrorCode = commonpb.ErrorCode_NotShardLeader
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	if req.FromShardLeader {
		tr := timerecord.NewTimeRecorder("SubSearch")
		log.Ctx(ctx).Debug("start do subsearch",
			zap.Bool("fromShardLeader", req.GetFromShardLeader()),
			zap.String("vChannel", dmlChannel),
			zap.Int64s("segmentIDs", req.GetSegmentIDs()))

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

		tr.CtxElapse(ctx, fmt.Sprintf("do subsearch done, vChannel = %s, segmentIDs = %v", dmlChannel, req.GetSegmentIDs()))

		failRet.Status.ErrorCode = commonpb.ErrorCode_Success
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.SearchLabel).Observe(float64(historicalTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.SearchLabel).Observe(float64(historicalTask.reduceDur.Milliseconds()))
		// In queue latency per user.
		metrics.QueryNodeSQPerUserLatencyInQueue.WithLabelValues(
			fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.SearchLabel,
			contextutil.GetUserFromGrpcMetadata(historicalTask.Ctx()),
		).Observe(float64(historicalTask.queueDur.Milliseconds()))
		latency := tr.ElapseSpan()
		metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
		metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.SuccessLabel).Inc()
		return historicalTask.Ret, nil
	}

	// from Proxy
	tr := timerecord.NewTimeRecorder("SearchShard")
	log.Ctx(ctx).Debug("start do search",
		zap.String("vChannel", dmlChannel),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()))

	cluster, ok := qs.clusterService.getShardCluster(dmlChannel)
	if !ok {
		failRet.Status.ErrorCode = commonpb.ErrorCode_NotShardLeader
		failRet.Status.Reason = fmt.Sprintf("channel %s leader is not here", dmlChannel)
		return failRet, nil
	}

	var (
		searchCtx, cancel = context.WithCancel(ctx)
		results           []*internalpb.SearchResults
		errCluster        error
	)
	defer cancel()
	// Passthrough username across grpc.
	searchCtx = contextutil.PassthroughUserInGrpcMetadata(searchCtx)

	// optimize search params
	if req.Req.GetSerializedExprPlan() != nil && node.queryHook != nil {
		channelNum := int(req.GetTotalChannelNum())
		if channelNum <= 0 {
			channelNum = 1
		}
		estSegmentNum := len(cluster.GetSegmentInfos()) * channelNum
		log.Debug("estimate segment num:", zap.Int("estSegmentNum", estSegmentNum), zap.Int("channleNum", channelNum))
		plan := &planpb.PlanNode{}
		err = proto.Unmarshal(req.Req.GetSerializedExprPlan(), plan)
		if err != nil {
			failRet.Status.Reason = err.Error()
			return failRet, nil
		}
		switch plan.GetNode().(type) {
		case *planpb.PlanNode_VectorAnns:
			withFilter := (plan.GetVectorAnns().GetPredicates() != nil)
			qinfo := plan.GetVectorAnns().GetQueryInfo()
			paramMap := map[string]interface{}{
				common.TopKKey:        qinfo.GetTopk(),
				common.SearchParamKey: qinfo.GetSearchParams(),
				common.SegmentNumKey:  estSegmentNum,
				common.WithFilterKey:  withFilter,
			}
			err := node.queryHook.Run(paramMap)
			if err != nil {
				failRet.Status.Reason = err.Error()
				return failRet, nil
			}
			qinfo.Topk = paramMap[common.TopKKey].(int64)
			qinfo.SearchParams = paramMap[common.SearchParamKey].(string)

			SerializedExprPlan, err := proto.Marshal(plan)
			if err != nil {
				failRet.Status.Reason = err.Error()
				return failRet, nil
			}
			req.Req.SerializedExprPlan = SerializedExprPlan
			log.Debug("optimized search params done", zap.Any("queryInfo", qinfo))
		}
	}

	// shard leader dispatches request to its shard cluster
	var withStreamingFunc searchWithStreaming
	if !req.Req.IgnoreGrowing {
		withStreamingFunc = getSearchWithStreamingFunc(searchCtx, req, node, qs, Params.QueryNodeCfg.GetNodeID())
	}
	results, errCluster = cluster.Search(searchCtx, req, withStreamingFunc)
	if errCluster != nil {
		log.Ctx(ctx).Warn("search shard cluster failed", zap.String("vChannel", dmlChannel), zap.Error(errCluster))
		failRet.Status.Reason = errCluster.Error()
		return failRet, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do search done in shard cluster, vChannel = %s, segmentIDs = %v", dmlChannel, req.GetSegmentIDs()))

	ret, err2 := reduceSearchResults(ctx, results, req.Req.GetNq(), req.Req.GetTopk(), req.Req.GetMetricType())
	if err2 != nil {
		failRet.Status.Reason = err2.Error()
		return failRet, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do reduce done in shard cluster, vChannel = %s, segmentIDs = %v", dmlChannel, req.GetSegmentIDs()))

	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.Leader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.SearchLabel, metrics.SuccessLabel).Inc()
	metrics.QueryNodeSearchNQ.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Observe(float64(req.Req.GetNq()))
	metrics.QueryNodeSearchTopK.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Observe(float64(req.Req.GetTopk()))

	return ret, nil
}

func (node *QueryNode) queryWithDmlChannel(ctx context.Context, req *querypb.QueryRequest, dmlChannel string) (*internalpb.RetrieveResults, error) {
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

	failRet, isUnavailable := isUnavailableCode(node, failRet, failRet.Status, nil)
	if isUnavailable {
		return failRet, nil
	}

	msgID := req.GetReq().GetBase().GetMsgID()
	log.Ctx(ctx).Debug("queryWithDmlChannel receives query request",
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
		log.Ctx(ctx).Warn("Query failed, failed to get query shard", zap.Int64("msgID", msgID), zap.String("dml channel", dmlChannel), zap.Error(err))
		failRet.Status.ErrorCode = commonpb.ErrorCode_NotShardLeader
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	log.Ctx(ctx).Debug("queryWithDmlChannel starts do query",
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

		tr.CtxElapse(ctx, fmt.Sprintf("do query done, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
			msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

		failRet.Status.ErrorCode = commonpb.ErrorCode_Success
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.QueryLabel).Observe(float64(queryTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.QueryLabel).Observe(float64(queryTask.reduceDur.Milliseconds()))
		// In queue latency per user.
		metrics.QueryNodeSQPerUserLatencyInQueue.WithLabelValues(
			fmt.Sprint(Params.QueryNodeCfg.GetNodeID()),
			metrics.QueryLabel,
			contextutil.GetUserFromGrpcMetadata(queryTask.Ctx()),
		).Observe(float64(queryTask.queueDur.Milliseconds()))
		latency := tr.ElapseSpan()
		metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
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

	// Passthrough username across grpc.
	queryCtx = contextutil.PassthroughUserInGrpcMetadata(queryCtx)

	var results []*internalpb.RetrieveResults
	var errCluster error
	var withStreamingFunc queryWithStreaming

	if !req.Req.IgnoreGrowing {
		withStreamingFunc = getQueryWithStreamingFunc(queryCtx, req, node, qs, Params.QueryNodeCfg.GetNodeID())
	}
	// shard leader dispatches request to its shard cluster
	results, errCluster = cluster.Query(queryCtx, req, withStreamingFunc)
	if errCluster != nil {
		log.Ctx(ctx).Warn("failed to query cluster", zap.Int64("msgID", msgID), zap.Int64("collectionID", req.Req.GetCollectionID()), zap.Error(errCluster))
		failRet.Status.Reason = errCluster.Error()
		return failRet, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("start reduce query result, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	ret, err2 := mergeInternalRetrieveResultsAndFillIfEmpty(ctx, results, req.Req.GetLimit(), req.GetReq().GetOutputFieldsId(), qs.collection.Schema())
	if err2 != nil {
		failRet.Status.Reason = err2.Error()
		return failRet, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do query done, msgID = %d, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		msgID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel, metrics.Leader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel).Inc()
	return ret, nil
}

// Query performs replica query tasks.
func (node *QueryNode) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	log.Ctx(ctx).Debug("Received QueryRequest", zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Bool("fromShardleader", req.GetFromShardLeader()),
		zap.Strings("vChannels", req.GetDmlChannels()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.Req.GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()),
		zap.Int64s("partitionIDs", req.GetReq().GetPartitionIDs()),
	)

	if req.GetReq().GetBase().GetTargetID() != Params.QueryNodeCfg.GetNodeID() {
		return &internalpb.RetrieveResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
				Reason: fmt.Sprintf("QueryNode %d can't serve, recovering: %s",
					Params.QueryNodeCfg.GetNodeID(),
					common.WrapNodeIDNotMatchMsg(req.GetReq().GetBase().GetTargetID(), Params.QueryNodeCfg.GetNodeID())),
			},
		}, nil
	}

	failRet := &internalpb.RetrieveResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}

	coll, err := node.metaReplica.getCollectionByID(req.GetReq().GetCollectionID())
	if err != nil {
		failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		failRet.Status.Reason = err.Error()
		return failRet, nil
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
				failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
				return err
			}
			if ret.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				failRet.Status.ErrorCode = ret.Status.ErrorCode
				return fmt.Errorf("%s", ret.Status.Reason)
			}
			toMergeResults = append(toMergeResults, ret)
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}
	ret, err := mergeInternalRetrieveResultsAndFillIfEmpty(ctx, toMergeResults, req.GetReq().GetLimit(), req.GetReq().GetOutputFieldsId(), coll.Schema())
	if err != nil {
		failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	if !req.FromShardLeader {
		rateCol.Add(metricsinfo.NQPerSecond, 1)
		metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(Params.QueryNodeCfg.GetNodeID(), 10), metrics.QueryLabel).Add(float64(proto.Size(req)))
	}
	return ret, nil
}

// SyncReplicaSegments syncs replica node & segments states
func (node *QueryNode) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	failStatus := &commonpb.Status{}
	failStatus, isUnavailable := isUnavailableCode(node, failStatus, failStatus, nil)
	if isUnavailable {
		return failStatus, nil
	}

	log.Info("Received SyncReplicaSegments request", zap.String("vchannelName", req.GetVchannelName()))

	err := node.ShardClusterService.SyncReplicaSegments(req.GetVchannelName(), req.GetReplicaSegments())
	if err != nil {
		log.Warn("failed to sync replica semgents,", zap.String("vchannel", req.GetVchannelName()), zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Info("SyncReplicaSegments Done", zap.String("vchannel", req.GetVchannelName()))

	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

// ShowConfigurations returns the configurations of queryNode matching req.Pattern
func (node *QueryNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	failResp := &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{},
	}

	failResp, isUnavailable := isUnavailableCode(node, failResp, failResp.Status, func() {
		log.Warn("QueryNode.ShowConfigurations failed",
			zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()),
			zap.String("req", req.Pattern),
			zap.Error(errQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())))
	})
	if isUnavailable {
		return failResp, nil
	}

	return getComponentConfigurations(ctx, req), nil
}

// GetMetrics return system infos of the query node, such as total memory, memory usage, cpu usage ...
func (node *QueryNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()),
		zap.String("req", req.GetRequest()))

	failResp := &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{},
	}

	failResp, isUnavailable := isUnavailableCode(node, failResp, failResp.Status, func() {
		log.Warn("QueryNode.GetMetrics failed",
			zap.Error(errQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())))
	})
	if isUnavailable {
		return failResp, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("QueryNode.GetMetrics failed to parse metric type",
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		queryNodeMetrics, err := getSystemInfoMetrics(ctx, req, node)
		if err != nil {
			log.Warn("QueryNode.GetMetrics failed",
				zap.String("metricType", metricType),
				zap.Error(err))
			return &milvuspb.GetMetricsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}
		return queryNodeMetrics, nil
	}

	log.RatedDebug(60, "QueryNode.GetMetrics failed, request metric type is not implemented yet",
		zap.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}

func (node *QueryNode) GetDataDistribution(ctx context.Context, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	log := log.With(
		zap.Int64("msg-id", req.GetBase().GetMsgID()),
		zap.Int64("node-id", Params.QueryNodeCfg.GetNodeID()),
	)

	failResp := &querypb.GetDataDistributionResponse{
		Status: &commonpb.Status{},
	}
	failResp, isUnavailable := isUnavailableCode(node, failResp, failResp.Status, func() {
		log.Warn("QueryNode.GetMetrics failed",
			zap.Error(errQueryNodeIsUnhealthy(Params.QueryNodeCfg.GetNodeID())))
	})
	if isUnavailable {
		return failResp, nil
	}

	// check target matches
	if req.GetBase().GetTargetID() != Params.QueryNodeCfg.GetNodeID() {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason: fmt.Sprintf("QueryNode %d can't serve, recovering: %s",
				Params.QueryNodeCfg.GetNodeID(),
				common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), Params.QueryNodeCfg.GetNodeID())),
		}
		return &querypb.GetDataDistributionResponse{Status: status}, nil
	}

	growingSegments := node.metaReplica.getGrowingSegments()
	sealedSegments := node.metaReplica.getSealedSegments()
	shardClusters := node.ShardClusterService.GetShardClusters()

	channelGrowingsMap := make(map[string]map[int64]*internalpb.MsgPosition)
	for _, s := range growingSegments {
		if _, ok := channelGrowingsMap[s.vChannelID]; !ok {
			channelGrowingsMap[s.vChannelID] = make(map[int64]*internalpb.MsgPosition)
		}

		channelGrowingsMap[s.vChannelID][s.ID()] = s.startPosition
	}

	segmentVersionInfos := make([]*querypb.SegmentVersionInfo, 0, len(sealedSegments))
	for _, s := range sealedSegments {
		info := &querypb.SegmentVersionInfo{
			ID:         s.ID(),
			Collection: s.collectionID,
			Partition:  s.partitionID,
			Channel:    s.vChannelID,
			Version:    s.version,
		}
		segmentVersionInfos = append(segmentVersionInfos, info)
	}

	channelVersionInfos := make([]*querypb.ChannelVersionInfo, 0, len(shardClusters))
	leaderViews := make([]*querypb.LeaderView, 0, len(shardClusters))
	for _, sc := range shardClusters {
		if !node.queryShardService.hasQueryShard(sc.vchannelName) {
			continue
		}
		segmentInfos := sc.GetSegmentInfos()
		mapping := make(map[int64]*querypb.SegmentDist)
		for _, info := range segmentInfos {
			mapping[info.SegmentID] = &querypb.SegmentDist{
				NodeID:  info.NodeID,
				Version: info.Version,
			}
		}
		view := &querypb.LeaderView{
			Collection:      sc.collectionID,
			Channel:         sc.vchannelName,
			SegmentDist:     mapping,
			GrowingSegments: channelGrowingsMap[sc.vchannelName],
		}
		leaderViews = append(leaderViews, view)

		channelInfo := &querypb.ChannelVersionInfo{
			Channel:    sc.vchannelName,
			Collection: sc.collectionID,
			Version:    sc.getVersion(),
		}
		channelVersionInfos = append(channelVersionInfos, channelInfo)
	}

	return &querypb.GetDataDistributionResponse{
		Status:      &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		NodeID:      Params.QueryNodeCfg.GetNodeID(),
		Segments:    segmentVersionInfos,
		Channels:    channelVersionInfos,
		LeaderViews: leaderViews,
	}, nil
}

func (node *QueryNode) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", req.GetCollectionID()), zap.String("channel", req.GetChannel()))
	// check node healthy
	failStatus := &commonpb.Status{}
	failStatus, isUnavailable := isUnavailableCode(node, failStatus, failStatus, nil)
	if isUnavailable {
		return failStatus, nil
	}

	// check target matches
	if req.GetBase().GetTargetID() != Params.QueryNodeCfg.GetNodeID() {
		log.Warn("failed to do match target id when sync ", zap.Int64("expect", req.GetBase().GetTargetID()), zap.Int64("actual", Params.QueryNodeCfg.GetNodeID()))
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), Params.QueryNodeCfg.GetNodeID()),
		}
		return status, nil
	}
	shardCluster, ok := node.ShardClusterService.getShardCluster(req.GetChannel())
	if !ok {
		log.Warn("failed to find shard cluster when sync ", zap.String("channel", req.GetChannel()))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "shard not exist",
		}, nil
	}
	for _, action := range req.GetActions() {
		log.Info("sync action", zap.String("Action", action.GetType().String()), zap.Int64("segmentID", action.SegmentID))
		switch action.GetType() {
		case querypb.SyncType_Remove:
			shardCluster.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{
				SegmentIDs: []UniqueID{action.GetSegmentID()},
				Scope:      querypb.DataScope_Historical,
			}, true)
		case querypb.SyncType_Set:
			shardCluster.SyncSegments([]*querypb.ReplicaSegmentsInfo{
				{
					NodeId:      action.GetNodeID(),
					PartitionId: action.GetPartitionID(),
					SegmentIds:  []int64{action.GetSegmentID()},
					Versions:    []int64{action.GetVersion()},
				},
			}, segmentStateLoaded)

		default:
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unexpected action type",
			}, nil
		}
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}
