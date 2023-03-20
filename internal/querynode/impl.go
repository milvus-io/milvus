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
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// GetComponentStates returns information about whether the node is healthy
func (node *QueryNode) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	stats := &milvuspb.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	code := node.lifetime.GetState()
	nodeID := common.NotRegisteredID
	if node.session != nil && node.session.Registered() {
		nodeID = node.GetSession().ServerID
	}
	info := &milvuspb.ComponentInfo{
		NodeID:    nodeID,
		Role:      typeutil.QueryNodeRole,
		StateCode: code,
	}
	stats.State = info
	log.Debug("Get QueryNode component state done", zap.String("stateCode", info.StateCode.String()))
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
		Value: Params.CommonCfg.QueryCoordTimeTick.GetValue(),
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
	log.Ctx(ctx).Debug("received GetStatisticsRequest",
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

	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		failRet.Status.Reason = msgQueryNodeIsUnhealthy(node.GetSession().ServerID)
		return failRet, nil
	}
	defer node.lifetime.Done()

	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()
	log.Ctx(ctx).Debug("received GetStatisticRequest",
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
			zap.String("dml channel", dmlChannel),
			zap.Error(err))
		failRet.Status.ErrorCode = commonpb.ErrorCode_NotShardLeader
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	log.Debug("start do statistics",
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

		tr.Elapse(fmt.Sprintf("do statistics done, traceID = %s, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
			traceID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))
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
		log.Warn("get statistics on cluster failed",
			zap.Int64("collectionID", req.Req.GetCollectionID()),
			zap.Error(errCluster))
		failRet.Status.Reason = errCluster.Error()
		return failRet, nil
	}

	tr.Elapse(fmt.Sprintf("start reduce statistic result, traceID = %s, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		traceID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	ret, err := reduceStatisticResponse(results)
	if err != nil {
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}
	log.Debug("reduce statistic result done",
		zap.Any("results", ret))

	tr.Elapse(fmt.Sprintf("do statistics done, traceID = %s, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		traceID, req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	return ret, nil
}

// WatchDmChannels create consumers on dmChannels to receive Incremental data，which is the important part of real-time query
func (node *QueryNode) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	nodeID := node.session.ServerID
	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		err := fmt.Errorf("query node %d is not ready", nodeID)
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	defer node.lifetime.Done()

	// check target matches
	if in.GetBase().GetTargetID() != nodeID {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(in.GetBase().GetTargetID(), nodeID),
		}
		return status, nil
	}

	log := log.With(
		zap.Int64("collectionID", in.GetCollectionID()),
		zap.Int64("nodeID", nodeID),
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
	// check node healthy
	nodeID := node.session.ServerID
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		err := fmt.Errorf("query node %d is not ready", nodeID)
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != nodeID {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), nodeID),
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
	nodeID := node.session.ServerID
	log.Info("wayblink", zap.Int64("nodeID", nodeID))
	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		err := fmt.Errorf("query node %d is not ready", nodeID)
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	defer node.lifetime.Done()

	// check target matches
	if in.GetBase().GetTargetID() != nodeID {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(in.GetBase().GetTargetID(), nodeID),
		}
		return status, nil
	}

	if in.GetNeedTransfer() {
		return node.TransferLoad(ctx, in)
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
		zap.Int64("nodeID", nodeID))

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

	log.Info("loadSegmentsTask Enqueue done", zap.Int64("collectionID", in.CollectionID), zap.Int64s("segmentIDs", segmentIDs), zap.Int64("nodeID", nodeID))

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
		log.Info("loadSegmentsTask WaitToFinish done", zap.Int64("collectionID", in.CollectionID), zap.Int64s("segmentIDs", segmentIDs), zap.Int64("nodeID", nodeID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	return waitFunc()
}

// ReleaseCollection clears all data related to this collection on the querynode
func (node *QueryNode) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		err := fmt.Errorf("query node %d is not ready", node.GetSession().ServerID)
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	defer node.lifetime.Done()

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

func (node *QueryNode) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	nodeID := node.session.ServerID
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		err := fmt.Errorf("query node %d is not ready", nodeID)
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != nodeID {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), nodeID),
		}
		return status, nil
	}

	log.Ctx(ctx).With(zap.Int64("colID", req.GetCollectionID()), zap.Int64s("partIDs", req.GetPartitionIDs()))
	log.Info("loading partitions")
	for _, part := range req.GetPartitionIDs() {
		err := node.metaReplica.addPartition(req.GetCollectionID(), part)
		if err != nil {
			log.Warn(err.Error())
		}
	}
	log.Info("load partitions done")
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
}

// ReleasePartitions clears all data related to this partition on the querynode
func (node *QueryNode) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	nodeID := node.session.ServerID
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		err := fmt.Errorf("query node %d is not ready", nodeID)
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != nodeID {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), nodeID),
		}
		return status, nil
	}

	log.Ctx(ctx).With(zap.Int64("colID", req.GetCollectionID()), zap.Int64s("partIDs", req.GetPartitionIDs()))
	log.Info("releasing partitions")
	for _, part := range req.GetPartitionIDs() {
		node.metaReplica.removePartition(part)
	}
	log.Info("release partitions done")
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
}

// ReleaseSegments remove the specified segments from query node according segmentIDs, partitionIDs, and collectionID
func (node *QueryNode) ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	nodeID := node.session.ServerID
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		err := fmt.Errorf("query node %d is not ready", nodeID)
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	defer node.lifetime.Done()

	// check target matches
	if in.GetBase().GetTargetID() != nodeID {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(in.GetBase().GetTargetID(), nodeID),
		}
		return status, nil
	}

	if in.GetNeedTransfer() {
		return node.TransferRelease(ctx, in)
	}

	log.Info("start to release segments", zap.Int64("collectionID", in.CollectionID), zap.Int64s("segmentIDs", in.SegmentIDs))

	for _, id := range in.SegmentIDs {
		switch in.GetScope() {
		case querypb.DataScope_Streaming:
			node.metaReplica.removeSegment(id, segmentTypeGrowing)
		case querypb.DataScope_Historical:
			node.metaReplica.removeSegment(id, segmentTypeSealed)
		case querypb.DataScope_All:
			node.metaReplica.removeSegment(id, segmentTypeSealed)
			node.metaReplica.removeSegment(id, segmentTypeGrowing)
		}
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
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		err := fmt.Errorf("query node %d is not ready", node.GetSession().ServerID)
		res := &querypb.GetSegmentInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}
		return res, nil
	}
	defer node.lifetime.Done()

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
	nodeID := node.session.ServerID
	if !node.IsStandAlone && req.GetReq().GetBase().GetTargetID() != nodeID {
		return &internalpb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
				Reason: fmt.Sprintf("QueryNode %d can't serve, recovering: %s",
					nodeID,
					common.WrapNodeIDNotMatchMsg(req.GetReq().GetBase().GetTargetID(), nodeID)),
			},
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "QueryNode-Search")
	defer sp.End()
	log := log.Ctx(ctx)

	failRet := &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}

	tr := timerecord.NewTimeRecorder("Search")
	if !req.GetFromShardLeader() {
		log.Debug("Received SearchRequest",
			zap.Strings("vChannels", req.GetDmlChannels()),
			zap.Int64s("segmentIDs", req.GetSegmentIDs()),
			zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
			zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))
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
		metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(nodeID, 10), metrics.SearchLabel).Add(float64(proto.Size(req)))
	}
	return ret, nil
}

func (node *QueryNode) searchWithDmlChannel(ctx context.Context, req *querypb.SearchRequest, dmlChannel string) (*internalpb.SearchResults, error) {
	nodeID := node.session.ServerID
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(nodeID), metrics.SearchLabel, metrics.TotalLabel).Inc()
	failRet := &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	defer func() {
		if failRet.Status.ErrorCode != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(nodeID), metrics.SearchLabel, metrics.FailLabel).Inc()
		}
	}()
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		failRet.Status.Reason = msgQueryNodeIsUnhealthy(nodeID)
		return failRet, nil
	}
	defer node.lifetime.Done()

	if node.queryShardService == nil {
		failRet.Status.Reason = "queryShardService is nil"
		return failRet, nil
	}

	qs, err := node.queryShardService.getQueryShard(dmlChannel)
	if err != nil {
		log.Ctx(ctx).Warn("Search failed, failed to get query shard",
			zap.String("dml channel", dmlChannel),
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
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(nodeID),
			metrics.SearchLabel).Observe(float64(historicalTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(nodeID),
			metrics.SearchLabel).Observe(float64(historicalTask.reduceDur.Milliseconds()))
		latency := tr.ElapseSpan()
		metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(nodeID), metrics.SearchLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
		metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(nodeID), metrics.SearchLabel, metrics.SuccessLabel).Inc()
		return historicalTask.Ret, nil
	}

	//from Proxy
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

	// shard leader dispatches request to its shard cluster
	var withStreamingFunc searchWithStreaming
	if !req.Req.IgnoreGrowing {
		withStreamingFunc = getSearchWithStreamingFunc(searchCtx, req, node, qs, nodeID)
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
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(nodeID), metrics.SearchLabel, metrics.Leader).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(nodeID), metrics.SearchLabel, metrics.SuccessLabel).Inc()
	metrics.QueryNodeSearchNQ.WithLabelValues(fmt.Sprint(nodeID)).Observe(float64(req.Req.GetNq()))
	metrics.QueryNodeSearchTopK.WithLabelValues(fmt.Sprint(nodeID)).Observe(float64(req.Req.GetTopk()))

	return ret, nil
}

func (node *QueryNode) queryWithDmlChannel(ctx context.Context, req *querypb.QueryRequest, dmlChannel string) (*internalpb.RetrieveResults, error) {
	nodeID := node.session.ServerID
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(nodeID), metrics.QueryLabel, metrics.TotalLabel).Inc()
	failRet := &internalpb.RetrieveResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "QueryNode-QueryWithDMLChannel")
	defer sp.End()

	defer func() {
		if failRet.Status.ErrorCode != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(nodeID), metrics.SearchLabel, metrics.FailLabel).Inc()
		}
	}()
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		failRet.Status.Reason = msgQueryNodeIsUnhealthy(nodeID)
		return failRet, nil
	}
	defer node.lifetime.Done()

	log.Ctx(ctx).Debug("queryWithDmlChannel receives query request",
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.String("vChannel", dmlChannel),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Bool("is_count", req.GetReq().GetIsCount()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))

	if node.queryShardService == nil {
		failRet.Status.Reason = "queryShardService is nil"
		return failRet, nil
	}

	qs, err := node.queryShardService.getQueryShard(dmlChannel)
	if err != nil {
		log.Ctx(ctx).Warn("Query failed, failed to get query shard",
			zap.String("dml channel", dmlChannel),
			zap.Error(err))
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	log.Ctx(ctx).Debug("queryWithDmlChannel starts do query",
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

		tr.CtxElapse(ctx, fmt.Sprintf("do query done, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
			req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

		failRet.Status.ErrorCode = commonpb.ErrorCode_Success
		metrics.QueryNodeSQLatencyInQueue.WithLabelValues(fmt.Sprint(nodeID),
			metrics.QueryLabel).Observe(float64(queryTask.queueDur.Milliseconds()))
		metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(nodeID),
			metrics.QueryLabel).Observe(float64(queryTask.reduceDur.Milliseconds()))
		latency := tr.ElapseSpan()
		metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(nodeID), metrics.QueryLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
		metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(nodeID), metrics.QueryLabel, metrics.SuccessLabel).Inc()
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
	var errCluster error
	var withStreamingFunc queryWithStreaming

	if !req.Req.IgnoreGrowing {
		withStreamingFunc = getQueryWithStreamingFunc(queryCtx, req, node, qs, nodeID)
	}
	// shard leader dispatches request to its shard cluster
	results, errCluster = cluster.Query(queryCtx, req, withStreamingFunc)
	if errCluster != nil {
		log.Ctx(ctx).Warn("failed to query cluster",
			zap.Int64("collectionID", req.Req.GetCollectionID()),
			zap.Error(errCluster))
		failRet.Status.Reason = errCluster.Error()
		return failRet, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("start reduce query result, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	reducer := createInternalReducer(ctx, req, qs.collection.Schema())

	ret, err2 := reducer.Reduce(results)
	if err != nil {
		failRet.Status.Reason = err2.Error()
		return failRet, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do query done, fromSharedLeader = %t, vChannel = %s, segmentIDs = %v",
		req.GetFromShardLeader(), dmlChannel, req.GetSegmentIDs()))

	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(nodeID), metrics.QueryLabel, metrics.Leader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(nodeID), metrics.QueryLabel, metrics.SuccessLabel).Inc()
	return ret, nil
}

// Query performs replica query tasks.
func (node *QueryNode) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	log.Ctx(ctx).Debug("Received QueryRequest",
		zap.Bool("fromShardleader", req.GetFromShardLeader()),
		zap.Strings("vChannels", req.GetDmlChannels()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Bool("is_count", req.GetReq().GetIsCount()),
		zap.Uint64("guaranteeTimestamp", req.Req.GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))

	nodeID := node.session.ServerID
	if req.GetReq().GetBase().GetTargetID() != nodeID {
		return &internalpb.RetrieveResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
				Reason: fmt.Sprintf("QueryNode %d can't serve, recovering: %s",
					nodeID,
					common.WrapNodeIDNotMatchMsg(req.GetReq().GetBase().GetTargetID(), nodeID)),
			},
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "QueryNode-Query")
	defer sp.End()

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

	reducer := createInternalReducer(ctx, req, coll.Schema())

	ret, err := reducer.Reduce(toMergeResults)
	if err != nil {
		failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}

	if !req.FromShardLeader {
		rateCol.Add(metricsinfo.NQPerSecond, 1)
		metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(nodeID, 10), metrics.QueryLabel).Add(float64(proto.Size(req)))
	}
	return ret, nil
}

// SyncReplicaSegments syncs replica node & segments states
func (node *QueryNode) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    msgQueryNodeIsUnhealthy(node.GetSession().ServerID),
		}, nil
	}
	defer node.lifetime.Done()

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
	nodeID := node.session.ServerID
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		log.Warn("QueryNode.ShowConfigurations failed",
			zap.Int64("nodeId", nodeID),
			zap.String("req", req.Pattern),
			zap.Error(errQueryNodeIsUnhealthy(nodeID)))

		return &internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgQueryNodeIsUnhealthy(nodeID),
			},
			Configuations: nil,
		}, nil
	}
	defer node.lifetime.Done()

	configList := make([]*commonpb.KeyValuePair, 0)
	for key, value := range Params.GetComponentConfigurations("querynode", req.Pattern) {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Configuations: configList,
	}, nil
}

// GetMetrics return system infos of the query node, such as total memory, memory usage, cpu usage ...
func (node *QueryNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	nodeID := node.session.ServerID
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		log.Ctx(ctx).Warn("QueryNode.GetMetrics failed",
			zap.Int64("nodeId", nodeID),
			zap.String("req", req.Request),
			zap.Error(errQueryNodeIsUnhealthy(nodeID)))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgQueryNodeIsUnhealthy(nodeID),
			},
			Response: "",
		}, nil
	}
	defer node.lifetime.Done()

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Ctx(ctx).Warn("QueryNode.GetMetrics failed to parse metric type",
			zap.Int64("nodeId", nodeID),
			zap.String("req", req.Request),
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
			log.Ctx(ctx).Warn("QueryNode.GetMetrics failed",
				zap.Int64("nodeId", nodeID),
				zap.String("req", req.Request),
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

	log.Ctx(ctx).RatedDebug(60, "QueryNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("nodeId", nodeID),
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

func (node *QueryNode) GetDataDistribution(ctx context.Context, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	nodeID := node.session.ServerID
	log := log.With(
		zap.Int64("msgID", req.GetBase().GetMsgID()),
		zap.Int64("nodeID", nodeID),
	)
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		log.Warn("QueryNode.GetMetrics failed",
			zap.Error(errQueryNodeIsUnhealthy(nodeID)))

		return &querypb.GetDataDistributionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgQueryNodeIsUnhealthy(nodeID),
			},
		}, nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != nodeID {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason: fmt.Sprintf("QueryNode %d can't serve, recovering: %s",
				nodeID,
				common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), nodeID)),
		}
		return &querypb.GetDataDistributionResponse{Status: status}, nil
	}

	growingSegments := node.metaReplica.getGrowingSegments()
	sealedSegments := node.metaReplica.getSealedSegments()
	shardClusters := node.ShardClusterService.GetShardClusters()

	channelGrowingsMap := make(map[string]map[int64]*msgpb.MsgPosition)
	for _, s := range growingSegments {
		if _, ok := channelGrowingsMap[s.vChannelID]; !ok {
			channelGrowingsMap[s.vChannelID] = make(map[int64]*msgpb.MsgPosition)
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
			mapping[info.segmentID] = &querypb.SegmentDist{
				NodeID:  info.nodeID,
				Version: info.version,
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
		NodeID:      nodeID,
		Segments:    segmentVersionInfos,
		Channels:    channelVersionInfos,
		LeaderViews: leaderViews,
	}, nil
}

func (node *QueryNode) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", req.GetCollectionID()), zap.String("channel", req.GetChannel()))
	nodeID := node.session.ServerID
	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		err := fmt.Errorf("query node %d is not ready", nodeID)
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != nodeID {
		log.Warn("failed to do match target id when sync ", zap.Int64("expect", req.GetBase().GetTargetID()), zap.Int64("actual", nodeID))
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), nodeID),
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

// to fix data race
func (node *QueryNode) SetSession(session *sessionutil.Session) {
	node.sessionMu.Lock()
	defer node.sessionMu.Unlock()
	node.session = session
}

// to fix data race
func (node *QueryNode) GetSession() *sessionutil.Session {
	node.sessionMu.Lock()
	defer node.sessionMu.Unlock()
	return node.session
}
