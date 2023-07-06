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
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
		nodeID = paramtable.GetNodeID()
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
		Value: paramtable.Get().CommonCfg.QueryCoordTimeTick.GetValue(),
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

// GetStatistics returns loaded statistics of collection.
func (node *QueryNode) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	log.Debug("received GetStatisticsRequest",
		zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Strings("vChannels", req.GetDmlChannels()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))

	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return &internalpb.GetStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	if !CheckTargetID(req.GetReq()) {
		targetID := req.GetReq().GetBase().GetTargetID()
		log.Warn("target ID not match",
			zap.Int64("targetID", targetID),
			zap.Int64("nodeID", paramtable.GetNodeID()),
		)
		return &internalpb.GetStatisticsResponse{
			Status: util.WrapStatus(commonpb.ErrorCode_NodeIDNotMatch,
				common.WrapNodeIDNotMatchMsg(targetID, paramtable.GetNodeID()),
			),
		}, nil
	}
	failRet := &internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}

	var toReduceResults []*internalpb.GetStatisticsResponse
	var mu sync.Mutex
	runningGp, runningCtx := errgroup.WithContext(ctx)
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
			ret, err := node.getChannelStatistics(runningCtx, req, ch)
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
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}
	log.Debug("reduce statistic result done")

	return ret, nil
}

// WatchDmChannels create consumers on dmChannels to receive Incremental data，which is the important part of real-time query
func (node *QueryNode) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	channel := req.GetInfos()[0]
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channel", channel.GetChannelName()),
	)

	log.Info("received watch channel request",
		zap.Int64("version", req.GetVersion()),
		zap.String("metricType", req.GetLoadMeta().GetMetricType()),
	)

	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != paramtable.GetNodeID() {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), paramtable.GetNodeID()),
		}
		return status, nil
	}

	// check metric type
	if req.GetLoadMeta().GetMetricType() == "" {
		err := fmt.Errorf("empty metric type, collection = %d", req.GetCollectionID())
		return merr.Status(err), nil
	}

	if !node.subscribingChannels.Insert(channel.GetChannelName()) {
		msg := "channel subscribing..."
		log.Warn(msg)
		return util.SuccessStatus(msg), nil
	}
	defer node.subscribingChannels.Remove(channel.GetChannelName())

	_, exist := node.delegators.Get(channel.GetChannelName())
	if exist {
		log.Info("channel already subscribed")
		return util.SuccessStatus(), nil
	}

	fieldIndexMetas := make([]*segcorepb.FieldIndexMeta, 0)
	for _, info := range req.GetIndexInfoList() {
		fieldIndexMetas = append(fieldIndexMetas, &segcorepb.FieldIndexMeta{
			CollectionID:    info.GetCollectionID(),
			FieldID:         info.GetFieldID(),
			IndexName:       info.GetIndexName(),
			TypeParams:      info.GetTypeParams(),
			IndexParams:     info.GetIndexParams(),
			IsAutoIndex:     info.GetIsAutoIndex(),
			UserIndexParams: info.GetUserIndexParams(),
		})
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(req.Schema)
	maxIndexRecordPerSegment := int64(0)
	if err != nil || sizePerRecord == 0 {
		log.Warn("failed to transfer segment size to collection, because failed to estimate size per record", zap.Error(err))
	} else {
		threshold := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024
		proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
		maxIndexRecordPerSegment = int64(threshold * proportion / float64(sizePerRecord))
	}
	node.manager.Collection.Put(req.GetCollectionID(), req.GetSchema(), &segcorepb.CollectionIndexMeta{
		IndexMetas:       fieldIndexMetas,
		MaxIndexRowCount: maxIndexRecordPerSegment,
	}, req.GetLoadMeta())
	collection := node.manager.Collection.Get(req.GetCollectionID())
	collection.SetMetricType(req.GetLoadMeta().GetMetricType())
	delegator, err := delegator.NewShardDelegator(req.GetCollectionID(), req.GetReplicaID(), channel.GetChannelName(), req.GetVersion(),
		node.clusterManager, node.manager, node.tSafeManager, node.loader, node.factory, channel.GetSeekPosition().GetTimestamp())
	if err != nil {
		log.Warn("failed to create shard delegator", zap.Error(err))
		return util.WrapStatus(commonpb.ErrorCode_UnexpectedError, "failed to create shard delegator", err), nil
	}
	node.delegators.Insert(channel.GetChannelName(), delegator)
	defer func() {
		if err != nil {
			node.delegators.GetAndRemove(channel.GetChannelName())
		}
	}()

	// create tSafe
	node.tSafeManager.Add(channel.ChannelName, channel.GetSeekPosition().GetTimestamp())
	defer func() {
		if err != nil {
			node.tSafeManager.Remove(channel.ChannelName)
		}
	}()

	pipeline, err := node.pipelineManager.Add(req.GetCollectionID(), channel.GetChannelName())
	if err != nil {
		msg := "failed to create pipeline"
		log.Warn(msg, zap.Error(err))
		return util.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err), nil
	}
	defer func() {
		if err != nil {
			node.pipelineManager.Remove(channel.GetChannelName())
		}
	}()

	pipeline.ExcludedSegments(lo.Values(req.GetSegmentInfos())...)
	for _, channelInfo := range req.GetInfos() {
		droppedInfos := lo.Map(channelInfo.GetDroppedSegmentIds(), func(id int64, _ int) *datapb.SegmentInfo {
			return &datapb.SegmentInfo{
				ID: id,
				DmlPosition: &msgpb.MsgPosition{
					Timestamp: typeutil.MaxTimestamp,
				},
			}
		})
		pipeline.ExcludedSegments(droppedInfos...)
	}

	err = loadGrowingSegments(ctx, delegator, req)
	if err != nil {
		msg := "failed to load growing segments"
		log.Warn(msg, zap.Error(err))
		return util.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg, err), nil
	}
	position := &msgpb.MsgPosition{
		ChannelName: channel.SeekPosition.ChannelName,
		MsgID:       channel.SeekPosition.MsgID,
		Timestamp:   channel.SeekPosition.Timestamp,
	}
	err = pipeline.ConsumeMsgStream(position)
	if err != nil {
		err = merr.WrapErrServiceUnavailable(err.Error(), "InitPipelineFailed")
		log.Warn(err.Error(),
			zap.Int64("collectionID", channel.CollectionID),
			zap.String("channel", channel.ChannelName),
		)
		return merr.Status(err), nil
	}

	// start pipeline
	pipeline.Start()
	// delegator after all steps done
	delegator.Start()
	log.Info("watch dml channel success")
	return util.SuccessStatus(), nil
}

func (node *QueryNode) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channel", req.GetChannelName()),
	)

	log.Info("received unsubscribe channel request")

	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthy) {

		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != paramtable.GetNodeID() {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), paramtable.GetNodeID()),
		}
		return status, nil
	}

	delegator, ok := node.delegators.GetAndRemove(req.GetChannelName())
	if ok {
		// close the delegator first to block all coming query/search requests
		delegator.Close()

		node.pipelineManager.Remove(req.GetChannelName())
		node.manager.Segment.RemoveBy(segments.WithChannel(req.GetChannelName()))
		node.tSafeManager.Remove(req.GetChannelName())
	}

	log.Info("unsubscribed channel")

	return util.SuccessStatus(), nil
}

func (node *QueryNode) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
	)

	log.Info("received load partitions request")
	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	collection := node.manager.Collection.Get(req.GetCollectionID())
	if collection != nil {
		collection.AddPartition(req.GetPartitionIDs()...)
		return merr.Status(nil), nil
	}

	if req.GetSchema() == nil {
		return merr.Status(merr.WrapErrCollectionNotLoaded(req.GetCollectionID(), "failed to load partitions")), nil
	}
	fieldIndexMetas := make([]*segcorepb.FieldIndexMeta, 0)
	for _, info := range req.GetIndexInfoList() {
		fieldIndexMetas = append(fieldIndexMetas, &segcorepb.FieldIndexMeta{
			CollectionID:    info.GetCollectionID(),
			FieldID:         info.GetFieldID(),
			IndexName:       info.GetIndexName(),
			TypeParams:      info.GetTypeParams(),
			IndexParams:     info.GetIndexParams(),
			IsAutoIndex:     info.GetIsAutoIndex(),
			UserIndexParams: info.GetUserIndexParams(),
		})
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(req.Schema)
	maxIndexRecordPerSegment := int64(0)
	if err != nil || sizePerRecord == 0 {
		log.Warn("failed to transfer segment size to collection, because failed to estimate size per record", zap.Error(err))
	} else {
		threshold := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024
		proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
		maxIndexRecordPerSegment = int64(threshold * proportion / float64(sizePerRecord))
	}
	vecField, err := typeutil.GetVectorFieldSchema(req.GetSchema())
	if err != nil {
		return merr.Status(err), nil
	}
	indexInfo, ok := lo.Find(req.GetIndexInfoList(), func(info *indexpb.IndexInfo) bool {
		return info.GetFieldID() == vecField.GetFieldID()
	})
	if !ok || indexInfo == nil {
		err = fmt.Errorf("cannot find index info for %s field", vecField.GetName())
		return merr.Status(err), nil
	}
	metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, indexInfo.GetIndexParams())
	if err != nil {
		return merr.Status(err), nil
	}
	node.manager.Collection.Put(req.GetCollectionID(), req.GetSchema(), &segcorepb.CollectionIndexMeta{
		IndexMetas:       fieldIndexMetas,
		MaxIndexRowCount: maxIndexRecordPerSegment,
	}, &querypb.LoadMetaInfo{
		CollectionID: req.GetCollectionID(),
		PartitionIDs: req.GetPartitionIDs(),
		LoadType:     querypb.LoadType_LoadCollection, // TODO: dyh, remove loadType in querynode
		MetricType:   metricType,
	})

	log.Info("load partitions done")
	return merr.Status(nil), nil
}

// LoadSegments load historical data into query node, historical data can be vector data or index
func (node *QueryNode) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	segment := req.GetInfos()[0]

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", segment.GetCollectionID()),
		zap.Int64("partitionID", segment.GetPartitionID()),
		zap.String("shard", segment.GetInsertChannel()),
		zap.Int64("segmentID", segment.GetSegmentID()),
	)

	log.Info("received load segments request",
		zap.Int64("version", req.GetVersion()),
		zap.Bool("needTransfer", req.GetNeedTransfer()),
	)
	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != paramtable.GetNodeID() {
		return util.WrapStatus(commonpb.ErrorCode_NodeIDNotMatch,
			common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), paramtable.GetNodeID())), nil
	}

	// Delegates request to workers
	if req.GetNeedTransfer() {
		delegator, ok := node.delegators.Get(segment.GetInsertChannel())
		if !ok {
			msg := "failed to load segments, delegator not found"
			log.Warn(msg)
			return util.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg), nil
		}

		req.NeedTransfer = false
		err := delegator.LoadSegments(ctx, req)
		if err != nil {
			log.Warn("delegator failed to load segments", zap.Error(err))
			return util.WrapStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
		}

		return util.SuccessStatus(), nil
	}

	if req.GetLoadScope() == querypb.LoadScope_Delta {
		return node.loadDeltaLogs(ctx, req), nil
	}

	node.manager.Collection.Put(req.GetCollectionID(), req.GetSchema(), nil, req.GetLoadMeta())

	// Actual load segment
	log.Info("start to load segments...")
	loaded, err := node.loader.Load(ctx,
		req.GetCollectionID(),
		segments.SegmentTypeSealed,
		req.GetVersion(),
		req.GetInfos()...,
	)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Info("load segments done...",
		zap.Int64s("segments", lo.Map(loaded, func(s segments.Segment, _ int) int64 { return s.ID() })))
	return util.SuccessStatus(), nil
}

// ReleaseCollection clears all data related to this collection on the querynode
func (node *QueryNode) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	return util.SuccessStatus(), nil
}

// ReleasePartitions clears all data related to this partition on the querynode
func (node *QueryNode) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collection", req.GetCollectionID()),
		zap.Int64s("partitions", req.GetPartitionIDs()),
	)

	log.Info("received release partitions request")

	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	collection := node.manager.Collection.Get(req.GetCollectionID())
	if collection != nil {
		for _, partition := range req.GetPartitionIDs() {
			collection.RemovePartition(partition)
		}
	}

	log.Info("release partitions done")
	return util.SuccessStatus(), nil
}

// ReleaseSegments remove the specified segments from query node according segmentIDs, partitionIDs, and collectionID
func (node *QueryNode) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("shard", req.GetShard()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)

	log.Info("received release segment request",
		zap.String("scope", req.GetScope().String()),
		zap.Bool("needTransfer", req.GetNeedTransfer()),
	)

	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != paramtable.GetNodeID() {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), paramtable.GetNodeID()),
		}
		return status, nil
	}

	if req.GetNeedTransfer() {
		delegator, ok := node.delegators.Get(req.GetShard())
		if !ok {
			msg := "failed to release segment, delegator not found"
			log.Warn(msg)
			return util.WrapStatus(commonpb.ErrorCode_UnexpectedError, msg), nil
		}

		req.NeedTransfer = false
		err := delegator.ReleaseSegments(ctx, req, false)
		if err != nil {
			log.Warn("delegator failed to release segment", zap.Error(err))
			return util.WrapStatus(commonpb.ErrorCode_UnexpectedError, err.Error()), nil
		}

		return util.SuccessStatus(), nil
	}

	log.Info("start to release segments")
	for _, id := range req.GetSegmentIDs() {
		node.manager.Segment.Remove(id, req.GetScope())
	}

	return util.SuccessStatus(), nil
}

// GetSegmentInfo returns segment information of the collection on the queryNode, and the information includes memSize, numRow, indexName, indexID ...
func (node *QueryNode) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return &querypb.GetSegmentInfoResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	var segmentInfos []*querypb.SegmentInfo
	for _, segmentID := range in.GetSegmentIDs() {
		segment := node.manager.Segment.Get(segmentID)
		if segment == nil {
			continue
		}
		collection := node.manager.Collection.Get(segment.Collection())
		if collection == nil {
			continue
		}

		// TODO(yah01): now Milvus supports only 1 vector field
		vecFields := funcutil.GetVecFieldIDs(collection.Schema())
		var (
			indexName  string
			indexID    int64
			indexInfos []*querypb.FieldIndexInfo
		)
		for _, field := range vecFields {
			index := segment.GetIndex(field)
			if index != nil {
				indexName = index.IndexInfo.GetIndexName()
				indexID = index.IndexInfo.GetIndexID()
				indexInfos = append(indexInfos, index.IndexInfo)
			}
		}

		info := &querypb.SegmentInfo{
			SegmentID:    segment.ID(),
			SegmentState: segment.Type(),
			DmChannel:    segment.Shard(),
			PartitionID:  segment.Partition(),
			CollectionID: segment.Collection(),
			NodeID:       paramtable.GetNodeID(),
			NodeIds:      []int64{paramtable.GetNodeID()},
			MemSize:      segment.MemSize(),
			NumRows:      segment.InsertCount(),
			IndexName:    indexName,
			IndexID:      indexID,
			IndexInfos:   indexInfos,
		}
		segmentInfos = append(segmentInfos, info)
	}

	return &querypb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Infos: segmentInfos,
	}, nil
}

// only used for shard delegator search segments from worker
func (node *QueryNode) SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	channel := req.GetDmlChannels()[0]
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Int64("collectionID", req.Req.GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)

	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		return nil, merr.WrapErrServiceNotReady(fmt.Sprintf("node id: %d is unhealthy", paramtable.GetNodeID()))
	}
	defer node.lifetime.Done()

	failRet := WrapSearchResult(commonpb.ErrorCode_UnexpectedError, "")
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.TotalLabel, metrics.FromLeader).Inc()
	defer func() {
		if failRet.Status.ErrorCode != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.FailLabel, metrics.FromLeader).Inc()
		}
	}()

	log.Debug("start to search segments on worker",
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)
	searchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := timerecord.NewTimeRecorder("searchSegments")
	log.Debug("search segments...")

	collection := node.manager.Collection.Get(req.Req.GetCollectionID())
	if collection == nil {
		log.Warn("failed to search segments", zap.Error(segments.ErrCollectionNotFound))
		err := segments.WrapCollectionNotFound(req.GetReq().GetCollectionID())
		failRet.Status.Reason = err.Error()
		return failRet, err
	}

	task := tasks.NewSearchTask(searchCtx, collection, node.manager, req)
	if err := node.scheduler.Add(task); err != nil {
		log.Warn("failed to search channel", zap.Error(err))
		failRet.Status.Reason = err.Error()
		return failRet, err
	}

	err := task.Wait()
	if err != nil {
		log.Warn("failed to search segments", zap.Error(err))
		failRet.Status.Reason = err.Error()
		return failRet, err
	}

	tr.CtxElapse(ctx, fmt.Sprintf("search segments done, channel = %s, segmentIDs = %v",
		channel,
		req.GetSegmentIDs(),
	))

	// TODO QueryNodeSQLatencyInQueue QueryNodeReduceLatency
	failRet.Status.ErrorCode = commonpb.ErrorCode_Success
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.SuccessLabel, metrics.FromLeader).Inc()

	result := task.Result()
	if result.CostAggregation != nil {
		// update channel's response time
		result.CostAggregation.ResponseTime = latency.Milliseconds()
	}
	return result, nil
}

// Search performs replica search tasks.
func (node *QueryNode) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	if req.FromShardLeader {
		// for compatible with rolling upgrade from version before v2.2.9
		return node.SearchSegments(ctx, req)
	}

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.Strings("channels", req.GetDmlChannels()),
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
	)

	log.Debug("Received SearchRequest",
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()))

	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return &internalpb.SearchResults{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	if !CheckTargetID(req.GetReq()) {
		targetID := req.GetReq().GetBase().GetTargetID()
		log.Warn("target ID not match",
			zap.Int64("targetID", targetID),
			zap.Int64("nodeID", paramtable.GetNodeID()),
		)
		return WrapSearchResult(commonpb.ErrorCode_NodeIDNotMatch,
			common.WrapNodeIDNotMatchMsg(targetID, paramtable.GetNodeID())), nil
	}

	failRet := &internalpb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	collection := node.manager.Collection.Get(req.GetReq().GetCollectionID())
	if collection == nil {
		failRet.Status = merr.Status(merr.WrapErrCollectionNotFound(req.GetReq().GetCollectionID()))
		return failRet, nil
	}

	// Check if the metric type specified in search params matches the metric type in the index info.
	if !req.GetFromShardLeader() && req.GetReq().GetMetricType() != "" {
		if req.GetReq().GetMetricType() != collection.GetMetricType() {
			failRet.Status = merr.Status(merr.WrapErrParameterInvalid(collection.GetMetricType(), req.GetReq().GetMetricType(),
				fmt.Sprintf("collection:%d, metric type not match", collection.ID())))
			return failRet, nil
		}
	}

	// Define the metric type when it has not been explicitly assigned by the user.
	if !req.GetFromShardLeader() && req.GetReq().GetMetricType() == "" {
		req.Req.MetricType = collection.GetMetricType()
	}

	var toReduceResults []*internalpb.SearchResults
	var mu sync.Mutex
	runningGp, runningCtx := errgroup.WithContext(ctx)
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
			ret, err := node.searchChannel(runningCtx, req, ch)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failRet.Status.Reason = err.Error()
				failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
				return err
			}
			if ret.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				return merr.Error(failRet.GetStatus())
			}
			toReduceResults = append(toReduceResults, ret)
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		return failRet, nil
	}

	tr := timerecord.NewTimeRecorderWithTrace(ctx, "searchRequestReduce")
	result, err := segments.ReduceSearchResults(ctx, toReduceResults, req.Req.GetNq(), req.Req.GetTopk(), req.Req.GetMetricType())
	if err != nil {
		log.Warn("failed to reduce search results", zap.Error(err))
		failRet.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		failRet.Status.Reason = err.Error()
		return failRet, nil
	}
	metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel).
		Observe(float64(tr.ElapseSpan().Milliseconds()))

	collector.Rate.Add(metricsinfo.NQPerSecond, float64(req.GetReq().GetNq()))
	collector.Rate.Add(metricsinfo.SearchThroughput, float64(proto.Size(req)))
	metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.SearchLabel).
		Add(float64(proto.Size(req)))

	if result.CostAggregation != nil {
		// update channel's response time
		currentTotalNQ := node.scheduler.GetWaitingTaskTotalNQ()
		result.CostAggregation.TotalNQ = currentTotalNQ
	}
	return result, nil
}

// only used for delegator query segments from worker
func (node *QueryNode) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	failRet := WrapRetrieveResult(commonpb.ErrorCode_UnexpectedError, "")
	msgID := req.Req.Base.GetMsgID()
	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()
	channel := req.GetDmlChannels()[0]
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", msgID),
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)

	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		err := merr.WrapErrServiceUnavailable(fmt.Sprintf("node id: %d is unhealthy", paramtable.GetNodeID()))
		failRet.Status = merr.Status(err)
		return failRet, nil
	}
	defer node.lifetime.Done()

	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.TotalLabel, metrics.FromLeader).Inc()
	defer func() {
		if failRet.Status.ErrorCode != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SearchLabel, metrics.FailLabel, metrics.FromLeader).Inc()
		}
	}()

	log.Debug("start do query segments",
		zap.Bool("fromShardLeader", req.GetFromShardLeader()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)
	// add cancel when error occurs
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := timerecord.NewTimeRecorder("querySegments")
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
	// TODO QueryNodeSQLatencyInQueue QueryNodeReduceLatency
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel, metrics.FromLeader).Inc()
	results.CostAggregation = &internalpb.CostAggregation{
		ServiceTime:  latency.Milliseconds(),
		ResponseTime: latency.Milliseconds(),
		TotalNQ:      0,
	}
	return results, nil
}

// Query performs replica query tasks.
func (node *QueryNode) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	if req.FromShardLeader {
		// for compatible with rolling upgrade from version before v2.2.9
		return node.querySegments(ctx, req)
	}

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.Strings("shards", req.GetDmlChannels()),
	)

	log.Debug("received query request",
		zap.Int64s("outputFields", req.GetReq().GetOutputFieldsId()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("travelTimestamp", req.GetReq().GetTravelTimestamp()),
		zap.Bool("isCount", req.GetReq().GetIsCount()),
	)

	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	if !CheckTargetID(req.GetReq()) {
		targetID := req.GetReq().GetBase().GetTargetID()
		log.Warn("target ID not match",
			zap.Int64("targetID", targetID),
			zap.Int64("nodeID", paramtable.GetNodeID()),
		)
		return WrapRetrieveResult(commonpb.ErrorCode_NodeIDNotMatch,
			common.WrapNodeIDNotMatchMsg(targetID, paramtable.GetNodeID())), nil
	}

	toMergeResults := make([]*internalpb.RetrieveResults, len(req.GetDmlChannels()))
	runningGp, runningCtx := errgroup.WithContext(ctx)

	for i, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.QueryRequest{
			Req:             req.Req,
			DmlChannels:     []string{ch},
			SegmentIDs:      req.SegmentIDs,
			FromShardLeader: req.FromShardLeader,
			Scope:           req.Scope,
		}

		idx := i
		runningGp.Go(func() error {
			ret, err := node.queryChannel(runningCtx, req, ch)
			if err != nil {
				return err
			}
			if ret.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				return fmt.Errorf("%s", ret.Status.Reason)
			}
			toMergeResults[idx] = ret
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		return WrapRetrieveResult(commonpb.ErrorCode_UnexpectedError, "failed to query channel", err), nil
	}

	reducer := segments.CreateInternalReducer(req, node.manager.Collection.Get(req.GetReq().GetCollectionID()).Schema())

	ret, err := reducer.Reduce(ctx, toMergeResults)
	if err != nil {
		return WrapRetrieveResult(commonpb.ErrorCode_UnexpectedError, "failed to query channel", err), nil
	}

	if !req.FromShardLeader {
		collector.Rate.Add(metricsinfo.NQPerSecond, 1)
		metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Add(float64(proto.Size(req)))
	}

	if ret.CostAggregation != nil {
		// update channel's response time
		currentTotalNQ := node.scheduler.GetWaitingTaskTotalNQ()
		ret.CostAggregation.TotalNQ = currentTotalNQ
	}
	return ret, nil
}

// SyncReplicaSegments syncs replica node & segments states
func (node *QueryNode) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	return util.SuccessStatus(), nil
}

// ShowConfigurations returns the configurations of queryNode matching req.Pattern
func (node *QueryNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		err := merr.WrapErrServiceNotReady(fmt.Sprintf("node id: %d is unhealthy", paramtable.GetNodeID()))
		log.Warn("QueryNode.ShowConfigurations failed",
			zap.Int64("nodeId", paramtable.GetNodeID()),
			zap.String("req", req.Pattern),
			zap.Error(err))

		return &internalpb.ShowConfigurationsResponse{
			Status:        merr.Status(err),
			Configuations: nil,
		}, nil
	}
	defer node.lifetime.Done()

	configList := make([]*commonpb.KeyValuePair, 0)
	for key, value := range paramtable.Get().GetComponentConfigurations("querynode", req.Pattern) {
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
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		err := merr.WrapErrServiceNotReady(fmt.Sprintf("node id: %d is unhealthy", paramtable.GetNodeID()))
		log.Warn("QueryNode.GetMetrics failed",
			zap.Int64("nodeId", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status:   merr.Status(err),
			Response: "",
		}, nil
	}
	defer node.lifetime.Done()

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("QueryNode.GetMetrics failed to parse metric type",
			zap.Int64("nodeId", paramtable.GetNodeID()),
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
			log.Warn("QueryNode.GetMetrics failed",
				zap.Int64("nodeId", paramtable.GetNodeID()),
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
		log.RatedDebug(50, "QueryNode.GetMetrics",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.String("metricType", metricType),
			zap.Any("queryNodeMetrics", queryNodeMetrics))

		return queryNodeMetrics, nil
	}

	log.Debug("QueryNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("nodeId", paramtable.GetNodeID()),
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
	log := log.With(
		zap.Int64("msgID", req.GetBase().GetMsgID()),
		zap.Int64("nodeID", paramtable.GetNodeID()),
	)
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		err := merr.WrapErrServiceNotReady(fmt.Sprintf("node id: %d is unhealthy", paramtable.GetNodeID()))
		log.Warn("QueryNode.GetMetrics failed",
			zap.Error(err))

		return &querypb.GetDataDistributionResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != paramtable.GetNodeID() {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), paramtable.GetNodeID()),
		}
		return &querypb.GetDataDistributionResponse{Status: status}, nil
	}

	sealedSegments := node.manager.Segment.GetBy(segments.WithType(commonpb.SegmentState_Sealed))
	segmentVersionInfos := make([]*querypb.SegmentVersionInfo, 0, len(sealedSegments))
	for _, s := range sealedSegments {
		segmentVersionInfos = append(segmentVersionInfos, &querypb.SegmentVersionInfo{
			ID:                 s.ID(),
			Collection:         s.Collection(),
			Partition:          s.Partition(),
			Channel:            s.Shard(),
			Version:            s.Version(),
			LastDeltaTimestamp: s.LastDeltaTimestamp(),
		})
	}

	channelVersionInfos := make([]*querypb.ChannelVersionInfo, 0)
	leaderViews := make([]*querypb.LeaderView, 0)

	node.delegators.Range(func(key string, value delegator.ShardDelegator) bool {
		if !value.Serviceable() {
			return true
		}
		channelVersionInfos = append(channelVersionInfos, &querypb.ChannelVersionInfo{
			Channel:    key,
			Collection: value.Collection(),
			Version:    value.Version(),
		})

		sealed, growing := value.GetSegmentInfo(false)
		sealedSegments := make(map[int64]*querypb.SegmentDist)
		for _, item := range sealed {
			for _, segment := range item.Segments {
				sealedSegments[segment.SegmentID] = &querypb.SegmentDist{
					NodeID:  item.NodeID,
					Version: segment.Version,
				}
			}
		}

		growingSegments := make(map[int64]*msgpb.MsgPosition)
		for _, entry := range growing {
			segment := node.manager.Segment.GetWithType(entry.SegmentID, segments.SegmentTypeGrowing)
			if segment == nil {
				log.Warn("leader view growing not found", zap.String("channel", key), zap.Int64("segmentID", entry.SegmentID))
				growingSegments[entry.SegmentID] = &msgpb.MsgPosition{}
			}
			growingSegments[entry.SegmentID] = segment.StartPosition()
		}

		leaderViews = append(leaderViews, &querypb.LeaderView{
			Collection:      value.Collection(),
			Channel:         key,
			SegmentDist:     sealedSegments,
			GrowingSegments: growingSegments,
			TargetVersion:   value.GetTargetVersion(),
		})
		return true
	})

	return &querypb.GetDataDistributionResponse{
		Status:      &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		NodeID:      paramtable.GetNodeID(),
		Segments:    segmentVersionInfos,
		Channels:    channelVersionInfos,
		LeaderViews: leaderViews,
	}, nil
}

func (node *QueryNode) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", req.GetCollectionID()), zap.String("channel", req.GetChannel()))
	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != paramtable.GetNodeID() {
		log.Warn("failed to do match target id when sync ", zap.Int64("expect", req.GetBase().GetTargetID()), zap.Int64("actual", node.session.ServerID))
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), paramtable.GetNodeID()),
		}
		return status, nil
	}

	// get shard delegator
	shardDelegator, ok := node.delegators.Get(req.GetChannel())
	if !ok {
		log.Warn("failed to find shard cluster when sync ", zap.String("channel", req.GetChannel()))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "shard not exist",
		}, nil
	}

	// translate segment action
	removeActions := make([]*querypb.SyncAction, 0)
	addSegments := make(map[int64][]*querypb.SegmentLoadInfo)
	for _, action := range req.GetActions() {
		log := log.With(zap.String("Action",
			action.GetType().String()),
			zap.Int64("segmentID", action.SegmentID),
			zap.Int64("TargetVersion", action.GetTargetVersion()),
		)
		log.Info("sync action")
		switch action.GetType() {
		case querypb.SyncType_Remove:
			removeActions = append(removeActions, action)
		case querypb.SyncType_Set:
			addSegments[action.GetNodeID()] = append(addSegments[action.GetNodeID()], action.GetInfo())
		case querypb.SyncType_UpdateVersion:
			shardDelegator.SyncTargetVersion(action.GetTargetVersion(), action.GetGrowingInTarget(), action.GetSealedInTarget())
		default:
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "unexpected action type",
			}, nil
		}
	}

	for nodeID, infos := range addSegments {
		err := shardDelegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_LoadSegments),
				commonpbutil.WithMsgID(req.Base.GetMsgID()),
			),
			Infos:        infos,
			Schema:       req.GetSchema(),
			LoadMeta:     req.GetLoadMeta(),
			CollectionID: req.GetCollectionID(),
			ReplicaID:    req.GetReplicaID(),
			DstNodeID:    nodeID,
			Version:      req.GetVersion(),
			NeedTransfer: false,
			LoadScope:    querypb.LoadScope_Delta,
		})
		if err != nil {
			return util.WrapStatus(commonpb.ErrorCode_UnexpectedError, "failed to sync(load) segment", err), nil
		}
	}

	for _, action := range removeActions {
		shardDelegator.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{
			NodeID:       action.GetNodeID(),
			SegmentIDs:   []int64{action.GetSegmentID()},
			Scope:        querypb.DataScope_Historical,
			CollectionID: req.GetCollectionID(),
		}, true)
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// Delete is used to forward delete message between delegator and workers.
func (node *QueryNode) Delete(ctx context.Context, req *querypb.DeleteRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionId()),
		zap.String("channel", req.GetVchannelName()),
		zap.Int64("segmentID", req.GetSegmentId()),
	)

	// check node healthy
	if !node.lifetime.Add(commonpbutil.IsHealthy) {
		msg := fmt.Sprintf("query node %d is not ready", paramtable.GetNodeID())
		err := merr.WrapErrServiceNotReady(msg)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check target matches
	if req.GetBase().GetTargetID() != paramtable.GetNodeID() {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NodeIDNotMatch,
			Reason:    common.WrapNodeIDNotMatchMsg(req.GetBase().GetTargetID(), paramtable.GetNodeID()),
		}
		return status, nil
	}

	log.Info("QueryNode received worker delete request")
	log.Debug("Worker delete detail",
		zap.String("pks", req.GetPrimaryKeys().String()),
		zap.Uint64s("tss", req.GetTimestamps()),
	)

	segments := node.manager.Segment.GetBy(segments.WithID(req.GetSegmentId()))
	if len(segments) == 0 {
		err := merr.WrapErrSegmentNotFound(req.GetSegmentId())
		log.Warn("segment not found for delete")
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SegmentNotFound,
			Reason:    fmt.Sprintf("segment %d not found", req.GetSegmentId()),
			Code:      merr.Code(err),
		}, nil
	}

	pks := storage.ParseIDs2PrimaryKeys(req.GetPrimaryKeys())
	for _, segment := range segments {
		err := segment.Delete(pks, req.GetTimestamps())
		if err != nil {
			log.Warn("segment delete failed", zap.Error(err))
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    fmt.Sprintf("delete on segment %d failed, %s", req.GetSegmentId(), err.Error()),
			}, nil
		}
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}
