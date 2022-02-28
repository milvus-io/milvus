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

package querycoord

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// GetComponentStates return information about whether the coord is healthy
func (qc *QueryCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	nodeID := common.NotRegisteredID
	if qc.session != nil && qc.session.Registered() {
		nodeID = qc.session.ServerID
	}
	serviceComponentInfo := &internalpb.ComponentInfo{
		// NodeID:    Params.QueryCoordID, // will race with QueryCoord.Register()
		NodeID:    nodeID,
		StateCode: qc.stateCode.Load().(internalpb.StateCode),
	}

	//subComponentInfos, err := qs.cluster.GetComponentInfos(ctx)
	//if err != nil {
	//	return &internalpb.ComponentStates{
	//		Status: &commonpb.Status{
	//			ErrorCode: commonpb.ErrorCode_UnexpectedError,
	//			Reason:    err.Error(),
	//		},
	//	}, err
	//}
	return &internalpb.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		State: serviceComponentInfo,
		//SubcomponentStates: subComponentInfos,
	}, nil
}

// GetTimeTickChannel returns the time tick channel
// TimeTickChannel contains many time tick messages, which has been sent by query nodes
func (qc *QueryCoord) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.MsgChannelCfg.QueryCoordTimeTick,
	}, nil
}

// GetStatisticsChannel return the statistics channel
// Statistics channel contains statistics infos of query nodes, such as segment infos, memory infos
func (qc *QueryCoord) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.MsgChannelCfg.QueryNodeStats,
	}, nil
}

// ShowCollections return all the collections that have been loaded
func (qc *QueryCoord) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	log.Debug("show collection start",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64s("collectionIDs", req.CollectionIDs),
		zap.Int64("msgID", req.Base.MsgID))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Error("show collection failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return &querypb.ShowCollectionsResponse{
			Status: status,
		}, nil
	}
	collectionInfos := qc.meta.showCollections()
	ID2collectionInfo := make(map[UniqueID]*querypb.CollectionInfo)
	inMemoryCollectionIDs := make([]UniqueID, 0)
	for _, info := range collectionInfos {
		ID2collectionInfo[info.CollectionID] = info
		inMemoryCollectionIDs = append(inMemoryCollectionIDs, info.CollectionID)
	}
	inMemoryPercentages := make([]int64, 0)
	if len(req.CollectionIDs) == 0 {
		for _, id := range inMemoryCollectionIDs {
			inMemoryPercentages = append(inMemoryPercentages, ID2collectionInfo[id].InMemoryPercentage)
		}
		log.Debug("show collection end",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64s("collections", inMemoryCollectionIDs),
			zap.Int64s("inMemoryPercentage", inMemoryPercentages),
			zap.Int64("msgID", req.Base.MsgID))
		return &querypb.ShowCollectionsResponse{
			Status:              status,
			CollectionIDs:       inMemoryCollectionIDs,
			InMemoryPercentages: inMemoryPercentages,
		}, nil
	}
	for _, id := range req.CollectionIDs {
		if _, ok := ID2collectionInfo[id]; !ok {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			err := fmt.Errorf("collection %d has not been loaded to memory or load failed", id)
			status.Reason = err.Error()
			log.Warn("show collection failed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("collectionID", id),
				zap.Int64("msgID", req.Base.MsgID),
				zap.Error(err))
			return &querypb.ShowCollectionsResponse{
				Status: status,
			}, nil
		}
		inMemoryPercentages = append(inMemoryPercentages, ID2collectionInfo[id].InMemoryPercentage)
	}
	log.Debug("show collection end",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64s("collections", req.CollectionIDs),
		zap.Int64("msgID", req.Base.MsgID),
		zap.Int64s("inMemoryPercentage", inMemoryPercentages))
	return &querypb.ShowCollectionsResponse{
		Status:              status,
		CollectionIDs:       req.CollectionIDs,
		InMemoryPercentages: inMemoryPercentages,
	}, nil
}

// LoadCollection loads all the sealed segments of this collection to queryNodes, and assigns watchDmChannelRequest to queryNodes
func (qc *QueryCoord) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelTotal).Inc()

	collectionID := req.CollectionID
	//schema := req.Schema
	log.Debug("loadCollectionRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", req.Base.MsgID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Error("load collection failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	if collectionInfo, err := qc.meta.getCollectionInfoByID(collectionID); err == nil {
		// if collection has been loaded by load collection request, return success
		if collectionInfo.LoadType == querypb.LoadType_loadCollection {
			log.Debug("collection has already been loaded, return load success directly",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("collectionID", collectionID),
				zap.Int64("msgID", req.Base.MsgID))

			metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelSuccess).Inc()
			return status, nil
		}
		// if some partitions of the collection have been loaded by load partitions request, return error
		// should release partitions first, then load collection again
		if collectionInfo.LoadType == querypb.LoadType_LoadPartition {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			err = fmt.Errorf("some partitions %v of collection %d has been loaded into QueryNode, please release partitions firstly",
				collectionInfo.PartitionIDs, collectionID)
			status.Reason = err.Error()
			log.Warn("loadCollectionRequest failed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("collectionID", collectionID),
				zap.Int64s("loaded partitionIDs", collectionInfo.PartitionIDs),
				zap.Int64("msgID", req.Base.MsgID),
				zap.Error(err))

			metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
			return status, nil
		}
	}

	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_GrpcRequest)
	loadCollectionTask := &loadCollectionTask{
		baseTask:              baseTask,
		LoadCollectionRequest: req,
		broker:                qc.broker,
		cluster:               qc.cluster,
		meta:                  qc.meta,
	}
	err := qc.scheduler.Enqueue(loadCollectionTask)
	if err != nil {
		log.Error("loadCollectionRequest failed to add execute task to scheduler",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	err = loadCollectionTask.waitToFinish()
	if err != nil {
		log.Error("load collection to query nodes failed",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	log.Debug("loadCollectionRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", req.Base.MsgID))

	return status, nil
}

// ReleaseCollection clears all data related to this collecion on the querynode
func (qc *QueryCoord) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelTotal).Inc()
	//dbID := req.DbID
	collectionID := req.CollectionID
	log.Debug("releaseCollectionRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", req.Base.MsgID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Error("release collection failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	// if collection has not been loaded into memory, return release collection successfully
	hasCollection := qc.meta.hasCollection(collectionID)
	if !hasCollection {
		log.Debug("release collection end, the collection has not been loaded into QueryNode",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", req.Base.MsgID))

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelSuccess).Inc()
		return status, nil
	}

	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_GrpcRequest)
	releaseCollectionTask := &releaseCollectionTask{
		baseTask:                 baseTask,
		ReleaseCollectionRequest: req,
		cluster:                  qc.cluster,
		meta:                     qc.meta,
		broker:                   qc.broker,
	}
	err := qc.scheduler.Enqueue(releaseCollectionTask)
	if err != nil {
		log.Error("releaseCollectionRequest failed to add execute task to scheduler",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	err = releaseCollectionTask.waitToFinish()
	if err != nil {
		log.Error("release collection from query nodes failed",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	log.Debug("releaseCollectionRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", req.Base.MsgID))
	//qc.MetaReplica.printMeta()
	//qc.cluster.printMeta()

	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelSuccess).Inc()
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(releaseCollectionTask.elapseSpan().Milliseconds()))
	return status, nil
}

// ShowPartitions return all the partitions that have been loaded
func (qc *QueryCoord) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	collectionID := req.CollectionID
	log.Debug("show partitions start",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", req.PartitionIDs),
		zap.Int64("msgID", req.Base.MsgID))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Error("show partition failed", zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return &querypb.ShowPartitionsResponse{
			Status: status,
		}, nil
	}

	partitionStates, err := qc.meta.showPartitions(collectionID)
	if err != nil {
		err = fmt.Errorf("collection %d has not been loaded into QueryNode", collectionID)
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		log.Warn("show partitions failed",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return &querypb.ShowPartitionsResponse{
			Status: status,
		}, nil
	}
	ID2PartitionState := make(map[UniqueID]*querypb.PartitionStates)
	inMemoryPartitionIDs := make([]UniqueID, 0)
	for _, state := range partitionStates {
		ID2PartitionState[state.PartitionID] = state
		inMemoryPartitionIDs = append(inMemoryPartitionIDs, state.PartitionID)
	}
	inMemoryPercentages := make([]int64, 0)
	if len(req.PartitionIDs) == 0 {
		for _, id := range inMemoryPartitionIDs {
			inMemoryPercentages = append(inMemoryPercentages, ID2PartitionState[id].InMemoryPercentage)
		}
		log.Debug("show partitions end",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Int64s("partitionIDs", inMemoryPartitionIDs),
			zap.Int64s("inMemoryPercentage", inMemoryPercentages))
		return &querypb.ShowPartitionsResponse{
			Status:              status,
			PartitionIDs:        inMemoryPartitionIDs,
			InMemoryPercentages: inMemoryPercentages,
		}, nil
	}
	for _, id := range req.PartitionIDs {
		if _, ok := ID2PartitionState[id]; !ok {
			err = fmt.Errorf("partition %d of collection %d has not been loaded into QueryNode", id, collectionID)
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			log.Warn("show partitions failed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", id),
				zap.Int64("msgID", req.Base.MsgID),
				zap.Error(err))
			return &querypb.ShowPartitionsResponse{
				Status: status,
			}, nil
		}
		inMemoryPercentages = append(inMemoryPercentages, ID2PartitionState[id].InMemoryPercentage)
	}

	log.Debug("show partitions end",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", req.PartitionIDs),
		zap.Int64("msgID", req.Base.MsgID),
		zap.Int64s("inMemoryPercentage", inMemoryPercentages))

	return &querypb.ShowPartitionsResponse{
		Status:              status,
		PartitionIDs:        req.PartitionIDs,
		InMemoryPercentages: inMemoryPercentages,
	}, nil
}

// LoadPartitions loads all the sealed segments of this partition to queryNodes, and assigns watchDmChannelRequest to queryNodes
func (qc *QueryCoord) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelTotal).Inc()
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs

	log.Debug("loadPartitionRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("msgID", req.Base.MsgID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Error("load partition failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	// if partitionIDs to load are empty, return error
	if len(partitionIDs) == 0 {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("partitionIDs are empty")
		status.Reason = err.Error()

		log.Warn("loadPartitionRequest failed",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	if collectionInfo, err := qc.meta.getCollectionInfoByID(collectionID); err == nil {
		// if the collection has been loaded into memory by load collection request, return error
		// should release collection first, then load partitions again
		if collectionInfo.LoadType == querypb.LoadType_loadCollection {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			err = fmt.Errorf("collection %d has been loaded into QueryNode, please release collection firstly", collectionID)
			status.Reason = err.Error()
		}

		if collectionInfo.LoadType == querypb.LoadType_LoadPartition {
			for _, toLoadPartitionID := range partitionIDs {
				needLoad := true
				for _, loadedPartitionID := range collectionInfo.PartitionIDs {
					if toLoadPartitionID == loadedPartitionID {
						needLoad = false
						break
					}
				}
				if needLoad {
					// if new partitions need to be loaded, return error
					// should release partitions first, then load partitions again
					status.ErrorCode = commonpb.ErrorCode_UnexpectedError
					err = fmt.Errorf("some partitions %v of collection %d has been loaded into QueryNode, please release partitions firstly",
						collectionInfo.PartitionIDs, collectionID)
					status.Reason = err.Error()
				}
			}
		}

		if status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("loadPartitionRequest failed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("collectionID", collectionID),
				zap.Int64s("partitionIDs", partitionIDs),
				zap.Int64("msgID", req.Base.MsgID),
				zap.Error(err))

			metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
			return status, nil
		}

		log.Debug("loadPartitionRequest completed, all partitions to load have already been loaded into memory",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", req.Base.MsgID))

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelSuccess).Inc()
		return status, nil
	}

	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_GrpcRequest)
	loadPartitionTask := &loadPartitionTask{
		baseTask:              baseTask,
		LoadPartitionsRequest: req,
		broker:                qc.broker,
		cluster:               qc.cluster,
		meta:                  qc.meta,
	}
	err := qc.scheduler.Enqueue(loadPartitionTask)
	if err != nil {
		log.Error("loadPartitionRequest failed to add execute task to scheduler",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	err = loadPartitionTask.waitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		log.Error("loadPartitionRequest failed",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	log.Debug("loadPartitionRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("msgID", req.Base.MsgID))

	return status, nil
}

// ReleasePartitions clears all data related to this partition on the querynode
func (qc *QueryCoord) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelTotal).Inc()

	//dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	log.Debug("releasePartitionRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("msgID", req.Base.MsgID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Error("release partition failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	if len(partitionIDs) == 0 {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("partitionIDs are empty")
		status.Reason = err.Error()
		log.Warn("releasePartitionsRequest failed",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", req.Base.MsgID), zap.Error(err))

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	releaseCollection := true
	var toReleasedPartitions []UniqueID
	if collectionInfo, err := qc.meta.getCollectionInfoByID(collectionID); err == nil {
		// if collection has been loaded into memory by load collection request, return error
		// part of the partitions released after load collection is temporarily not supported, and will be supported soon
		if collectionInfo.LoadType == querypb.LoadType_loadCollection {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			err := errors.New("releasing some partitions after load collection is not supported")
			status.Reason = err.Error()
			log.Warn("releasePartitionsRequest failed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("collectionID", req.CollectionID),
				zap.Int64s("partitionIDs", partitionIDs),
				zap.Int64("msgID", req.Base.MsgID),
				zap.Error(err))

			metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
			return status, nil
		}

		for _, partitionID := range collectionInfo.PartitionIDs {
			toRelease := false
			for _, releasedPartitionID := range partitionIDs {
				if partitionID == releasedPartitionID {
					toRelease = true
					toReleasedPartitions = append(toReleasedPartitions, releasedPartitionID)
				}
			}
			if !toRelease {
				releaseCollection = false
			}
		}
	} else {
		log.Debug("release partitions end, the collection has not been loaded into QueryNode",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64("msgID", req.Base.MsgID))

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelSuccess).Inc()
		return status, nil
	}

	if len(toReleasedPartitions) == 0 {
		log.Debug("release partitions end, the partitions has not been loaded into QueryNode",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", req.Base.MsgID))

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelSuccess).Inc()
		return status, nil
	}

	var releaseTask task
	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_GrpcRequest)
	if releaseCollection {
		// if all loaded partitions will be released from memory, then upgrade release partitions request to release collection request
		log.Debug(fmt.Sprintf("all partitions of collection %d will released from QueryNode, so release the collection directly", collectionID),
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("msgID", req.Base.MsgID))
		msgBase := req.Base
		msgBase.MsgType = commonpb.MsgType_ReleaseCollection
		releaseCollectionRequest := &querypb.ReleaseCollectionRequest{
			Base:         msgBase,
			CollectionID: req.CollectionID,
		}
		releaseTask = &releaseCollectionTask{
			baseTask:                 baseTask,
			ReleaseCollectionRequest: releaseCollectionRequest,
			cluster:                  qc.cluster,
			meta:                     qc.meta,
			broker:                   qc.broker,
		}
	} else {
		req.PartitionIDs = toReleasedPartitions
		releaseTask = &releasePartitionTask{
			baseTask:                 baseTask,
			ReleasePartitionsRequest: req,
			cluster:                  qc.cluster,
			meta:                     qc.meta,
		}
	}
	err := qc.scheduler.Enqueue(releaseTask)
	if err != nil {
		log.Error("releasePartitionRequest failed to add execute task to scheduler",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	err = releaseTask.waitToFinish()
	if err != nil {
		log.Error("releasePartitionRequest failed",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelFail).Inc()
		return status, nil
	}

	log.Debug("releasePartitionRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("msgID", req.Base.MsgID))

	//qc.MetaReplica.printMeta()
	//qc.cluster.printMeta()

	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.QueryCoordMetricLabelSuccess).Inc()
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(releaseTask.elapseSpan().Milliseconds()))
	return status, nil
}

// CreateQueryChannel assigns unique querychannel and resultchannel to the specified collecion
func (qc *QueryCoord) CreateQueryChannel(ctx context.Context, req *querypb.CreateQueryChannelRequest) (*querypb.CreateQueryChannelResponse, error) {
	log.Debug("createQueryChannelRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Error("createQueryChannel failed", zap.String("role", typeutil.QueryCoordRole), zap.Error(err))
		return &querypb.CreateQueryChannelResponse{
			Status: status,
		}, nil
	}

	collectionID := req.CollectionID
	info := qc.meta.getQueryChannelInfoByID(collectionID)
	log.Debug("createQueryChannelRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.String("request channel", info.QueryChannel),
		zap.String("result channel", info.QueryResultChannel))
	return &querypb.CreateQueryChannelResponse{
		Status:             status,
		QueryChannel:       info.QueryChannel,
		QueryResultChannel: info.QueryResultChannel,
	}, nil
}

// GetPartitionStates returns state of the partition, including notExist, notPresent, onDisk, partitionInMemory, inMemory, partitionInGPU, InGPU
func (qc *QueryCoord) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	log.Debug("getPartitionStatesRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64s("partitionIDs", req.PartitionIDs),
		zap.Int64("msgID", req.Base.MsgID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Error("getPartitionStates failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return &querypb.GetPartitionStatesResponse{
			Status: status,
		}, nil
	}

	partitionIDs := req.PartitionIDs
	partitionStates := make([]*querypb.PartitionStates, 0)
	for _, partitionID := range partitionIDs {
		res, err := qc.meta.getPartitionStatesByID(req.CollectionID, partitionID)
		if err != nil {
			err = fmt.Errorf("partition %d of collection %d has not been loaded into QueryNode", partitionID, req.CollectionID)
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			log.Warn("getPartitionStatesRequest failed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("collectionID", req.CollectionID),
				zap.Int64("partitionID", partitionID),
				zap.Int64("msgID", req.Base.MsgID),
				zap.Error(err))
			return &querypb.GetPartitionStatesResponse{
				Status: status,
			}, nil
		}
		partitionState := &querypb.PartitionStates{
			PartitionID: partitionID,
			State:       res.State,
		}
		partitionStates = append(partitionStates, partitionState)
	}
	log.Debug("getPartitionStatesRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64s("partitionIDs", req.PartitionIDs),
		zap.Int64("msgID", req.Base.MsgID))
	return &querypb.GetPartitionStatesResponse{
		Status:                status,
		PartitionDescriptions: partitionStates,
	}, nil
}

// GetSegmentInfo returns information of all the segments on queryNodes, and the information includes memSize, numRow, indexName, indexID ...
func (qc *QueryCoord) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	log.Debug("getSegmentInfoRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64s("segmentIDs", req.SegmentIDs),
		zap.Int64("msgID", req.Base.MsgID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Error("getSegmentInfo failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return &querypb.GetSegmentInfoResponse{
			Status: status,
		}, nil
	}

	totalMemSize := int64(0)
	totalNumRows := int64(0)
	//TODO::get segment infos from MetaReplica
	//segmentIDs := req.SegmentIDs
	//segmentInfos, err := qs.MetaReplica.getSegmentInfos(segmentIDs)
	segmentInfos, err := qc.cluster.getSegmentInfo(ctx, req)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		log.Error("getSegmentInfoRequest failed",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64s("segmentIDs", req.SegmentIDs),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		return &querypb.GetSegmentInfoResponse{
			Status: status,
		}, nil
	}
	for _, info := range segmentInfos {
		totalNumRows += info.NumRows
		totalMemSize += info.MemSize
	}
	log.Debug("getSegmentInfoRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64("msgID", req.Base.MsgID),
		zap.Int64("num rows", totalNumRows),
		zap.Int64("memory size", totalMemSize))
	return &querypb.GetSegmentInfoResponse{
		Status: status,
		Infos:  segmentInfos,
	}, nil
}

// LoadBalance would do a load balancing operation between query nodes
func (qc *QueryCoord) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	log.Debug("loadBalanceRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64s("source nodeIDs", req.SourceNodeIDs),
		zap.Int64s("dst nodeIDs", req.DstNodeIDs),
		zap.Int64s("balanced segments", req.SealedSegmentIDs),
		zap.Int64("msgID", req.Base.MsgID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Error("loadBalance failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return status, nil
	}

	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_LoadBalance)
	req.BalanceReason = querypb.TriggerCondition_LoadBalance
	loadBalanceTask := &loadBalanceTask{
		baseTask:           baseTask,
		LoadBalanceRequest: req,
		broker:             qc.broker,
		cluster:            qc.cluster,
		meta:               qc.meta,
	}
	err := qc.scheduler.Enqueue(loadBalanceTask)
	if err != nil {
		log.Error("loadBalanceRequest failed to add execute task to scheduler",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	err = loadBalanceTask.waitToFinish()
	if err != nil {
		log.Error("loadBalanceRequest failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	log.Debug("loadBalanceRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64s("source nodeIDs", req.SourceNodeIDs),
		zap.Int64s("dst nodeIDs", req.DstNodeIDs),
		zap.Int64s("balanced segments", req.SealedSegmentIDs),
		zap.Int64("msgID", req.Base.MsgID))

	return status, nil
}

// GetMetrics returns all the queryCoord's metrics
func (qc *QueryCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log.Debug("getMetricsRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.String("req", req.Request),
		zap.Int64("msgID", req.Base.MsgID))

	getMetricsResponse := &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, Params.QueryCoordCfg.QueryCoordID),
	}

	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		err := errors.New("QueryCoord is not healthy")
		getMetricsResponse.Status.Reason = err.Error()
		log.Error("getMetrics failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return getMetricsResponse, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		getMetricsResponse.Status.Reason = err.Error()
		log.Error("getMetrics failed to parse metric type",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		return getMetricsResponse, nil
	}

	log.Debug("getMetrics",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("msgID", req.Base.MsgID),
		zap.String("metric_type", metricType))

	if metricType == metricsinfo.SystemInfoMetrics {
		ret, err := qc.metricsCacheManager.GetSystemInfoMetrics()
		if err == nil && ret != nil {
			log.Debug("getMetrics completed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("msgID", req.Base.MsgID))
			return ret, nil
		}

		log.Debug("failed to get system info metrics from cache, recompute instead",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("msgID", req.Base.MsgID))

		metrics, err := getSystemInfoMetrics(ctx, req, qc)
		if err != nil {
			log.Error("getSystemInfoMetrics failed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("msgID", req.Base.MsgID),
				zap.Error(err))
			getMetricsResponse.Status.Reason = err.Error()
			return getMetricsResponse, nil
		}

		// get metric success, the set the status.ErrorCode to success
		getMetricsResponse.Response = metrics
		qc.metricsCacheManager.UpdateSystemInfoMetrics(getMetricsResponse)
		log.Debug("getMetrics completed",
			zap.String("role", typeutil.QueryCoordRole),
			zap.String("req", req.Request),
			zap.Int64("msgID", req.Base.MsgID))
		getMetricsResponse.Status.ErrorCode = commonpb.ErrorCode_Success
		return getMetricsResponse, nil
	}
	err = errors.New(metricsinfo.MsgUnimplementedMetric)
	getMetricsResponse.Status.Reason = err.Error()

	log.Error("getMetrics failed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.String("req", req.Request),
		zap.Int64("msgID", req.Base.MsgID),
		zap.Error(err))

	return getMetricsResponse, nil
}
