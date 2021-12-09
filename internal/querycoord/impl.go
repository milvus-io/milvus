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

	"github.com/milvus-io/milvus/internal/common"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
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
		Value: Params.TimeTickChannelName,
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
		Value: Params.StatsChannelName,
	}, nil
}

// ShowCollections return all the collections that have been loaded
func (qc *QueryCoord) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	dbID := req.DbID
	log.Debug("show collection start", zap.Int64("dbID", dbID))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("query coordinator is not healthy")
		status.Reason = err.Error()
		log.Debug("show collection end with query coordinator not healthy")
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
		log.Debug("show collection end", zap.Int64s("collections", inMemoryCollectionIDs), zap.Int64s("inMemoryPercentage", inMemoryPercentages))
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
			return &querypb.ShowCollectionsResponse{
				Status: status,
			}, nil
		}
		inMemoryPercentages = append(inMemoryPercentages, ID2collectionInfo[id].InMemoryPercentage)
	}
	log.Debug("show collection end", zap.Int64s("collections", req.CollectionIDs), zap.Int64s("inMemoryPercentage", inMemoryPercentages))
	return &querypb.ShowCollectionsResponse{
		Status:              status,
		CollectionIDs:       req.CollectionIDs,
		InMemoryPercentages: inMemoryPercentages,
	}, nil
}

// LoadCollection loads all the sealed segments of this collection to queryNodes, and assigns watchDmChannelRequest to queryNodes
func (qc *QueryCoord) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	collectionID := req.CollectionID
	//schema := req.Schema
	log.Debug("loadCollectionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID),
		zap.Stringer("schema", req.Schema))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("query coordinator is not healthy")
		status.Reason = err.Error()
		log.Debug("load collection end with query coordinator not healthy")
		return status, nil
	}

	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_grpcRequest)
	loadCollectionTask := &loadCollectionTask{
		baseTask:              baseTask,
		LoadCollectionRequest: req,
		rootCoord:             qc.rootCoordClient,
		dataCoord:             qc.dataCoordClient,
		indexCoord:            qc.indexCoordClient,
		cluster:               qc.cluster,
		meta:                  qc.meta,
	}
	err := qc.scheduler.Enqueue(loadCollectionTask)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	err = loadCollectionTask.waitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	log.Debug("loadCollectionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID), zap.Any("status", status))
	return status, nil
}

// ReleaseCollection clears all data related to this collecion on the querynode
func (qc *QueryCoord) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	//dbID := req.DbID
	collectionID := req.CollectionID
	log.Debug("releaseCollectionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("query coordinator is not healthy")
		status.Reason = err.Error()
		log.Debug("release collection end with query coordinator not healthy")
		return status, nil
	}

	hasCollection := qc.meta.hasCollection(collectionID)
	if !hasCollection {
		log.Warn("release collection end, query coordinator don't have the log of", zap.Int64("collectionID", collectionID))
		return status, nil
	}

	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_grpcRequest)
	releaseCollectionTask := &releaseCollectionTask{
		baseTask:                 baseTask,
		ReleaseCollectionRequest: req,
		cluster:                  qc.cluster,
		meta:                     qc.meta,
		rootCoord:                qc.rootCoordClient,
	}
	err := qc.scheduler.Enqueue(releaseCollectionTask)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	err = releaseCollectionTask.waitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	log.Debug("releaseCollectionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID))
	//qc.MetaReplica.printMeta()
	//qc.cluster.printMeta()
	return status, nil
}

// ShowPartitions return all the partitions that have been loaded
func (qc *QueryCoord) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	collectionID := req.CollectionID
	log.Debug("show partitions start, ", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", req.PartitionIDs))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("query coordinator is not healthy")
		status.Reason = err.Error()
		log.Debug("show partition end with query coordinator not healthy")
		return &querypb.ShowPartitionsResponse{
			Status: status,
		}, nil
	}

	partitionStates, err := qc.meta.showPartitions(collectionID)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
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
		log.Debug("show partitions end", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", inMemoryPartitionIDs), zap.Int64s("inMemoryPercentage", inMemoryPercentages))
		return &querypb.ShowPartitionsResponse{
			Status:              status,
			PartitionIDs:        inMemoryPartitionIDs,
			InMemoryPercentages: inMemoryPercentages,
		}, nil
	}
	for _, id := range req.PartitionIDs {
		if _, ok := ID2PartitionState[id]; !ok {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			err := errors.New("partition has not been loaded to memory or load failed")
			status.Reason = err.Error()
			return &querypb.ShowPartitionsResponse{
				Status: status,
			}, nil
		}
		inMemoryPercentages = append(inMemoryPercentages, ID2PartitionState[id].InMemoryPercentage)
	}

	log.Debug("show partitions end", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", req.PartitionIDs), zap.Int64s("inMemoryPercentage", inMemoryPercentages))

	return &querypb.ShowPartitionsResponse{
		Status:              status,
		PartitionIDs:        req.PartitionIDs,
		InMemoryPercentages: inMemoryPercentages,
	}, nil
}

// LoadPartitions loads all the sealed segments of this partition to queryNodes, and assigns watchDmChannelRequest to queryNodes
func (qc *QueryCoord) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	log.Debug("loadPartitionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("query coordinator is not healthy")
		status.Reason = err.Error()
		log.Debug("load partition end with query coordinator not healthy")
		return status, nil
	}

	if len(partitionIDs) == 0 {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("partitionIDs are empty")
		status.Reason = err.Error()
		log.Debug("loadPartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID), zap.Int64s("partitionIDs", partitionIDs))
		return status, nil
	}

	hasCollection := qc.meta.hasCollection(collectionID)
	if hasCollection {
		partitionIDsToLoad := make([]UniqueID, 0)
		loadType, _ := qc.meta.getLoadType(collectionID)
		if loadType == querypb.LoadType_loadCollection {
			for _, partitionID := range partitionIDs {
				hasReleasePartition := qc.meta.hasReleasePartition(collectionID, partitionID)
				if hasReleasePartition {
					partitionIDsToLoad = append(partitionIDsToLoad, partitionID)
				}
			}
		} else {
			for _, partitionID := range partitionIDs {
				hasPartition := qc.meta.hasPartition(collectionID, partitionID)
				if !hasPartition {
					partitionIDsToLoad = append(partitionIDsToLoad, partitionID)
				}
			}
		}

		if len(partitionIDsToLoad) == 0 {
			log.Debug("loadPartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID), zap.Int64s("partitionIDs", partitionIDs))
			return status, nil
		}
		req.PartitionIDs = partitionIDsToLoad
	}

	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_grpcRequest)
	loadPartitionTask := &loadPartitionTask{
		baseTask:              baseTask,
		LoadPartitionsRequest: req,
		rootCoord:             qc.rootCoordClient,
		dataCoord:             qc.dataCoordClient,
		indexCoord:            qc.indexCoordClient,
		cluster:               qc.cluster,
		meta:                  qc.meta,
	}
	err := qc.scheduler.Enqueue(loadPartitionTask)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	err = loadPartitionTask.waitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		log.Debug("loadPartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID), zap.Int64s("partitionIDs", partitionIDs))
		return status, nil
	}

	log.Debug("loadPartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID), zap.Int64s("partitionIDs", partitionIDs))
	return status, nil
}

// ReleasePartitions clears all data related to this partition on the querynode
func (qc *QueryCoord) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	//dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	log.Debug("releasePartitionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID), zap.Int64s("partitionIDs", partitionIDs))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("query coordinator is not healthy")
		status.Reason = err.Error()
		log.Debug("release partition end with query coordinator not healthy")
		return status, nil
	}

	hasCollection := qc.meta.hasCollection(collectionID)
	if !hasCollection {
		log.Warn("release partitions end, query coordinator don't have the log of", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
		return status, nil
	}

	if len(partitionIDs) == 0 {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("partitionIDs are empty")
		status.Reason = err.Error()
		log.Debug("releasePartitionsRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID), zap.Int64s("partitionIDs", partitionIDs))
		return status, nil
	}

	toReleasedPartitions := make([]UniqueID, 0)
	for _, id := range partitionIDs {
		hasPartition := qc.meta.hasPartition(collectionID, id)
		if hasPartition {
			toReleasedPartitions = append(toReleasedPartitions, id)
		}
	}
	if len(toReleasedPartitions) == 0 {
		log.Warn("release partitions end, query coordinator don't have the log of", zap.Int64("collectionID", req.CollectionID), zap.Int64s("partitionIDs", partitionIDs))
		return status, nil
	}

	req.PartitionIDs = toReleasedPartitions
	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_grpcRequest)
	releasePartitionTask := &releasePartitionTask{
		baseTask:                 baseTask,
		ReleasePartitionsRequest: req,
		cluster:                  qc.cluster,
	}
	err := qc.scheduler.Enqueue(releasePartitionTask)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	err = releasePartitionTask.waitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}
	log.Debug("releasePartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
	//qc.MetaReplica.printMeta()
	//qc.cluster.printMeta()
	return status, nil
}

// CreateQueryChannel assigns unique querychannel and resultchannel to the specified collecion
func (qc *QueryCoord) CreateQueryChannel(ctx context.Context, req *querypb.CreateQueryChannelRequest) (*querypb.CreateQueryChannelResponse, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("query coordinator is not healthy")
		status.Reason = err.Error()
		log.Debug("createQueryChannel end with query coordinator not healthy")
		return &querypb.CreateQueryChannelResponse{
			Status: status,
		}, nil
	}

	collectionID := req.CollectionID
	info, err := qc.meta.getQueryChannelInfoByID(collectionID)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		log.Debug("createQueryChannel end with error")
		return &querypb.CreateQueryChannelResponse{
			Status: status,
		}, nil
	}

	return &querypb.CreateQueryChannelResponse{
		Status:         status,
		RequestChannel: info.QueryChannelID,
		ResultChannel:  info.QueryResultChannelID,
	}, nil
}

// GetPartitionStates returns state of the partition, including notExist, notPresent, onDisk, partitionInMemory, inMemory, partitionInGPU, InGPU
func (qc *QueryCoord) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("query coordinator is not healthy")
		status.Reason = err.Error()
		log.Debug("getPartitionStates end with query coordinator not healthy")
		return &querypb.GetPartitionStatesResponse{
			Status: status,
		}, nil
	}

	partitionIDs := req.PartitionIDs
	partitionStates := make([]*querypb.PartitionStates, 0)
	for _, partitionID := range partitionIDs {
		res, err := qc.meta.getPartitionStatesByID(req.CollectionID, partitionID)
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
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

	return &querypb.GetPartitionStatesResponse{
		Status:                status,
		PartitionDescriptions: partitionStates,
	}, nil
}

// GetSegmentInfo returns information of all the segments on queryNodes, and the information includes memSize, numRow, indexName, indexID ...
func (qc *QueryCoord) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("query coordinator is not healthy")
		status.Reason = err.Error()
		log.Debug("getSegmentInfo end with query coordinator not healthy")
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
		return &querypb.GetSegmentInfoResponse{
			Status: status,
		}, nil
	}
	for _, info := range segmentInfos {
		totalNumRows += info.NumRows
		totalMemSize += info.MemSize
	}
	log.Debug("getSegmentInfo", zap.Int64("num rows", totalNumRows), zap.Int64("memory size", totalMemSize))
	return &querypb.GetSegmentInfoResponse{
		Status: status,
		Infos:  segmentInfos,
	}, nil
}

// LoadBalance would do a load balancing operation between query nodes
func (qc *QueryCoord) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	log.Debug("LoadBalanceRequest received",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", req.Base.MsgID),
		zap.Any("req", req),
	)
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("query coordinator is not healthy")
		status.Reason = err.Error()
		log.Debug("LoadBalance failed", zap.Error(err))
		return status, nil
	}

	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_loadBalance)
	loadBalanceTask := &loadBalanceTask{
		baseTask:           baseTask,
		LoadBalanceRequest: req,
		rootCoord:          qc.rootCoordClient,
		dataCoord:          qc.dataCoordClient,
		indexCoord:         qc.indexCoordClient,
		cluster:            qc.cluster,
		meta:               qc.meta,
	}
	err := qc.scheduler.Enqueue(loadBalanceTask)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	err = loadBalanceTask.waitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}
	log.Debug("LoadBalanceRequest completed",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", req.Base.MsgID),
		zap.Any("req", req),
	)
	return status, nil
}

func (qc *QueryCoord) isHealthy() bool {
	code := qc.stateCode.Load().(internalpb.StateCode)
	return code == internalpb.StateCode_Healthy
}

// GetMetrics returns all the queryCoord's metrics
func (qc *QueryCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log.Debug("QueryCoord.GetMetrics",
		zap.Int64("node_id", Params.QueryCoordID),
		zap.String("req", req.Request))

	if !qc.isHealthy() {
		log.Warn("QueryCoord.GetMetrics failed",
			zap.Int64("node_id", Params.QueryCoordID),
			zap.String("req", req.Request),
			zap.Error(errQueryCoordIsUnhealthy(Params.QueryCoordID)))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgQueryCoordIsUnhealthy(Params.QueryCoordID),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("QueryCoord.GetMetrics failed to parse metric type",
			zap.Int64("node_id", Params.QueryCoordID),
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

	log.Debug("QueryCoord.GetMetrics",
		zap.String("metric_type", metricType))

	if metricType == metricsinfo.SystemInfoMetrics {
		ret, err := qc.metricsCacheManager.GetSystemInfoMetrics()
		if err == nil && ret != nil {
			return ret, nil
		}
		log.Debug("failed to get system info metrics from cache, recompute instead",
			zap.Error(err))

		metrics, err := getSystemInfoMetrics(ctx, req, qc)

		log.Debug("QueryCoord.GetMetrics",
			zap.Int64("node_id", Params.QueryCoordID),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Any("metrics", metrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		qc.metricsCacheManager.UpdateSystemInfoMetrics(metrics)

		return metrics, nil
	}
	err = errors.New(metricsinfo.MsgUnimplementedMetric)
	log.Debug("QueryCoord.GetMetrics failed",
		zap.Int64("node_id", Params.QueryCoordID),
		zap.String("req", req.Request),
		zap.String("metric_type", metricType),
		zap.Error(err))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		},
		Response: "",
	}, nil
}
