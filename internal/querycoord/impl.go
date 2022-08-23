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
		Value: Params.CommonCfg.QueryCoordTimeTick,
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
	}, nil
}

// checkAnyReplicaAvailable checks if the collection has enough distinct available shards. These shards
// may come from different replica group. We only need these shards to form a replica that serves query
// requests.
func (qc *QueryCoord) checkAnyReplicaAvailable(collectionID UniqueID) bool {
	replicas, _ := qc.meta.getReplicasByCollectionID(collectionID)
	shardNames := qc.meta.getDmChannelNamesByCollectionID(collectionID)
	shardNodes := getShardNodes(collectionID, qc.meta)
	availableShards := make(map[string]struct{}, len(shardNames))
	for _, replica := range replicas {
		for _, shard := range replica.ShardReplicas {
			if _, ok := availableShards[shard.DmChannelName]; ok {
				continue
			}
			// check leader
			isShardAvailable, err := qc.cluster.IsOnline(shard.LeaderID)
			if err != nil || !isShardAvailable {
				log.Warn("shard leader is unavailable",
					zap.Int64("collectionID", replica.CollectionID),
					zap.Int64("replicaID", replica.ReplicaID),
					zap.String("DmChannel", shard.DmChannelName),
					zap.Int64("shardLeaderID", shard.LeaderID),
					zap.Error(err))
				continue
			}
			// check other nodes
			nodes := shardNodes[shard.DmChannelName]
			for _, nodeID := range replica.NodeIds {
				if _, ok := nodes[nodeID]; ok {
					if ok, err := qc.cluster.IsOnline(nodeID); err != nil || !ok {
						isShardAvailable = false
						break
					}
				}
			}
			// set if available
			if isShardAvailable {
				availableShards[shard.DmChannelName] = struct{}{}
			}
		}
	}

	// check if there are enough available distinct shards
	return len(availableShards) == len(shardNames)
}

// ShowCollections return all the collections that have been loaded
func (qc *QueryCoord) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	log.Info("show collection start",
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
	// parse meta
	collectionInfos := qc.meta.showCollections()
	collectionID2Info := make(map[UniqueID]*querypb.CollectionInfo)
	for _, info := range collectionInfos {
		collectionID2Info[info.CollectionID] = info
	}

	// validate collectionIDs in request
	collectionIDs := req.CollectionIDs
	for _, cid := range collectionIDs {
		if _, ok := collectionID2Info[cid]; !ok {
			err := fmt.Errorf("collection %d has not been loaded to memory or load failed", cid)
			log.Warn("show collection failed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("collectionID", cid),
				zap.Int64("msgID", req.Base.MsgID),
				zap.Error(err))
			return &querypb.ShowCollectionsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}
	}

	if len(collectionIDs) == 0 {
		for _, info := range collectionInfos {
			collectionIDs = append(collectionIDs, info.CollectionID)
		}
	}

	inMemoryPercentages := make([]int64, 0, len(collectionIDs))
	queryServiceAvailable := make([]bool, 0, len(collectionIDs))
	for _, cid := range collectionIDs {
		inMemoryPercentages = append(inMemoryPercentages, collectionID2Info[cid].InMemoryPercentage)
		queryServiceAvailable = append(queryServiceAvailable, qc.checkAnyReplicaAvailable(cid))
	}

	log.Info("show collection end",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64s("collectionIDs", req.CollectionIDs),
		zap.Int64("msgID", req.Base.MsgID),
		zap.Int64s("inMemoryPercentage", inMemoryPercentages),
		zap.Bools("queryServiceAvailable", queryServiceAvailable))
	return &querypb.ShowCollectionsResponse{
		Status:                status,
		CollectionIDs:         collectionIDs,
		InMemoryPercentages:   inMemoryPercentages,
		QueryServiceAvailable: queryServiceAvailable,
	}, nil
}

func handleLoadError(err error, loadType querypb.LoadType, msgID, collectionID UniqueID, partitionIDs []UniqueID) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if errors.Is(err, ErrCollectionLoaded) {
		log.Info("collection or partitions has already been loaded, return load success directly",
			zap.String("loadType", loadType.String()),
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", msgID))

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
		return status, nil
	} else if errors.Is(err, ErrLoadParametersMismatch) {
		status.ErrorCode = commonpb.ErrorCode_IllegalArgument
		status.Reason = err.Error()

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return status, nil
	} else {
		log.Error("load collection or partitions to query nodes failed",
			zap.String("loadType", loadType.String()),
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", msgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return status, nil
	}
}

// LoadCollection loads all the sealed segments of this collection to queryNodes, and assigns watchDmChannelRequest to queryNodes
func (qc *QueryCoord) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel).Inc()

	collectionID := req.CollectionID
	//schema := req.Schema
	log.Info("loadCollectionRequest received",
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

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return status, nil
	}

	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_GrpcRequest)
	loadCollectionTask := &loadCollectionTask{
		baseTask:              baseTask,
		LoadCollectionRequest: req,
		broker:                qc.broker,
		cluster:               qc.cluster,
		meta:                  qc.meta,
	}

	LastTaskType := qc.scheduler.triggerTaskQueue.willLoadOrRelease(req.GetCollectionID())
	if LastTaskType == commonpb.MsgType_LoadCollection {
		// collection will be loaded, remove idempotent loadCollection task, return success directly
		return status, nil
	}
	if LastTaskType != commonpb.MsgType_ReleaseCollection {
		err := checkLoadCollection(req, qc.meta)
		if err != nil {
			return handleLoadError(err, querypb.LoadType_LoadCollection, req.GetBase().GetMsgID(), req.GetCollectionID(), nil)
		}
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

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return status, nil
	}

	err = loadCollectionTask.waitToFinish()
	if err != nil {
		return handleLoadError(err, querypb.LoadType_LoadCollection, req.GetBase().GetMsgID(), req.GetCollectionID(), nil)
	}

	log.Info("loadCollectionRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", req.Base.MsgID))

	return status, nil
}

// ReleaseCollection clears all data related to this collecion on the querynode
func (qc *QueryCoord) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.TotalLabel).Inc()
	//dbID := req.DbID
	collectionID := req.CollectionID
	log.Info("releaseCollectionRequest received",
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

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return status, nil
	}

	// if collection has not been loaded into memory, return release collection successfully
	hasCollection := qc.meta.hasCollection(collectionID)
	if !hasCollection {
		log.Info("release collection end, the collection has not been loaded into QueryNode",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64("msgID", req.Base.MsgID))

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
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

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
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

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return status, nil
	}

	log.Info("releaseCollectionRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64("msgID", req.Base.MsgID))
	//qc.MetaReplica.printMeta()
	//qc.cluster.printMeta()

	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(releaseCollectionTask.elapseSpan().Milliseconds()))
	return status, nil
}

// ShowPartitions return all the partitions that have been loaded
func (qc *QueryCoord) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	collectionID := req.CollectionID
	log.Ctx(ctx).Debug("show partitions start",
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
		log.Ctx(ctx).Warn("show partition failed", zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return &querypb.ShowPartitionsResponse{
			Status: status,
		}, nil
	}

	partitionStates, err := qc.meta.showPartitions(collectionID)
	if err != nil {
		err = fmt.Errorf("collection %d has not been loaded into QueryNode", collectionID)
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		log.Ctx(ctx).Warn("show partitions failed",
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
		log.Ctx(ctx).Debug("show partitions end",
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
			log.Ctx(ctx).Warn("show partitions failed",
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

	log.Ctx(ctx).Debug("show partitions end",
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
	metrics.QueryCoordLoadCount.WithLabelValues(metrics.TotalLabel).Inc()
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs

	log.Info("loadPartitionRequest received",
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

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
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

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
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

	LastTaskType := qc.scheduler.triggerTaskQueue.willLoadOrRelease(req.GetCollectionID())
	if LastTaskType == commonpb.MsgType_LoadPartitions {
		// partitions will be loaded, remove idempotent loadPartition task, return success directly
		return status, nil
	}
	if LastTaskType != commonpb.MsgType_ReleasePartitions {
		err := checkLoadPartition(req, qc.meta)
		if err != nil {
			return handleLoadError(err, querypb.LoadType_LoadPartition, req.GetBase().GetMsgID(), req.GetCollectionID(), req.GetPartitionIDs())
		}
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

		metrics.QueryCoordLoadCount.WithLabelValues(metrics.FailLabel).Inc()
		return status, nil
	}

	err = loadPartitionTask.waitToFinish()
	if err != nil {
		return handleLoadError(err, querypb.LoadType_LoadPartition, req.GetBase().GetMsgID(), req.GetCollectionID(), req.GetPartitionIDs())
	}

	log.Info("loadPartitionRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("msgID", req.Base.MsgID))

	return status, nil
}

// ReleasePartitions clears all data related to this partition on the querynode
func (qc *QueryCoord) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.TotalLabel).Inc()

	//dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	log.Info("releasePartitionRequest received",
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
		log.Warn("release partition failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
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

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return status, nil
	}

	releaseCollection := true
	var toReleasedPartitions []UniqueID
	if collectionInfo, err := qc.meta.getCollectionInfoByID(collectionID); err == nil {
		// if collection has been loaded into memory by load collection request, return error
		// part of the partitions released after load collection is temporarily not supported, and will be supported soon
		if collectionInfo.LoadType == querypb.LoadType_LoadCollection {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			err := errors.New("releasing some partitions after load collection is not supported")
			status.Reason = err.Error()
			log.Warn("releasePartitionsRequest failed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("collectionID", req.CollectionID),
				zap.Int64s("partitionIDs", partitionIDs),
				zap.Int64("msgID", req.Base.MsgID),
				zap.Error(err))

			metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
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
		log.Info("release partitions end, the collection has not been loaded into QueryNode",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64("msgID", req.Base.MsgID))

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
		return status, nil
	}

	if len(toReleasedPartitions) == 0 {
		log.Info("release partitions end, the partitions has not been loaded into QueryNode",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", req.Base.MsgID))

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
		return status, nil
	}

	var releaseTask task
	baseTask := newBaseTask(qc.loopCtx, querypb.TriggerCondition_GrpcRequest)
	if releaseCollection {
		// if all loaded partitions will be released from memory, then upgrade release partitions request to release collection request
		log.Info(fmt.Sprintf("all partitions of collection %d will released from QueryNode, so release the collection directly", collectionID),
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
		log.Warn("releasePartitionRequest failed to add execute task to scheduler",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return status, nil
	}

	err = releaseTask.waitToFinish()
	if err != nil {
		log.Warn("releasePartitionRequest failed",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()

		metrics.QueryCoordReleaseCount.WithLabelValues(metrics.FailLabel).Inc()
		return status, nil
	}

	log.Info("releasePartitionRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("msgID", req.Base.MsgID))

	//qc.MetaReplica.printMeta()
	//qc.cluster.printMeta()

	metrics.QueryCoordReleaseCount.WithLabelValues(metrics.SuccessLabel).Inc()
	metrics.QueryCoordReleaseLatency.WithLabelValues().Observe(float64(releaseTask.elapseSpan().Milliseconds()))
	return status, nil
}

// GetPartitionStates returns state of the partition, including notExist, notPresent, onDisk, partitionInMemory, inMemory, partitionInGPU, InGPU
func (qc *QueryCoord) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	log.Info("getPartitionStatesRequest received",
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
		log.Warn("getPartitionStates failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
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
	log.Info("getPartitionStatesRequest completed",
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
	log.Info("getSegmentInfoRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collection ID", req.GetCollectionID()),
		zap.Int64s("segment IDs", req.GetSegmentIDs()),
		zap.Int64("msg ID", req.GetBase().GetMsgID()))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Warn("getSegmentInfo failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return &querypb.GetSegmentInfoResponse{
			Status: status,
		}, nil
	}

	totalMemSize := int64(0)
	totalNumRows := int64(0)

	segmentInfos, err := qc.cluster.GetSegmentInfo(ctx, req)
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
	log.Info("getSegmentInfoRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64("msgID", req.Base.MsgID),
		zap.Int64("num rows", totalNumRows),
		zap.Int64("memory size", totalMemSize),
		zap.Int("segmentNum", len(segmentInfos)))

	return &querypb.GetSegmentInfoResponse{
		Status: status,
		Infos:  segmentInfos,
	}, nil
}

// LoadBalance would do a load balancing operation between query nodes
func (qc *QueryCoord) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	log.Info("loadBalanceRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64s("source nodeIDs", req.SourceNodeIDs),
		zap.Int64s("dst nodeIDs", req.DstNodeIDs),
		zap.Int64s("balanced segments", req.SealedSegmentIDs),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64("msgID", req.Base.MsgID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Warn("loadBalance failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
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
		log.Warn("loadBalanceRequest failed to add execute task to scheduler",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	err = loadBalanceTask.waitToFinish()
	if err != nil {
		log.Warn("loadBalanceRequest failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, nil
	}

	log.Info("loadBalanceRequest completed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64s("source nodeIDs", req.SourceNodeIDs),
		zap.Int64s("dst nodeIDs", req.DstNodeIDs),
		zap.Int64s("balanced segments", req.SealedSegmentIDs),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64("msgID", req.Base.MsgID))

	return status, nil
}

//ShowConfigurations returns the configurations of queryCoord matching req.Pattern
func (qc *QueryCoord) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	log.Debug("ShowConfigurations received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.String("pattern", req.Pattern),
		zap.Int64("msgID", req.GetBase().GetMsgID()))

	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		err := errors.New("QueryCoord is not healthy")
		log.Warn("ShowConfigurations failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.GetBase().GetMsgID()), zap.Error(err))
		return &internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Configuations: nil,
		}, nil
	}

	return getComponentConfigurations(ctx, req), nil
}

// GetMetrics returns all the queryCoord's metrics
func (qc *QueryCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log.Debug("getMetricsRequest received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.String("req", req.Request),
		zap.Int64("msgID", req.GetBase().GetMsgID()))

	getMetricsResponse := &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryCoordRole, Params.QueryCoordCfg.GetNodeID()),
	}

	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		err := errors.New("QueryCoord is not healthy")
		getMetricsResponse.Status.Reason = err.Error()
		log.Warn("getMetrics failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.GetBase().GetMsgID()), zap.Error(err))
		return getMetricsResponse, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		getMetricsResponse.Status.Reason = err.Error()
		log.Warn("getMetrics failed to parse metric type",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))
		return getMetricsResponse, nil
	}

	log.Debug("getMetrics",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("msgID", req.GetBase().GetMsgID()),
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
			zap.Int64("msgID", req.GetBase().GetMsgID()))

		metrics, err := getSystemInfoMetrics(ctx, req, qc)
		if err != nil {
			log.Error("getSystemInfoMetrics failed",
				zap.String("role", typeutil.QueryCoordRole),
				zap.Int64("msgID", req.GetBase().GetMsgID()),
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

	log.Warn("getMetrics failed",
		zap.String("role", typeutil.QueryCoordRole),
		zap.String("req", req.Request),
		zap.Int64("msgID", req.GetBase().GetMsgID()),
		zap.Error(err))

	return getMetricsResponse, nil
}

// GetReplicas gets replicas of a certain collection
func (qc *QueryCoord) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	log.Info("GetReplicas received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64("msgID", req.Base.MsgID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}

	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Warn("GetReplicasResponse failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return &milvuspb.GetReplicasResponse{
			Status: status,
		}, nil
	}

	replicas, err := qc.meta.getReplicasByCollectionID(req.CollectionID)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_MetaFailed
		status.Reason = err.Error()
		log.Warn("GetReplicasResponse failed to get replicas",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))

		return &milvuspb.GetReplicasResponse{
			Status: status,
		}, nil
	}

	if req.WithShardNodes {
		shardNodes := getShardNodes(req.CollectionID, qc.meta)

		for _, replica := range replicas {
			for _, shard := range replica.ShardReplicas {
				shard.NodeIds = append(shard.NodeIds, shard.LeaderID)
				nodes := shardNodes[shard.DmChannelName]
				for _, nodeID := range replica.NodeIds {
					if _, ok := nodes[nodeID]; ok && nodeID != shard.LeaderID {
						shard.NodeIds = append(shard.NodeIds, nodeID)
					}
				}
			}
		}
	}

	log.Info("GetReplicas finished",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Any("replicas", replicas))

	return &milvuspb.GetReplicasResponse{
		Status:   status,
		Replicas: replicas,
	}, nil
}

// GetShardLeaders gets shard leaders of a certain collection
func (qc *QueryCoord) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	log.Info("GetShardLeaders received",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Int64("msgID", req.Base.MsgID))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}

	if qc.stateCode.Load() != internalpb.StateCode_Healthy {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		err := errors.New("QueryCoord is not healthy")
		status.Reason = err.Error()
		log.Warn("GetShardLeadersResponse failed", zap.String("role", typeutil.QueryCoordRole), zap.Int64("msgID", req.Base.MsgID), zap.Error(err))
		return &querypb.GetShardLeadersResponse{
			Status: status,
		}, nil
	}

	replicas, err := qc.meta.getReplicasByCollectionID(req.CollectionID)
	shardNames := qc.meta.getDmChannelNamesByCollectionID(req.CollectionID)

	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_MetaFailed
		status.Reason = err.Error()
		log.Warn("GetShardLeadersResponse failed to get replicas",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Int64("msgID", req.Base.MsgID),
			zap.Error(err))

		return &querypb.GetShardLeadersResponse{
			Status: status,
		}, nil
	}

	shards := make(map[string]*querypb.ShardLeadersList)
	shardNodes := getShardNodes(req.CollectionID, qc.meta)
	for _, replica := range replicas {
		for _, shard := range replica.ShardReplicas {
			list, ok := shards[shard.DmChannelName]
			if !ok {
				list = &querypb.ShardLeadersList{
					ChannelName: shard.DmChannelName,
					NodeIds:     make([]int64, 0),
					NodeAddrs:   make([]string, 0),
				}
			}

			isShardAvailable, err := qc.cluster.IsOnline(shard.LeaderID)
			if err != nil || !isShardAvailable {
				log.Warn("shard leader is unavailable",
					zap.Int64("collectionID", replica.CollectionID),
					zap.Int64("replicaID", replica.ReplicaID),
					zap.String("DmChannel", shard.DmChannelName),
					zap.Int64("shardLeaderID", shard.LeaderID),
					zap.Error(err))
				continue
			}

			nodes := shardNodes[shard.DmChannelName]
			for _, nodeID := range replica.NodeIds {
				if _, ok := nodes[nodeID]; ok {
					if ok, err := qc.cluster.IsOnline(nodeID); err != nil || !ok {
						isShardAvailable = false
						break
					}
				}
			}

			if isShardAvailable {
				list.NodeIds = append(list.NodeIds, shard.LeaderID)
				list.NodeAddrs = append(list.NodeAddrs, shard.LeaderAddr)
				shards[shard.DmChannelName] = list
			}
		}
	}

	shardLeaderLists := make([]*querypb.ShardLeadersList, 0, len(shards))
	for _, shard := range shards {
		shardLeaderLists = append(shardLeaderLists, shard)
	}

	// check if there are enough available distinct shards
	if len(shardLeaderLists) != len(shardNames) {
		log.Warn("no replica available",
			zap.String("role", typeutil.QueryCoordRole),
			zap.Int64("collectionID", req.CollectionID),
			zap.Any("replicasLists", shardLeaderLists),
			zap.Any("replicaNames", shardNames))

		return &querypb.GetShardLeadersResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_NoReplicaAvailable,
				Reason:    "no replica available",
			},
		}, nil
	}

	log.Info("GetShardLeaders finished",
		zap.String("role", typeutil.QueryCoordRole),
		zap.Int64("collectionID", req.CollectionID),
		zap.Any("replicas", shardLeaderLists))

	return &querypb.GetShardLeadersResponse{
		Status: status,
		Shards: shardLeaderLists,
	}, nil
}
