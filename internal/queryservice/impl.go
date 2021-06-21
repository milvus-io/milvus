// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package queryservice

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

func (qs *QueryService) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	serviceComponentInfo := &internalpb.ComponentInfo{
		NodeID:    Params.QueryServiceID,
		StateCode: qs.stateCode.Load().(internalpb.StateCode),
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

func (qs *QueryService) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.TimeTickChannelName,
	}, nil
}

func (qs *QueryService) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.StatsChannelName,
	}, nil
}

func (qs *QueryService) RegisterNode(ctx context.Context, req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	nodeID := req.Base.SourceID
	log.Debug("register query node", zap.Any("QueryNodeID", nodeID), zap.String("address", req.Address.String()))

	if _, ok := qs.cluster.nodes[nodeID]; ok {
		err := errors.New("nodeID already exists")
		log.Debug("register query node Failed nodeID already exist", zap.Any("QueryNodeID", nodeID), zap.String("address", req.Address.String()))

		return &querypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    err.Error(),
			},
		}, err
	}

	session := &sessionutil.Session{
		ServerID: nodeID,
		Address:  fmt.Sprintf("%s:%d", req.Address.Ip, req.Address.Port),
	}
	err := qs.cluster.RegisterNode(session, req.Base.SourceID)
	if err != nil {
		log.Debug("register query node new NodeClient failed", zap.Any("QueryNodeID", nodeID), zap.String("address", req.Address.String()))
		return &querypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, err
	}

	log.Debug("register query node success", zap.Any("QueryNodeID", nodeID), zap.String("address", req.Address.String()))
	return &querypb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		InitParams: &internalpb.InitParams{
			NodeID: nodeID,
			//StartParams: startParams,
		},
	}, nil
}

func (qs *QueryService) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	dbID := req.DbID
	log.Debug("show collection start", zap.Int64("dbID", dbID))
	collectionIDs := qs.meta.showCollections()
	log.Debug("show collection end", zap.Int64s("collections", collectionIDs))
	return &querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		CollectionIDs: collectionIDs,
	}, nil
}

func (qs *QueryService) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	collectionID := req.CollectionID
	//schema := req.Schema
	log.Debug("LoadCollectionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID),
		zap.Stringer("schema", req.Schema))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	hasCollection := qs.meta.hasCollection(collectionID)
	if hasCollection {
		status.ErrorCode = commonpb.ErrorCode_Success
		status.Reason = "collection has been loaded"
		return status, nil
	}
	//err := qs.meta.addCollection(collectionID, schema)
	//if err != nil {
	//	log.Error(err.Error())
	//	return status, err
	//}

	loadCollectionTask := &LoadCollectionTask{
		BaseTask: BaseTask{
			ctx:              qs.loopCtx,
			Condition:        NewTaskCondition(qs.loopCtx),
			triggerCondition: querypb.TriggerCondition_grpcRequest,
		},
		LoadCollectionRequest: req,
		rootCoord:             qs.rootCoordClient,
		dataService:           qs.dataServiceClient,
		cluster:               qs.cluster,
		meta:                  qs.meta,
	}
	qs.scheduler.Enqueue([]task{loadCollectionTask})

	//err := loadCollectionTask.WaitToFinish()
	//if err != nil {
	//	status.Reason = err.Error()
	//	return status, err
	//}
	//qs.meta.setLoadCollection(collectionID, true)

	log.Debug("LoadCollectionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID))
	status.ErrorCode = commonpb.ErrorCode_Success
	return status, nil
}

func (qs *QueryService) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	//dbID := req.DbID
	collectionID := req.CollectionID
	log.Debug("ReleaseCollectionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	hasCollection := qs.meta.hasCollection(collectionID)
	if !hasCollection {
		log.Error("release collection end, query service don't have the log of", zap.String("collectionID", fmt.Sprintln(collectionID)))
		return status, nil
	}

	releaseCollectionTask := &ReleaseCollectionTask{
		BaseTask: BaseTask{
			ctx:              qs.loopCtx,
			Condition:        NewTaskCondition(qs.loopCtx),
			triggerCondition: querypb.TriggerCondition_grpcRequest,
		},
		ReleaseCollectionRequest: req,
		cluster:                  qs.cluster,
	}
	qs.scheduler.Enqueue([]task{releaseCollectionTask})

	err := releaseCollectionTask.WaitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, err
	}

	log.Debug("ReleaseCollectionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID))
	qs.meta.printMeta()
	qs.cluster.printMeta()
	return status, nil
}

func (qs *QueryService) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	collectionID := req.CollectionID
	log.Debug("show partitions start, ", zap.Int64("collectionID", collectionID))
	partitionIDs, err := qs.meta.showPartitions(collectionID)
	if err != nil {
		return &querypb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, err
	}

	log.Debug("show partitions end", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))

	return &querypb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionIDs: partitionIDs,
	}, nil
}

func (qs *QueryService) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	log.Debug("LoadPartitionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	hasCollection := qs.meta.hasCollection(collectionID)
	if hasCollection && qs.meta.collectionInfos[collectionID].LoadCollection {
		status.ErrorCode = commonpb.ErrorCode_Success
		status.Reason = "collection has been loaded"
		return status, nil
	}

	if len(partitionIDs) == 0 {
		err := errors.New("partitionIDs are empty")
		status.Reason = err.Error()
		return status, err
	}

	partitionIDsToLoad := make([]UniqueID, 0)
	for _, partitionID := range partitionIDs {
		hasPartition := qs.meta.hasPartition(collectionID, partitionID)
		if !hasPartition {
			partitionIDsToLoad = append(partitionIDsToLoad, partitionID)
		}
	}
	req.PartitionIDs = partitionIDsToLoad

	if len(req.PartitionIDs) > 0 {
		loadPartitionTask := &LoadPartitionTask{
			BaseTask: BaseTask{
				ctx:              qs.loopCtx,
				Condition:        NewTaskCondition(qs.loopCtx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			LoadPartitionsRequest: req,
			dataService:           qs.dataServiceClient,
			cluster:               qs.cluster,
			meta:                  qs.meta,
		}
		qs.scheduler.Enqueue([]task{loadPartitionTask})

		//err := loadPartitionTask.WaitToFinish()
		//if err != nil {
		//	status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		//	status.Reason = err.Error()
		//	return status, err
		//}
	}

	status.ErrorCode = commonpb.ErrorCode_Success
	log.Debug("LoadPartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID))
	return status, nil
}

func (qs *QueryService) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	//dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	log.Debug("ReleasePartitionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID), zap.Int64s("partitionIDs", partitionIDs))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	toReleasedPartitionID := make([]UniqueID, 0)
	for _, partitionID := range partitionIDs {
		hasPartition := qs.meta.hasPartition(collectionID, partitionID)
		if hasPartition {
			toReleasedPartitionID = append(toReleasedPartitionID, partitionID)
		}
	}

	if len(toReleasedPartitionID) > 0 {
		req.PartitionIDs = toReleasedPartitionID
		releasePartitionTask := &ReleasePartitionTask{
			BaseTask: BaseTask{
				ctx:              qs.loopCtx,
				Condition:        NewTaskCondition(qs.loopCtx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			ReleasePartitionsRequest: req,
			cluster:                  qs.cluster,
		}
		qs.scheduler.Enqueue([]task{releasePartitionTask})

		err := releasePartitionTask.WaitToFinish()
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			return status, err
		}
	}
	log.Debug("ReleasePartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
	qs.meta.printMeta()
	qs.cluster.printMeta()
	return status, nil
}

func (qs *QueryService) CreateQueryChannel(ctx context.Context, req *querypb.CreateQueryChannelRequest) (*querypb.CreateQueryChannelResponse, error) {
	collectionID := req.CollectionID
	queryChannel, queryResultChannel := qs.meta.GetQueryChannel(collectionID)

	return &querypb.CreateQueryChannelResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		RequestChannel: queryChannel,
		ResultChannel:  queryResultChannel,
	}, nil
}

func (qs *QueryService) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	partitionIDs := req.PartitionIDs
	partitionStates := make([]*querypb.PartitionStates, 0)
	for _, partitionID := range partitionIDs {
		state, err := qs.meta.getPartitionStateByID(partitionID)
		if err != nil {
			return &querypb.GetPartitionStatesResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, err
		}
		partitionState := &querypb.PartitionStates{
			PartitionID: partitionID,
			State:       state,
		}
		partitionStates = append(partitionStates, partitionState)
	}

	return &querypb.GetPartitionStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionDescriptions: partitionStates,
	}, nil
}

func (qs *QueryService) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	totalMemSize := int64(0)
	totalNumRows := int64(0)
	//TODO::get segment infos from meta
	//segmentIDs := req.SegmentIDs
	//segmentInfos, err := qs.meta.getSegmentInfos(segmentIDs)
	segmentInfos, err := qs.cluster.getSegmentInfo(ctx, req)
	if err != nil {
		return &querypb.GetSegmentInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, err
	}
	for _, info := range segmentInfos {
		totalNumRows += info.NumRows
		totalMemSize += info.MemSize
	}
	log.Debug("getSegmentInfo", zap.Int64("num rows", totalNumRows), zap.Int64("memory size", totalMemSize))
	return &querypb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Infos: segmentInfos,
	}, nil
}
