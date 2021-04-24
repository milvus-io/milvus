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
	"strconv"
	"time"

	"go.uber.org/zap"

	nodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/retry"
)

func (qs *QueryService) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	serviceComponentInfo := &internalpb.ComponentInfo{
		NodeID:    Params.QueryServiceID,
		StateCode: qs.stateCode.Load().(internalpb.StateCode),
	}
	subComponentInfos := make([]*internalpb.ComponentInfo, 0)
	for nodeID, node := range qs.queryNodes {
		componentStates, err := node.GetComponentStates(ctx)
		if err != nil {
			subComponentInfos = append(subComponentInfos, &internalpb.ComponentInfo{
				NodeID:    nodeID,
				StateCode: internalpb.StateCode_Abnormal,
			})
			continue
		}
		subComponentInfos = append(subComponentInfos, componentStates.State)
	}
	return &internalpb.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		State:              serviceComponentInfo,
		SubcomponentStates: subComponentInfos,
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
	log.Debug("register query node", zap.String("address", req.Address.String()))
	// TODO:: add mutex
	nodeID := req.Base.SourceID
	if _, ok := qs.queryNodes[nodeID]; ok {
		err := errors.New("nodeID already exists")
		return &querypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    err.Error(),
			},
		}, err
	}

	registerNodeAddress := req.Address.Ip + ":" + strconv.FormatInt(req.Address.Port, 10)
	client := nodeclient.NewClient(registerNodeAddress)
	if err := client.Init(); err != nil {
		return &querypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			InitParams: new(internalpb.InitParams),
		}, err
	}
	if err := client.Start(); err != nil {
		return nil, err
	}
	qs.queryNodes[nodeID] = newQueryNodeInfo(client)

	//TODO::return init params to queryNode
	startParams := []*commonpb.KeyValuePair{
		{Key: "StatsChannelName", Value: Params.StatsChannelName},
		{Key: "TimeTickChannelName", Value: Params.TimeTickChannelName},
	}
	qs.qcMutex.Lock()
	for _, queryChannel := range qs.queryChannels {
		startParams = append(startParams, &commonpb.KeyValuePair{
			Key:   "SearchChannelName",
			Value: queryChannel.requestChannel,
		})
		startParams = append(startParams, &commonpb.KeyValuePair{
			Key:   "SearchResultChannelName",
			Value: queryChannel.responseChannel,
		})
	}
	qs.qcMutex.Unlock()

	return &querypb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		InitParams: &internalpb.InitParams{
			NodeID:      nodeID,
			StartParams: startParams,
		},
	}, nil
}

func (qs *QueryService) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	dbID := req.DbID
	log.Debug("show collection start, dbID = ", zap.String("dbID", strconv.FormatInt(dbID, 10)))
	collections, err := qs.replica.getCollections(dbID)
	collectionIDs := make([]UniqueID, 0)
	for _, collection := range collections {
		collectionIDs = append(collectionIDs, collection.id)
	}
	if err != nil {
		return &querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    err.Error(),
			},
		}, err
	}
	log.Debug("show collection end")
	return &querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		CollectionIDs: collectionIDs,
	}, nil
}

func (qs *QueryService) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	schema := req.Schema
	watchNeeded := false
	log.Debug("LoadCollectionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID),
		zap.Stringer("schema", req.Schema))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	_, err := qs.replica.getCollectionByID(dbID, collectionID)
	if err != nil {
		watchNeeded = true
		err = qs.replica.addCollection(dbID, collectionID, schema)
		if err != nil {
			status.Reason = err.Error()
			return status, err
		}
	}
	loadCollectionTask := &LoadCollectionTask{
		BaseTask: BaseTask{
			ctx:       qs.loopCtx,
			Condition: NewTaskCondition(qs.loopCtx),
		},
		LoadCollectionRequest: req,
		masterService:         qs.masterServiceClient,
		dataService:           qs.dataServiceClient,
		queryNodes:            qs.queryNodes,
		meta:                  qs.replica,
		watchNeeded:           watchNeeded,
	}
	err = qs.sched.DdQueue.Enqueue(loadCollectionTask)
	if err != nil {
		status.Reason = err.Error()
		return status, err
	}

	err = loadCollectionTask.WaitToFinish()
	if err != nil {
		status.Reason = err.Error()
		return status, err
	}

	log.Debug("LoadCollectionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID))
	status.ErrorCode = commonpb.ErrorCode_Success
	return status, nil
}

func (qs *QueryService) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	log.Debug("ReleaseCollectionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	_, err := qs.replica.getCollectionByID(dbID, collectionID)
	if err != nil {
		log.Error("release collection end, query service don't have the log of", zap.String("collectionID", fmt.Sprintln(collectionID)))
		return status, nil
	}

	releaseCollectionTask := &ReleaseCollectionTask{
		BaseTask: BaseTask{
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
		},
		ReleaseCollectionRequest: req,
		queryNodes:               qs.queryNodes,
		meta:                     qs.replica,
	}
	err = qs.sched.DdQueue.Enqueue(releaseCollectionTask)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, err
	}
	err = releaseCollectionTask.WaitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, err
	}

	log.Debug("ReleaseCollectionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID))
	//TODO:: queryNode cancel subscribe dmChannels
	return status, nil
}

func (qs *QueryService) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	partitions, err := qs.replica.getPartitions(dbID, collectionID)
	partitionIDs := make([]UniqueID, 0)
	for _, partition := range partitions {
		partitionIDs = append(partitionIDs, partition.id)
	}
	if err != nil {
		return &querypb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, err
	}
	return &querypb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionIDs: partitionIDs,
	}, nil
}

func (qs *QueryService) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	//TODO::suggest different partitions have different dm channel
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	log.Debug("LoadPartitionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
	status, watchNeeded, err := LoadPartitionMetaCheck(qs.replica, req)
	if err != nil {
		return status, err
	}

	loadPartitionTask := &LoadPartitionTask{
		BaseTask: BaseTask{
			ctx:       qs.loopCtx,
			Condition: NewTaskCondition(qs.loopCtx),
		},
		LoadPartitionsRequest: req,
		masterService:         qs.masterServiceClient,
		dataService:           qs.dataServiceClient,
		queryNodes:            qs.queryNodes,
		meta:                  qs.replica,
		watchNeeded:           watchNeeded,
	}
	err = qs.sched.DdQueue.Enqueue(loadPartitionTask)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, err
	}

	err = loadPartitionTask.WaitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, err
	}

	log.Debug("LoadPartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID))
	return status, nil
}

func LoadPartitionMetaCheck(meta Replica, req *querypb.LoadPartitionsRequest) (*commonpb.Status, bool, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	schema := req.Schema
	watchNeeded := false

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if len(partitionIDs) == 0 {
		err := errors.New("partitionIDs are empty")
		status.Reason = err.Error()
		return status, watchNeeded, err
	}

	_, err := meta.getCollectionByID(dbID, collectionID)
	if err != nil {
		err = meta.addCollection(dbID, collectionID, schema)
		if err != nil {
			status.Reason = err.Error()
			return status, watchNeeded, err
		}
		watchNeeded = true
	}

	for _, partitionID := range partitionIDs {
		_, err = meta.getPartitionByID(dbID, collectionID, partitionID)
		if err == nil {
			continue
		}
		err = meta.addPartition(dbID, collectionID, partitionID)
		if err != nil {
			status.Reason = err.Error()
			return status, watchNeeded, err
		}
	}

	status.ErrorCode = commonpb.ErrorCode_Success
	return status, watchNeeded, nil
}

func (qs *QueryService) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	log.Debug("ReleasePartitionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID), zap.Int64s("partitionIDs", partitionIDs))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	toReleasedPartitionID := make([]UniqueID, 0)
	for _, partitionID := range partitionIDs {
		_, err := qs.replica.getPartitionByID(dbID, collectionID, partitionID)
		if err == nil {
			toReleasedPartitionID = append(toReleasedPartitionID, partitionID)
		}
	}

	if len(toReleasedPartitionID) > 0 {
		req.PartitionIDs = toReleasedPartitionID
		releasePartitionTask := &ReleasePartitionTask{
			BaseTask: BaseTask{
				ctx:       ctx,
				Condition: NewTaskCondition(ctx),
			},
			ReleasePartitionsRequest: req,
			queryNodes:               qs.queryNodes,
			meta:                     qs.replica,
		}
		err := qs.sched.DdQueue.Enqueue(releasePartitionTask)
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			return status, err
		}

		err = releasePartitionTask.WaitToFinish()
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			return status, err
		}
	}
	log.Debug("ReleasePartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
	//TODO:: queryNode cancel subscribe dmChannels
	return status, nil
}

func (qs *QueryService) CreateQueryChannel(ctx context.Context) (*querypb.CreateQueryChannelResponse, error) {
	channelID := len(qs.queryChannels)
	searchPrefix := Params.SearchChannelPrefix
	searchResultPrefix := Params.SearchResultChannelPrefix
	allocatedQueryChannel := searchPrefix + "-" + strconv.FormatInt(int64(channelID), 10)
	allocatedQueryResultChannel := searchResultPrefix + "-" + strconv.FormatInt(int64(channelID), 10)

	qs.qcMutex.Lock()
	qs.queryChannels = append(qs.queryChannels, &queryChannelInfo{
		requestChannel:  allocatedQueryChannel,
		responseChannel: allocatedQueryResultChannel,
	})

	addQueryChannelsRequest := &querypb.AddQueryChannelRequest{
		RequestChannelID: allocatedQueryChannel,
		ResultChannelID:  allocatedQueryResultChannel,
	}
	log.Debug("query service create query channel", zap.String("queryChannelName", allocatedQueryChannel))
	for nodeID, node := range qs.queryNodes {
		log.Debug("node watch query channel", zap.String("nodeID", fmt.Sprintln(nodeID)))
		fn := func() error {
			_, err := node.AddQueryChannel(ctx, addQueryChannelsRequest)
			return err
		}
		err := retry.Retry(10, time.Millisecond*200, fn)
		if err != nil {
			qs.qcMutex.Unlock()
			return &querypb.CreateQueryChannelResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, err
		}
	}
	qs.qcMutex.Unlock()

	return &querypb.CreateQueryChannelResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		RequestChannel: allocatedQueryChannel,
		ResultChannel:  allocatedQueryResultChannel,
	}, nil
}

func (qs *QueryService) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	states, err := qs.replica.getPartitionStates(req.DbID, req.CollectionID, req.PartitionIDs)
	if err != nil {
		return &querypb.GetPartitionStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			PartitionDescriptions: states,
		}, err
	}
	return &querypb.GetPartitionStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionDescriptions: states,
	}, nil
}

func (qs *QueryService) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	segmentInfos := make([]*querypb.SegmentInfo, 0)
	totalMemSize := int64(0)
	for nodeID, node := range qs.queryNodes {
		segmentInfo, err := node.client.GetSegmentInfo(ctx, req)
		if err != nil {
			return &querypb.GetSegmentInfoResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, err
		}
		segmentInfos = append(segmentInfos, segmentInfo.Infos...)
		for _, info := range segmentInfo.Infos {
			totalMemSize = totalMemSize + info.MemSize
		}
		log.Debug("getSegmentInfo", zap.Int64("nodeID", nodeID), zap.Int64("memory size", totalMemSize))
	}
	return &querypb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Infos: segmentInfos,
	}, nil
}
