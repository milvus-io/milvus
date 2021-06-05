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
	subComponentInfos := qs.cluster.GetComponentInfos(ctx)
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
	if _, ok := qs.cluster.nodes[nodeID]; ok {
		err := errors.New("nodeID already exists")
		return &querypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    err.Error(),
			},
		}, err
	}

	err := qs.cluster.RegisterNode(req.Address.Ip, req.Address.Port, req.Base.SourceID)
	//registerNodeAddress := req.Address.Ip + ":" + strconv.FormatInt(req.Address.Port, 10)
	//client, err := nodeclient.NewClient(registerNodeAddress)
	if err != nil {
		return &querypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
			//InitParams: new(internalpb.InitParams),
		}, err
	}
	//if err := client.Init(); err != nil {
	//	return &querypb.RegisterNodeResponse{
	//		Status: &commonpb.Status{
	//			ErrorCode: commonpb.ErrorCode_Success,
	//		},
	//		InitParams: new(internalpb.InitParams),
	//	}, err
	//}
	//if err := client.Start(); err != nil {
	//	return nil, err
	//}
	//qs.cluster.nodes[nodeID] = newQueryNode(client)

	//TODO::return init params to queryNodeCluster
	//startParams := []*commonpb.KeyValuePair{
	//	{Key: "StatsChannelName", Value: Params.StatsChannelName},
	//	{Key: "TimeTickChannelName", Value: Params.TimeTickChannelName},
	//}
	//qs.qcMutex.Lock()
	//for _, queryChannel := range qs.queryChannels {
	//	startParams = append(startParams, &commonpb.KeyValuePair{
	//		Key:   "SearchChannelName",
	//		Value: queryChannel.requestChannel,
	//	})
	//	startParams = append(startParams, &commonpb.KeyValuePair{
	//		Key:   "SearchResultChannelName",
	//		Value: queryChannel.responseChannel,
	//	})
	//}
	//qs.qcMutex.Unlock()

	return &querypb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		InitParams: &internalpb.InitParams{
			NodeID:      nodeID,
			//StartParams: startParams,
		},
	}, nil
}

func (qs *QueryService) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	dbID := req.DbID
	log.Debug("show collection start, dbID = ", zap.String("dbID", strconv.FormatInt(dbID, 10)))
	collectionIDs := qs.meta.showCollections()
	//collectionIDs := make([]UniqueID, 0)
	//for _, collection := range collections {
	//	collectionIDs = append(collectionIDs, collection.id)
	//}
	//if err != nil {
	//	return &querypb.ShowCollectionsResponse{
	//		Status: &commonpb.Status{
	//			ErrorCode: commonpb.ErrorCode_Success,
	//			Reason:    err.Error(),
	//		},
	//	}, err
	//}
	log.Debug("show collection end")
	return &querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		CollectionIDs: collectionIDs,
	}, nil
}

func (qs *QueryService) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	//dbID := req.DbID
	collectionID := req.CollectionID
	schema := req.Schema
	watchNeeded := false
	log.Debug("LoadCollectionRequest received", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID),
		zap.Stringer("schema", req.Schema))
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	hasCollection := qs.meta.hasCollection(collectionID)
	if !hasCollection {
		watchNeeded = true
		err := qs.meta.addCollection(collectionID, schema)
		log.Error(err.Error())
		//if err != nil {
		//	status.Reason = err.Error()
		//	return status, err
		//}
	}
	loadCollectionTask := &LoadCollectionTask{
		BaseTask: BaseTask{
			ctx:       qs.loopCtx,
			Condition: NewTaskCondition(qs.loopCtx),
		},
		LoadCollectionRequest: req,
		masterService:         qs.masterServiceClient,
		dataService:           qs.dataServiceClient,
		cluster:               qs.cluster,
		meta:                  qs.meta,
		toWatchPosition:       make(map[string]*internalpb.MsgPosition),
		excludeSegment:        make(map[string][]UniqueID),
		watchNeeded:           watchNeeded,
	}
	qs.scheduler.triggerTaskQueue.Enqueue([]task{loadCollectionTask})

	err := loadCollectionTask.WaitToFinish()
	if err != nil {
		status.Reason = err.Error()
		return status, err
	}

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
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
		},
		ReleaseCollectionRequest: req,
		cluster:                  qs.cluster,
		meta:                     qs.meta,
	}
	qs.scheduler.triggerTaskQueue.Enqueue([]task{releaseCollectionTask})

	err := releaseCollectionTask.WaitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, err
	}

	log.Debug("ReleaseCollectionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID))
	//TODO:: queryNodeCluster cancel subscribe dmChannels
	return status, nil
}

func (qs *QueryService) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	//dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs, err := qs.meta.showPartitions(collectionID)
	//partitionIDs := make([]UniqueID, 0)
	//for _, partition := range partitions {
	//	partitionIDs = append(partitionIDs, partition.id)
	//}
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
	status, watchNeeded, err := LoadPartitionMetaCheck(qs.meta, req)
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
		cluster:               qs.cluster,
		meta:                  qs.meta,
		toWatchPosition:       make(map[string]*internalpb.MsgPosition),
		excludeSegment:        make(map[string][]UniqueID),
		watchNeeded:           watchNeeded,
	}
	qs.scheduler.triggerTaskQueue.Enqueue([]task{loadPartitionTask})

	err = loadPartitionTask.WaitToFinish()
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, err
	}

	log.Debug("LoadPartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", req.CollectionID))
	return status, nil
}

func LoadPartitionMetaCheck(meta *meta, req *querypb.LoadPartitionsRequest) (*commonpb.Status, bool, error) {
	//dbID := req.DbID
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

	hasCollection := meta.hasCollection(collectionID)
	if !hasCollection {
		err := meta.addCollection(collectionID, schema)
		log.Error(err.Error())
		//if err != nil {
		//	status.Reason = err.Error()
		//	return status, watchNeeded, err
		//}
		watchNeeded = true
	}

	for _, partitionID := range partitionIDs {
		hasPartition := meta.hasPartition(collectionID, partitionID)
		//if err == nil {
		//	continue
		//}
		if !hasPartition {
			err := meta.addPartition(collectionID, partitionID)
			if err != nil {
				status.Reason = err.Error()
				return status, watchNeeded, err
			}
		}
	}

	status.ErrorCode = commonpb.ErrorCode_Success
	return status, watchNeeded, nil
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
				ctx:       ctx,
				Condition: NewTaskCondition(ctx),
			},
			ReleasePartitionsRequest: req,
			cluster:                  qs.cluster,
			meta:                     qs.meta,
		}
		qs.scheduler.triggerTaskQueue.Enqueue([]task{releasePartitionTask})

		err := releasePartitionTask.WaitToFinish()
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			return status, err
		}
	}
	log.Debug("ReleasePartitionRequest completed", zap.String("role", Params.RoleName), zap.Int64("msgID", req.Base.MsgID), zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
	//TODO:: queryNodeCluster cancel subscribe dmChannels
	return status, nil
}

func (qs *QueryService) CreateQueryChannel(ctx context.Context, req *querypb.CreateQueryChannelRequest) (*querypb.CreateQueryChannelResponse, error) {
	collectionID :=req.CollectionID
	var queryChannel string
	var queryResultChannel string
	if info, ok := qs.meta.queryChannelInfos[collectionID]; ok {
		queryChannel = info.QueryChannelID
		queryResultChannel = info.QueryResultChannelID
	} else {
		searchPrefix := Params.SearchChannelPrefix
		searchResultPrefix := Params.SearchResultChannelPrefix
		allocatedQueryChannel := searchPrefix + "-" + strconv.FormatInt(collectionID, 10)
		allocatedQueryResultChannel := searchResultPrefix + "-" + strconv.FormatInt(collectionID, 10)

		qs.meta.setQueryChannel(collectionID, allocatedQueryChannel, allocatedQueryResultChannel)

		addQueryChannelsRequest := &querypb.AddQueryChannelRequest{
			CollectionID: collectionID,
			RequestChannelID: allocatedQueryChannel,
			ResultChannelID:  allocatedQueryResultChannel,
		}
		log.Debug("query service create query channel", zap.String("queryChannelName", allocatedQueryChannel))
		for nodeID := range qs.cluster.nodes {
			log.Debug("node watch query channel", zap.String("nodeID", fmt.Sprintln(nodeID)))
			fn := func() error {
				_, err := qs.cluster.AddQueryChannel(ctx, nodeID, addQueryChannelsRequest)
				return err
			}
			err := retry.Retry(10, time.Millisecond*200, fn)
			if err != nil {
				return &querypb.CreateQueryChannelResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    err.Error(),
					},
				}, err
			}
		}
	}

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
	segmentIDs := req.SegmentIDs
	segmentInfos, err := qs.meta.getSegmentInfos(segmentIDs)
	if err != nil {
		return &querypb.GetSegmentInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, err
	}
	for _, info := range segmentInfos {
		totalMemSize = totalMemSize + info.MemSize
	}
	log.Debug("getSegmentInfo", zap.Int64("memory size", totalMemSize))
	return &querypb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Infos: segmentInfos,
	}, nil
}
