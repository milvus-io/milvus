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

package querycoord

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
)

const (
	triggerTaskPrefix     = "queryCoord-triggerTask"
	activeTaskPrefix      = "queryCoord-activeTask"
	taskInfoPrefix        = "queryCoord-taskInfo"
	loadBalanceInfoPrefix = "queryCoord-loadBalanceInfo"
)

type taskState int

const (
	taskUndo    taskState = 0
	taskDoing   taskState = 1
	taskDone    taskState = 3
	taskExpired taskState = 4
)

type task interface {
	TraceCtx() context.Context
	ID() UniqueID // return ReqId
	SetID(id UniqueID)
	Type() commonpb.MsgType
	Timestamp() Timestamp
	PreExecute()
	Execute(ctx context.Context) error
	PostExecute()
	WaitToFinish() error
	Notify(err error)
	TaskPriority() querypb.TriggerCondition
	GetParentTask() task
	GetChildTask() []task
	AddChildTask(t task)
	IsValid() bool
	Reschedule() ([]task, error)
	Marshal() string
	State() taskState
	SetState(state taskState)
}

type BaseTask struct {
	Condition
	ctx    context.Context
	cancel context.CancelFunc
	result *commonpb.Status
	state  taskState

	taskID           UniqueID
	triggerCondition querypb.TriggerCondition
	parentTask       task
	childTasks       []task
}

func (bt *BaseTask) ID() UniqueID {
	return bt.taskID
}

func (bt *BaseTask) SetID(id UniqueID) {
	bt.taskID = id
}

func (bt *BaseTask) TraceCtx() context.Context {
	return bt.ctx
}

func (bt *BaseTask) TaskPriority() querypb.TriggerCondition {
	return bt.triggerCondition
}

func (bt *BaseTask) GetParentTask() task {
	return bt.parentTask
}

func (bt *BaseTask) GetChildTask() []task {
	return bt.childTasks
}

func (bt *BaseTask) AddChildTask(t task) {
	bt.childTasks = append(bt.childTasks, t)
}

func (bt *BaseTask) IsValid() bool {
	return true
}

func (bt *BaseTask) Reschedule() ([]task, error) {
	return nil, nil
}

func (bt *BaseTask) State() taskState {
	return bt.state
}

func (bt *BaseTask) SetState(state taskState) {
	bt.state = state
}

//************************grpcTask***************************//
type LoadCollectionTask struct {
	BaseTask
	*querypb.LoadCollectionRequest
	rootCoord types.RootCoord
	dataCoord types.DataCoord
	cluster   *queryNodeCluster
	meta      *meta
}

func (lct *LoadCollectionTask) Marshal() string {
	return proto.MarshalTextString(lct.LoadCollectionRequest)
}

func (lct *LoadCollectionTask) Type() commonpb.MsgType {
	return lct.Base.MsgType
}

func (lct *LoadCollectionTask) Timestamp() Timestamp {
	return lct.Base.Timestamp
}

func (lct *LoadCollectionTask) PreExecute() {
	collectionID := lct.CollectionID
	schema := lct.Schema
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	lct.result = status
	log.Debug("start do LoadCollectionTask",
		zap.Int64("msgID", lct.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Stringer("schema", schema))
}

func (lct *LoadCollectionTask) Execute(ctx context.Context) error {
	collectionID := lct.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowPartitions,
		},
		CollectionID: collectionID,
	}
	showPartitionResponse, err := lct.rootCoord.ShowPartitions(ctx, showPartitionRequest)
	if err != nil {
		status.Reason = err.Error()
		lct.result = status
		return err
	}
	log.Debug("loadCollectionTask: get collection's all partitionIDs", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", showPartitionResponse.PartitionIDs))

	partitionIDs := showPartitionResponse.PartitionIDs
	toLoadPartitionIDs := make([]UniqueID, 0)
	hasCollection := lct.meta.hasCollection(collectionID)
	if hasCollection {
		loadCollection, _ := lct.meta.getLoadCollection(collectionID)
		if loadCollection {
			for _, partitionID := range partitionIDs {
				hasReleasePartition := lct.meta.hasReleasePartition(collectionID, partitionID)
				if hasReleasePartition {
					toLoadPartitionIDs = append(toLoadPartitionIDs, partitionID)
				}
			}
		} else {
			for _, partitionID := range partitionIDs {
				hasPartition := lct.meta.hasPartition(collectionID, partitionID)
				if !hasPartition {
					toLoadPartitionIDs = append(toLoadPartitionIDs, partitionID)
				}
			}
		}
	} else {
		toLoadPartitionIDs = partitionIDs
	}

	log.Debug("loadCollectionTask: toLoadPartitionIDs", zap.Int64s("partitionIDs", toLoadPartitionIDs))
	lct.meta.addCollection(collectionID, lct.Schema)
	lct.meta.setLoadCollection(collectionID, true)
	for _, id := range toLoadPartitionIDs {
		lct.meta.addPartition(collectionID, id)
	}

	segment2Binlog := make(map[UniqueID]*querypb.SegmentLoadInfo)
	watchRequests := make(map[string]*querypb.WatchDmChannelsRequest)
	channelsToWatch := make([]string, 0)
	segmentsToLoad := make([]UniqueID, 0)
	for _, partitionID := range toLoadPartitionIDs {
		getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
			Base:         lct.Base,
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		recoveryInfo, err := lct.dataCoord.GetRecoveryInfo(lct.ctx, getRecoveryInfoRequest)
		if err != nil {
			status.Reason = err.Error()
			lct.result = status
			return err
		}

		for _, segmentBingLog := range recoveryInfo.Binlogs {
			segmentID := segmentBingLog.SegmentID
			segmentLoadInfo := &querypb.SegmentLoadInfo{
				SegmentID:    segmentBingLog.SegmentID,
				PartitionID:  partitionID,
				CollectionID: collectionID,
				BinlogPaths:  make([]*datapb.FieldBinlog, 0),
			}
			segmentLoadInfo.BinlogPaths = append(segmentLoadInfo.BinlogPaths, segmentBingLog.FieldBinlogs...)
			segmentsToLoad = append(segmentsToLoad, segmentID)
			segment2Binlog[segmentID] = segmentLoadInfo
		}

		for _, info := range recoveryInfo.Channels {
			channel := info.ChannelName
			if _, ok := watchRequests[channel]; !ok {
				watchRequest := &querypb.WatchDmChannelsRequest{
					Base:         lct.Base,
					CollectionID: collectionID,
					Infos:        []*datapb.VchannelInfo{info},
					Schema:       lct.Schema,
				}
				watchRequests[channel] = watchRequest
				channelsToWatch = append(channelsToWatch, channel)
				continue
			}
			oldInfo := watchRequests[channel].Infos[0]
			newInfo := mergeVChannelInfo(oldInfo, info)
			watchRequests[channel].Infos = []*datapb.VchannelInfo{newInfo}
		}
	}

	log.Debug("loadCollectionTask: segments and channels are ready to load or watch")
	segment2Nodes := shuffleSegmentsToQueryNode(segmentsToLoad, lct.cluster)
	watchRequest2Nodes := shuffleChannelsToQueryNode(channelsToWatch, lct.cluster)

	watchQueryChannelInfo := make(map[int64]bool)
	node2Segments := make(map[int64][]*querypb.SegmentLoadInfo)
	for segmentID, nodeID := range segment2Nodes {
		if _, ok := node2Segments[nodeID]; !ok {
			node2Segments[nodeID] = make([]*querypb.SegmentLoadInfo, 0)
		}
		node2Segments[nodeID] = append(node2Segments[nodeID], segment2Binlog[segmentID])
		if lct.cluster.hasWatchedQueryChannel(lct.ctx, nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}
	for _, nodeID := range watchRequest2Nodes {
		if lct.cluster.hasWatchedQueryChannel(lct.ctx, nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}

	for nodeID, segmentInfos := range node2Segments {
		loadSegmentsRequest := &querypb.LoadSegmentsRequest{
			Base:          lct.Base,
			NodeID:        nodeID,
			Infos:         segmentInfos,
			Schema:        lct.Schema,
			LoadCondition: querypb.TriggerCondition_grpcRequest,
		}

		loadSegmentTask := &LoadSegmentTask{
			BaseTask: BaseTask{
				ctx:              lct.ctx,
				Condition:        NewTaskCondition(lct.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},

			LoadSegmentsRequest: loadSegmentsRequest,
			meta:                lct.meta,
			cluster:             lct.cluster,
		}
		lct.AddChildTask(loadSegmentTask)
		log.Debug("loadCollectionTask: add a loadSegmentTask to loadCollectionTask's childTask", zap.Any("task", loadSegmentTask))
	}

	for index, nodeID := range watchRequest2Nodes {
		channel := channelsToWatch[index]
		watchRequests[channel].NodeID = nodeID
		watchDmChannelTask := &WatchDmChannelTask{
			BaseTask: BaseTask{
				ctx:              lct.ctx,
				Condition:        NewTaskCondition(lct.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			WatchDmChannelsRequest: watchRequests[channel],
			meta:                   lct.meta,
			cluster:                lct.cluster,
		}
		//TODO::open when queryNode watchDmChannel work
		lct.AddChildTask(watchDmChannelTask)
		log.Debug("loadCollectionTask: add a watchDmChannelTask to loadCollectionTask's childTask", zap.Any("task", watchDmChannelTask))
	}

	for nodeID, watched := range watchQueryChannelInfo {
		if !watched {
			queryChannel, queryResultChannel := lct.meta.GetQueryChannel(collectionID)

			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:             lct.Base,
				NodeID:           nodeID,
				CollectionID:     collectionID,
				RequestChannelID: queryChannel,
				ResultChannelID:  queryResultChannel,
			}
			watchQueryChannelTask := &WatchQueryChannelTask{
				BaseTask: BaseTask{
					ctx:              lct.ctx,
					Condition:        NewTaskCondition(lct.ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				AddQueryChannelRequest: addQueryChannelRequest,
				cluster:                lct.cluster,
			}
			lct.AddChildTask(watchQueryChannelTask)
			log.Debug("loadCollectionTask: add a watchQueryChannelTask to loadCollectionTask's childTask", zap.Any("task", watchQueryChannelTask))
		}
	}

	log.Debug("LoadCollection execute done",
		zap.Int64("msgID", lct.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lct *LoadCollectionTask) PostExecute() {
	collectionID := lct.CollectionID
	lct.meta.addCollection(collectionID, lct.Schema)
	if lct.result.ErrorCode != commonpb.ErrorCode_Success {
		lct.childTasks = make([]task, 0)
		for nodeID, node := range lct.cluster.nodes {
			if !node.isOnService() {
				continue
			}
			req := &querypb.ReleaseCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_ReleaseCollection,
					MsgID:     lct.Base.MsgID,
					Timestamp: lct.Base.Timestamp,
					SourceID:  lct.Base.SourceID,
				},
				DbID:         lct.DbID,
				CollectionID: lct.CollectionID,
				NodeID:       nodeID,
			}
			releaseCollectionTask := &ReleaseCollectionTask{
				BaseTask: BaseTask{
					ctx:              lct.ctx,
					Condition:        NewTaskCondition(lct.ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},
				ReleaseCollectionRequest: req,
				cluster:                  lct.cluster,
			}
			lct.AddChildTask(releaseCollectionTask)
			log.Debug("loadCollectionTask: add a releaseCollectionTask to loadCollectionTask's childTask", zap.Any("task", releaseCollectionTask))
		}
	}
	log.Debug("LoadCollectionTask postExecute done",
		zap.Int64("msgID", lct.ID()),
		zap.Int64("collectionID", collectionID))
}

type ReleaseCollectionTask struct {
	BaseTask
	*querypb.ReleaseCollectionRequest
	cluster *queryNodeCluster
}

func (rct *ReleaseCollectionTask) Marshal() string {
	return proto.MarshalTextString(rct.ReleaseCollectionRequest)
}

func (rct *ReleaseCollectionTask) Type() commonpb.MsgType {
	return rct.Base.MsgType
}

func (rct *ReleaseCollectionTask) Timestamp() Timestamp {
	return rct.Base.Timestamp
}

func (rct *ReleaseCollectionTask) PreExecute() {
	collectionID := rct.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	rct.result = status
	log.Debug("start do ReleaseCollectionTask",
		zap.Int64("msgID", rct.ID()),
		zap.Int64("collectionID", collectionID))
}

func (rct *ReleaseCollectionTask) Execute(ctx context.Context) error {
	collectionID := rct.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if rct.NodeID <= 0 {
		for nodeID, node := range rct.cluster.nodes {
			if !node.isOnService() {
				continue
			}
			req := proto.Clone(rct.ReleaseCollectionRequest).(*querypb.ReleaseCollectionRequest)
			req.NodeID = nodeID
			releaseCollectionTask := &ReleaseCollectionTask{
				BaseTask: BaseTask{
					ctx:              rct.ctx,
					Condition:        NewTaskCondition(rct.ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},
				ReleaseCollectionRequest: req,
				cluster:                  rct.cluster,
			}
			rct.AddChildTask(releaseCollectionTask)
			log.Debug("ReleaseCollectionTask: add a releaseCollectionTask to releaseCollectionTask's childTask", zap.Any("task", releaseCollectionTask))
		}
	} else {
		res, err := rct.cluster.releaseCollection(ctx, rct.NodeID, rct.ReleaseCollectionRequest)
		if err != nil {
			log.Error("ReleaseCollectionTask: release collection end, node occur error", zap.String("nodeID", fmt.Sprintln(rct.NodeID)))
			status.Reason = err.Error()
			rct.result = status
			return err
		}
		if res.ErrorCode != commonpb.ErrorCode_Success {
			log.Error("ReleaseCollectionTask: release collection end, node occur error", zap.String("nodeID", fmt.Sprintln(rct.NodeID)))
			err = errors.New("queryNode releaseCollection failed")
			status.Reason = err.Error()
			rct.result = status
			return err
		}
	}

	log.Debug("ReleaseCollectionTask Execute done",
		zap.Int64("msgID", rct.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("nodeID", rct.NodeID))
	return nil
}

func (rct *ReleaseCollectionTask) PostExecute() {
	collectionID := rct.CollectionID

	log.Debug("ReleaseCollectionTask postExecute done",
		zap.Int64("msgID", rct.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("nodeID", rct.NodeID))
}

type LoadPartitionTask struct {
	BaseTask
	*querypb.LoadPartitionsRequest
	dataCoord types.DataCoord
	cluster   *queryNodeCluster
	meta      *meta
	addCol    bool
}

func (lpt *LoadPartitionTask) Marshal() string {
	return proto.MarshalTextString(lpt.LoadPartitionsRequest)
}

func (lpt *LoadPartitionTask) Type() commonpb.MsgType {
	return lpt.Base.MsgType
}

func (lpt *LoadPartitionTask) Timestamp() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *LoadPartitionTask) PreExecute() {
	collectionID := lpt.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	lpt.result = status
	log.Debug("start do LoadPartitionTask",
		zap.Int64("msgID", lpt.ID()),
		zap.Int64("collectionID", collectionID))
}

func (lpt *LoadPartitionTask) Execute(ctx context.Context) error {
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs

	if !lpt.meta.hasCollection(collectionID) {
		lpt.meta.addCollection(collectionID, lpt.Schema)
		lpt.addCol = true
	}
	for _, id := range partitionIDs {
		lpt.meta.addPartition(collectionID, id)
	}
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	segmentsToLoad := make([]UniqueID, 0)
	segment2BingLog := make(map[UniqueID]*querypb.SegmentLoadInfo)
	channelsToWatch := make([]string, 0)
	watchRequests := make([]*querypb.WatchDmChannelsRequest, 0)
	for _, partitionID := range partitionIDs {
		getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
			Base:         lpt.Base,
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		recoveryInfo, err := lpt.dataCoord.GetRecoveryInfo(lpt.ctx, getRecoveryInfoRequest)
		if err != nil {
			status.Reason = err.Error()
			lpt.result = status
			return err
		}

		for _, segmentBingLog := range recoveryInfo.Binlogs {
			segmentID := segmentBingLog.SegmentID
			segmentLoadInfo := &querypb.SegmentLoadInfo{
				SegmentID:    segmentID,
				PartitionID:  partitionID,
				CollectionID: collectionID,
				BinlogPaths:  make([]*datapb.FieldBinlog, 0),
			}
			segmentLoadInfo.BinlogPaths = append(segmentLoadInfo.BinlogPaths, segmentBingLog.FieldBinlogs...)
			segmentsToLoad = append(segmentsToLoad, segmentID)
			segment2BingLog[segmentID] = segmentLoadInfo
		}

		for _, info := range recoveryInfo.Channels {
			channel := info.ChannelName
			watchRequest := &querypb.WatchDmChannelsRequest{
				Base:         lpt.Base,
				CollectionID: collectionID,
				PartitionID:  partitionID,
				Infos:        []*datapb.VchannelInfo{info},
				Schema:       lpt.Schema,
			}
			channelsToWatch = append(channelsToWatch, channel)
			watchRequests = append(watchRequests, watchRequest)
			log.Debug("LoadPartitionTask: set watchDmChannelsRequests", zap.Any("request", watchRequest), zap.Int64("collectionID", collectionID))
		}
	}

	segment2Nodes := shuffleSegmentsToQueryNode(segmentsToLoad, lpt.cluster)
	watchRequest2Nodes := shuffleChannelsToQueryNode(channelsToWatch, lpt.cluster)
	log.Debug("LoadPartitionTask: watch request to node", zap.Any("request map", watchRequest2Nodes), zap.Int64("collectionID", collectionID))

	watchQueryChannelInfo := make(map[int64]bool)
	node2Segments := make(map[int64][]*querypb.SegmentLoadInfo)
	for segmentID, nodeID := range segment2Nodes {
		if _, ok := node2Segments[nodeID]; !ok {
			node2Segments[nodeID] = make([]*querypb.SegmentLoadInfo, 0)
		}
		node2Segments[nodeID] = append(node2Segments[nodeID], segment2BingLog[segmentID])
		if lpt.cluster.hasWatchedQueryChannel(lpt.ctx, nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}
	for _, nodeID := range watchRequest2Nodes {
		if lpt.cluster.hasWatchedQueryChannel(lpt.ctx, nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}

	for nodeID, segmentInfos := range node2Segments {
		loadSegmentsRequest := &querypb.LoadSegmentsRequest{
			Base:          lpt.Base,
			NodeID:        nodeID,
			Infos:         segmentInfos,
			Schema:        lpt.Schema,
			LoadCondition: querypb.TriggerCondition_grpcRequest,
		}

		loadSegmentTask := &LoadSegmentTask{
			BaseTask: BaseTask{
				ctx:              lpt.ctx,
				Condition:        NewTaskCondition(lpt.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},

			LoadSegmentsRequest: loadSegmentsRequest,
			meta:                lpt.meta,
			cluster:             lpt.cluster,
		}
		lpt.AddChildTask(loadSegmentTask)
		log.Debug("LoadPartitionTask: add a loadSegmentTask to loadPartitionTask's childTask")
	}

	for index, nodeID := range watchRequest2Nodes {
		watchRequests[index].NodeID = nodeID
		watchDmChannelTask := &WatchDmChannelTask{
			BaseTask: BaseTask{
				ctx:              lpt.ctx,
				Condition:        NewTaskCondition(lpt.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			WatchDmChannelsRequest: watchRequests[index],
			meta:                   lpt.meta,
			cluster:                lpt.cluster,
		}
		lpt.AddChildTask(watchDmChannelTask)
		log.Debug("LoadPartitionTask: add a watchDmChannelTask to loadPartitionTask's childTask", zap.Any("task", watchDmChannelTask))
	}

	for nodeID, watched := range watchQueryChannelInfo {
		if !watched {
			queryChannel, queryResultChannel := lpt.meta.GetQueryChannel(collectionID)

			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:             lpt.Base,
				NodeID:           nodeID,
				CollectionID:     collectionID,
				RequestChannelID: queryChannel,
				ResultChannelID:  queryResultChannel,
			}
			watchQueryChannelTask := &WatchQueryChannelTask{
				BaseTask: BaseTask{
					ctx:              lpt.ctx,
					Condition:        NewTaskCondition(lpt.ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				AddQueryChannelRequest: addQueryChannelRequest,
				cluster:                lpt.cluster,
			}
			lpt.AddChildTask(watchQueryChannelTask)
			log.Debug("LoadPartitionTask: add a watchQueryChannelTask to loadPartitionTask's childTask", zap.Any("task", watchQueryChannelTask))
		}
	}

	log.Debug("LoadPartitionTask Execute done",
		zap.Int64("msgID", lpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	return nil
}

func (lpt *LoadPartitionTask) PostExecute() {
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs
	if lpt.result.ErrorCode != commonpb.ErrorCode_Success {
		lpt.childTasks = make([]task, 0)
		if lpt.addCol {
			for nodeID, node := range lpt.cluster.nodes {
				if !node.isOnService() {
					continue
				}
				req := &querypb.ReleaseCollectionRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_ReleaseCollection,
						MsgID:     lpt.Base.MsgID,
						Timestamp: lpt.Base.Timestamp,
						SourceID:  lpt.Base.SourceID,
					},
					DbID:         lpt.DbID,
					CollectionID: lpt.CollectionID,
					NodeID:       nodeID,
				}
				releaseCollectionTask := &ReleaseCollectionTask{
					BaseTask: BaseTask{
						ctx:              lpt.ctx,
						Condition:        NewTaskCondition(lpt.ctx),
						triggerCondition: querypb.TriggerCondition_grpcRequest,
					},
					ReleaseCollectionRequest: req,
					cluster:                  lpt.cluster,
				}
				lpt.AddChildTask(releaseCollectionTask)
				log.Debug("loadPartitionTask: add a releaseCollectionTask to loadPartitionTask's childTask", zap.Any("task", releaseCollectionTask))
			}
		} else {
			for nodeID, node := range lpt.cluster.nodes {
				if !node.isOnService() {
					continue
				}
				req := &querypb.ReleasePartitionsRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_ReleasePartitions,
						MsgID:     lpt.Base.MsgID,
						Timestamp: lpt.Base.Timestamp,
						SourceID:  lpt.Base.SourceID,
					},
					DbID:         lpt.DbID,
					CollectionID: lpt.CollectionID,
					PartitionIDs: partitionIDs,
					NodeID:       nodeID,
				}

				releasePartitionTask := &ReleasePartitionTask{
					BaseTask: BaseTask{
						ctx:              lpt.ctx,
						Condition:        NewTaskCondition(lpt.ctx),
						triggerCondition: querypb.TriggerCondition_grpcRequest,
					},

					ReleasePartitionsRequest: req,
					cluster:                  lpt.cluster,
				}
				lpt.AddChildTask(releasePartitionTask)
				log.Debug("loadPartitionTask: add a releasePartitionTask to loadPartitionTask's childTask", zap.Any("task", releasePartitionTask))
			}
		}
	}
	log.Debug("LoadPartitionTask postExecute done",
		zap.Int64("msgID", lpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
}

type ReleasePartitionTask struct {
	BaseTask
	*querypb.ReleasePartitionsRequest
	cluster *queryNodeCluster
}

func (rpt *ReleasePartitionTask) Marshal() string {
	return proto.MarshalTextString(rpt.ReleasePartitionsRequest)
}

func (rpt *ReleasePartitionTask) Type() commonpb.MsgType {
	return rpt.Base.MsgType
}

func (rpt *ReleasePartitionTask) Timestamp() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *ReleasePartitionTask) PreExecute() {
	collectionID := rpt.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	rpt.result = status
	log.Debug("start do releasePartitionTask",
		zap.Int64("msgID", rpt.ID()),
		zap.Int64("collectionID", collectionID))
}

func (rpt *ReleasePartitionTask) Execute(ctx context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if rpt.NodeID <= 0 {
		for nodeID, node := range rpt.cluster.nodes {
			if !node.isOnService() {
				continue
			}
			req := proto.Clone(rpt.ReleasePartitionsRequest).(*querypb.ReleasePartitionsRequest)
			req.NodeID = nodeID
			releasePartitionTask := &ReleasePartitionTask{
				BaseTask: BaseTask{
					ctx:              rpt.ctx,
					Condition:        NewTaskCondition(rpt.ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				ReleasePartitionsRequest: req,
				cluster:                  rpt.cluster,
			}
			rpt.AddChildTask(releasePartitionTask)
			log.Debug("ReleasePartitionTask: add a releasePartitionTask to releasePartitionTask's childTask", zap.Any("task", releasePartitionTask))
		}
	} else {
		res, err := rpt.cluster.releasePartitions(ctx, rpt.NodeID, rpt.ReleasePartitionsRequest)
		if err != nil {
			log.Error("ReleasePartitionsTask: release partition end, node occur error", zap.String("nodeID", fmt.Sprintln(rpt.NodeID)))
			status.Reason = err.Error()
			rpt.result = status
			return err
		}
		if res.ErrorCode != commonpb.ErrorCode_Success {
			log.Error("ReleasePartitionsTask: release partition end, node occur error", zap.String("nodeID", fmt.Sprintln(rpt.NodeID)))
			err = errors.New("queryNode releasePartition failed")
			status.Reason = err.Error()
			rpt.result = status
			return err
		}
	}

	log.Debug("ReleasePartitionTask Execute done",
		zap.Int64("msgID", rpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("nodeID", rpt.NodeID))
	return nil
}

func (rpt *ReleasePartitionTask) PostExecute() {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs

	log.Debug("ReleasePartitionTask postExecute done",
		zap.Int64("msgID", rpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("nodeID", rpt.NodeID))
}

//****************************internal task*******************************//
type LoadSegmentTask struct {
	BaseTask
	*querypb.LoadSegmentsRequest
	meta    *meta
	cluster *queryNodeCluster
}

func (lst *LoadSegmentTask) Marshal() string {
	return proto.MarshalTextString(lst.LoadSegmentsRequest)
}

func (lst *LoadSegmentTask) IsValid() bool {
	return lst.ctx != nil && lst.cluster.nodes[lst.NodeID].isOnService()
}

func (lst *LoadSegmentTask) Type() commonpb.MsgType {
	return lst.Base.MsgType
}

func (lst *LoadSegmentTask) Timestamp() Timestamp {
	return lst.Base.Timestamp
}

func (lst *LoadSegmentTask) PreExecute() {
	segmentIDs := make([]UniqueID, 0)
	for _, info := range lst.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	log.Debug("start do loadSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("loaded nodeID", lst.NodeID),
		zap.Int64("taskID", lst.ID()))
}

func (lst *LoadSegmentTask) Execute(ctx context.Context) error {
	status, err := lst.cluster.LoadSegments(lst.ctx, lst.NodeID, lst.LoadSegmentsRequest)
	if err != nil {
		lst.result = status
		return err
	}

	lst.result = status
	log.Debug("loadSegmentTask Execute done",
		zap.Int64("taskID", lst.ID()))
	return nil
}
func (lst *LoadSegmentTask) PostExecute() {
	log.Debug("loadSegmentTask postExecute done",
		zap.Int64("taskID", lst.ID()))
}

func (lst *LoadSegmentTask) Reschedule() ([]task, error) {
	segmentIDs := make([]UniqueID, 0)
	collectionID := lst.Infos[0].CollectionID
	reScheduledTask := make([]task, 0)
	for _, info := range lst.Infos {
		segmentID := info.SegmentID
		segmentIDs = append(segmentIDs, segmentID)
	}
	segment2Nodes := shuffleSegmentsToQueryNode(segmentIDs, lst.cluster)
	node2segmentInfos := make(map[int64][]*querypb.SegmentLoadInfo)
	for _, info := range lst.Infos {
		segmentID := info.SegmentID
		nodeID := segment2Nodes[segmentID]
		if _, ok := node2segmentInfos[nodeID]; !ok {
			node2segmentInfos[nodeID] = make([]*querypb.SegmentLoadInfo, 0)
		}
		node2segmentInfos[nodeID] = append(node2segmentInfos[nodeID], info)
	}

	for nodeID, infos := range node2segmentInfos {
		loadSegmentTask := &LoadSegmentTask{
			BaseTask: lst.BaseTask,
			LoadSegmentsRequest: &querypb.LoadSegmentsRequest{
				Base:          lst.Base,
				NodeID:        nodeID,
				Infos:         infos,
				Schema:        lst.Schema,
				LoadCondition: lst.LoadCondition,
			},
			meta:    lst.meta,
			cluster: lst.cluster,
		}
		reScheduledTask = append(reScheduledTask, loadSegmentTask)
		log.Debug("LoadSegmentTask: add a loadSegmentTask to RescheduleTasks", zap.Any("task", loadSegmentTask))

		hasWatchQueryChannel := lst.cluster.hasWatchedQueryChannel(lst.ctx, nodeID, collectionID)
		if !hasWatchQueryChannel {
			queryChannel, queryResultChannel := lst.meta.GetQueryChannel(collectionID)

			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:             lst.Base,
				NodeID:           nodeID,
				CollectionID:     collectionID,
				RequestChannelID: queryChannel,
				ResultChannelID:  queryResultChannel,
			}
			watchQueryChannelTask := &WatchQueryChannelTask{
				BaseTask: BaseTask{
					ctx:              lst.ctx,
					Condition:        NewTaskCondition(lst.ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				AddQueryChannelRequest: addQueryChannelRequest,
				cluster:                lst.cluster,
			}
			reScheduledTask = append(reScheduledTask, watchQueryChannelTask)
			log.Debug("LoadSegmentTask: add a watchQueryChannelTask to RescheduleTasks", zap.Any("task", watchQueryChannelTask))
		}
	}

	return reScheduledTask, nil
}

type ReleaseSegmentTask struct {
	BaseTask
	*querypb.ReleaseSegmentsRequest
	cluster *queryNodeCluster
}

func (rst *ReleaseSegmentTask) Marshal() string {
	return proto.MarshalTextString(rst.ReleaseSegmentsRequest)
}

func (rst *ReleaseSegmentTask) IsValid() bool {
	return rst.ctx != nil && rst.cluster.nodes[rst.NodeID].isOnService()
}

func (rst *ReleaseSegmentTask) Type() commonpb.MsgType {
	return rst.Base.MsgType
}

func (rst *ReleaseSegmentTask) Timestamp() Timestamp {
	return rst.Base.Timestamp
}

func (rst *ReleaseSegmentTask) PreExecute() {
	segmentIDs := rst.SegmentIDs
	log.Debug("start do releaseSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("loaded nodeID", rst.NodeID),
		zap.Int64("taskID", rst.ID()))
}

func (rst *ReleaseSegmentTask) Execute(ctx context.Context) error {
	status, err := rst.cluster.ReleaseSegments(rst.ctx, rst.NodeID, rst.ReleaseSegmentsRequest)
	if err != nil {
		rst.result = status
		return err
	}

	rst.result = status
	log.Debug("releaseSegmentTask Execute done",
		zap.Int64s("segmentIDs", rst.SegmentIDs),
		zap.Int64("taskID", rst.ID()))
	return nil
}

func (rst *ReleaseSegmentTask) PostExecute() {
	segmentIDs := rst.SegmentIDs
	log.Debug("releaseSegmentTask postExecute done",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("taskID", rst.ID()))
}

type WatchDmChannelTask struct {
	BaseTask
	*querypb.WatchDmChannelsRequest
	meta    *meta
	cluster *queryNodeCluster
}

func (wdt *WatchDmChannelTask) Marshal() string {
	return proto.MarshalTextString(wdt.WatchDmChannelsRequest)
}

func (wdt *WatchDmChannelTask) IsValid() bool {
	return wdt.ctx != nil && wdt.cluster.nodes[wdt.NodeID].isOnService()
}

func (wdt *WatchDmChannelTask) Type() commonpb.MsgType {
	return wdt.Base.MsgType
}

func (wdt *WatchDmChannelTask) Timestamp() Timestamp {
	return wdt.Base.Timestamp
}

func (wdt *WatchDmChannelTask) PreExecute() {
	channelInfos := wdt.Infos
	channels := make([]string, 0)
	for _, info := range channelInfos {
		channels = append(channels, info.ChannelName)
	}
	log.Debug("start do watchDmChannelTask",
		zap.Strings("dmChannels", channels),
		zap.Int64("loaded nodeID", wdt.NodeID),
		zap.Int64("taskID", wdt.ID()))
}

func (wdt *WatchDmChannelTask) Execute(ctx context.Context) error {
	status, err := wdt.cluster.WatchDmChannels(wdt.ctx, wdt.NodeID, wdt.WatchDmChannelsRequest)
	if err != nil {
		wdt.result = status
		return err
	}

	wdt.result = status
	log.Debug("watchDmChannelsTask Execute done",
		zap.Int64("taskID", wdt.ID()))
	return nil
}

func (wdt *WatchDmChannelTask) PostExecute() {
	log.Debug("watchDmChannelTask postExecute done",
		zap.Int64("taskID", wdt.ID()))
}

func (wdt *WatchDmChannelTask) Reschedule() ([]task, error) {
	collectionID := wdt.CollectionID
	channelIDs := make([]string, 0)
	reScheduledTask := make([]task, 0)
	for _, info := range wdt.Infos {
		channelID := info.ChannelName
		channelIDs = append(channelIDs, channelID)
	}

	channel2Nodes := shuffleChannelsToQueryNode(channelIDs, wdt.cluster)
	node2channelInfos := make(map[int64][]*datapb.VchannelInfo)
	for index, info := range wdt.Infos {
		nodeID := channel2Nodes[index]
		if _, ok := node2channelInfos[nodeID]; !ok {
			node2channelInfos[nodeID] = make([]*datapb.VchannelInfo, 0)
		}
		node2channelInfos[nodeID] = append(node2channelInfos[nodeID], info)
	}

	for nodeID, infos := range node2channelInfos {
		loadSegmentTask := &WatchDmChannelTask{
			BaseTask: wdt.BaseTask,
			WatchDmChannelsRequest: &querypb.WatchDmChannelsRequest{
				Base:         wdt.Base,
				NodeID:       nodeID,
				CollectionID: wdt.CollectionID,
				PartitionID:  wdt.PartitionID,
				Infos:        infos,
				Schema:       wdt.Schema,
				ExcludeInfos: wdt.ExcludeInfos,
			},
			meta:    wdt.meta,
			cluster: wdt.cluster,
		}
		reScheduledTask = append(reScheduledTask, loadSegmentTask)
		log.Debug("WatchDmChannelTask: add a watchDmChannelTask to RescheduleTasks", zap.Any("task", loadSegmentTask))

		hasWatchQueryChannel := wdt.cluster.hasWatchedQueryChannel(wdt.ctx, nodeID, collectionID)
		if !hasWatchQueryChannel {
			queryChannel, queryResultChannel := wdt.meta.GetQueryChannel(collectionID)

			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:             wdt.Base,
				NodeID:           nodeID,
				CollectionID:     collectionID,
				RequestChannelID: queryChannel,
				ResultChannelID:  queryResultChannel,
			}
			watchQueryChannelTask := &WatchQueryChannelTask{
				BaseTask: BaseTask{
					ctx:              wdt.ctx,
					Condition:        NewTaskCondition(wdt.ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				AddQueryChannelRequest: addQueryChannelRequest,
				cluster:                wdt.cluster,
			}
			reScheduledTask = append(reScheduledTask, watchQueryChannelTask)
			log.Debug("WatchDmChannelTask: add a watchQueryChannelTask to RescheduleTasks", zap.Any("task", watchQueryChannelTask))
		}
	}

	return reScheduledTask, nil
}

type WatchQueryChannelTask struct {
	BaseTask
	*querypb.AddQueryChannelRequest
	cluster *queryNodeCluster
}

func (wqt *WatchQueryChannelTask) Marshal() string {
	return proto.MarshalTextString(wqt.AddQueryChannelRequest)
}

func (wqt *WatchQueryChannelTask) IsValid() bool {
	return wqt.ctx != nil && wqt.cluster.nodes[wqt.NodeID].isOnService()
}

func (wqt *WatchQueryChannelTask) Type() commonpb.MsgType {
	return wqt.Base.MsgType
}

func (wqt *WatchQueryChannelTask) Timestamp() Timestamp {
	return wqt.Base.Timestamp
}

func (wqt *WatchQueryChannelTask) PreExecute() {
	log.Debug("start do WatchQueryChannelTask",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.RequestChannelID),
		zap.String("queryResultChannel", wqt.ResultChannelID),
		zap.Int64("loaded nodeID", wqt.NodeID),
		zap.Int64("taskID", wqt.ID()))
}

func (wqt *WatchQueryChannelTask) Execute(ctx context.Context) error {
	status, err := wqt.cluster.AddQueryChannel(wqt.ctx, wqt.NodeID, wqt.AddQueryChannelRequest)
	if err != nil {
		wqt.result = status
		return err
	}

	wqt.result = status
	log.Debug("watchQueryChannelTask Execute done",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.RequestChannelID),
		zap.String("queryResultChannel", wqt.ResultChannelID),
		zap.Int64("taskID", wqt.ID()))
	return nil
}

func (wqt *WatchQueryChannelTask) PostExecute() {
	log.Debug("WatchQueryChannelTask postExecute done",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.RequestChannelID),
		zap.String("queryResultChannel", wqt.ResultChannelID),
		zap.Int64("taskID", wqt.ID()))
}

//****************************handoff task********************************//
type HandoffTask struct {
}

//*********************** ***load balance task*** ************************//
type LoadBalanceTask struct {
	BaseTask
	*querypb.LoadBalanceRequest
	rootCoord types.RootCoord
	dataCoord types.DataCoord
	cluster   *queryNodeCluster
	meta      *meta
}

func (lbt *LoadBalanceTask) Marshal() string {
	return proto.MarshalTextString(lbt.LoadBalanceRequest)
}

func (lbt *LoadBalanceTask) Type() commonpb.MsgType {
	return lbt.Base.MsgType
}

func (lbt *LoadBalanceTask) Timestamp() Timestamp {
	return lbt.Base.Timestamp
}

func (lbt *LoadBalanceTask) PreExecute() {
	log.Debug("start do LoadBalanceTask",
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.ID()))
}

func (lbt *LoadBalanceTask) Execute(ctx context.Context) error {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if lbt.triggerCondition == querypb.TriggerCondition_nodeDown {
		for _, nodeID := range lbt.SourceNodeIDs {
			lbt.meta.deleteSegmentInfoByNodeID(nodeID)
			collectionInfos := lbt.cluster.nodes[nodeID].collectionInfos
			for collectionID, info := range collectionInfos {
				loadCollection := lbt.meta.collectionInfos[collectionID].LoadCollection
				schema := lbt.meta.collectionInfos[collectionID].Schema
				partitionIDs := info.PartitionIDs

				segmentsToLoad := make([]UniqueID, 0)
				segment2BingLog := make(map[UniqueID]*querypb.SegmentLoadInfo)
				channelsToWatch := make([]string, 0)
				watchRequestsInPartition := make([]*querypb.WatchDmChannelsRequest, 0)
				watchRequestsInCollection := make(map[string]*querypb.WatchDmChannelsRequest)

				dmChannels, err := lbt.meta.getDmChannelsByNodeID(collectionID, nodeID)
				if err != nil {
					status.Reason = err.Error()
					lbt.result = status
					return err
				}

				for _, partitionID := range partitionIDs {
					getRecoveryInfo := &datapb.GetRecoveryInfoRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_LoadBalanceSegments,
						},
						CollectionID: collectionID,
						PartitionID:  partitionID,
					}
					recoveryInfo, err := lbt.dataCoord.GetRecoveryInfo(lbt.ctx, getRecoveryInfo)
					if err != nil {
						status.Reason = err.Error()
						lbt.result = status
						return err
					}

					for _, channelInfo := range recoveryInfo.Channels {
						for _, channel := range dmChannels {
							if channelInfo.ChannelName == channel {
								watchRequest := &querypb.WatchDmChannelsRequest{
									Base:         lbt.Base,
									CollectionID: collectionID,
									Infos:        []*datapb.VchannelInfo{channelInfo},
									Schema:       schema,
								}
								if loadCollection {
									if _, ok := watchRequestsInCollection[channel]; !ok {
										watchRequestsInCollection[channel] = watchRequest
										channelsToWatch = append(channelsToWatch, channel)
									} else {
										oldInfo := watchRequestsInCollection[channel].Infos[0]
										newInfo := mergeVChannelInfo(oldInfo, channelInfo)
										watchRequestsInCollection[channel].Infos = []*datapb.VchannelInfo{newInfo}
									}
								} else {
									watchRequest.PartitionID = partitionID
									channelsToWatch = append(channelsToWatch, channel)
									watchRequestsInPartition = append(watchRequestsInPartition, watchRequest)
								}
								break
							}
						}
					}

					for _, binlog := range recoveryInfo.Binlogs {
						segmentID := binlog.SegmentID
						if lbt.meta.hasSegmentInfo(segmentID) {
							continue
						}
						segmentLoadInfo := &querypb.SegmentLoadInfo{
							SegmentID:    segmentID,
							PartitionID:  partitionID,
							CollectionID: collectionID,
							BinlogPaths:  make([]*datapb.FieldBinlog, 0),
						}
						segmentLoadInfo.BinlogPaths = append(segmentLoadInfo.BinlogPaths, binlog.FieldBinlogs...)
						segmentsToLoad = append(segmentsToLoad, segmentID)
						segment2BingLog[segmentID] = segmentLoadInfo
					}
				}

				segment2Nodes := shuffleSegmentsToQueryNode(segmentsToLoad, lbt.cluster)
				watchRequest2Nodes := shuffleChannelsToQueryNode(channelsToWatch, lbt.cluster)

				watchQueryChannelInfo := make(map[int64]bool)
				node2Segments := make(map[int64][]*querypb.SegmentLoadInfo)
				for segmentID, id := range segment2Nodes {
					if _, ok := node2Segments[id]; !ok {
						node2Segments[id] = make([]*querypb.SegmentLoadInfo, 0)
					}
					node2Segments[id] = append(node2Segments[id], segment2BingLog[segmentID])
					if lbt.cluster.hasWatchedQueryChannel(lbt.ctx, id, collectionID) {
						watchQueryChannelInfo[id] = true
						continue
					}
					watchQueryChannelInfo[id] = false
				}
				for _, id := range watchRequest2Nodes {
					if lbt.cluster.hasWatchedQueryChannel(lbt.ctx, id, collectionID) {
						watchQueryChannelInfo[id] = true
						continue
					}
					watchQueryChannelInfo[id] = false
				}

				for id, segmentInfos := range node2Segments {
					loadSegmentsRequest := &querypb.LoadSegmentsRequest{
						Base:          lbt.Base,
						NodeID:        id,
						Infos:         segmentInfos,
						Schema:        schema,
						LoadCondition: querypb.TriggerCondition_grpcRequest,
					}

					loadSegmentTask := &LoadSegmentTask{
						BaseTask: BaseTask{
							ctx:              lbt.ctx,
							Condition:        NewTaskCondition(lbt.ctx),
							triggerCondition: querypb.TriggerCondition_grpcRequest,
						},

						LoadSegmentsRequest: loadSegmentsRequest,
						meta:                lbt.meta,
						cluster:             lbt.cluster,
					}
					lbt.AddChildTask(loadSegmentTask)
					log.Debug("LoadBalanceTask: add a loadSegmentTask to loadBalanceTask's childTask", zap.Any("task", loadSegmentTask))
				}

				for index, id := range watchRequest2Nodes {
					var watchRequest *querypb.WatchDmChannelsRequest
					if loadCollection {
						channel := channelsToWatch[index]
						watchRequest = watchRequestsInCollection[channel]
					} else {
						watchRequest = watchRequestsInPartition[index]
					}
					watchRequest.NodeID = id
					watchDmChannelTask := &WatchDmChannelTask{
						BaseTask: BaseTask{
							ctx:              lbt.ctx,
							Condition:        NewTaskCondition(lbt.ctx),
							triggerCondition: querypb.TriggerCondition_grpcRequest,
						},
						WatchDmChannelsRequest: watchRequest,
						meta:                   lbt.meta,
						cluster:                lbt.cluster,
					}
					lbt.AddChildTask(watchDmChannelTask)
					log.Debug("LoadBalanceTask: add a watchDmChannelTask to loadBalanceTask's childTask", zap.Any("task", watchDmChannelTask))
				}

				for id, watched := range watchQueryChannelInfo {
					if !watched {
						queryChannel, queryResultChannel := lbt.meta.GetQueryChannel(collectionID)

						addQueryChannelRequest := &querypb.AddQueryChannelRequest{
							Base:             lbt.Base,
							NodeID:           id,
							CollectionID:     collectionID,
							RequestChannelID: queryChannel,
							ResultChannelID:  queryResultChannel,
						}
						watchQueryChannelTask := &WatchQueryChannelTask{
							BaseTask: BaseTask{
								ctx:              lbt.ctx,
								Condition:        NewTaskCondition(lbt.ctx),
								triggerCondition: querypb.TriggerCondition_grpcRequest,
							},

							AddQueryChannelRequest: addQueryChannelRequest,
							cluster:                lbt.cluster,
						}
						lbt.AddChildTask(watchQueryChannelTask)
						log.Debug("LoadBalanceTask: add a watchQueryChannelTask to loadBalanceTask's childTask", zap.Any("task", watchQueryChannelTask))
					}
				}
			}
		}
	}

	//TODO::
	//if lbt.triggerCondition == querypb.TriggerCondition_loadBalance {
	//	return nil
	//}

	log.Debug("LoadBalanceTask Execute done",
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.ID()))
	return nil
}

func (lbt *LoadBalanceTask) PostExecute() {
	for _, id := range lbt.SourceNodeIDs {
		err := lbt.cluster.removeNodeInfo(id)
		if err != nil {
			log.Error("LoadBalanceTask: remove mode info error", zap.Int64("nodeID", id))
		}
	}
	log.Debug("LoadBalanceTask postExecute done",
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.ID()))
}

func shuffleChannelsToQueryNode(dmChannels []string, cluster *queryNodeCluster) []int64 {
	maxNumChannels := 0
	for nodeID, node := range cluster.nodes {
		if !node.onService {
			continue
		}
		numChannels, _ := cluster.getNumDmChannels(nodeID)
		if numChannels > maxNumChannels {
			maxNumChannels = numChannels
		}
	}
	res := make([]int64, 0)
	if len(dmChannels) == 0 {
		return res
	}

	offset := 0
	loopAll := false
	for {
		lastOffset := offset
		if !loopAll {
			for id, node := range cluster.nodes {
				if !node.isOnService() {
					continue
				}
				numSegments, _ := cluster.getNumSegments(id)
				if numSegments >= maxNumChannels {
					continue
				}
				res = append(res, id)
				offset++
				if offset == len(dmChannels) {
					return res
				}
			}
		} else {
			for id, node := range cluster.nodes {
				if !node.isOnService() {
					continue
				}
				res = append(res, id)
				offset++
				if offset == len(dmChannels) {
					return res
				}
			}
		}
		if lastOffset == offset {
			loopAll = true
		}
	}
}

func shuffleSegmentsToQueryNode(segmentIDs []UniqueID, cluster *queryNodeCluster) map[UniqueID]int64 {
	maxNumSegments := 0
	for nodeID, node := range cluster.nodes {
		if !node.isOnService() {
			continue
		}
		numSegments, _ := cluster.getNumSegments(nodeID)
		if numSegments > maxNumSegments {
			maxNumSegments = numSegments
		}
	}
	res := make(map[UniqueID]int64)

	if len(segmentIDs) == 0 {
		return res
	}

	offset := 0
	loopAll := false
	for {
		lastOffset := offset
		if !loopAll {
			for id, node := range cluster.nodes {
				if !node.isOnService() {
					continue
				}
				numSegments, _ := cluster.getNumSegments(id)
				if numSegments >= maxNumSegments {
					continue
				}
				res[segmentIDs[offset]] = id
				offset++
				if offset == len(segmentIDs) {
					return res
				}
			}
		} else {
			for id, node := range cluster.nodes {
				if !node.isOnService() {
					continue
				}
				res[segmentIDs[offset]] = id
				offset++
				if offset == len(segmentIDs) {
					return res
				}
			}
		}
		if lastOffset == offset {
			loopAll = true
		}
	}
}

func mergeVChannelInfo(info1 *datapb.VchannelInfo, info2 *datapb.VchannelInfo) *datapb.VchannelInfo {
	collectionID := info1.CollectionID
	channelName := info1.ChannelName
	var seekPosition *internalpb.MsgPosition
	if info1.SeekPosition == nil || info2.SeekPosition == nil {
		seekPosition = &internalpb.MsgPosition{
			ChannelName: channelName,
		}
	} else {
		seekPosition = info1.SeekPosition
		if info1.SeekPosition.Timestamp > info2.SeekPosition.Timestamp {
			seekPosition = info2.SeekPosition
		}
	}

	checkPoints := make([]*datapb.SegmentInfo, 0)
	checkPoints = append(checkPoints, info1.UnflushedSegments...)
	checkPoints = append(checkPoints, info2.UnflushedSegments...)

	flushedSegments := make([]UniqueID, 0)
	flushedSegments = append(flushedSegments, info1.FlushedSegments...)
	flushedSegments = append(flushedSegments, info2.FlushedSegments...)

	return &datapb.VchannelInfo{
		CollectionID:      collectionID,
		ChannelName:       channelName,
		SeekPosition:      seekPosition,
		UnflushedSegments: checkPoints,
		FlushedSegments:   flushedSegments,
	}
}
