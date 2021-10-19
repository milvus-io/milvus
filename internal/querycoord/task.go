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
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
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
	MsgBase() *commonpb.MsgBase
	Type() commonpb.MsgType
	Timestamp() Timestamp
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
	TaskPriority() querypb.TriggerCondition
	GetParentTask() task
	GetChildTask() []task
	AddChildTask(t task)
	IsValid() bool
	Reschedule() ([]task, error)
	Marshal() ([]byte, error)
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
	cluster   Cluster
	meta      Meta
}

func (lct *LoadCollectionTask) MsgBase() *commonpb.MsgBase {
	return lct.Base
}

func (lct *LoadCollectionTask) Marshal() ([]byte, error) {
	return proto.Marshal(lct.LoadCollectionRequest)
}

func (lct *LoadCollectionTask) Type() commonpb.MsgType {
	return lct.Base.MsgType
}

func (lct *LoadCollectionTask) Timestamp() Timestamp {
	return lct.Base.Timestamp
}

func (lct *LoadCollectionTask) PreExecute(ctx context.Context) error {
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
	return nil
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
	watchPartition := false
	if hasCollection {
		watchPartition = true
		loadType, _ := lct.meta.getLoadType(collectionID)
		if loadType == querypb.LoadType_loadCollection {
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
	lct.meta.setLoadType(collectionID, querypb.LoadType_loadCollection)
	for _, id := range toLoadPartitionIDs {
		lct.meta.addPartition(collectionID, id)
	}

	loadSegmentReqs := make([]*querypb.LoadSegmentsRequest, 0)
	watchDmChannelReqs := make([]*querypb.WatchDmChannelsRequest, 0)
	channelsToWatch := make([]string, 0)
	segmentsToLoad := make([]UniqueID, 0)
	for _, partitionID := range toLoadPartitionIDs {
		getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
			Base:         lct.Base,
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		recoveryInfo, err := lct.dataCoord.GetRecoveryInfo(ctx, getRecoveryInfoRequest)
		if err != nil {
			status.Reason = err.Error()
			lct.result = status
			return err
		}

		for _, segmentBingLog := range recoveryInfo.Binlogs {
			segmentID := segmentBingLog.SegmentID
			segmentLoadInfo := &querypb.SegmentLoadInfo{
				SegmentID:    segmentID,
				PartitionID:  partitionID,
				CollectionID: collectionID,
				BinlogPaths:  segmentBingLog.FieldBinlogs,
				NumOfRows:    segmentBingLog.NumOfRows,
			}

			msgBase := proto.Clone(lct.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_LoadSegments
			loadSegmentReq := &querypb.LoadSegmentsRequest{
				Base:          msgBase,
				Infos:         []*querypb.SegmentLoadInfo{segmentLoadInfo},
				Schema:        lct.Schema,
				LoadCondition: querypb.TriggerCondition_grpcRequest,
			}

			segmentsToLoad = append(segmentsToLoad, segmentID)
			loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
		}

		for _, info := range recoveryInfo.Channels {
			channel := info.ChannelName
			if !watchPartition {
				merged := false
				for index, channelName := range channelsToWatch {
					if channel == channelName {
						merged = true
						oldInfo := watchDmChannelReqs[index].Infos[0]
						newInfo := mergeVChannelInfo(oldInfo, info)
						watchDmChannelReqs[index].Infos = []*datapb.VchannelInfo{newInfo}
						break
					}
				}
				if !merged {
					msgBase := proto.Clone(lct.Base).(*commonpb.MsgBase)
					msgBase.MsgType = commonpb.MsgType_WatchDmChannels
					watchRequest := &querypb.WatchDmChannelsRequest{
						Base:         msgBase,
						CollectionID: collectionID,
						Infos:        []*datapb.VchannelInfo{info},
						Schema:       lct.Schema,
					}
					channelsToWatch = append(channelsToWatch, channel)
					watchDmChannelReqs = append(watchDmChannelReqs, watchRequest)
				}
			} else {
				msgBase := proto.Clone(lct.Base).(*commonpb.MsgBase)
				msgBase.MsgType = commonpb.MsgType_WatchDmChannels
				watchRequest := &querypb.WatchDmChannelsRequest{
					Base:         msgBase,
					CollectionID: collectionID,
					PartitionID:  partitionID,
					Infos:        []*datapb.VchannelInfo{info},
					Schema:       lct.Schema,
				}
				channelsToWatch = append(channelsToWatch, channel)
				watchDmChannelReqs = append(watchDmChannelReqs, watchRequest)
			}
		}
	}

	err = assignInternalTask(ctx, collectionID, lct, lct.meta, lct.cluster, loadSegmentReqs, watchDmChannelReqs)
	if err != nil {
		status.Reason = err.Error()
		lct.result = status
		return err
	}
	log.Debug("loadCollectionTask: assign child task done", zap.Int64("collectionID", collectionID))

	log.Debug("LoadCollection execute done",
		zap.Int64("msgID", lct.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lct *LoadCollectionTask) PostExecute(ctx context.Context) error {
	collectionID := lct.CollectionID
	if lct.State() == taskDone {
		err := lct.meta.setLoadPercentage(collectionID, 0, 100, querypb.LoadType_loadCollection)
		if err != nil {
			log.Debug("loadCollectionTask: set load percentage to meta's collectionInfo", zap.Int64("collectionID", collectionID))
			return err
		}
		return nil
	}
	if lct.result.ErrorCode != commonpb.ErrorCode_Success {
		lct.childTasks = make([]task, 0)
		nodes, err := lct.cluster.onlineNodes()
		if err != nil {
			log.Debug(err.Error())
		}
		for nodeID := range nodes {
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
					ctx:              ctx,
					Condition:        NewTaskCondition(ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},
				ReleaseCollectionRequest: req,
				cluster:                  lct.cluster,
			}
			lct.AddChildTask(releaseCollectionTask)
			log.Debug("loadCollectionTask: add a releaseCollectionTask to loadCollectionTask's childTask", zap.Any("task", releaseCollectionTask))
		}
	}
	lct.meta.addCollection(collectionID, lct.Schema)
	log.Debug("LoadCollectionTask postExecute done",
		zap.Int64("msgID", lct.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

type ReleaseCollectionTask struct {
	BaseTask
	*querypb.ReleaseCollectionRequest
	cluster   Cluster
	meta      Meta
	rootCoord types.RootCoord
}

func (rct *ReleaseCollectionTask) MsgBase() *commonpb.MsgBase {
	return rct.Base
}

func (rct *ReleaseCollectionTask) Marshal() ([]byte, error) {
	return proto.Marshal(rct.ReleaseCollectionRequest)
}

func (rct *ReleaseCollectionTask) Type() commonpb.MsgType {
	return rct.Base.MsgType
}

func (rct *ReleaseCollectionTask) Timestamp() Timestamp {
	return rct.Base.Timestamp
}

func (rct *ReleaseCollectionTask) PreExecute(context.Context) error {
	collectionID := rct.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	rct.result = status
	log.Debug("start do ReleaseCollectionTask",
		zap.Int64("msgID", rct.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (rct *ReleaseCollectionTask) Execute(ctx context.Context) error {
	collectionID := rct.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	// if nodeID ==0, it means that the release request has not been assigned to the specified query node
	if rct.NodeID <= 0 {
		rct.meta.releaseCollection(collectionID)
		releaseDQLMessageStreamReq := &proxypb.ReleaseDQLMessageStreamRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_RemoveQueryChannels,
				MsgID:     rct.Base.MsgID,
				Timestamp: rct.Base.Timestamp,
				SourceID:  rct.Base.SourceID,
			},
			DbID:         rct.DbID,
			CollectionID: rct.CollectionID,
		}
		res, err := rct.rootCoord.ReleaseDQLMessageStream(rct.ctx, releaseDQLMessageStreamReq)
		if err != nil {
			log.Error("ReleaseCollectionTask: release collection end, releaseDQLMessageStream occur error", zap.Int64("collectionID", rct.CollectionID))
			status.Reason = err.Error()
			rct.result = status
			return err
		}
		if res.ErrorCode != commonpb.ErrorCode_Success {
			log.Error("ReleaseCollectionTask: release collection end, releaseDQLMessageStream occur error", zap.Int64("collectionID", rct.CollectionID))
			err = errors.New("rootCoord releaseDQLMessageStream failed")
			status.Reason = err.Error()
			rct.result = status
			return err
		}

		nodes, err := rct.cluster.onlineNodes()
		if err != nil {
			log.Debug(err.Error())
		}
		for nodeID := range nodes {
			req := proto.Clone(rct.ReleaseCollectionRequest).(*querypb.ReleaseCollectionRequest)
			req.NodeID = nodeID
			releaseCollectionTask := &ReleaseCollectionTask{
				BaseTask: BaseTask{
					ctx:              ctx,
					Condition:        NewTaskCondition(ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},
				ReleaseCollectionRequest: req,
				cluster:                  rct.cluster,
			}
			rct.AddChildTask(releaseCollectionTask)
			log.Debug("ReleaseCollectionTask: add a releaseCollectionTask to releaseCollectionTask's childTask", zap.Any("task", releaseCollectionTask))
		}
	} else {
		err := rct.cluster.releaseCollection(ctx, rct.NodeID, rct.ReleaseCollectionRequest)
		if err != nil {
			log.Error("ReleaseCollectionTask: release collection end, node occur error", zap.Int64("nodeID", rct.NodeID))
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

func (rct *ReleaseCollectionTask) PostExecute(context.Context) error {
	collectionID := rct.CollectionID

	log.Debug("ReleaseCollectionTask postExecute done",
		zap.Int64("msgID", rct.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("nodeID", rct.NodeID))
	return nil
}

type LoadPartitionTask struct {
	BaseTask
	*querypb.LoadPartitionsRequest
	dataCoord types.DataCoord
	cluster   Cluster
	meta      Meta
	addCol    bool
}

func (lpt *LoadPartitionTask) MsgBase() *commonpb.MsgBase {
	return lpt.Base
}

func (lpt *LoadPartitionTask) Marshal() ([]byte, error) {
	return proto.Marshal(lpt.LoadPartitionsRequest)
}

func (lpt *LoadPartitionTask) Type() commonpb.MsgType {
	return lpt.Base.MsgType
}

func (lpt *LoadPartitionTask) Timestamp() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *LoadPartitionTask) PreExecute(context.Context) error {
	collectionID := lpt.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	lpt.result = status
	log.Debug("start do LoadPartitionTask",
		zap.Int64("msgID", lpt.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
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
	loadSegmentReqs := make([]*querypb.LoadSegmentsRequest, 0)
	channelsToWatch := make([]string, 0)
	watchDmReqs := make([]*querypb.WatchDmChannelsRequest, 0)
	for _, partitionID := range partitionIDs {
		getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
			Base:         lpt.Base,
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		recoveryInfo, err := lpt.dataCoord.GetRecoveryInfo(ctx, getRecoveryInfoRequest)
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
				BinlogPaths:  segmentBingLog.FieldBinlogs,
				NumOfRows:    segmentBingLog.NumOfRows,
			}

			msgBase := proto.Clone(lpt.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_LoadSegments
			loadSegmentReq := &querypb.LoadSegmentsRequest{
				Base:          msgBase,
				Infos:         []*querypb.SegmentLoadInfo{segmentLoadInfo},
				Schema:        lpt.Schema,
				LoadCondition: querypb.TriggerCondition_grpcRequest,
			}
			segmentsToLoad = append(segmentsToLoad, segmentID)
			loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
		}

		for _, info := range recoveryInfo.Channels {
			channel := info.ChannelName
			msgBase := proto.Clone(lpt.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchDmChannels
			watchDmRequest := &querypb.WatchDmChannelsRequest{
				Base:         msgBase,
				CollectionID: collectionID,
				PartitionID:  partitionID,
				Infos:        []*datapb.VchannelInfo{info},
				Schema:       lpt.Schema,
			}
			channelsToWatch = append(channelsToWatch, channel)
			watchDmReqs = append(watchDmReqs, watchDmRequest)
			log.Debug("LoadPartitionTask: set watchDmChannelsRequests", zap.Any("request", watchDmRequest), zap.Int64("collectionID", collectionID))
		}
	}
	err := assignInternalTask(ctx, collectionID, lpt, lpt.meta, lpt.cluster, loadSegmentReqs, watchDmReqs)
	if err != nil {
		status.Reason = err.Error()
		lpt.result = status
		return err
	}
	log.Debug("LoadPartitionTask: assign child task done", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))

	log.Debug("LoadPartitionTask Execute done",
		zap.Int64("msgID", lpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	return nil
}

func (lpt *LoadPartitionTask) PostExecute(ctx context.Context) error {
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs
	if lpt.State() == taskDone {
		for _, id := range partitionIDs {
			err := lpt.meta.setLoadPercentage(collectionID, id, 100, querypb.LoadType_LoadPartition)
			if err != nil {
				log.Debug("loadPartitionTask: set load percentage to meta's collectionInfo", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", id))
				return err
			}
		}
		return nil
	}
	if lpt.result.ErrorCode != commonpb.ErrorCode_Success {
		lpt.childTasks = make([]task, 0)
		if lpt.addCol {
			nodes, err := lpt.cluster.onlineNodes()
			if err != nil {
				log.Debug(err.Error())
			}
			for nodeID := range nodes {
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
						ctx:              ctx,
						Condition:        NewTaskCondition(ctx),
						triggerCondition: querypb.TriggerCondition_grpcRequest,
					},
					ReleaseCollectionRequest: req,
					cluster:                  lpt.cluster,
				}
				lpt.AddChildTask(releaseCollectionTask)
				log.Debug("loadPartitionTask: add a releaseCollectionTask to loadPartitionTask's childTask", zap.Any("task", releaseCollectionTask))
			}
		} else {
			nodes, err := lpt.cluster.onlineNodes()
			if err != nil {
				log.Debug(err.Error())
			}
			for nodeID := range nodes {
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
						ctx:              ctx,
						Condition:        NewTaskCondition(ctx),
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
	return nil
}

type ReleasePartitionTask struct {
	BaseTask
	*querypb.ReleasePartitionsRequest
	cluster Cluster
}

func (rpt *ReleasePartitionTask) MsgBase() *commonpb.MsgBase {
	return rpt.Base
}

func (rpt *ReleasePartitionTask) Marshal() ([]byte, error) {
	return proto.Marshal(rpt.ReleasePartitionsRequest)
}

func (rpt *ReleasePartitionTask) Type() commonpb.MsgType {
	return rpt.Base.MsgType
}

func (rpt *ReleasePartitionTask) Timestamp() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *ReleasePartitionTask) PreExecute(context.Context) error {
	collectionID := rpt.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	rpt.result = status
	log.Debug("start do releasePartitionTask",
		zap.Int64("msgID", rpt.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (rpt *ReleasePartitionTask) Execute(ctx context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	// if nodeID ==0, it means that the release request has not been assigned to the specified query node
	if rpt.NodeID <= 0 {
		nodes, err := rpt.cluster.onlineNodes()
		if err != nil {
			log.Debug(err.Error())
		}
		for nodeID := range nodes {
			req := proto.Clone(rpt.ReleasePartitionsRequest).(*querypb.ReleasePartitionsRequest)
			req.NodeID = nodeID
			releasePartitionTask := &ReleasePartitionTask{
				BaseTask: BaseTask{
					ctx:              ctx,
					Condition:        NewTaskCondition(ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				ReleasePartitionsRequest: req,
				cluster:                  rpt.cluster,
			}
			rpt.AddChildTask(releasePartitionTask)
			log.Debug("ReleasePartitionTask: add a releasePartitionTask to releasePartitionTask's childTask", zap.Any("task", releasePartitionTask))
		}
	} else {
		err := rpt.cluster.releasePartitions(ctx, rpt.NodeID, rpt.ReleasePartitionsRequest)
		if err != nil {
			log.Error("ReleasePartitionsTask: release partition end, node occur error", zap.String("nodeID", fmt.Sprintln(rpt.NodeID)))
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

func (rpt *ReleasePartitionTask) PostExecute(context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs

	log.Debug("ReleasePartitionTask postExecute done",
		zap.Int64("msgID", rpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("nodeID", rpt.NodeID))
	return nil
}

//****************************internal task*******************************//
type LoadSegmentTask struct {
	BaseTask
	*querypb.LoadSegmentsRequest
	meta    Meta
	cluster Cluster
}

func (lst *LoadSegmentTask) MsgBase() *commonpb.MsgBase {
	return lst.Base
}

func (lst *LoadSegmentTask) Marshal() ([]byte, error) {
	return proto.Marshal(lst.LoadSegmentsRequest)
}

func (lst *LoadSegmentTask) IsValid() bool {
	onService, err := lst.cluster.isOnline(lst.NodeID)
	if err != nil {
		return false
	}

	return lst.ctx != nil && onService
}

func (lst *LoadSegmentTask) Type() commonpb.MsgType {
	return lst.Base.MsgType
}

func (lst *LoadSegmentTask) Timestamp() Timestamp {
	return lst.Base.Timestamp
}

func (lst *LoadSegmentTask) PreExecute(context.Context) error {
	segmentIDs := make([]UniqueID, 0)
	for _, info := range lst.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	lst.result = status
	log.Debug("start do loadSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("loaded nodeID", lst.NodeID),
		zap.Int64("taskID", lst.ID()))
	return nil
}

func (lst *LoadSegmentTask) Execute(ctx context.Context) error {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	err := lst.cluster.loadSegments(ctx, lst.NodeID, lst.LoadSegmentsRequest)
	if err != nil {
		log.Error("LoadSegmentTask: loadSegment occur error", zap.Int64("taskID", lst.ID()))
		status.Reason = err.Error()
		lst.result = status
		return err
	}

	log.Debug("loadSegmentTask Execute done",
		zap.Int64("taskID", lst.ID()))
	return nil
}
func (lst *LoadSegmentTask) PostExecute(context.Context) error {
	log.Debug("loadSegmentTask postExecute done",
		zap.Int64("taskID", lst.ID()))
	return nil
}

func (lst *LoadSegmentTask) Reschedule() ([]task, error) {
	segmentIDs := make([]UniqueID, 0)
	collectionID := lst.Infos[0].CollectionID
	reScheduledTask := make([]task, 0)
	for _, info := range lst.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	segment2Nodes := shuffleSegmentsToQueryNode(segmentIDs, lst.cluster)
	node2segmentInfos := make(map[int64][]*querypb.SegmentLoadInfo)
	for index, info := range lst.Infos {
		nodeID := segment2Nodes[index]
		if _, ok := node2segmentInfos[nodeID]; !ok {
			node2segmentInfos[nodeID] = make([]*querypb.SegmentLoadInfo, 0)
		}
		node2segmentInfos[nodeID] = append(node2segmentInfos[nodeID], info)
	}

	for nodeID, infos := range node2segmentInfos {
		loadSegmentTask := &LoadSegmentTask{
			BaseTask: BaseTask{
				ctx:              lst.ctx,
				Condition:        NewTaskCondition(lst.ctx),
				triggerCondition: lst.LoadCondition,
			},
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
			queryChannel, queryResultChannel, err := lst.meta.GetQueryChannel(collectionID)
			if err != nil {
				return nil, err
			}

			msgBase := proto.Clone(lst.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchQueryChannels
			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:             msgBase,
				NodeID:           nodeID,
				CollectionID:     collectionID,
				RequestChannelID: queryChannel,
				ResultChannelID:  queryResultChannel,
			}
			watchQueryChannelTask := &WatchQueryChannelTask{
				BaseTask: BaseTask{
					ctx:              lst.ctx,
					Condition:        NewTaskCondition(lst.ctx),
					triggerCondition: lst.LoadCondition,
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
	cluster Cluster
}

func (rst *ReleaseSegmentTask) MsgBase() *commonpb.MsgBase {
	return rst.Base
}

func (rst *ReleaseSegmentTask) Marshal() ([]byte, error) {
	return proto.Marshal(rst.ReleaseSegmentsRequest)
}

func (rst *ReleaseSegmentTask) IsValid() bool {
	onService, err := rst.cluster.isOnline(rst.NodeID)
	if err != nil {
		return false
	}
	return rst.ctx != nil && onService
}

func (rst *ReleaseSegmentTask) Type() commonpb.MsgType {
	return rst.Base.MsgType
}

func (rst *ReleaseSegmentTask) Timestamp() Timestamp {
	return rst.Base.Timestamp
}

func (rst *ReleaseSegmentTask) PreExecute(context.Context) error {
	segmentIDs := rst.SegmentIDs
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	rst.result = status
	log.Debug("start do releaseSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("loaded nodeID", rst.NodeID),
		zap.Int64("taskID", rst.ID()))
	return nil
}

func (rst *ReleaseSegmentTask) Execute(ctx context.Context) error {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	err := rst.cluster.releaseSegments(rst.ctx, rst.NodeID, rst.ReleaseSegmentsRequest)
	if err != nil {
		log.Error("ReleaseSegmentTask: releaseSegment occur error", zap.Int64("taskID", rst.ID()))
		status.Reason = err.Error()
		rst.result = status
		return err
	}

	log.Debug("releaseSegmentTask Execute done",
		zap.Int64s("segmentIDs", rst.SegmentIDs),
		zap.Int64("taskID", rst.ID()))
	return nil
}

func (rst *ReleaseSegmentTask) PostExecute(context.Context) error {
	segmentIDs := rst.SegmentIDs
	log.Debug("releaseSegmentTask postExecute done",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("taskID", rst.ID()))
	return nil
}

type WatchDmChannelTask struct {
	BaseTask
	*querypb.WatchDmChannelsRequest
	meta    Meta
	cluster Cluster
}

func (wdt *WatchDmChannelTask) MsgBase() *commonpb.MsgBase {
	return wdt.Base
}

func (wdt *WatchDmChannelTask) Marshal() ([]byte, error) {
	return proto.Marshal(wdt.WatchDmChannelsRequest)
}

func (wdt *WatchDmChannelTask) IsValid() bool {
	onService, err := wdt.cluster.isOnline(wdt.NodeID)
	if err != nil {
		return false
	}
	return wdt.ctx != nil && onService
}

func (wdt *WatchDmChannelTask) Type() commonpb.MsgType {
	return wdt.Base.MsgType
}

func (wdt *WatchDmChannelTask) Timestamp() Timestamp {
	return wdt.Base.Timestamp
}

func (wdt *WatchDmChannelTask) PreExecute(context.Context) error {
	channelInfos := wdt.Infos
	channels := make([]string, 0)
	for _, info := range channelInfos {
		channels = append(channels, info.ChannelName)
	}
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	wdt.result = status
	log.Debug("start do watchDmChannelTask",
		zap.Strings("dmChannels", channels),
		zap.Int64("loaded nodeID", wdt.NodeID),
		zap.Int64("taskID", wdt.ID()))
	return nil
}

func (wdt *WatchDmChannelTask) Execute(ctx context.Context) error {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	err := wdt.cluster.watchDmChannels(wdt.ctx, wdt.NodeID, wdt.WatchDmChannelsRequest)
	if err != nil {
		log.Error("WatchDmChannelTask: watchDmChannel occur error", zap.Int64("taskID", wdt.ID()))
		status.Reason = err.Error()
		wdt.result = status
		return err
	}

	log.Debug("watchDmChannelsTask Execute done",
		zap.Int64("taskID", wdt.ID()))
	return nil
}

func (wdt *WatchDmChannelTask) PostExecute(context.Context) error {
	log.Debug("watchDmChannelTask postExecute done",
		zap.Int64("taskID", wdt.ID()))
	return nil
}

func (wdt *WatchDmChannelTask) Reschedule() ([]task, error) {
	collectionID := wdt.CollectionID
	channelIDs := make([]string, 0)
	reScheduledTask := make([]task, 0)
	for _, info := range wdt.Infos {
		channelIDs = append(channelIDs, info.ChannelName)
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
			BaseTask: BaseTask{
				ctx:              wdt.ctx,
				Condition:        NewTaskCondition(wdt.ctx),
				triggerCondition: wdt.triggerCondition,
			},
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
			queryChannel, queryResultChannel, err := wdt.meta.GetQueryChannel(collectionID)
			if err != nil {
				return nil, err
			}

			msgBase := proto.Clone(wdt.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchQueryChannels
			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:             msgBase,
				NodeID:           nodeID,
				CollectionID:     collectionID,
				RequestChannelID: queryChannel,
				ResultChannelID:  queryResultChannel,
			}
			watchQueryChannelTask := &WatchQueryChannelTask{
				BaseTask: BaseTask{
					ctx:              wdt.ctx,
					Condition:        NewTaskCondition(wdt.ctx),
					triggerCondition: wdt.triggerCondition,
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
	cluster Cluster
}

func (wqt *WatchQueryChannelTask) MsgBase() *commonpb.MsgBase {
	return wqt.Base
}

func (wqt *WatchQueryChannelTask) Marshal() ([]byte, error) {
	return proto.Marshal(wqt.AddQueryChannelRequest)
}

func (wqt *WatchQueryChannelTask) IsValid() bool {
	onService, err := wqt.cluster.isOnline(wqt.NodeID)
	if err != nil {
		return false
	}

	return wqt.ctx != nil && onService
}

func (wqt *WatchQueryChannelTask) Type() commonpb.MsgType {
	return wqt.Base.MsgType
}

func (wqt *WatchQueryChannelTask) Timestamp() Timestamp {
	return wqt.Base.Timestamp
}

func (wqt *WatchQueryChannelTask) PreExecute(context.Context) error {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	wqt.result = status
	log.Debug("start do WatchQueryChannelTask",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.RequestChannelID),
		zap.String("queryResultChannel", wqt.ResultChannelID),
		zap.Int64("loaded nodeID", wqt.NodeID),
		zap.Int64("taskID", wqt.ID()))
	return nil
}

func (wqt *WatchQueryChannelTask) Execute(ctx context.Context) error {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	err := wqt.cluster.addQueryChannel(wqt.ctx, wqt.NodeID, wqt.AddQueryChannelRequest)
	if err != nil {
		log.Error("WatchQueryChannelTask: watchQueryChannel occur error", zap.Int64("taskID", wqt.ID()))
		status.Reason = err.Error()
		wqt.result = status
		return err
	}

	log.Debug("watchQueryChannelTask Execute done",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.RequestChannelID),
		zap.String("queryResultChannel", wqt.ResultChannelID),
		zap.Int64("taskID", wqt.ID()))
	return nil
}

func (wqt *WatchQueryChannelTask) PostExecute(context.Context) error {
	log.Debug("WatchQueryChannelTask postExecute done",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.RequestChannelID),
		zap.String("queryResultChannel", wqt.ResultChannelID),
		zap.Int64("taskID", wqt.ID()))
	return nil
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
	cluster   Cluster
	meta      Meta
}

func (lbt *LoadBalanceTask) MsgBase() *commonpb.MsgBase {
	return lbt.Base
}

func (lbt *LoadBalanceTask) Marshal() ([]byte, error) {
	return proto.Marshal(lbt.LoadBalanceRequest)
}

func (lbt *LoadBalanceTask) Type() commonpb.MsgType {
	return lbt.Base.MsgType
}

func (lbt *LoadBalanceTask) Timestamp() Timestamp {
	return lbt.Base.Timestamp
}

func (lbt *LoadBalanceTask) PreExecute(context.Context) error {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	lbt.result = status
	log.Debug("start do LoadBalanceTask",
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.ID()))
	return nil
}

func (lbt *LoadBalanceTask) Execute(ctx context.Context) error {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if lbt.triggerCondition == querypb.TriggerCondition_nodeDown {
		for _, nodeID := range lbt.SourceNodeIDs {
			lbt.meta.deleteSegmentInfoByNodeID(nodeID)
			collectionInfos := lbt.cluster.getCollectionInfosByID(lbt.ctx, nodeID)
			for _, info := range collectionInfos {
				collectionID := info.CollectionID
				metaInfo, err := lbt.meta.getCollectionInfoByID(collectionID)
				if err != nil {
					log.Error("LoadBalanceTask: getCollectionInfoByID occur error", zap.String("error", err.Error()))
					continue
				}
				loadType := metaInfo.LoadType
				schema := metaInfo.Schema
				partitionIDs := info.PartitionIDs

				segmentsToLoad := make([]UniqueID, 0)
				loadSegmentReqs := make([]*querypb.LoadSegmentsRequest, 0)
				channelsToWatch := make([]string, 0)
				watchDmChannelReqs := make([]*querypb.WatchDmChannelsRequest, 0)

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
					recoveryInfo, err := lbt.dataCoord.GetRecoveryInfo(ctx, getRecoveryInfo)
					if err != nil {
						status.Reason = err.Error()
						lbt.result = status
						return err
					}

					for _, segmentBingLog := range recoveryInfo.Binlogs {
						segmentID := segmentBingLog.SegmentID
						segmentLoadInfo := &querypb.SegmentLoadInfo{
							SegmentID:    segmentID,
							PartitionID:  partitionID,
							CollectionID: collectionID,
							BinlogPaths:  segmentBingLog.FieldBinlogs,
							NumOfRows:    segmentBingLog.NumOfRows,
						}

						msgBase := proto.Clone(lbt.Base).(*commonpb.MsgBase)
						msgBase.MsgType = commonpb.MsgType_LoadSegments
						loadSegmentReq := &querypb.LoadSegmentsRequest{
							Base:          msgBase,
							Infos:         []*querypb.SegmentLoadInfo{segmentLoadInfo},
							Schema:        schema,
							LoadCondition: querypb.TriggerCondition_nodeDown,
						}

						segmentsToLoad = append(segmentsToLoad, segmentID)
						loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
					}

					for _, channelInfo := range recoveryInfo.Channels {
						for _, channel := range dmChannels {
							if channelInfo.ChannelName == channel {
								if loadType == querypb.LoadType_loadCollection {
									merged := false
									for index, channelName := range channelsToWatch {
										if channel == channelName {
											merged = true
											oldInfo := watchDmChannelReqs[index].Infos[0]
											newInfo := mergeVChannelInfo(oldInfo, channelInfo)
											watchDmChannelReqs[index].Infos = []*datapb.VchannelInfo{newInfo}
											break
										}
									}
									if !merged {
										msgBase := proto.Clone(lbt.Base).(*commonpb.MsgBase)
										msgBase.MsgType = commonpb.MsgType_WatchDmChannels
										watchRequest := &querypb.WatchDmChannelsRequest{
											Base:         msgBase,
											CollectionID: collectionID,
											Infos:        []*datapb.VchannelInfo{channelInfo},
											Schema:       schema,
										}
										channelsToWatch = append(channelsToWatch, channel)
										watchDmChannelReqs = append(watchDmChannelReqs, watchRequest)
									}
								} else {
									msgBase := proto.Clone(lbt.Base).(*commonpb.MsgBase)
									msgBase.MsgType = commonpb.MsgType_WatchDmChannels
									watchRequest := &querypb.WatchDmChannelsRequest{
										Base:         msgBase,
										CollectionID: collectionID,
										PartitionID:  partitionID,
										Infos:        []*datapb.VchannelInfo{channelInfo},
										Schema:       schema,
									}
									channelsToWatch = append(channelsToWatch, channel)
									watchDmChannelReqs = append(watchDmChannelReqs, watchRequest)
								}
								break
							}
						}
					}
				}
				err = assignInternalTask(ctx, collectionID, lbt, lbt.meta, lbt.cluster, loadSegmentReqs, watchDmChannelReqs)
				if err != nil {
					status.Reason = err.Error()
					lbt.result = status
					return err
				}
				log.Debug("loadBalanceTask: assign child task done", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
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

func (lbt *LoadBalanceTask) PostExecute(context.Context) error {
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
	return nil
}

func shuffleChannelsToQueryNode(dmChannels []string, cluster Cluster) []int64 {
	maxNumChannels := 0
	nodes := make(map[int64]Node)
	var err error
	for {
		nodes, err = cluster.onlineNodes()
		if err != nil {
			log.Debug(err.Error())
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	for nodeID := range nodes {
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
			for nodeID := range nodes {
				numSegments, _ := cluster.getNumSegments(nodeID)
				if numSegments >= maxNumChannels {
					continue
				}
				res = append(res, nodeID)
				offset++
				if offset == len(dmChannels) {
					return res
				}
			}
		} else {
			for nodeID := range nodes {
				res = append(res, nodeID)
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

func shuffleSegmentsToQueryNode(segmentIDs []UniqueID, cluster Cluster) []int64 {
	maxNumSegments := 0
	nodes := make(map[int64]Node)
	var err error
	for {
		nodes, err = cluster.onlineNodes()
		if err != nil {
			log.Debug(err.Error())
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	for nodeID := range nodes {
		numSegments, _ := cluster.getNumSegments(nodeID)
		if numSegments > maxNumSegments {
			maxNumSegments = numSegments
		}
	}
	res := make([]int64, 0)

	if len(segmentIDs) == 0 {
		return res
	}

	offset := 0
	loopAll := false
	for {
		lastOffset := offset
		if !loopAll {
			for nodeID := range nodes {
				numSegments, _ := cluster.getNumSegments(nodeID)
				if numSegments >= maxNumSegments {
					continue
				}
				res = append(res, nodeID)
				offset++
				if offset == len(segmentIDs) {
					return res
				}
			}
		} else {
			for nodeID := range nodes {
				res = append(res, nodeID)
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
func assignInternalTask(ctx context.Context,
	collectionID UniqueID,
	parentTask task,
	meta Meta,
	cluster Cluster,
	loadSegmentRequests []*querypb.LoadSegmentsRequest,
	watchDmChannelRequests []*querypb.WatchDmChannelsRequest) error {

	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	segmentsToLoad := make([]UniqueID, 0)
	for _, req := range loadSegmentRequests {
		segmentsToLoad = append(segmentsToLoad, req.Infos[0].SegmentID)
	}
	channelsToWatch := make([]string, 0)
	for _, req := range watchDmChannelRequests {
		channelsToWatch = append(channelsToWatch, req.Infos[0].ChannelName)
	}
	segment2Nodes := shuffleSegmentsToQueryNode(segmentsToLoad, cluster)
	watchRequest2Nodes := shuffleChannelsToQueryNode(channelsToWatch, cluster)
	log.Debug("assignInternalTask: segment to node", zap.Any("segments map", segment2Nodes), zap.Int64("collectionID", collectionID))
	log.Debug("assignInternalTask: watch request to node", zap.Any("request map", watchRequest2Nodes), zap.Int64("collectionID", collectionID))

	watchQueryChannelInfo := make(map[int64]bool)
	node2Segments := make(map[int64]*querypb.LoadSegmentsRequest)
	for index, nodeID := range segment2Nodes {
		if _, ok := node2Segments[nodeID]; !ok {
			node2Segments[nodeID] = loadSegmentRequests[index]
		} else {
			node2Segments[nodeID].Infos = append(node2Segments[nodeID].Infos, loadSegmentRequests[index].Infos...)
		}
		if cluster.hasWatchedQueryChannel(parentTask.TraceCtx(), nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}
	for _, nodeID := range watchRequest2Nodes {
		if cluster.hasWatchedQueryChannel(parentTask.TraceCtx(), nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}

	for nodeID, loadSegmentsReq := range node2Segments {
		ctx = opentracing.ContextWithSpan(context.Background(), sp)
		loadSegmentsReq.NodeID = nodeID
		loadSegmentTask := &LoadSegmentTask{
			BaseTask: BaseTask{
				ctx:              ctx,
				Condition:        NewTaskCondition(ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},

			LoadSegmentsRequest: loadSegmentsReq,
			meta:                meta,
			cluster:             cluster,
		}
		parentTask.AddChildTask(loadSegmentTask)
		log.Debug("assignInternalTask: add a loadSegmentTask childTask", zap.Any("task", loadSegmentTask))
	}

	for index, nodeID := range watchRequest2Nodes {
		ctx = opentracing.ContextWithSpan(context.Background(), sp)
		watchDmChannelReq := watchDmChannelRequests[index]
		watchDmChannelReq.NodeID = nodeID
		watchDmChannelTask := &WatchDmChannelTask{
			BaseTask: BaseTask{
				ctx:              ctx,
				Condition:        NewTaskCondition(ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			WatchDmChannelsRequest: watchDmChannelReq,
			meta:                   meta,
			cluster:                cluster,
		}
		parentTask.AddChildTask(watchDmChannelTask)
		log.Debug("assignInternalTask: add a watchDmChannelTask childTask", zap.Any("task", watchDmChannelTask))
	}

	for nodeID, watched := range watchQueryChannelInfo {
		if !watched {
			ctx = opentracing.ContextWithSpan(context.Background(), sp)
			queryChannel, queryResultChannel, err := meta.GetQueryChannel(collectionID)
			if err != nil {
				return err
			}

			msgBase := proto.Clone(parentTask.MsgBase()).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchQueryChannels
			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:             msgBase,
				NodeID:           nodeID,
				CollectionID:     collectionID,
				RequestChannelID: queryChannel,
				ResultChannelID:  queryResultChannel,
			}
			watchQueryChannelTask := &WatchQueryChannelTask{
				BaseTask: BaseTask{
					ctx:              ctx,
					Condition:        NewTaskCondition(ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				AddQueryChannelRequest: addQueryChannelRequest,
				cluster:                cluster,
			}
			parentTask.AddChildTask(watchQueryChannelTask)
			log.Debug("assignInternalTask: add a watchQueryChannelTask childTask", zap.Any("task", watchQueryChannelTask))
		}
	}

	return nil
}
