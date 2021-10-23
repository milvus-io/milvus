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
	"sync"
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

const (
	// MaxRetryNum is the maximum number of times that each task can be retried
	MaxRetryNum = 5
	// MaxSendSizeToEtcd is the default limit size of etcd messages that can be sent and received
	MaxSendSizeToEtcd = 2097152
)

type taskState int

const (
	taskUndo    taskState = 0
	taskDoing   taskState = 1
	taskDone    taskState = 3
	taskExpired taskState = 4
	taskFailed  taskState = 5
)

type task interface {
	traceCtx() context.Context
	getTaskID() UniqueID // return ReqId
	setTaskID(id UniqueID)
	msgBase() *commonpb.MsgBase
	msgType() commonpb.MsgType
	timestamp() Timestamp
	getTriggerCondition() querypb.TriggerCondition
	preExecute(ctx context.Context) error
	execute(ctx context.Context) error
	postExecute(ctx context.Context) error
	reschedule(ctx context.Context) ([]task, error)
	rollBack(ctx context.Context) []task
	waitToFinish() error
	notify(err error)
	taskPriority() querypb.TriggerCondition
	setParentTask(t task)
	getParentTask() task
	getChildTask() []task
	addChildTask(t task)
	removeChildTaskByID(taskID UniqueID)
	isValid() bool
	marshal() ([]byte, error)
	getState() taskState
	setState(state taskState)
	isRetryable() bool
	setResultInfo(err error)
	getResultInfo() *commonpb.Status
	updateTaskProcess()
}

type baseTask struct {
	condition
	ctx        context.Context
	cancel     context.CancelFunc
	result     *commonpb.Status
	resultMu   sync.RWMutex
	state      taskState
	stateMu    sync.RWMutex
	retryCount int
	//sync.RWMutex

	taskID           UniqueID
	triggerCondition querypb.TriggerCondition
	parentTask       task
	childTasks       []task
	childTasksMu     sync.RWMutex
}

func newBaseTask(ctx context.Context, triggerType querypb.TriggerCondition) *baseTask {
	childCtx, cancel := context.WithCancel(ctx)
	condition := newTaskCondition(childCtx)

	baseTask := &baseTask{
		ctx:              childCtx,
		cancel:           cancel,
		condition:        condition,
		state:            taskUndo,
		retryCount:       MaxRetryNum,
		triggerCondition: triggerType,
		childTasks:       []task{},
	}

	return baseTask
}

// getTaskID function returns the unique taskID of the trigger task
func (bt *baseTask) getTaskID() UniqueID {
	return bt.taskID
}

// setTaskID function sets the trigger task with a unique id, which is allocated by tso
func (bt *baseTask) setTaskID(id UniqueID) {
	bt.taskID = id
}

func (bt *baseTask) traceCtx() context.Context {
	return bt.ctx
}

func (bt *baseTask) getTriggerCondition() querypb.TriggerCondition {
	return bt.triggerCondition
}

func (bt *baseTask) taskPriority() querypb.TriggerCondition {
	return bt.triggerCondition
}

func (bt *baseTask) setParentTask(t task) {
	bt.parentTask = t
}

func (bt *baseTask) getParentTask() task {
	return bt.parentTask
}

// GetChildTask function returns all the child tasks of the trigger task
// Child task may be loadSegmentTask, watchDmChannelTask or watchQueryChannelTask
func (bt *baseTask) getChildTask() []task {
	bt.childTasksMu.RLock()
	defer bt.childTasksMu.RUnlock()

	return bt.childTasks
}

func (bt *baseTask) addChildTask(t task) {
	bt.childTasksMu.Lock()
	defer bt.childTasksMu.Unlock()

	bt.childTasks = append(bt.childTasks, t)
}

func (bt *baseTask) removeChildTaskByID(taskID UniqueID) {
	bt.childTasksMu.Lock()
	defer bt.childTasksMu.Unlock()

	result := make([]task, 0)
	for _, t := range bt.childTasks {
		if t.getTaskID() != taskID {
			result = append(result, t)
		}
	}
	bt.childTasks = result
}

func (bt *baseTask) isValid() bool {
	return true
}

func (bt *baseTask) reschedule(ctx context.Context) ([]task, error) {
	return nil, nil
}

// State returns the state of task, such as taskUndo, taskDoing, taskDone, taskExpired, taskFailed
func (bt *baseTask) getState() taskState {
	bt.stateMu.RLock()
	defer bt.stateMu.RUnlock()
	return bt.state
}

func (bt *baseTask) setState(state taskState) {
	bt.stateMu.Lock()
	defer bt.stateMu.Unlock()
	bt.state = state
}

func (bt *baseTask) isRetryable() bool {
	return bt.retryCount > 0
}

func (bt *baseTask) setResultInfo(err error) {
	bt.resultMu.Lock()
	defer bt.resultMu.Unlock()

	if bt.result == nil {
		bt.result = &commonpb.Status{}
	}
	if err == nil {
		bt.result.ErrorCode = commonpb.ErrorCode_Success
		bt.result.Reason = ""
		return
	}

	bt.result.ErrorCode = commonpb.ErrorCode_UnexpectedError
	bt.result.Reason = bt.result.Reason + ", " + err.Error()
}

func (bt *baseTask) getResultInfo() *commonpb.Status {
	bt.resultMu.RLock()
	defer bt.resultMu.RUnlock()
	return proto.Clone(bt.result).(*commonpb.Status)
}

func (bt *baseTask) updateTaskProcess() {
	// TODO::
}

func (bt *baseTask) rollBack(ctx context.Context) []task {
	//TODO::
	return nil
}

type loadCollectionTask struct {
	*baseTask
	*querypb.LoadCollectionRequest
	rootCoord types.RootCoord
	dataCoord types.DataCoord
	cluster   Cluster
	meta      Meta
}

func (lct *loadCollectionTask) msgBase() *commonpb.MsgBase {
	return lct.Base
}

func (lct *loadCollectionTask) marshal() ([]byte, error) {
	return proto.Marshal(lct.LoadCollectionRequest)
}

func (lct *loadCollectionTask) msgType() commonpb.MsgType {
	return lct.Base.MsgType
}

func (lct *loadCollectionTask) timestamp() Timestamp {
	return lct.Base.Timestamp
}

func (lct *loadCollectionTask) updateTaskProcess() {
	collectionID := lct.CollectionID
	childTasks := lct.getChildTask()
	allDone := true
	for _, t := range childTasks {
		if t.getState() != taskDone {
			allDone = false
		}
	}
	if allDone {
		err := lct.meta.setLoadPercentage(collectionID, 0, 100, querypb.LoadType_loadCollection)
		if err != nil {
			log.Error("loadCollectionTask: set load percentage to meta's collectionInfo", zap.Int64("collectionID", collectionID))
			lct.setResultInfo(err)
		}
	}
}

func (lct *loadCollectionTask) preExecute(ctx context.Context) error {
	collectionID := lct.CollectionID
	schema := lct.Schema
	lct.setResultInfo(nil)
	log.Debug("start do loadCollectionTask",
		zap.Int64("msgID", lct.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Stringer("schema", schema))
	return nil
}

func (lct *loadCollectionTask) execute(ctx context.Context) error {
	defer func() {
		lct.retryCount--
	}()
	collectionID := lct.CollectionID

	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowPartitions,
		},
		CollectionID: collectionID,
	}
	showPartitionResponse, err := lct.rootCoord.ShowPartitions(ctx, showPartitionRequest)
	if err != nil {
		lct.setResultInfo(err)
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
			lct.setResultInfo(err)
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
				Statslogs:    segmentBingLog.Statslogs,
				Deltalogs:    segmentBingLog.Deltalogs,
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

	err = assignInternalTask(ctx, collectionID, lct, lct.meta, lct.cluster, loadSegmentReqs, watchDmChannelReqs, false)
	if err != nil {
		log.Warn("loadCollectionTask: assign child task failed", zap.Int64("collectionID", collectionID))
		lct.setResultInfo(err)
		return err
	}
	log.Debug("loadCollectionTask: assign child task done", zap.Int64("collectionID", collectionID))

	log.Debug("LoadCollection execute done",
		zap.Int64("msgID", lct.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lct *loadCollectionTask) postExecute(ctx context.Context) error {
	collectionID := lct.CollectionID
	if lct.result.ErrorCode != commonpb.ErrorCode_Success {
		lct.childTasks = []task{}
		err := lct.meta.releaseCollection(collectionID)
		if err != nil {
			log.Error("loadCollectionTask: occur error when release collection info from meta", zap.Error(err))
		}
	}

	log.Debug("loadCollectionTask postExecute done",
		zap.Int64("msgID", lct.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lct *loadCollectionTask) rollBack(ctx context.Context) []task {
	nodes, _ := lct.cluster.onlineNodes()
	resultTasks := make([]task, 0)
	//TODO::call rootCoord.ReleaseDQLMessageStream
	for nodeID := range nodes {
		//brute force rollBack, should optimize
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
		baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
		baseTask.setParentTask(lct)
		releaseCollectionTask := &releaseCollectionTask{
			baseTask:                 baseTask,
			ReleaseCollectionRequest: req,
			cluster:                  lct.cluster,
		}
		resultTasks = append(resultTasks, releaseCollectionTask)
	}
	log.Debug("loadCollectionTask: rollBack loadCollectionTask", zap.Any("loadCollectionTask", lct), zap.Any("rollBack task", resultTasks))
	return resultTasks
}

// releaseCollectionTask will release all the data of this collection on query nodes
type releaseCollectionTask struct {
	*baseTask
	*querypb.ReleaseCollectionRequest
	cluster   Cluster
	meta      Meta
	rootCoord types.RootCoord
}

func (rct *releaseCollectionTask) msgBase() *commonpb.MsgBase {
	return rct.Base
}

func (rct *releaseCollectionTask) marshal() ([]byte, error) {
	return proto.Marshal(rct.ReleaseCollectionRequest)
}

func (rct *releaseCollectionTask) msgType() commonpb.MsgType {
	return rct.Base.MsgType
}

func (rct *releaseCollectionTask) timestamp() Timestamp {
	return rct.Base.Timestamp
}

func (rct *releaseCollectionTask) preExecute(context.Context) error {
	collectionID := rct.CollectionID
	rct.setResultInfo(nil)
	log.Debug("start do releaseCollectionTask",
		zap.Int64("msgID", rct.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (rct *releaseCollectionTask) execute(ctx context.Context) error {
	defer func() {
		rct.retryCount--
	}()
	collectionID := rct.CollectionID

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
		if res.ErrorCode != commonpb.ErrorCode_Success || err != nil {
			log.Warn("releaseCollectionTask: release collection end, releaseDQLMessageStream occur error", zap.Int64("collectionID", rct.CollectionID))
			err = errors.New("rootCoord releaseDQLMessageStream failed")
			rct.setResultInfo(err)
			return err
		}

		nodes, err := rct.cluster.onlineNodes()
		if err != nil {
			log.Debug(err.Error())
		}
		for nodeID := range nodes {
			req := proto.Clone(rct.ReleaseCollectionRequest).(*querypb.ReleaseCollectionRequest)
			req.NodeID = nodeID
			baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
			baseTask.setParentTask(rct)
			releaseCollectionTask := &releaseCollectionTask{
				baseTask:                 baseTask,
				ReleaseCollectionRequest: req,
				cluster:                  rct.cluster,
			}

			rct.addChildTask(releaseCollectionTask)
			log.Debug("releaseCollectionTask: add a releaseCollectionTask to releaseCollectionTask's childTask", zap.Any("task", releaseCollectionTask))
		}
	} else {
		err := rct.cluster.releaseCollection(ctx, rct.NodeID, rct.ReleaseCollectionRequest)
		if err != nil {
			log.Warn("releaseCollectionTask: release collection end, node occur error", zap.Int64("nodeID", rct.NodeID))
			rct.setResultInfo(err)
			return err
		}
	}

	log.Debug("releaseCollectionTask Execute done",
		zap.Int64("msgID", rct.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("nodeID", rct.NodeID))
	return nil
}

func (rct *releaseCollectionTask) postExecute(context.Context) error {
	collectionID := rct.CollectionID
	if rct.result.ErrorCode != commonpb.ErrorCode_Success {
		rct.childTasks = []task{}
	}

	log.Debug("releaseCollectionTask postExecute done",
		zap.Int64("msgID", rct.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("nodeID", rct.NodeID))
	return nil
}

func (rct *releaseCollectionTask) rollBack(ctx context.Context) []task {
	//TODO::
	//if taskID == 0, recovery meta
	//if taskID != 0, recovery collection on queryNode
	return nil
}

// loadPartitionTask will load all the data of this partition to query nodes
type loadPartitionTask struct {
	*baseTask
	*querypb.LoadPartitionsRequest
	dataCoord types.DataCoord
	cluster   Cluster
	meta      Meta
	addCol    bool
}

func (lpt *loadPartitionTask) msgBase() *commonpb.MsgBase {
	return lpt.Base
}

func (lpt *loadPartitionTask) marshal() ([]byte, error) {
	return proto.Marshal(lpt.LoadPartitionsRequest)
}

func (lpt *loadPartitionTask) msgType() commonpb.MsgType {
	return lpt.Base.MsgType
}

func (lpt *loadPartitionTask) timestamp() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *loadPartitionTask) updateTaskProcess() {
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs
	childTasks := lpt.getChildTask()
	allDone := true
	for _, t := range childTasks {
		if t.getState() != taskDone {
			allDone = false
		}
	}
	if allDone {
		for _, id := range partitionIDs {
			err := lpt.meta.setLoadPercentage(collectionID, id, 100, querypb.LoadType_LoadPartition)
			if err != nil {
				log.Error("loadPartitionTask: set load percentage to meta's collectionInfo", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", id))
				lpt.setResultInfo(err)
			}
		}
	}
}

func (lpt *loadPartitionTask) preExecute(context.Context) error {
	collectionID := lpt.CollectionID
	lpt.setResultInfo(nil)
	log.Debug("start do loadPartitionTask",
		zap.Int64("msgID", lpt.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lpt *loadPartitionTask) execute(ctx context.Context) error {
	defer func() {
		lpt.retryCount--
	}()
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs

	if !lpt.meta.hasCollection(collectionID) {
		lpt.meta.addCollection(collectionID, lpt.Schema)
		lpt.addCol = true
	}
	for _, id := range partitionIDs {
		lpt.meta.addPartition(collectionID, id)
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
			lpt.setResultInfo(err)
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
				Statslogs:    segmentBingLog.Statslogs,
				Deltalogs:    segmentBingLog.Deltalogs,
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
			log.Debug("loadPartitionTask: set watchDmChannelsRequests", zap.Any("request", watchDmRequest), zap.Int64("collectionID", collectionID))
		}
	}
	err := assignInternalTask(ctx, collectionID, lpt, lpt.meta, lpt.cluster, loadSegmentReqs, watchDmReqs, false)
	if err != nil {
		log.Warn("loadPartitionTask: assign child task failed", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
		lpt.setResultInfo(err)
		return err
	}
	log.Debug("loadPartitionTask: assign child task done", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))

	log.Debug("loadPartitionTask Execute done",
		zap.Int64("msgID", lpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	return nil
}

func (lpt *loadPartitionTask) postExecute(ctx context.Context) error {
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs
	if lpt.result.ErrorCode != commonpb.ErrorCode_Success {
		lpt.childTasks = []task{}
		if lpt.addCol {
			err := lpt.meta.releaseCollection(collectionID)
			if err != nil {
				log.Error("loadPartitionTask: occur error when release collection info from meta", zap.Error(err))
			}
		} else {
			for _, partitionID := range partitionIDs {
				err := lpt.meta.releasePartition(collectionID, partitionID)
				if err != nil {
					log.Error("loadPartitionTask: occur error when release partition info from meta", zap.Error(err))
				}
			}
		}
	}

	log.Debug("loadPartitionTask postExecute done",
		zap.Int64("msgID", lpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	return nil
}

func (lpt *loadPartitionTask) rollBack(ctx context.Context) []task {
	partitionIDs := lpt.PartitionIDs
	resultTasks := make([]task, 0)
	//brute force rollBack, should optimize
	if lpt.addCol {
		nodes, _ := lpt.cluster.onlineNodes()
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
			baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
			baseTask.setParentTask(lpt)
			releaseCollectionTask := &releaseCollectionTask{
				baseTask:                 baseTask,
				ReleaseCollectionRequest: req,
				cluster:                  lpt.cluster,
			}
			resultTasks = append(resultTasks, releaseCollectionTask)
		}
	} else {
		nodes, _ := lpt.cluster.onlineNodes()
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

			baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
			baseTask.setParentTask(lpt)
			releasePartitionTask := &releasePartitionTask{
				baseTask:                 baseTask,
				ReleasePartitionsRequest: req,
				cluster:                  lpt.cluster,
			}
			resultTasks = append(resultTasks, releasePartitionTask)
		}
	}
	log.Debug("loadPartitionTask: rollBack loadPartitionTask", zap.Any("loadPartitionTask", lpt), zap.Any("rollBack task", resultTasks))
	return resultTasks
}

// releasePartitionTask will release all the data of this partition on query nodes
type releasePartitionTask struct {
	*baseTask
	*querypb.ReleasePartitionsRequest
	cluster Cluster
}

func (rpt *releasePartitionTask) msgBase() *commonpb.MsgBase {
	return rpt.Base
}

func (rpt *releasePartitionTask) marshal() ([]byte, error) {
	return proto.Marshal(rpt.ReleasePartitionsRequest)
}

func (rpt *releasePartitionTask) msgType() commonpb.MsgType {
	return rpt.Base.MsgType
}

func (rpt *releasePartitionTask) timestamp() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *releasePartitionTask) preExecute(context.Context) error {
	collectionID := rpt.CollectionID
	rpt.setResultInfo(nil)
	log.Debug("start do releasePartitionTask",
		zap.Int64("msgID", rpt.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (rpt *releasePartitionTask) execute(ctx context.Context) error {
	defer func() {
		rpt.retryCount--
	}()
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs

	// if nodeID ==0, it means that the release request has not been assigned to the specified query node
	if rpt.NodeID <= 0 {
		nodes, err := rpt.cluster.onlineNodes()
		if err != nil {
			log.Debug(err.Error())
		}
		for nodeID := range nodes {
			req := proto.Clone(rpt.ReleasePartitionsRequest).(*querypb.ReleasePartitionsRequest)
			req.NodeID = nodeID
			baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
			baseTask.setParentTask(rpt)
			releasePartitionTask := &releasePartitionTask{
				baseTask:                 baseTask,
				ReleasePartitionsRequest: req,
				cluster:                  rpt.cluster,
			}
			rpt.addChildTask(releasePartitionTask)
			log.Debug("releasePartitionTask: add a releasePartitionTask to releasePartitionTask's childTask", zap.Any("task", releasePartitionTask))
		}
	} else {
		err := rpt.cluster.releasePartitions(ctx, rpt.NodeID, rpt.ReleasePartitionsRequest)
		if err != nil {
			log.Warn("ReleasePartitionsTask: release partition end, node occur error", zap.String("nodeID", fmt.Sprintln(rpt.NodeID)))
			rpt.setResultInfo(err)
			return err
		}
	}

	log.Debug("releasePartitionTask Execute done",
		zap.Int64("msgID", rpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("nodeID", rpt.NodeID))
	return nil
}

func (rpt *releasePartitionTask) postExecute(context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	if rpt.result.ErrorCode != commonpb.ErrorCode_Success {
		rpt.childTasks = []task{}
	}

	log.Debug("releasePartitionTask postExecute done",
		zap.Int64("msgID", rpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("nodeID", rpt.NodeID))
	return nil
}

func (rpt *releasePartitionTask) rollBack(ctx context.Context) []task {
	//TODO::
	//if taskID == 0, recovery meta
	//if taskID != 0, recovery partition on queryNode
	return nil
}

type loadSegmentTask struct {
	*baseTask
	*querypb.LoadSegmentsRequest
	meta           Meta
	cluster        Cluster
	excludeNodeIDs []int64
}

func (lst *loadSegmentTask) msgBase() *commonpb.MsgBase {
	return lst.Base
}

func (lst *loadSegmentTask) marshal() ([]byte, error) {
	return proto.Marshal(lst.LoadSegmentsRequest)
}

func (lst *loadSegmentTask) isValid() bool {
	online, err := lst.cluster.isOnline(lst.DstNodeID)
	if err != nil {
		return false
	}

	return lst.ctx != nil && online
}

func (lst *loadSegmentTask) msgType() commonpb.MsgType {
	return lst.Base.MsgType
}

func (lst *loadSegmentTask) timestamp() Timestamp {
	return lst.Base.Timestamp
}

func (lst *loadSegmentTask) updateTaskProcess() {
	parentTask := lst.getParentTask()
	if parentTask == nil {
		log.Warn("loadSegmentTask: parentTask should not be nil")
		return
	}
	parentTask.updateTaskProcess()
}

func (lst *loadSegmentTask) preExecute(context.Context) error {
	segmentIDs := make([]UniqueID, 0)
	for _, info := range lst.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	lst.setResultInfo(nil)
	log.Debug("start do loadSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("loaded nodeID", lst.DstNodeID),
		zap.Int64("taskID", lst.getTaskID()))
	return nil
}

func (lst *loadSegmentTask) execute(ctx context.Context) error {
	defer func() {
		lst.retryCount--
	}()

	err := lst.cluster.loadSegments(ctx, lst.DstNodeID, lst.LoadSegmentsRequest)
	if err != nil {
		log.Warn("loadSegmentTask: loadSegment occur error", zap.Int64("taskID", lst.getTaskID()))
		lst.setResultInfo(err)
		return err
	}

	log.Debug("loadSegmentTask Execute done",
		zap.Int64("taskID", lst.getTaskID()))
	return nil
}

func (lst *loadSegmentTask) postExecute(context.Context) error {
	log.Debug("loadSegmentTask postExecute done",
		zap.Int64("taskID", lst.getTaskID()))
	return nil
}

func (lst *loadSegmentTask) reschedule(ctx context.Context) ([]task, error) {
	segmentIDs := make([]UniqueID, 0)
	collectionID := lst.Infos[0].CollectionID
	reScheduledTask := make([]task, 0)
	for _, info := range lst.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	lst.excludeNodeIDs = append(lst.excludeNodeIDs, lst.DstNodeID)
	segment2Nodes, err := shuffleSegmentsToQueryNode(segmentIDs, lst.cluster, false, lst.excludeNodeIDs)
	if err != nil {
		log.Error("loadSegment reschedule failed", zap.Int64s("excludeNodes", lst.excludeNodeIDs), zap.Error(err))
		return nil, err
	}
	node2segmentInfos := make(map[int64][]*querypb.SegmentLoadInfo)
	for index, info := range lst.Infos {
		nodeID := segment2Nodes[index]
		if _, ok := node2segmentInfos[nodeID]; !ok {
			node2segmentInfos[nodeID] = make([]*querypb.SegmentLoadInfo, 0)
		}
		node2segmentInfos[nodeID] = append(node2segmentInfos[nodeID], info)
	}

	for nodeID, infos := range node2segmentInfos {
		loadSegmentBaseTask := newBaseTask(ctx, lst.getTriggerCondition())
		loadSegmentBaseTask.setParentTask(lst.getParentTask())
		loadSegmentTask := &loadSegmentTask{
			baseTask: loadSegmentBaseTask,
			LoadSegmentsRequest: &querypb.LoadSegmentsRequest{
				Base:          lst.Base,
				DstNodeID:     nodeID,
				Infos:         infos,
				Schema:        lst.Schema,
				LoadCondition: lst.LoadCondition,
			},
			meta:           lst.meta,
			cluster:        lst.cluster,
			excludeNodeIDs: lst.excludeNodeIDs,
		}
		reScheduledTask = append(reScheduledTask, loadSegmentTask)
		log.Debug("loadSegmentTask: add a loadSegmentTask to RescheduleTasks", zap.Any("task", loadSegmentTask))

		hasWatchQueryChannel := lst.cluster.hasWatchedQueryChannel(lst.ctx, nodeID, collectionID)
		if !hasWatchQueryChannel {
			queryChannelInfo, err := lst.meta.getQueryChannelInfoByID(collectionID)
			if err != nil {
				return nil, err
			}

			msgBase := proto.Clone(lst.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchQueryChannels
			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:                 msgBase,
				NodeID:               nodeID,
				CollectionID:         collectionID,
				RequestChannelID:     queryChannelInfo.QueryChannelID,
				ResultChannelID:      queryChannelInfo.QueryResultChannelID,
				GlobalSealedSegments: queryChannelInfo.GlobalSealedSegments,
				SeekPosition:         queryChannelInfo.SeekPosition,
			}
			watchQueryChannelBaseTask := newBaseTask(ctx, lst.getTriggerCondition())
			watchQueryChannelBaseTask.setParentTask(lst.getParentTask())
			watchQueryChannelTask := &watchQueryChannelTask{
				baseTask:               watchQueryChannelBaseTask,
				AddQueryChannelRequest: addQueryChannelRequest,
				cluster:                lst.cluster,
			}
			reScheduledTask = append(reScheduledTask, watchQueryChannelTask)
			log.Debug("loadSegmentTask: add a watchQueryChannelTask to RescheduleTasks", zap.Any("task", watchQueryChannelTask))
		}
	}

	return reScheduledTask, nil
}

type releaseSegmentTask struct {
	*baseTask
	*querypb.ReleaseSegmentsRequest
	cluster Cluster
}

func (rst *releaseSegmentTask) msgBase() *commonpb.MsgBase {
	return rst.Base
}

func (rst *releaseSegmentTask) marshal() ([]byte, error) {
	return proto.Marshal(rst.ReleaseSegmentsRequest)
}

func (rst *releaseSegmentTask) isValid() bool {
	online, err := rst.cluster.isOnline(rst.NodeID)
	if err != nil {
		return false
	}
	return rst.ctx != nil && online
}

func (rst *releaseSegmentTask) msgType() commonpb.MsgType {
	return rst.Base.MsgType
}

func (rst *releaseSegmentTask) timestamp() Timestamp {
	return rst.Base.Timestamp
}

func (rst *releaseSegmentTask) preExecute(context.Context) error {
	segmentIDs := rst.SegmentIDs
	rst.setResultInfo(nil)
	log.Debug("start do releaseSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("loaded nodeID", rst.NodeID),
		zap.Int64("taskID", rst.getTaskID()))
	return nil
}

func (rst *releaseSegmentTask) execute(ctx context.Context) error {
	defer func() {
		rst.retryCount--
	}()

	err := rst.cluster.releaseSegments(rst.ctx, rst.NodeID, rst.ReleaseSegmentsRequest)
	if err != nil {
		log.Warn("releaseSegmentTask: releaseSegment occur error", zap.Int64("taskID", rst.getTaskID()))
		rst.setResultInfo(err)
		return err
	}

	log.Debug("releaseSegmentTask Execute done",
		zap.Int64s("segmentIDs", rst.SegmentIDs),
		zap.Int64("taskID", rst.getTaskID()))
	return nil
}

func (rst *releaseSegmentTask) postExecute(context.Context) error {
	segmentIDs := rst.SegmentIDs
	log.Debug("releaseSegmentTask postExecute done",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("taskID", rst.getTaskID()))
	return nil
}

type watchDmChannelTask struct {
	*baseTask
	*querypb.WatchDmChannelsRequest
	meta           Meta
	cluster        Cluster
	excludeNodeIDs []int64
}

func (wdt *watchDmChannelTask) msgBase() *commonpb.MsgBase {
	return wdt.Base
}

func (wdt *watchDmChannelTask) marshal() ([]byte, error) {
	return proto.Marshal(wdt.WatchDmChannelsRequest)
}

func (wdt *watchDmChannelTask) isValid() bool {
	online, err := wdt.cluster.isOnline(wdt.NodeID)
	if err != nil {
		return false
	}
	return wdt.ctx != nil && online
}

func (wdt *watchDmChannelTask) msgType() commonpb.MsgType {
	return wdt.Base.MsgType
}

func (wdt *watchDmChannelTask) timestamp() Timestamp {
	return wdt.Base.Timestamp
}

func (wdt *watchDmChannelTask) updateTaskProcess() {
	parentTask := wdt.getParentTask()
	if parentTask == nil {
		log.Warn("watchDmChannelTask: parentTask should not be nil")
		return
	}
	parentTask.updateTaskProcess()
}

func (wdt *watchDmChannelTask) preExecute(context.Context) error {
	channelInfos := wdt.Infos
	channels := make([]string, 0)
	for _, info := range channelInfos {
		channels = append(channels, info.ChannelName)
	}
	wdt.setResultInfo(nil)
	log.Debug("start do watchDmChannelTask",
		zap.Strings("dmChannels", channels),
		zap.Int64("loaded nodeID", wdt.NodeID),
		zap.Int64("taskID", wdt.getTaskID()))
	return nil
}

func (wdt *watchDmChannelTask) execute(ctx context.Context) error {
	defer func() {
		wdt.retryCount--
	}()

	err := wdt.cluster.watchDmChannels(wdt.ctx, wdt.NodeID, wdt.WatchDmChannelsRequest)
	if err != nil {
		log.Warn("watchDmChannelTask: watchDmChannel occur error", zap.Int64("taskID", wdt.getTaskID()))
		wdt.setResultInfo(err)
		return err
	}

	log.Debug("watchDmChannelsTask Execute done",
		zap.Int64("taskID", wdt.getTaskID()))
	return nil
}

func (wdt *watchDmChannelTask) postExecute(context.Context) error {
	log.Debug("watchDmChannelTask postExecute done",
		zap.Int64("taskID", wdt.getTaskID()))
	return nil
}

func (wdt *watchDmChannelTask) reschedule(ctx context.Context) ([]task, error) {
	collectionID := wdt.CollectionID
	channelIDs := make([]string, 0)
	reScheduledTask := make([]task, 0)
	for _, info := range wdt.Infos {
		channelIDs = append(channelIDs, info.ChannelName)
	}

	wdt.excludeNodeIDs = append(wdt.excludeNodeIDs, wdt.NodeID)
	channel2Nodes, err := shuffleChannelsToQueryNode(channelIDs, wdt.cluster, false, wdt.excludeNodeIDs)
	if err != nil {
		log.Error("watchDmChannel reschedule failed", zap.Int64s("excludeNodes", wdt.excludeNodeIDs), zap.Error(err))
		return nil, err
	}
	node2channelInfos := make(map[int64][]*datapb.VchannelInfo)
	for index, info := range wdt.Infos {
		nodeID := channel2Nodes[index]
		if _, ok := node2channelInfos[nodeID]; !ok {
			node2channelInfos[nodeID] = make([]*datapb.VchannelInfo, 0)
		}
		node2channelInfos[nodeID] = append(node2channelInfos[nodeID], info)
	}

	for nodeID, infos := range node2channelInfos {
		watchDmChannelBaseTask := newBaseTask(ctx, wdt.getTriggerCondition())
		watchDmChannelBaseTask.setParentTask(wdt.getParentTask())
		watchDmChannelTask := &watchDmChannelTask{
			baseTask: watchDmChannelBaseTask,
			WatchDmChannelsRequest: &querypb.WatchDmChannelsRequest{
				Base:         wdt.Base,
				NodeID:       nodeID,
				CollectionID: wdt.CollectionID,
				PartitionID:  wdt.PartitionID,
				Infos:        infos,
				Schema:       wdt.Schema,
				ExcludeInfos: wdt.ExcludeInfos,
			},
			meta:           wdt.meta,
			cluster:        wdt.cluster,
			excludeNodeIDs: wdt.excludeNodeIDs,
		}
		reScheduledTask = append(reScheduledTask, watchDmChannelTask)
		log.Debug("watchDmChannelTask: add a watchDmChannelTask to RescheduleTasks", zap.Any("task", watchDmChannelTask))

		hasWatchQueryChannel := wdt.cluster.hasWatchedQueryChannel(wdt.ctx, nodeID, collectionID)
		if !hasWatchQueryChannel {
			queryChannelInfo, err := wdt.meta.getQueryChannelInfoByID(collectionID)
			if err != nil {
				return nil, err
			}

			msgBase := proto.Clone(wdt.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchQueryChannels
			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:                 msgBase,
				NodeID:               nodeID,
				CollectionID:         collectionID,
				RequestChannelID:     queryChannelInfo.QueryChannelID,
				ResultChannelID:      queryChannelInfo.QueryResultChannelID,
				GlobalSealedSegments: queryChannelInfo.GlobalSealedSegments,
				SeekPosition:         queryChannelInfo.SeekPosition,
			}
			watchQueryChannelBaseTask := newBaseTask(ctx, wdt.getTriggerCondition())
			watchQueryChannelBaseTask.setParentTask(wdt.getParentTask())
			watchQueryChannelTask := &watchQueryChannelTask{
				baseTask:               watchQueryChannelBaseTask,
				AddQueryChannelRequest: addQueryChannelRequest,
				cluster:                wdt.cluster,
			}
			reScheduledTask = append(reScheduledTask, watchQueryChannelTask)
			log.Debug("watchDmChannelTask: add a watchQueryChannelTask to RescheduleTasks", zap.Any("task", watchQueryChannelTask))
		}
	}

	return reScheduledTask, nil
}

type watchQueryChannelTask struct {
	*baseTask
	*querypb.AddQueryChannelRequest
	cluster Cluster
}

func (wqt *watchQueryChannelTask) msgBase() *commonpb.MsgBase {
	return wqt.Base
}

func (wqt *watchQueryChannelTask) marshal() ([]byte, error) {
	return proto.Marshal(wqt.AddQueryChannelRequest)
}

func (wqt *watchQueryChannelTask) isValid() bool {
	online, err := wqt.cluster.isOnline(wqt.NodeID)
	if err != nil {
		return false
	}

	return wqt.ctx != nil && online
}

func (wqt *watchQueryChannelTask) msgType() commonpb.MsgType {
	return wqt.Base.MsgType
}

func (wqt *watchQueryChannelTask) timestamp() Timestamp {
	return wqt.Base.Timestamp
}

func (wqt *watchQueryChannelTask) updateTaskProcess() {
	parentTask := wqt.getParentTask()
	if parentTask == nil {
		log.Warn("watchQueryChannelTask: parentTask should not be nil")
		return
	}
	parentTask.updateTaskProcess()
}

func (wqt *watchQueryChannelTask) preExecute(context.Context) error {
	wqt.setResultInfo(nil)
	log.Debug("start do watchQueryChannelTask",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.RequestChannelID),
		zap.String("queryResultChannel", wqt.ResultChannelID),
		zap.Int64("loaded nodeID", wqt.NodeID),
		zap.Int64("taskID", wqt.getTaskID()))
	return nil
}

func (wqt *watchQueryChannelTask) execute(ctx context.Context) error {
	defer func() {
		wqt.retryCount--
	}()

	err := wqt.cluster.addQueryChannel(wqt.ctx, wqt.NodeID, wqt.AddQueryChannelRequest)
	if err != nil {
		log.Warn("watchQueryChannelTask: watchQueryChannel occur error", zap.Int64("taskID", wqt.getTaskID()))
		wqt.setResultInfo(err)
		return err
	}

	log.Debug("watchQueryChannelTask Execute done",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.RequestChannelID),
		zap.String("queryResultChannel", wqt.ResultChannelID),
		zap.Int64("taskID", wqt.getTaskID()))
	return nil
}

func (wqt *watchQueryChannelTask) postExecute(context.Context) error {
	log.Debug("watchQueryChannelTask postExecute done",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.RequestChannelID),
		zap.String("queryResultChannel", wqt.ResultChannelID),
		zap.Int64("taskID", wqt.getTaskID()))
	return nil
}

type handoffTask struct {
}

type loadBalanceTask struct {
	*baseTask
	*querypb.LoadBalanceRequest
	rootCoord types.RootCoord
	dataCoord types.DataCoord
	cluster   Cluster
	meta      Meta
}

func (lbt *loadBalanceTask) msgBase() *commonpb.MsgBase {
	return lbt.Base
}

func (lbt *loadBalanceTask) marshal() ([]byte, error) {
	return proto.Marshal(lbt.LoadBalanceRequest)
}

func (lbt *loadBalanceTask) msgType() commonpb.MsgType {
	return lbt.Base.MsgType
}

func (lbt *loadBalanceTask) timestamp() Timestamp {
	return lbt.Base.Timestamp
}

func (lbt *loadBalanceTask) preExecute(context.Context) error {
	lbt.setResultInfo(nil)
	log.Debug("start do loadBalanceTask",
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))
	return nil
}

func (lbt *loadBalanceTask) execute(ctx context.Context) error {
	defer func() {
		lbt.retryCount--
	}()

	if lbt.triggerCondition == querypb.TriggerCondition_nodeDown {
		for _, nodeID := range lbt.SourceNodeIDs {
			collectionInfos := lbt.cluster.getCollectionInfosByID(lbt.ctx, nodeID)
			for _, info := range collectionInfos {
				collectionID := info.CollectionID
				metaInfo, err := lbt.meta.getCollectionInfoByID(collectionID)
				if err != nil {
					log.Warn("loadBalanceTask: getCollectionInfoByID occur error", zap.String("error", err.Error()))
					lbt.setResultInfo(err)
					return err
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
					lbt.setResultInfo(err)
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
						lbt.setResultInfo(err)
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
							Statslogs:    segmentBingLog.Statslogs,
							Deltalogs:    segmentBingLog.Deltalogs,
						}

						msgBase := proto.Clone(lbt.Base).(*commonpb.MsgBase)
						msgBase.MsgType = commonpb.MsgType_LoadSegments
						loadSegmentReq := &querypb.LoadSegmentsRequest{
							Base:          msgBase,
							Infos:         []*querypb.SegmentLoadInfo{segmentLoadInfo},
							Schema:        schema,
							LoadCondition: querypb.TriggerCondition_nodeDown,
							SourceNodeID:  nodeID,
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
				err = assignInternalTask(ctx, collectionID, lbt, lbt.meta, lbt.cluster, loadSegmentReqs, watchDmChannelReqs, true)
				if err != nil {
					log.Warn("loadBalanceTask: assign child task failed", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
					lbt.setResultInfo(err)
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

	log.Debug("loadBalanceTask Execute done",
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))
	return nil
}

func (lbt *loadBalanceTask) postExecute(context.Context) error {
	if lbt.result.ErrorCode == commonpb.ErrorCode_Success {
		for _, id := range lbt.SourceNodeIDs {
			err := lbt.cluster.removeNodeInfo(id)
			if err != nil {
				log.Error("loadBalanceTask: occur error when removing node info from cluster", zap.Int64("nodeID", id))
			}
		}
	} else {
		lbt.childTasks = []task{}
	}

	log.Debug("loadBalanceTask postExecute done",
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))
	return nil
}

func shuffleChannelsToQueryNode(dmChannels []string, cluster Cluster, wait bool, excludeNodeIDs []int64) ([]int64, error) {
	maxNumChannels := 0
	nodes := make(map[int64]Node)
	var err error
	for {
		nodes, err = cluster.onlineNodes()
		if err != nil {
			log.Debug(err.Error())
			if !wait {
				return nil, err
			}
			time.Sleep(1 * time.Second)
			continue
		}
		for _, id := range excludeNodeIDs {
			delete(nodes, id)
		}
		if len(nodes) > 0 {
			break
		}
		if !wait {
			return nil, errors.New("no queryNode to allocate")
		}
	}

	for nodeID := range nodes {
		numChannels, _ := cluster.getNumDmChannels(nodeID)
		if numChannels > maxNumChannels {
			maxNumChannels = numChannels
		}
	}
	res := make([]int64, 0)
	if len(dmChannels) == 0 {
		return res, nil
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
					return res, nil
				}
			}
		} else {
			for nodeID := range nodes {
				res = append(res, nodeID)
				offset++
				if offset == len(dmChannels) {
					return res, nil
				}
			}
		}
		if lastOffset == offset {
			loopAll = true
		}
	}
}

// shuffleSegmentsToQueryNode shuffle segments to online nodes
// returned are noded id for each segment, which satisfies:
//     len(returnedNodeIds) == len(segmentIDs) && segmentIDs[i] is assigned to returnedNodeIds[i]
func shuffleSegmentsToQueryNode(segmentIDs []UniqueID, cluster Cluster, wait bool, excludeNodeIDs []int64) ([]int64, error) {
	maxNumSegments := 0
	nodes := make(map[int64]Node)
	var err error
	for {
		nodes, err = cluster.onlineNodes()
		if err != nil {
			log.Debug(err.Error())
			if !wait {
				return nil, err
			}
			time.Sleep(1 * time.Second)
			continue
		}
		for _, id := range excludeNodeIDs {
			delete(nodes, id)
		}
		if len(nodes) > 0 {
			break
		}
		if !wait {
			return nil, errors.New("no queryNode to allocate")
		}
	}
	for nodeID := range nodes {
		numSegments, _ := cluster.getNumSegments(nodeID)
		if numSegments > maxNumSegments {
			maxNumSegments = numSegments
		}
	}
	res := make([]int64, 0)

	if len(segmentIDs) == 0 {
		return res, nil
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
					return res, nil
				}
			}
		} else {
			for nodeID := range nodes {
				res = append(res, nodeID)
				offset++
				if offset == len(segmentIDs) {
					return res, nil
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

	flushedSegments := make([]*datapb.SegmentInfo, 0)
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
	watchDmChannelRequests []*querypb.WatchDmChannelsRequest,
	wait bool) error {
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
	segment2Nodes, err := shuffleSegmentsToQueryNode(segmentsToLoad, cluster, wait, nil)
	if err != nil {
		log.Error("assignInternalTask: segment to node failed", zap.Any("segments map", segment2Nodes), zap.Int64("collectionID", collectionID))
		return err
	}
	log.Debug("assignInternalTask: segment to node", zap.Any("segments map", segment2Nodes), zap.Int64("collectionID", collectionID))
	watchRequest2Nodes, err := shuffleChannelsToQueryNode(channelsToWatch, cluster, wait, nil)
	if err != nil {
		log.Error("assignInternalTask: watch request to node failed", zap.Any("request map", watchRequest2Nodes), zap.Int64("collectionID", collectionID))
		return err
	}
	log.Debug("assignInternalTask: watch request to node", zap.Any("request map", watchRequest2Nodes), zap.Int64("collectionID", collectionID))

	watchQueryChannelInfo := make(map[int64]bool)
	node2Segments := make(map[int64][]*querypb.LoadSegmentsRequest)
	sizeCounts := make(map[int64]int)
	for index, nodeID := range segment2Nodes {
		sizeOfReq := getSizeOfLoadSegmentReq(loadSegmentRequests[index])
		if _, ok := node2Segments[nodeID]; !ok {
			node2Segments[nodeID] = make([]*querypb.LoadSegmentsRequest, 0)
			node2Segments[nodeID] = append(node2Segments[nodeID], loadSegmentRequests[index])
			sizeCounts[nodeID] = sizeOfReq
		} else {
			if sizeCounts[nodeID]+sizeOfReq > 2097152 {
				node2Segments[nodeID] = append(node2Segments[nodeID], loadSegmentRequests[index])
				sizeCounts[nodeID] = sizeOfReq
			} else {
				lastReq := node2Segments[nodeID][len(node2Segments[nodeID])-1]
				lastReq.Infos = append(lastReq.Infos, loadSegmentRequests[index].Infos...)
				sizeCounts[nodeID] += sizeOfReq
			}
		}

		if cluster.hasWatchedQueryChannel(parentTask.traceCtx(), nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}
	for _, nodeID := range watchRequest2Nodes {
		if cluster.hasWatchedQueryChannel(parentTask.traceCtx(), nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}

	for nodeID, loadSegmentsReqs := range node2Segments {
		for _, req := range loadSegmentsReqs {
			ctx = opentracing.ContextWithSpan(context.Background(), sp)
			req.DstNodeID = nodeID
			baseTask := newBaseTask(ctx, parentTask.getTriggerCondition())
			baseTask.setParentTask(parentTask)
			loadSegmentTask := &loadSegmentTask{
				baseTask:            baseTask,
				LoadSegmentsRequest: req,
				meta:                meta,
				cluster:             cluster,
				excludeNodeIDs:      []int64{},
			}
			parentTask.addChildTask(loadSegmentTask)
			log.Debug("assignInternalTask: add a loadSegmentTask childTask", zap.Any("task", loadSegmentTask))
		}
	}

	for index, nodeID := range watchRequest2Nodes {
		ctx = opentracing.ContextWithSpan(context.Background(), sp)
		watchDmChannelReq := watchDmChannelRequests[index]
		watchDmChannelReq.NodeID = nodeID
		baseTask := newBaseTask(ctx, parentTask.getTriggerCondition())
		baseTask.setParentTask(parentTask)
		watchDmChannelTask := &watchDmChannelTask{
			baseTask:               baseTask,
			WatchDmChannelsRequest: watchDmChannelReq,
			meta:                   meta,
			cluster:                cluster,
			excludeNodeIDs:         []int64{},
		}
		parentTask.addChildTask(watchDmChannelTask)
		log.Debug("assignInternalTask: add a watchDmChannelTask childTask", zap.Any("task", watchDmChannelTask))
	}

	for nodeID, watched := range watchQueryChannelInfo {
		if !watched {
			ctx = opentracing.ContextWithSpan(context.Background(), sp)
			queryChannelInfo, err := meta.getQueryChannelInfoByID(collectionID)
			if err != nil {
				return err
			}

			msgBase := proto.Clone(parentTask.msgBase()).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchQueryChannels
			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:                 msgBase,
				NodeID:               nodeID,
				CollectionID:         collectionID,
				RequestChannelID:     queryChannelInfo.QueryChannelID,
				ResultChannelID:      queryChannelInfo.QueryResultChannelID,
				GlobalSealedSegments: queryChannelInfo.GlobalSealedSegments,
				SeekPosition:         queryChannelInfo.SeekPosition,
			}
			baseTask := newBaseTask(ctx, parentTask.getTriggerCondition())
			baseTask.setParentTask(parentTask)
			watchQueryChannelTask := &watchQueryChannelTask{
				baseTask: baseTask,

				AddQueryChannelRequest: addQueryChannelRequest,
				cluster:                cluster,
			}
			parentTask.addChildTask(watchQueryChannelTask)
			log.Debug("assignInternalTask: add a watchQueryChannelTask childTask", zap.Any("task", watchQueryChannelTask))
		}
	}
	return nil
}

func getSizeOfLoadSegmentReq(req *querypb.LoadSegmentsRequest) int {
	return proto.Size(req)
}
