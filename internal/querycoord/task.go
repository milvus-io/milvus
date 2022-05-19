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
	"sort"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const timeoutForRPC = 10 * time.Second

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
	// MaxSendSizeToEtcd = 2097152
	// Limit size of every loadSegmentReq to 200k
	MaxSendSizeToEtcd = 200000
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
	setTriggerCondition(trigger querypb.TriggerCondition)
	preExecute(ctx context.Context) error
	execute(ctx context.Context) error
	postExecute(ctx context.Context) error
	globalPostExecute(ctx context.Context) error // execute after all child task completed
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
	elapseSpan() time.Duration
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
	retryMu    sync.RWMutex
	//sync.RWMutex

	taskID           UniqueID
	triggerCondition querypb.TriggerCondition
	triggerMu        sync.RWMutex
	parentTask       task
	childTasks       []task
	childTasksMu     sync.RWMutex

	timeRecorder *timerecord.TimeRecorder
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
		timeRecorder:     timerecord.NewTimeRecorder("QueryCoordBaseTask"),
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
	bt.triggerMu.RLock()
	defer bt.triggerMu.RUnlock()

	return bt.triggerCondition
}

func (bt *baseTask) setTriggerCondition(trigger querypb.TriggerCondition) {
	bt.triggerMu.Lock()
	defer bt.triggerMu.Unlock()

	bt.triggerCondition = trigger
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
	metrics.QueryCoordNumChildTasks.WithLabelValues().Dec()
}

func (bt *baseTask) clearChildTasks() {
	bt.childTasksMu.Lock()
	defer bt.childTasksMu.Unlock()

	bt.childTasks = []task{}
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
	bt.retryMu.RLock()
	defer bt.retryMu.RUnlock()
	return bt.retryCount > 0
}

func (bt *baseTask) reduceRetryCount() {
	bt.retryMu.Lock()
	defer bt.retryMu.Unlock()

	bt.retryCount--
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

func (bt *baseTask) globalPostExecute(ctx context.Context) error {
	return nil
}

func (bt *baseTask) updateTaskProcess() {
	// TODO::
}

func (bt *baseTask) rollBack(ctx context.Context) []task {
	//TODO::
	return nil
}

func (bt *baseTask) elapseSpan() time.Duration {
	return bt.timeRecorder.ElapseSpan()
}

type loadCollectionTask struct {
	*baseTask
	*querypb.LoadCollectionRequest
	broker  *globalMetaBroker
	cluster Cluster
	meta    Meta
	once    sync.Once
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
			break
		}

		// wait watchDeltaChannel and watchQueryChannel task done after loading segment
		nodeID := getDstNodeIDByTask(t)
		if t.msgType() == commonpb.MsgType_LoadSegments {
			if !lct.cluster.hasWatchedDeltaChannel(lct.ctx, nodeID, collectionID) ||
				!lct.cluster.hasWatchedQueryChannel(lct.ctx, nodeID, collectionID) {
				allDone = false
				break
			}
		}

		// wait watchQueryChannel task done after watch dmChannel
		if t.msgType() == commonpb.MsgType_WatchDmChannels {
			if !lct.cluster.hasWatchedQueryChannel(lct.ctx, nodeID, collectionID) {
				allDone = false
				break
			}
		}

	}
	if allDone {
		err := syncReplicaSegments(lct.ctx, lct.cluster, childTasks)
		if err != nil {
			log.Error("loadCollectionTask: failed to sync replica segments to shard leader",
				zap.Int64("taskID", lct.getTaskID()),
				zap.Int64("collectionID", collectionID),
				zap.Error(err))
			lct.setResultInfo(err)
			return
		}

		err = lct.meta.setLoadPercentage(collectionID, 0, 100, querypb.LoadType_LoadCollection)
		if err != nil {
			log.Error("loadCollectionTask: set load percentage to meta's collectionInfo", zap.Int64("collectionID", collectionID))
			lct.setResultInfo(err)
			return
		}

		lct.once.Do(func() {
			metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
			metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(lct.elapseSpan().Milliseconds()))
			metrics.QueryCoordNumChildTasks.WithLabelValues().Sub(float64(len(lct.getChildTask())))
		})
	}
}

func (lct *loadCollectionTask) preExecute(ctx context.Context) error {
	if lct.ReplicaNumber < 1 {
		log.Warn("replicaNumber is less than 1 for load collection request, will set it to 1",
			zap.Int32("replicaNumber", lct.ReplicaNumber))
		lct.ReplicaNumber = 1
	}

	collectionID := lct.CollectionID
	schema := lct.Schema
	lct.setResultInfo(nil)
	log.Info("start do loadCollectionTask",
		zap.Int64("msgID", lct.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Stringer("schema", schema),
		zap.Int32("replicaNumber", lct.ReplicaNumber))
	return nil
}

func (lct *loadCollectionTask) execute(ctx context.Context) error {
	defer lct.reduceRetryCount()
	collectionID := lct.CollectionID

	partitionIds, err := lct.broker.showPartitionIDs(ctx, collectionID)
	if err != nil {
		log.Error("loadCollectionTask: showPartition failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
		lct.setResultInfo(err)
		return err
	}
	log.Info("loadCollectionTask: get collection's all partitionIDs", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIds), zap.Int64("msgID", lct.Base.MsgID))

	var (
		replicas          = make([]*milvuspb.ReplicaInfo, lct.ReplicaNumber)
		replicaIds        = make([]int64, lct.ReplicaNumber)
		segmentLoadInfos  = make([]*querypb.SegmentLoadInfo, 0)
		deltaChannelInfos = make([]*datapb.VchannelInfo, 0)
		dmChannelInfos    = make([]*datapb.VchannelInfo, 0)
		collectionSize    uint64
	)

	for _, partitionID := range partitionIds {
		vChannelInfos, binlogs, err := lct.broker.getRecoveryInfo(lct.ctx, collectionID, partitionID)
		if err != nil {
			log.Error("loadCollectionTask: getRecoveryInfo failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
			lct.setResultInfo(err)
			return err
		}

		for _, segmentBinlog := range binlogs {
			segmentLoadInfo := lct.broker.generateSegmentLoadInfo(ctx, collectionID, partitionID, segmentBinlog, true, lct.Schema)
			collectionSize += uint64(segmentLoadInfo.SegmentSize)
			segmentLoadInfos = append(segmentLoadInfos, segmentLoadInfo)
		}

		for _, info := range vChannelInfos {
			deltaChannelInfo, err := generateWatchDeltaChannelInfo(info)
			if err != nil {
				log.Error("loadCollectionTask: generateWatchDeltaChannelInfo failed", zap.Int64("collectionID", collectionID), zap.String("channelName", info.ChannelName), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
				lct.setResultInfo(err)
				return err
			}
			deltaChannelInfos = append(deltaChannelInfos, deltaChannelInfo)
			dmChannelInfos = append(dmChannelInfos, info)
		}
	}

	mergedDeltaChannels := mergeWatchDeltaChannelInfo(deltaChannelInfos)
	// If meta is not updated here, deltaChannel meta will not be available when loadSegment reschedule
	err = lct.meta.setDeltaChannel(collectionID, mergedDeltaChannels)
	if err != nil {
		log.Error("loadCollectionTask: set delta channel info failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
		lct.setResultInfo(err)
		return err
	}

	mergedDmChannel := mergeDmChannelInfo(dmChannelInfos)

	for i := range replicas {
		replica, err := lct.meta.generateReplica(lct.CollectionID, partitionIds)
		if err != nil {
			lct.setResultInfo(err)
			return err
		}

		replicas[i] = replica
		replicaIds[i] = replica.ReplicaID
	}

	err = lct.cluster.assignNodesToReplicas(ctx, replicas, collectionSize)
	if err != nil {
		log.Error("failed to assign nodes to replicas",
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIds),
			zap.Int64("msgID", lct.Base.MsgID),
			zap.Int32("replicaNumber", lct.ReplicaNumber),
			zap.Error(err))
		lct.setResultInfo(err)
		return err
	}

	for _, replica := range replicas {
		var (
			loadSegmentReqs    = []*querypb.LoadSegmentsRequest{}
			watchDmChannelReqs = []*querypb.WatchDmChannelsRequest{}
		)

		for _, segmentLoadInfo := range segmentLoadInfos {
			msgBase := proto.Clone(lct.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_LoadSegments
			loadSegmentReq := &querypb.LoadSegmentsRequest{
				Base:         msgBase,
				Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
				Schema:       lct.Schema,
				CollectionID: collectionID,
				LoadMeta: &querypb.LoadMetaInfo{
					LoadType:     querypb.LoadType_LoadCollection,
					CollectionID: collectionID,
					PartitionIDs: partitionIds,
				},
				ReplicaID: replica.ReplicaID,
			}

			loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
		}

		//TODO:: queryNode receive dm message according partitionID cache
		//TODO:: queryNode add partitionID to cache if receive create partition message from dmChannel
		for _, info := range mergedDmChannel {
			msgBase := proto.Clone(lct.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchDmChannels
			watchRequest := &querypb.WatchDmChannelsRequest{
				Base:         msgBase,
				CollectionID: collectionID,
				//PartitionIDs: toLoadPartitionIDs,
				Infos:  []*datapb.VchannelInfo{info},
				Schema: lct.Schema,
				LoadMeta: &querypb.LoadMetaInfo{
					LoadType:     querypb.LoadType_LoadCollection,
					CollectionID: collectionID,
					PartitionIDs: partitionIds,
				},
				ReplicaID: replica.GetReplicaID(),
			}

			watchDmChannelReqs = append(watchDmChannelReqs, watchRequest)
		}

		internalTasks, err := assignInternalTask(ctx, lct, lct.meta, lct.cluster, loadSegmentReqs, watchDmChannelReqs, false, nil, replica.GetNodeIds(), -1)
		if err != nil {
			log.Error("loadCollectionTask: assign child task failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
			lct.setResultInfo(err)
			return err
		}
		for _, internalTask := range internalTasks {
			lct.addChildTask(internalTask)
			if task, ok := internalTask.(*watchDmChannelTask); ok {
				nodeInfo, err := lct.cluster.getNodeInfoByID(task.NodeID)
				if err != nil {
					log.Error("loadCollectionTask: get shard leader node info failed",
						zap.Int64("collectionID", collectionID),
						zap.Int64("msgID", lct.Base.MsgID),
						zap.Int64("nodeID", task.NodeID),
						zap.Error(err))
					lct.setResultInfo(err)
					return err
				}
				replica.ShardReplicas = append(replica.ShardReplicas, &milvuspb.ShardReplica{
					LeaderID:      task.NodeID,
					LeaderAddr:    nodeInfo.(*queryNode).address,
					DmChannelName: task.WatchDmChannelsRequest.Infos[0].ChannelName,
				})
			}
			log.Info("loadCollectionTask: add a childTask", zap.Int64("collectionID", collectionID), zap.Int32("task type", int32(internalTask.msgType())), zap.Int64("msgID", lct.Base.MsgID))
		}
		metrics.QueryCoordNumChildTasks.WithLabelValues().Add(float64(len(internalTasks)))
		log.Info("loadCollectionTask: assign child task done", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID))
	}

	err = lct.meta.addCollection(collectionID, querypb.LoadType_LoadCollection, lct.Schema)
	if err != nil {
		log.Error("loadCollectionTask: add collection to meta failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
		lct.setResultInfo(err)
		return err
	}

	err = lct.meta.addPartitions(collectionID, partitionIds)
	if err != nil {
		log.Error("loadCollectionTask: add partitions to meta failed", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIds), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
		lct.setResultInfo(err)
		return err
	}

	for _, replica := range replicas {
		err = lct.meta.addReplica(replica)
		if err != nil {
			log.Error("failed to add replica", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIds), zap.Int64("msgID", lct.Base.MsgID), zap.Int32("replicaNumber", lct.ReplicaNumber))
			lct.setResultInfo(err)
			return err
		}
	}

	log.Info("LoadCollection execute done",
		zap.Int64("msgID", lct.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lct *loadCollectionTask) postExecute(ctx context.Context) error {
	collectionID := lct.CollectionID
	if lct.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		lct.clearChildTasks()
		err := lct.meta.releaseCollection(collectionID)
		if err != nil {
			log.Error("loadCollectionTask: occur error when release collection info from meta", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
			panic(err)
		}
	}

	log.Info("loadCollectionTask postExecute done",
		zap.Int64("msgID", lct.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lct *loadCollectionTask) rollBack(ctx context.Context) []task {
	onlineNodeIDs := lct.cluster.onlineNodeIDs()
	resultTasks := make([]task, 0)
	for _, nodeID := range onlineNodeIDs {
		//brute force rollBack, should optimize
		msgBase := proto.Clone(lct.Base).(*commonpb.MsgBase)
		msgBase.MsgType = commonpb.MsgType_ReleaseCollection
		req := &querypb.ReleaseCollectionRequest{
			Base:         msgBase,
			DbID:         lct.DbID,
			CollectionID: lct.CollectionID,
			NodeID:       nodeID,
		}
		baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
		baseTask.setParentTask(lct)
		releaseCollectionTask := &releaseCollectionTask{
			baseTask:                 baseTask,
			ReleaseCollectionRequest: req,
			cluster:                  lct.cluster,
		}
		resultTasks = append(resultTasks, releaseCollectionTask)
	}

	err := lct.meta.releaseCollection(lct.CollectionID)
	if err != nil {
		log.Error("releaseCollectionTask: release collectionInfo from meta failed", zap.Int64("collectionID", lct.CollectionID), zap.Int64("msgID", lct.Base.MsgID), zap.Error(err))
		panic(err)
	}

	log.Info("loadCollectionTask: generate rollBack task for loadCollectionTask", zap.Int64("collectionID", lct.CollectionID), zap.Int64("msgID", lct.Base.MsgID))
	return resultTasks
}

// releaseCollectionTask will release all the data of this collection on query nodes
type releaseCollectionTask struct {
	*baseTask
	*querypb.ReleaseCollectionRequest
	cluster Cluster
	meta    Meta
	broker  *globalMetaBroker
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

func (rct *releaseCollectionTask) updateTaskProcess() {
	collectionID := rct.CollectionID
	parentTask := rct.getParentTask()
	if parentTask == nil {
		// all queryNodes have successfully released the data, clean up collectionMeta
		err := rct.meta.releaseCollection(collectionID)
		if err != nil {
			log.Error("releaseCollectionTask: release collectionInfo from meta failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", rct.Base.MsgID), zap.Error(err))
			panic(err)
		}
	}
}

func (rct *releaseCollectionTask) preExecute(context.Context) error {
	collectionID := rct.CollectionID
	rct.setResultInfo(nil)
	log.Info("start do releaseCollectionTask",
		zap.Int64("msgID", rct.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (rct *releaseCollectionTask) execute(ctx context.Context) error {
	collectionID := rct.CollectionID

	// if nodeID ==0, it means that the release request has not been assigned to the specified query node
	if rct.NodeID <= 0 {
		ctx2, cancel2 := context.WithTimeout(rct.ctx, timeoutForRPC)
		defer cancel2()
		err := rct.broker.releaseDQLMessageStream(ctx2, collectionID)
		if err != nil {
			log.Error("releaseCollectionTask: release collection end, releaseDQLMessageStream occur error", zap.Int64("collectionID", rct.CollectionID), zap.Int64("msgID", rct.Base.MsgID), zap.Error(err))
			rct.setResultInfo(err)
			return err
		}

		// invalidate all the collection meta cache with the specified collectionID
		err = rct.broker.invalidateCollectionMetaCache(ctx, collectionID)
		if err != nil {
			log.Error("releaseCollectionTask: release collection end, invalidateCollectionMetaCache occur error", zap.Int64("collectionID", rct.CollectionID), zap.Int64("msgID", rct.Base.MsgID), zap.Error(err))
			rct.setResultInfo(err)
			return err
		}

		// TODO(yah01): broadcast to all nodes? Or only nodes serve the collection
		onlineNodeIDs := rct.cluster.onlineNodeIDs()
		for _, nodeID := range onlineNodeIDs {
			req := proto.Clone(rct.ReleaseCollectionRequest).(*querypb.ReleaseCollectionRequest)
			req.NodeID = nodeID
			baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
			baseTask.setParentTask(rct)
			releaseCollectionTask := &releaseCollectionTask{
				baseTask:                 baseTask,
				ReleaseCollectionRequest: req,
				cluster:                  rct.cluster,
			}

			rct.addChildTask(releaseCollectionTask)
			log.Info("releaseCollectionTask: add a releaseCollectionTask to releaseCollectionTask's childTask", zap.Any("task", releaseCollectionTask))
		}
	} else {
		// If the node crashed or be offline, the loaded segments are lost
		defer rct.reduceRetryCount()
		err := rct.cluster.releaseCollection(ctx, rct.NodeID, rct.ReleaseCollectionRequest)
		if err != nil {
			log.Warn("releaseCollectionTask: release collection end, node occur error", zap.Int64("collectionID", collectionID), zap.Int64("nodeID", rct.NodeID))
			// after release failed, the task will always redo
			// if the query node happens to be down, the node release was judged to have succeeded
			return err
		}
	}

	log.Info("releaseCollectionTask Execute done",
		zap.Int64("msgID", rct.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("nodeID", rct.NodeID))
	return nil
}

func (rct *releaseCollectionTask) postExecute(context.Context) error {
	collectionID := rct.CollectionID
	if rct.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		rct.clearChildTasks()
	}

	log.Info("releaseCollectionTask postExecute done",
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
	broker  *globalMetaBroker
	cluster Cluster
	meta    Meta
	addCol  bool
	once    sync.Once
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

		// wait watchDeltaChannel and watchQueryChannel task done after loading segment
		nodeID := getDstNodeIDByTask(t)
		if t.msgType() == commonpb.MsgType_LoadSegments {
			if !lpt.cluster.hasWatchedDeltaChannel(lpt.ctx, nodeID, collectionID) ||
				!lpt.cluster.hasWatchedQueryChannel(lpt.ctx, nodeID, collectionID) {
				allDone = false
				break
			}
		}

		// wait watchQueryChannel task done after watching dmChannel
		if t.msgType() == commonpb.MsgType_WatchDmChannels {
			if !lpt.cluster.hasWatchedQueryChannel(lpt.ctx, nodeID, collectionID) {
				allDone = false
				break
			}
		}
	}
	if allDone {
		err := syncReplicaSegments(lpt.ctx, lpt.cluster, childTasks)
		if err != nil {
			log.Error("loadPartitionTask: failed to sync replica segments to shard leader",
				zap.Int64("taskID", lpt.getTaskID()),
				zap.Int64("collectionID", collectionID),
				zap.Error(err))
			lpt.setResultInfo(err)
			return
		}

		for _, id := range partitionIDs {
			err := lpt.meta.setLoadPercentage(collectionID, id, 100, querypb.LoadType_LoadPartition)
			if err != nil {
				log.Error("loadPartitionTask: set load percentage to meta's collectionInfo", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", id))
				lpt.setResultInfo(err)
				return
			}
		}
		lpt.once.Do(func() {
			metrics.QueryCoordLoadCount.WithLabelValues(metrics.SuccessLabel).Inc()
			metrics.QueryCoordLoadLatency.WithLabelValues().Observe(float64(lpt.elapseSpan().Milliseconds()))
			metrics.QueryCoordNumChildTasks.WithLabelValues().Sub(float64(len(lpt.getChildTask())))
		})
	}
}

func (lpt *loadPartitionTask) preExecute(context.Context) error {
	if lpt.ReplicaNumber < 1 {
		log.Warn("replicaNumber is less than 1 for load partitions request, will set it to 1",
			zap.Int32("replicaNumber", lpt.ReplicaNumber))
		lpt.ReplicaNumber = 1
	}

	collectionID := lpt.CollectionID
	lpt.setResultInfo(nil)
	log.Info("start do loadPartitionTask",
		zap.Int64("msgID", lpt.getTaskID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lpt *loadPartitionTask) execute(ctx context.Context) error {
	defer lpt.reduceRetryCount()
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs

	var (
		replicas          = make([]*milvuspb.ReplicaInfo, lpt.ReplicaNumber)
		replicaIds        = make([]int64, lpt.ReplicaNumber)
		segmentLoadInfos  = make([]*querypb.SegmentLoadInfo, 0)
		deltaChannelInfos = make([]*datapb.VchannelInfo, 0)
		dmChannelInfos    = make([]*datapb.VchannelInfo, 0)
		collectionSize    uint64
	)

	for _, partitionID := range partitionIDs {
		vChannelInfos, binlogs, err := lpt.broker.getRecoveryInfo(lpt.ctx, collectionID, partitionID)
		if err != nil {
			log.Error("loadPartitionTask: getRecoveryInfo failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
			lpt.setResultInfo(err)
			return err
		}

		for _, segmentBingLog := range binlogs {
			segmentLoadInfo := lpt.broker.generateSegmentLoadInfo(ctx, collectionID, partitionID, segmentBingLog, true, lpt.Schema)
			segmentLoadInfos = append(segmentLoadInfos, segmentLoadInfo)
			collectionSize += uint64(segmentLoadInfo.SegmentSize)
		}

		for _, info := range vChannelInfos {
			deltaChannelInfo, err := generateWatchDeltaChannelInfo(info)
			if err != nil {
				log.Error("loadPartitionTask: generateWatchDeltaChannelInfo failed", zap.Int64("collectionID", collectionID), zap.String("channelName", info.ChannelName), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
				lpt.setResultInfo(err)
				return err
			}
			deltaChannelInfos = append(deltaChannelInfos, deltaChannelInfo)
			dmChannelInfos = append(dmChannelInfos, info)
		}
	}
	mergedDeltaChannels := mergeWatchDeltaChannelInfo(deltaChannelInfos)
	// If meta is not updated here, deltaChannel meta will not be available when loadSegment reschedule
	err := lpt.meta.setDeltaChannel(collectionID, mergedDeltaChannels)
	if err != nil {
		log.Error("loadPartitionTask: set delta channel info failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
		lpt.setResultInfo(err)
		return err
	}

	mergedDmChannel := mergeDmChannelInfo(dmChannelInfos)

	for i := range replicas {
		replica, err := lpt.meta.generateReplica(lpt.CollectionID, partitionIDs)
		if err != nil {
			lpt.setResultInfo(err)
			return err
		}

		replicas[i] = replica
		replicaIds[i] = replica.ReplicaID
	}

	err = lpt.cluster.assignNodesToReplicas(ctx, replicas, collectionSize)
	if err != nil {
		log.Error("failed to assign nodes to replicas",
			zap.Int64("collectionID", collectionID),
			zap.Int64s("partitionIDs", partitionIDs),
			zap.Int64("msgID", lpt.Base.MsgID),
			zap.Int32("replicaNumber", lpt.ReplicaNumber),
			zap.Error(err))
		lpt.setResultInfo(err)
		return err
	}

	for _, replica := range replicas {
		var (
			loadSegmentReqs    = []*querypb.LoadSegmentsRequest{}
			watchDmChannelReqs = []*querypb.WatchDmChannelsRequest{}
		)

		for _, segmentLoadInfo := range segmentLoadInfos {
			msgBase := proto.Clone(lpt.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_LoadSegments
			loadSegmentReq := &querypb.LoadSegmentsRequest{
				Base:         msgBase,
				Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
				Schema:       lpt.Schema,
				CollectionID: collectionID,
				LoadMeta: &querypb.LoadMetaInfo{
					LoadType:     querypb.LoadType_LoadPartition,
					CollectionID: collectionID,
					PartitionIDs: partitionIDs,
				},
				ReplicaID: replica.ReplicaID,
			}
			loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
		}

		for _, info := range mergedDmChannel {
			msgBase := proto.Clone(lpt.Base).(*commonpb.MsgBase)
			msgBase.MsgType = commonpb.MsgType_WatchDmChannels
			watchRequest := &querypb.WatchDmChannelsRequest{
				Base:         msgBase,
				CollectionID: collectionID,
				PartitionIDs: partitionIDs,
				Infos:        []*datapb.VchannelInfo{info},
				Schema:       lpt.Schema,
				LoadMeta: &querypb.LoadMetaInfo{
					LoadType:     querypb.LoadType_LoadPartition,
					CollectionID: collectionID,
					PartitionIDs: partitionIDs,
				},
				ReplicaID: replica.GetReplicaID(),
			}

			watchDmChannelReqs = append(watchDmChannelReqs, watchRequest)
		}

		internalTasks, err := assignInternalTask(ctx, lpt, lpt.meta, lpt.cluster, loadSegmentReqs, watchDmChannelReqs, false, nil, replica.GetNodeIds(), -1)
		if err != nil {
			log.Error("loadPartitionTask: assign child task failed", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
			lpt.setResultInfo(err)
			return err
		}
		for _, internalTask := range internalTasks {
			lpt.addChildTask(internalTask)
			if task, ok := internalTask.(*watchDmChannelTask); ok {
				nodeInfo, err := lpt.cluster.getNodeInfoByID(task.NodeID)
				if err != nil {
					log.Error("loadCollectionTask: get shard leader node info failed",
						zap.Int64("collectionID", collectionID),
						zap.Int64("msgID", lpt.Base.MsgID),
						zap.Int64("nodeID", task.NodeID),
						zap.Error(err))
					lpt.setResultInfo(err)
					return err
				}

				replica.ShardReplicas = append(replica.ShardReplicas, &milvuspb.ShardReplica{
					LeaderID:      task.NodeID,
					LeaderAddr:    nodeInfo.(*queryNode).address,
					DmChannelName: task.WatchDmChannelsRequest.Infos[0].ChannelName,
				})
			}
			log.Info("loadPartitionTask: add a childTask", zap.Int64("collectionID", collectionID), zap.Int32("task type", int32(internalTask.msgType())))
		}
		metrics.QueryCoordNumChildTasks.WithLabelValues().Add(float64(len(internalTasks)))
		log.Info("loadPartitionTask: assign child task done", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs), zap.Int64("msgID", lpt.Base.MsgID))
	}

	err = lpt.meta.addCollection(collectionID, querypb.LoadType_LoadPartition, lpt.Schema)
	if err != nil {
		log.Error("loadPartitionTask: add collection to meta failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
		lpt.setResultInfo(err)
		return err
	}

	err = lpt.meta.addPartitions(collectionID, partitionIDs)
	if err != nil {
		log.Error("loadPartitionTask: add partition to meta failed", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
		lpt.setResultInfo(err)
		return err
	}

	for _, replica := range replicas {
		err = lpt.meta.addReplica(replica)
		if err != nil {
			log.Error("failed to add replica", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs), zap.Int64("msgID", lpt.Base.MsgID), zap.Int32("replicaNumber", lpt.ReplicaNumber))
			lpt.setResultInfo(err)
			return err
		}
	}

	log.Info("loadPartitionTask Execute done",
		zap.Int64("msgID", lpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("msgID", lpt.Base.MsgID))
	return nil
}

func (lpt *loadPartitionTask) postExecute(ctx context.Context) error {
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs
	if lpt.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		lpt.clearChildTasks()
		err := lpt.meta.releaseCollection(collectionID)
		if err != nil {
			log.Error("loadPartitionTask: occur error when release collection info from meta", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
			panic(err)
		}
	}

	log.Info("loadPartitionTask postExecute done",
		zap.Int64("msgID", lpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	return nil
}

func (lpt *loadPartitionTask) rollBack(ctx context.Context) []task {
	collectionID := lpt.CollectionID
	resultTasks := make([]task, 0)
	//brute force rollBack, should optimize
	onlineNodeIDs := lpt.cluster.onlineNodeIDs()
	for _, nodeID := range onlineNodeIDs {
		req := &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ReleaseCollection,
				MsgID:     lpt.Base.MsgID,
				Timestamp: lpt.Base.Timestamp,
				SourceID:  lpt.Base.SourceID,
			},
			DbID:         lpt.DbID,
			CollectionID: collectionID,
			NodeID:       nodeID,
		}
		baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
		baseTask.setParentTask(lpt)
		releaseCollectionTask := &releaseCollectionTask{
			baseTask:                 baseTask,
			ReleaseCollectionRequest: req,
			cluster:                  lpt.cluster,
		}
		resultTasks = append(resultTasks, releaseCollectionTask)
	}

	err := lpt.meta.releaseCollection(collectionID)
	if err != nil {
		log.Error("loadPartitionTask: release collection info from meta failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lpt.Base.MsgID), zap.Error(err))
		panic(err)
	}
	log.Info("loadPartitionTask: generate rollBack task for loadPartitionTask", zap.Int64("collectionID", collectionID), zap.Int64("msgID", lpt.Base.MsgID))
	return resultTasks
}

// releasePartitionTask will release all the data of this partition on query nodes
type releasePartitionTask struct {
	*baseTask
	*querypb.ReleasePartitionsRequest
	cluster Cluster
	meta    Meta
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

func (rpt *releasePartitionTask) updateTaskProcess() {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	parentTask := rpt.getParentTask()
	if parentTask == nil {
		// all queryNodes have successfully released the data, clean up collectionMeta
		err := rpt.meta.releasePartitions(collectionID, partitionIDs)
		if err != nil {
			log.Error("releasePartitionTask: release collectionInfo from meta failed", zap.Int64("collectionID", collectionID), zap.Int64("msgID", rpt.Base.MsgID), zap.Error(err))
			panic(err)
		}

	}
}

func (rpt *releasePartitionTask) preExecute(context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	rpt.setResultInfo(nil)
	log.Info("start do releasePartitionTask",
		zap.Int64("msgID", rpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	return nil
}

func (rpt *releasePartitionTask) execute(ctx context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs

	// if nodeID ==0, it means that the release request has not been assigned to the specified query node
	if rpt.NodeID <= 0 {
		onlineNodeIDs := rpt.cluster.onlineNodeIDs()
		for _, nodeID := range onlineNodeIDs {
			req := proto.Clone(rpt.ReleasePartitionsRequest).(*querypb.ReleasePartitionsRequest)
			req.NodeID = nodeID
			baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
			baseTask.setParentTask(rpt)
			releasePartitionTask := &releasePartitionTask{
				baseTask:                 baseTask,
				ReleasePartitionsRequest: req,
				cluster:                  rpt.cluster,
				meta:                     rpt.meta,
			}
			rpt.addChildTask(releasePartitionTask)
			log.Info("releasePartitionTask: add a releasePartitionTask to releasePartitionTask's childTask", zap.Int64("collectionID", collectionID), zap.Int64("msgID", rpt.Base.MsgID))
		}
	} else {
		// If the node crashed or be offline, the loaded segments are lost
		defer rpt.reduceRetryCount()
		err := rpt.cluster.releasePartitions(ctx, rpt.NodeID, rpt.ReleasePartitionsRequest)
		if err != nil {
			log.Warn("ReleasePartitionsTask: release partition end, node occur error", zap.Int64("collectionID", collectionID), zap.String("nodeID", fmt.Sprintln(rpt.NodeID)))
			// after release failed, the task will always redo
			// if the query node happens to be down, the node release was judged to have succeeded
			return err
		}
	}

	log.Info("releasePartitionTask Execute done",
		zap.Int64("msgID", rpt.getTaskID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("nodeID", rpt.NodeID))
	return nil
}

func (rpt *releasePartitionTask) postExecute(context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	if rpt.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		rpt.clearChildTasks()
	}

	log.Info("releasePartitionTask postExecute done",
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
	log.Info("start do loadSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("loaded nodeID", lst.DstNodeID),
		zap.Int64("taskID", lst.getTaskID()))
	return nil
}

func (lst *loadSegmentTask) execute(ctx context.Context) error {
	defer lst.reduceRetryCount()

	err := lst.cluster.loadSegments(ctx, lst.DstNodeID, lst.LoadSegmentsRequest)
	if err != nil {
		log.Warn("loadSegmentTask: loadSegment occur error", zap.Int64("taskID", lst.getTaskID()))
		lst.setResultInfo(err)
		return err
	}

	log.Info("loadSegmentTask Execute done",
		zap.Int64("taskID", lst.getTaskID()))
	return nil
}

func (lst *loadSegmentTask) postExecute(context.Context) error {
	log.Info("loadSegmentTask postExecute done",
		zap.Int64("taskID", lst.getTaskID()))
	return nil
}

func (lst *loadSegmentTask) reschedule(ctx context.Context) ([]task, error) {
	loadSegmentReqs := make([]*querypb.LoadSegmentsRequest, 0)
	for _, info := range lst.Infos {
		msgBase := proto.Clone(lst.Base).(*commonpb.MsgBase)
		msgBase.MsgType = commonpb.MsgType_LoadSegments
		req := &querypb.LoadSegmentsRequest{
			Base:         msgBase,
			Infos:        []*querypb.SegmentLoadInfo{info},
			Schema:       lst.Schema,
			SourceNodeID: lst.SourceNodeID,
			CollectionID: lst.CollectionID,
			LoadMeta: &querypb.LoadMetaInfo{
				LoadType:     lst.GetLoadMeta().GetLoadType(),
				CollectionID: lst.GetCollectionID(),
				PartitionIDs: lst.GetLoadMeta().GetPartitionIDs(),
			},
			ReplicaID: lst.ReplicaID,
		}
		loadSegmentReqs = append(loadSegmentReqs, req)
	}
	if lst.excludeNodeIDs == nil {
		lst.excludeNodeIDs = []int64{}
	}
	lst.excludeNodeIDs = append(lst.excludeNodeIDs, lst.DstNodeID)

	wait2AssignTaskSuccess := false
	if lst.getParentTask().getTriggerCondition() == querypb.TriggerCondition_NodeDown {
		wait2AssignTaskSuccess = true
	}
	reScheduledTasks, err := assignInternalTask(ctx, lst.getParentTask(), lst.meta, lst.cluster, loadSegmentReqs, nil, wait2AssignTaskSuccess, lst.excludeNodeIDs, nil, lst.ReplicaID)
	if err != nil {
		log.Error("loadSegment reschedule failed", zap.Int64s("excludeNodes", lst.excludeNodeIDs), zap.Int64("taskID", lst.getTaskID()), zap.Error(err))
		return nil, err
	}

	return reScheduledTasks, nil
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
	log.Info("start do releaseSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("loaded nodeID", rst.NodeID),
		zap.Int64("taskID", rst.getTaskID()))
	return nil
}

func (rst *releaseSegmentTask) execute(ctx context.Context) error {
	defer rst.reduceRetryCount()

	err := rst.cluster.releaseSegments(rst.ctx, rst.NodeID, rst.ReleaseSegmentsRequest)
	if err != nil {
		log.Warn("releaseSegmentTask: releaseSegment occur error", zap.Int64("taskID", rst.getTaskID()))
		rst.setResultInfo(err)
		return err
	}

	log.Info("releaseSegmentTask Execute done",
		zap.Int64s("segmentIDs", rst.SegmentIDs),
		zap.Int64("taskID", rst.getTaskID()))
	return nil
}

func (rst *releaseSegmentTask) postExecute(context.Context) error {
	segmentIDs := rst.SegmentIDs
	log.Info("releaseSegmentTask postExecute done",
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
	log.Info("start do watchDmChannelTask",
		zap.Strings("dmChannels", channels),
		zap.Int64("loaded nodeID", wdt.NodeID),
		zap.Int64("taskID", wdt.getTaskID()))
	return nil
}

func (wdt *watchDmChannelTask) execute(ctx context.Context) error {
	defer wdt.reduceRetryCount()

	err := wdt.cluster.watchDmChannels(wdt.ctx, wdt.NodeID, wdt.WatchDmChannelsRequest)
	if err != nil {
		log.Warn("watchDmChannelTask: watchDmChannel occur error", zap.Int64("taskID", wdt.getTaskID()))
		wdt.setResultInfo(err)
		return err
	}

	log.Info("watchDmChannelsTask Execute done",
		zap.Int64("taskID", wdt.getTaskID()))
	return nil
}

func (wdt *watchDmChannelTask) postExecute(context.Context) error {
	log.Info("watchDmChannelTask postExecute done",
		zap.Int64("taskID", wdt.getTaskID()))
	return nil
}

func (wdt *watchDmChannelTask) reschedule(ctx context.Context) ([]task, error) {
	collectionID := wdt.CollectionID
	watchDmChannelReqs := make([]*querypb.WatchDmChannelsRequest, 0)
	for _, info := range wdt.Infos {
		msgBase := proto.Clone(wdt.Base).(*commonpb.MsgBase)
		msgBase.MsgType = commonpb.MsgType_WatchDmChannels
		req := &querypb.WatchDmChannelsRequest{
			Base:         msgBase,
			CollectionID: collectionID,
			PartitionIDs: wdt.PartitionIDs,
			Infos:        []*datapb.VchannelInfo{info},
			Schema:       wdt.Schema,
			ExcludeInfos: wdt.ExcludeInfos,
			LoadMeta: &querypb.LoadMetaInfo{
				LoadType:     wdt.GetLoadMeta().GetLoadType(),
				CollectionID: collectionID,
				PartitionIDs: wdt.GetLoadMeta().GetPartitionIDs(),
			},
			ReplicaID: wdt.GetReplicaID(),
		}
		watchDmChannelReqs = append(watchDmChannelReqs, req)
	}

	if wdt.excludeNodeIDs == nil {
		wdt.excludeNodeIDs = []int64{}
	}
	wdt.excludeNodeIDs = append(wdt.excludeNodeIDs, wdt.NodeID)
	wait2AssignTaskSuccess := false
	if wdt.getParentTask().getTriggerCondition() == querypb.TriggerCondition_NodeDown {
		wait2AssignTaskSuccess = true
	}
	reScheduledTasks, err := assignInternalTask(ctx, wdt.parentTask, wdt.meta, wdt.cluster, nil, watchDmChannelReqs, wait2AssignTaskSuccess, wdt.excludeNodeIDs, nil, wdt.ReplicaID)
	if err != nil {
		log.Error("watchDmChannel reschedule failed", zap.Int64("taskID", wdt.getTaskID()), zap.Int64s("excludeNodes", wdt.excludeNodeIDs), zap.Error(err))
		return nil, err
	}

	return reScheduledTasks, nil
}

type watchDeltaChannelTask struct {
	*baseTask
	*querypb.WatchDeltaChannelsRequest
	cluster Cluster
}

func (wdt *watchDeltaChannelTask) msgBase() *commonpb.MsgBase {
	return wdt.Base
}

func (wdt *watchDeltaChannelTask) marshal() ([]byte, error) {
	return proto.Marshal(wdt.WatchDeltaChannelsRequest)
}

func (wdt *watchDeltaChannelTask) isValid() bool {
	online, err := wdt.cluster.isOnline(wdt.NodeID)
	if err != nil {
		return false
	}

	return wdt.ctx != nil && online
}

func (wdt *watchDeltaChannelTask) msgType() commonpb.MsgType {
	return wdt.Base.MsgType
}

func (wdt *watchDeltaChannelTask) timestamp() Timestamp {
	return wdt.Base.Timestamp
}

func (wdt *watchDeltaChannelTask) updateTaskProcess() {
	parentTask := wdt.getParentTask()
	if parentTask == nil {
		log.Warn("watchDeltaChannel: parentTask should not be nil")
		return
	}
	parentTask.updateTaskProcess()
}

func (wdt *watchDeltaChannelTask) preExecute(context.Context) error {
	channelInfos := wdt.Infos
	channels := make([]string, 0)
	for _, info := range channelInfos {
		channels = append(channels, info.ChannelName)
	}
	wdt.setResultInfo(nil)
	log.Info("start do watchDeltaChannelTask",
		zap.Strings("deltaChannels", channels),
		zap.Int64("loaded nodeID", wdt.NodeID),
		zap.Int64("taskID", wdt.getTaskID()))
	return nil
}

func (wdt *watchDeltaChannelTask) execute(ctx context.Context) error {
	defer wdt.reduceRetryCount()

	err := wdt.cluster.watchDeltaChannels(wdt.ctx, wdt.NodeID, wdt.WatchDeltaChannelsRequest)
	if err != nil {
		log.Warn("watchDeltaChannelTask: watchDeltaChannel occur error", zap.Int64("taskID", wdt.getTaskID()), zap.Error(err))
		wdt.setResultInfo(err)
		return err
	}

	log.Info("watchDeltaChannelsTask Execute done",
		zap.Int64("taskID", wdt.getTaskID()))
	return nil
}

func (wdt *watchDeltaChannelTask) postExecute(context.Context) error {
	log.Info("watchDeltaChannelTask postExecute done",
		zap.Int64("taskID", wdt.getTaskID()))
	return nil
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
	log.Info("start do watchQueryChannelTask",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.QueryChannel),
		zap.String("queryResultChannel", wqt.QueryResultChannel),
		zap.Int64("loaded nodeID", wqt.NodeID),
		zap.Int64("taskID", wqt.getTaskID()))
	return nil
}

func (wqt *watchQueryChannelTask) execute(ctx context.Context) error {
	defer wqt.reduceRetryCount()

	err := wqt.cluster.addQueryChannel(wqt.ctx, wqt.NodeID, wqt.AddQueryChannelRequest)
	if err != nil {
		log.Warn("watchQueryChannelTask: watchQueryChannel occur error",
			zap.Int64("taskID", wqt.getTaskID()),
			zap.String("channel", wqt.AddQueryChannelRequest.QueryChannel),
			zap.Error(err))
		wqt.setResultInfo(err)
		return err
	}

	log.Info("watchQueryChannelTask Execute done",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.QueryChannel),
		zap.String("queryResultChannel", wqt.QueryResultChannel),
		zap.Int64("taskID", wqt.getTaskID()))
	return nil
}

func (wqt *watchQueryChannelTask) postExecute(context.Context) error {
	log.Info("watchQueryChannelTask postExecute done",
		zap.Int64("collectionID", wqt.CollectionID),
		zap.String("queryChannel", wqt.QueryChannel),
		zap.String("queryResultChannel", wqt.QueryResultChannel),
		zap.Int64("taskID", wqt.getTaskID()))
	return nil
}

//****************************handoff task********************************//
type handoffTask struct {
	*baseTask
	*querypb.HandoffSegmentsRequest
	broker  *globalMetaBroker
	cluster Cluster
	meta    Meta
}

func (ht *handoffTask) msgBase() *commonpb.MsgBase {
	return ht.Base
}

func (ht *handoffTask) marshal() ([]byte, error) {
	return proto.Marshal(ht.HandoffSegmentsRequest)
}

func (ht *handoffTask) msgType() commonpb.MsgType {
	return ht.Base.MsgType
}

func (ht *handoffTask) timestamp() Timestamp {
	return ht.Base.Timestamp
}

func (ht *handoffTask) preExecute(context.Context) error {
	ht.setResultInfo(nil)
	segmentIDs := make([]UniqueID, 0)
	segmentInfos := ht.HandoffSegmentsRequest.SegmentInfos
	for _, info := range segmentInfos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	log.Info("start do handoff segments task",
		zap.Int64s("segmentIDs", segmentIDs))
	return nil
}

func (ht *handoffTask) execute(ctx context.Context) error {
	segmentInfos := ht.HandoffSegmentsRequest.SegmentInfos
	for _, segmentInfo := range segmentInfos {
		collectionID := segmentInfo.CollectionID
		partitionID := segmentInfo.PartitionID
		segmentID := segmentInfo.SegmentID

		collectionInfo, err := ht.meta.getCollectionInfoByID(collectionID)
		if err != nil {
			log.Warn("handoffTask: collection has not been loaded into memory", zap.Int64("collectionID", collectionID), zap.Int64("segmentID", segmentID))
			continue
		}

		if collectionInfo.LoadType == querypb.LoadType_LoadCollection && ht.meta.hasReleasePartition(collectionID, partitionID) {
			log.Warn("handoffTask: partition has not been released", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID))
			continue
		}

		partitionLoaded := false
		for _, id := range collectionInfo.PartitionIDs {
			if id == partitionID {
				partitionLoaded = true
			}
		}

		if collectionInfo.LoadType != querypb.LoadType_LoadCollection && !partitionLoaded {
			log.Warn("handoffTask: partition has not been loaded into memory", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Int64("segmentID", segmentID))
			continue
		}

		//  segment which is compacted from should exist in query node
		for _, compactedSegID := range segmentInfo.CompactionFrom {
			_, err = ht.meta.getSegmentInfoByID(compactedSegID)
			if err != nil {
				log.Error("handoffTask: compacted segment has not been loaded into memory", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Int64("segmentID", segmentID))
				ht.setResultInfo(err)
				return err
			}
		}

		// segment which is compacted to should not exist in query node
		_, err = ht.meta.getSegmentInfoByID(segmentID)
		if err != nil {
			dmChannelInfos, binlogs, err := ht.broker.getRecoveryInfo(ht.ctx, collectionID, partitionID)
			if err != nil {
				log.Error("handoffTask: getRecoveryInfo failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
				ht.setResultInfo(err)
				return err
			}

			findBinlog := false
			var loadSegmentReq *querypb.LoadSegmentsRequest
			var watchDeltaChannels []*datapb.VchannelInfo
			for _, segmentBinlog := range binlogs {
				if segmentBinlog.SegmentID == segmentID {
					findBinlog = true
					segmentLoadInfo := ht.broker.generateSegmentLoadInfo(ctx, collectionID, partitionID, segmentBinlog, false, nil)
					segmentLoadInfo.CompactionFrom = segmentInfo.CompactionFrom
					segmentLoadInfo.IndexInfos = segmentInfo.IndexInfos
					msgBase := proto.Clone(ht.Base).(*commonpb.MsgBase)
					msgBase.MsgType = commonpb.MsgType_LoadSegments
					loadSegmentReq = &querypb.LoadSegmentsRequest{
						Base:         msgBase,
						Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
						Schema:       collectionInfo.Schema,
						CollectionID: collectionID,
					}
				}
			}
			for _, info := range dmChannelInfos {
				deltaChannel, err := generateWatchDeltaChannelInfo(info)
				if err != nil {
					return err
				}
				watchDeltaChannels = append(watchDeltaChannels, deltaChannel)
			}

			if !findBinlog {
				err = fmt.Errorf("segmnet has not been flushed, segmentID is %d", segmentID)
				ht.setResultInfo(err)
				return err
			}
			mergedDeltaChannels := mergeWatchDeltaChannelInfo(watchDeltaChannels)
			// If meta is not updated here, deltaChannel meta will not be available when loadSegment reschedule
			err = ht.meta.setDeltaChannel(collectionID, mergedDeltaChannels)
			if err != nil {
				log.Error("handoffTask: set delta channel info to meta failed", zap.Int64("collectionID", collectionID), zap.Int64("segmentID", segmentID), zap.Error(err))
				ht.setResultInfo(err)
				return err
			}
			replicas, err := ht.meta.getReplicasByCollectionID(collectionID)
			if err != nil {
				ht.setResultInfo(err)
				return err
			}
			var internalTasks []task
			for _, replica := range replicas {
				if len(replica.NodeIds) == 0 {
					log.Warn("handoffTask: find empty replica", zap.Int64("collectionID", collectionID), zap.Int64("segmentID", segmentID), zap.Int64("replicaID", replica.GetReplicaID()))
					err := fmt.Errorf("replica %d of collection %d is empty", replica.GetReplicaID(), collectionID)
					ht.setResultInfo(err)
					return err
				}
				// we should copy a request because assignInternalTask will change DstNodeID of LoadSegmentRequest
				clonedReq := proto.Clone(loadSegmentReq).(*querypb.LoadSegmentsRequest)
				clonedReq.ReplicaID = replica.ReplicaID
				tasks, err := assignInternalTask(ctx, ht, ht.meta, ht.cluster, []*querypb.LoadSegmentsRequest{clonedReq}, nil, true, nil, nil, replica.GetReplicaID())
				if err != nil {
					log.Error("handoffTask: assign child task failed", zap.Int64("collectionID", collectionID), zap.Int64("segmentID", segmentID), zap.Error(err))
					ht.setResultInfo(err)
					return err
				}
				internalTasks = append(internalTasks, tasks...)
			}
			for _, internalTask := range internalTasks {
				ht.addChildTask(internalTask)
				log.Info("handoffTask: add a childTask", zap.Int32("task type", int32(internalTask.msgType())), zap.Int64("segmentID", segmentID))
			}
		} else {
			err = fmt.Errorf("sealed segment has been exist on query node, segmentID is %d", segmentID)
			log.Error("handoffTask: handoff segment failed", zap.Int64("segmentID", segmentID), zap.Error(err))
			ht.setResultInfo(err)
			return err
		}
	}

	log.Info("handoffTask: assign child task done", zap.Any("segmentInfos", segmentInfos), zap.Int64("taskID", ht.getTaskID()))
	return nil
}

func (ht *handoffTask) postExecute(context.Context) error {
	if ht.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		ht.clearChildTasks()
	}

	log.Info("handoffTask postExecute done", zap.Int64("taskID", ht.getTaskID()))
	return nil
}

func (ht *handoffTask) rollBack(ctx context.Context) []task {
	resultTasks := make([]task, 0)
	childTasks := ht.getChildTask()
	for _, childTask := range childTasks {
		if childTask.msgType() == commonpb.MsgType_LoadSegments {
			// TODO:: add release segment to rollBack, no release does not affect correctness of query
		}
	}

	return resultTasks
}

type loadBalanceTask struct {
	*baseTask
	*querypb.LoadBalanceRequest
	broker    *globalMetaBroker
	cluster   Cluster
	meta      Meta
	replicaID int64
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
	log.Info("start do loadBalanceTask",
		zap.Int32("trigger type", int32(lbt.triggerCondition)),
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))
	return nil
}

func (lbt *loadBalanceTask) checkForManualLoadBalance() error {
	// check segments belong to the same collection
	collectionID := lbt.GetCollectionID()
	for _, sid := range lbt.SealedSegmentIDs {
		segment, err := lbt.meta.getSegmentInfoByID(sid)
		if err != nil {
			return err
		}
		if collectionID == 0 {
			collectionID = segment.GetCollectionID()
		} else if collectionID != segment.GetCollectionID() {
			err := errors.New("segments of a load balance task do not belong to the same collection")
			return err
		}
	}

	if collectionID == 0 {
		err := errors.New("a load balance task has to specify a collectionID or pass segments of a collection")
		return err
	}

	// check source and dst nodes belong to the same replica
	var replicaID int64 = -1
	for _, nodeID := range lbt.SourceNodeIDs {
		replica, err := lbt.getReplica(nodeID, collectionID)
		if err != nil {
			return err
		}
		if replicaID == -1 {
			replicaID = replica.GetReplicaID()
		} else if replicaID != replica.GetReplicaID() {
			err := errors.New("source nodes and destination nodes must be in the same replica group")
			return err
		}
	}

	if replicaID == -1 {
		return errors.New("source nodes is empty")
	}

	for _, nodeID := range lbt.DstNodeIDs {
		replica, err := lbt.getReplica(nodeID, collectionID)
		if err != nil {
			return err
		}
		if replicaID != replica.GetReplicaID() {
			err := errors.New("source nodes and destination nodes must be in the same replica group")
			return err
		}
	}

	lbt.replicaID = replicaID

	log.Info("start do loadBalanceTask",
		zap.Int32("trigger type", int32(lbt.triggerCondition)),
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))
	return nil
}

func (lbt *loadBalanceTask) execute(ctx context.Context) error {
	defer lbt.reduceRetryCount()

	if lbt.triggerCondition == querypb.TriggerCondition_NodeDown {
		var internalTasks []task
		for _, nodeID := range lbt.SourceNodeIDs {
			segmentID2Info := make(map[UniqueID]*querypb.SegmentInfo)
			dmChannel2WatchInfo := make(map[string]*querypb.DmChannelWatchInfo)
			recoveredCollectionIDs := make(typeutil.UniqueSet)
			segmentInfos := lbt.meta.getSegmentInfosByNode(nodeID)
			for _, segmentInfo := range segmentInfos {
				segmentID2Info[segmentInfo.SegmentID] = segmentInfo
				recoveredCollectionIDs.Insert(segmentInfo.CollectionID)
			}
			dmChannelWatchInfos := lbt.meta.getDmChannelInfosByNodeID(nodeID)
			for _, watchInfo := range dmChannelWatchInfos {
				dmChannel2WatchInfo[watchInfo.DmChannel] = watchInfo
				recoveredCollectionIDs.Insert(watchInfo.CollectionID)
			}

			for collectionID := range recoveredCollectionIDs {
				loadSegmentReqs := make([]*querypb.LoadSegmentsRequest, 0)
				watchDmChannelReqs := make([]*querypb.WatchDmChannelsRequest, 0)
				collectionInfo, err := lbt.meta.getCollectionInfoByID(collectionID)
				if err != nil {
					log.Error("loadBalanceTask: get collectionInfo from meta failed", zap.Int64("collectionID", collectionID), zap.Error(err))
					lbt.setResultInfo(err)
					return err
				}
				schema := collectionInfo.Schema
				var deltaChannelInfos []*datapb.VchannelInfo
				var dmChannelInfos []*datapb.VchannelInfo

				var toRecoverPartitionIDs []UniqueID
				if collectionInfo.LoadType == querypb.LoadType_LoadCollection {
					toRecoverPartitionIDs, err = lbt.broker.showPartitionIDs(ctx, collectionID)
					if err != nil {
						log.Error("loadBalanceTask: show collection's partitionIDs failed", zap.Int64("collectionID", collectionID), zap.Error(err))
						lbt.setResultInfo(err)
						panic(err)
					}
				} else {
					toRecoverPartitionIDs = collectionInfo.PartitionIDs
				}
				log.Info("loadBalanceTask: get collection's all partitionIDs", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", toRecoverPartitionIDs))
				replica, err := lbt.getReplica(nodeID, collectionID)
				if err != nil {
					lbt.setResultInfo(err)
					return err
				}

				for _, partitionID := range toRecoverPartitionIDs {
					vChannelInfos, binlogs, err := lbt.broker.getRecoveryInfo(lbt.ctx, collectionID, partitionID)
					if err != nil {
						log.Error("loadBalanceTask: getRecoveryInfo failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
						lbt.setResultInfo(err)
						panic(err)
					}

					for _, segmentBingLog := range binlogs {
						segmentID := segmentBingLog.SegmentID
						if _, ok := segmentID2Info[segmentID]; ok {
							segmentLoadInfo := lbt.broker.generateSegmentLoadInfo(ctx, collectionID, partitionID, segmentBingLog, true, schema)

							msgBase := proto.Clone(lbt.Base).(*commonpb.MsgBase)
							msgBase.MsgType = commonpb.MsgType_LoadSegments

							loadSegmentReq := &querypb.LoadSegmentsRequest{
								Base:         msgBase,
								Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
								Schema:       schema,
								CollectionID: collectionID,

								LoadMeta: &querypb.LoadMetaInfo{
									LoadType:     collectionInfo.LoadType,
									CollectionID: collectionID,
									PartitionIDs: toRecoverPartitionIDs,
								},
								ReplicaID: replica.ReplicaID,
							}

							loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
						}
					}

					for _, info := range vChannelInfos {
						deltaChannel, err := generateWatchDeltaChannelInfo(info)
						if err != nil {
							log.Error("loadBalanceTask: generateWatchDeltaChannelInfo failed", zap.Int64("collectionID", collectionID), zap.String("channelName", info.ChannelName), zap.Error(err))
							lbt.setResultInfo(err)
							panic(err)
						}
						deltaChannelInfos = append(deltaChannelInfos, deltaChannel)
						dmChannelInfos = append(dmChannelInfos, info)
					}
				}

				mergedDeltaChannel := mergeWatchDeltaChannelInfo(deltaChannelInfos)
				// If meta is not updated here, deltaChannel meta will not be available when loadSegment reschedule
				err = lbt.meta.setDeltaChannel(collectionID, mergedDeltaChannel)
				if err != nil {
					log.Error("loadBalanceTask: set delta channel info meta failed", zap.Int64("collectionID", collectionID), zap.Error(err))
					lbt.setResultInfo(err)
					panic(err)
				}

				mergedDmChannel := mergeDmChannelInfo(dmChannelInfos)
				for channelName, vChannelInfo := range mergedDmChannel {
					if _, ok := dmChannel2WatchInfo[channelName]; ok {
						msgBase := proto.Clone(lbt.Base).(*commonpb.MsgBase)
						msgBase.MsgType = commonpb.MsgType_WatchDmChannels
						watchRequest := &querypb.WatchDmChannelsRequest{
							Base:         msgBase,
							CollectionID: collectionID,
							Infos:        []*datapb.VchannelInfo{vChannelInfo},
							Schema:       schema,
							LoadMeta: &querypb.LoadMetaInfo{
								LoadType:     collectionInfo.LoadType,
								CollectionID: collectionID,
								PartitionIDs: toRecoverPartitionIDs,
							},
							ReplicaID: replica.ReplicaID,
						}

						if collectionInfo.LoadType == querypb.LoadType_LoadPartition {
							watchRequest.PartitionIDs = toRecoverPartitionIDs
						}

						watchDmChannelReqs = append(watchDmChannelReqs, watchRequest)
					}
				}

				tasks, err := assignInternalTask(ctx, lbt, lbt.meta, lbt.cluster, loadSegmentReqs, watchDmChannelReqs, true, lbt.SourceNodeIDs, lbt.DstNodeIDs, replica.GetReplicaID())
				if err != nil {
					log.Error("loadBalanceTask: assign child task failed", zap.Int64("sourceNodeID", nodeID))
					lbt.setResultInfo(err)
					panic(err)
				}
				internalTasks = append(internalTasks, tasks...)
			}
		}
		for _, internalTask := range internalTasks {
			lbt.addChildTask(internalTask)
			log.Info("loadBalanceTask: add a childTask", zap.Int32("task type", int32(internalTask.msgType())), zap.Any("task", internalTask))
		}
		log.Info("loadBalanceTask: assign child task done", zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs))
	} else if lbt.triggerCondition == querypb.TriggerCondition_LoadBalance {
		if err := lbt.checkForManualLoadBalance(); err != nil {
			lbt.setResultInfo(err)
			return err
		}
		if len(lbt.SourceNodeIDs) == 0 {
			err := errors.New("loadBalanceTask: empty source Node list to balance")
			log.Error(err.Error())
			lbt.setResultInfo(err)
			return err
		}

		balancedSegmentInfos := make(map[UniqueID]*querypb.SegmentInfo)
		balancedSegmentIDs := make([]UniqueID, 0)
		for _, nodeID := range lbt.SourceNodeIDs {
			nodeExist := lbt.cluster.hasNode(nodeID)
			if !nodeExist {
				err := fmt.Errorf("loadBalanceTask: query node %d is not exist to balance", nodeID)
				log.Error(err.Error())
				lbt.setResultInfo(err)
				return err
			}
			segmentInfos := lbt.meta.getSegmentInfosByNode(nodeID)
			for _, info := range segmentInfos {
				balancedSegmentInfos[info.SegmentID] = info
				balancedSegmentIDs = append(balancedSegmentIDs, info.SegmentID)
			}
		}

		// check balanced sealedSegmentIDs in request whether exist in query node
		for _, segmentID := range lbt.SealedSegmentIDs {
			if _, ok := balancedSegmentInfos[segmentID]; !ok {
				err := fmt.Errorf("loadBalanceTask: unloaded segment %d", segmentID)
				log.Warn(err.Error())
				lbt.setResultInfo(err)
				return err
			}
		}

		if len(lbt.SealedSegmentIDs) != 0 {
			balancedSegmentIDs = lbt.SealedSegmentIDs
		}

		col2PartitionIDs := make(map[UniqueID][]UniqueID)
		par2Segments := make(map[UniqueID][]*querypb.SegmentInfo)
		for _, segmentID := range balancedSegmentIDs {
			info := balancedSegmentInfos[segmentID]
			collectionID := info.CollectionID
			partitionID := info.PartitionID
			if _, ok := col2PartitionIDs[collectionID]; !ok {
				col2PartitionIDs[collectionID] = make([]UniqueID, 0)
			}
			if _, ok := par2Segments[partitionID]; !ok {
				col2PartitionIDs[collectionID] = append(col2PartitionIDs[collectionID], partitionID)
				par2Segments[partitionID] = make([]*querypb.SegmentInfo, 0)
			}
			par2Segments[partitionID] = append(par2Segments[partitionID], info)
		}

		loadSegmentReqs := make([]*querypb.LoadSegmentsRequest, 0)
		for collectionID, partitionIDs := range col2PartitionIDs {
			var watchDeltaChannels []*datapb.VchannelInfo
			collectionInfo, err := lbt.meta.getCollectionInfoByID(collectionID)
			if err != nil {
				log.Error("loadBalanceTask: can't find collectionID in meta", zap.Int64("collectionID", collectionID), zap.Error(err))
				lbt.setResultInfo(err)
				return err
			}
			for _, partitionID := range partitionIDs {
				dmChannelInfos, binlogs, err := lbt.broker.getRecoveryInfo(lbt.ctx, collectionID, partitionID)
				if err != nil {
					log.Error("loadBalanceTask: getRecoveryInfo failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
					lbt.setResultInfo(err)
					return err
				}

				segmentID2Binlog := make(map[UniqueID]*datapb.SegmentBinlogs)
				for _, binlog := range binlogs {
					segmentID2Binlog[binlog.SegmentID] = binlog
				}

				for _, segmentInfo := range par2Segments[partitionID] {
					segmentID := segmentInfo.SegmentID
					if _, ok := segmentID2Binlog[segmentID]; !ok {
						log.Warn("loadBalanceTask: can't find binlog of segment to balance, may be has been compacted", zap.Int64("segmentID", segmentID))
						continue
					}

					segmentBingLog := segmentID2Binlog[segmentID]
					segmentLoadInfo := lbt.broker.generateSegmentLoadInfo(ctx, collectionID, partitionID, segmentBingLog, true, collectionInfo.Schema)
					msgBase := proto.Clone(lbt.Base).(*commonpb.MsgBase)
					msgBase.MsgType = commonpb.MsgType_LoadSegments
					loadSegmentReq := &querypb.LoadSegmentsRequest{
						Base:         msgBase,
						Infos:        []*querypb.SegmentLoadInfo{segmentLoadInfo},
						Schema:       collectionInfo.Schema,
						CollectionID: collectionID,
						ReplicaID:    lbt.replicaID,
					}
					loadSegmentReqs = append(loadSegmentReqs, loadSegmentReq)
				}

				for _, info := range dmChannelInfos {
					deltaChannel, err := generateWatchDeltaChannelInfo(info)
					if err != nil {
						return err
					}
					watchDeltaChannels = append(watchDeltaChannels, deltaChannel)
				}
			}
			mergedDeltaChannels := mergeWatchDeltaChannelInfo(watchDeltaChannels)
			// If meta is not updated here, deltaChannel meta will not be available when loadSegment reschedule
			err = lbt.meta.setDeltaChannel(collectionID, mergedDeltaChannels)
			if err != nil {
				log.Error("loadBalanceTask: set delta channel info to meta failed", zap.Error(err))
				lbt.setResultInfo(err)
				return err
			}
		}
		internalTasks, err := assignInternalTask(ctx, lbt, lbt.meta, lbt.cluster, loadSegmentReqs, nil, false, lbt.SourceNodeIDs, lbt.DstNodeIDs, lbt.replicaID)
		if err != nil {
			log.Error("loadBalanceTask: assign child task failed", zap.Any("balance request", lbt.LoadBalanceRequest))
			lbt.setResultInfo(err)
			return err
		}
		for _, internalTask := range internalTasks {
			lbt.addChildTask(internalTask)
			log.Info("loadBalanceTask: add a childTask", zap.Int32("task type", int32(internalTask.msgType())), zap.Any("balance request", lbt.LoadBalanceRequest))
		}
		log.Info("loadBalanceTask: assign child task done", zap.Any("balance request", lbt.LoadBalanceRequest))
	}

	log.Info("loadBalanceTask Execute done",
		zap.Int32("trigger type", int32(lbt.triggerCondition)),
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))
	return nil
}

func (lbt *loadBalanceTask) getReplica(nodeID, collectionID int64) (*milvuspb.ReplicaInfo, error) {
	replicas, err := lbt.meta.getReplicasByNodeID(nodeID)
	if err != nil {
		return nil, err
	}
	for _, replica := range replicas {
		if replica.GetCollectionID() == collectionID {
			return replica, nil
		}
	}
	return nil, fmt.Errorf("unable to find replicas of collection %d and node %d", collectionID, nodeID)
}

func (lbt *loadBalanceTask) postExecute(context.Context) error {
	if lbt.getResultInfo().ErrorCode != commonpb.ErrorCode_Success {
		lbt.clearChildTasks()
	}

	// if loadBalanceTask execute failed after query node down, the lbt.getResultInfo().ErrorCode will be set to commonpb.ErrorCode_UnexpectedError
	// then the queryCoord will panic, and the nodeInfo should not be removed immediately
	// after queryCoord recovery, the balanceTask will redo
	if lbt.triggerCondition == querypb.TriggerCondition_NodeDown && lbt.getResultInfo().ErrorCode == commonpb.ErrorCode_Success {
		for _, offlineNodeID := range lbt.SourceNodeIDs {
			err := lbt.cluster.removeNodeInfo(offlineNodeID)
			if err != nil {
				//TODO:: clear node info after removeNodeInfo failed
				log.Warn("loadBalanceTask: occur error when removing node info from cluster",
					zap.Int64("nodeID", offlineNodeID),
					zap.Error(err))
				continue
			}
		}
	}

	log.Info("loadBalanceTask postExecute done",
		zap.Int32("trigger type", int32(lbt.triggerCondition)),
		zap.Int64s("sourceNodeIDs", lbt.SourceNodeIDs),
		zap.Any("balanceReason", lbt.BalanceReason),
		zap.Int64("taskID", lbt.getTaskID()))
	return nil
}

func (lbt *loadBalanceTask) globalPostExecute(ctx context.Context) error {
	if len(lbt.getChildTask()) > 0 {
		replicas := make(map[UniqueID]*milvuspb.ReplicaInfo)
		segments := make(map[UniqueID]*querypb.SegmentInfo)

		for _, id := range lbt.SourceNodeIDs {
			for _, segment := range lbt.meta.getSegmentInfosByNode(id) {
				segments[segment.SegmentID] = segment
			}

			nodeReplicas, err := lbt.meta.getReplicasByNodeID(id)
			if err != nil {
				log.Warn("failed to get replicas for removing offline querynode from it",
					zap.Int64("querynodeID", id),
					zap.Error(err))

				continue
			}
			for _, replica := range nodeReplicas {
				replicas[replica.ReplicaID] = replica
			}
		}

		log.Debug("removing offline nodes from replicas and segments...",
			zap.Int("len(replicas)", len(replicas)),
			zap.Int("len(segments)", len(segments)),
			zap.Int64("trigger task ID", lbt.getTaskID()),
		)

		// Remove offline nodes from replica
		wg := errgroup.Group{}
		if lbt.triggerCondition == querypb.TriggerCondition_NodeDown {
			offlineNodes := make(typeutil.UniqueSet, len(lbt.SourceNodeIDs))
			for _, nodeID := range lbt.SourceNodeIDs {
				offlineNodes.Insert(nodeID)
			}

			log.Debug("removing offline nodes from replicas and segments...",
				zap.Int("len(replicas)", len(replicas)),
				zap.Int("len(segments)", len(segments)),
				zap.Int64("trigger task ID", lbt.getTaskID()),
			)

			for _, replica := range replicas {
				replica := replica
				wg.Go(func() error {
					onlineNodes := make([]UniqueID, 0, len(replica.NodeIds))
					for _, nodeID := range replica.NodeIds {
						if !offlineNodes.Contain(nodeID) {
							onlineNodes = append(onlineNodes, nodeID)
						}
					}
					replica.NodeIds = onlineNodes

					err := lbt.meta.setReplicaInfo(replica)
					if err != nil {
						log.Error("failed to remove offline nodes from replica info",
							zap.Int64("replicaID", replica.ReplicaID),
							zap.Error(err))
						return err
					}

					return nil
				})
			}
		}

		for _, segment := range segments {
			segment := segment
			wg.Go(func() error {
				segment.NodeID = -1
				segment.NodeIds = removeFromSlice(segment.NodeIds, lbt.SourceNodeIDs...)
				if len(segment.NodeIds) > 0 {
					segment.NodeID = segment.NodeIds[0]
				}

				err := lbt.meta.saveSegmentInfo(segment)
				if err != nil {
					log.Error("failed to remove offline nodes from segment info",
						zap.Int64("segmentID", segment.SegmentID),
						zap.Error(err))

					return err
				}

				return nil
			})
		}
		for _, childTask := range lbt.getChildTask() {
			if task, ok := childTask.(*watchDmChannelTask); ok {
				wg.Go(func() error {
					nodeInfo, err := lbt.cluster.getNodeInfoByID(task.NodeID)
					if err != nil {
						log.Error("failed to get node info to update shard leader info",
							zap.Int64("triggerTaskID", lbt.getTaskID()),
							zap.Int64("taskID", task.getTaskID()),
							zap.Int64("nodeID", task.NodeID),
							zap.String("dmChannel", task.Infos[0].ChannelName),
							zap.Error(err))
						return err
					}

					replica, err := lbt.meta.getReplicaByID(task.ReplicaID)
					if err != nil {
						log.Error("failed to get replica to update shard leader info",
							zap.Int64("triggerTaskID", lbt.getTaskID()),
							zap.Int64("taskID", task.getTaskID()),
							zap.Int64("replicaID", task.ReplicaID),
							zap.String("dmChannel", task.Infos[0].ChannelName),
							zap.Error(err))
						return err
					}

					for _, shard := range replica.ShardReplicas {
						if shard.DmChannelName == task.Infos[0].ChannelName {
							log.Debug("LoadBalance: update shard leader",
								zap.Int64("triggerTaskID", lbt.getTaskID()),
								zap.Int64("taskID", task.getTaskID()),
								zap.Int64("oldLeader", shard.LeaderID),
								zap.Int64("newLeader", task.NodeID))
							shard.LeaderID = task.NodeID
							shard.LeaderAddr = nodeInfo.(*queryNode).address
							break
						}
					}

					err = lbt.meta.setReplicaInfo(replica)
					if err != nil {
						log.Error("failed to remove offline nodes from replica info",
							zap.Int64("triggerTaskID", lbt.getTaskID()),
							zap.Int64("taskID", task.getTaskID()),
							zap.Int64("replicaID", replica.ReplicaID),
							zap.Error(err))
						return err
					}

					return nil
				})
			}
		}
		err := wg.Wait()
		if err != nil {
			return err
		}

		err = syncReplicaSegments(ctx, lbt.cluster, lbt.getChildTask())
		if err != nil {
			return err
		}
	}

	return nil
}

func assignInternalTask(ctx context.Context,
	parentTask task, meta Meta, cluster Cluster,
	loadSegmentRequests []*querypb.LoadSegmentsRequest,
	watchDmChannelRequests []*querypb.WatchDmChannelsRequest,
	wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64) ([]task, error) {

	internalTasks := make([]task, 0)
	err := cluster.allocateSegmentsToQueryNode(ctx, loadSegmentRequests, wait, excludeNodeIDs, includeNodeIDs, replicaID)
	if err != nil {
		log.Error("assignInternalTask: assign segment to node failed", zap.Error(err))
		return nil, err
	}
	log.Info("assignInternalTask: assign segment to node success", zap.Int("load segments", len(loadSegmentRequests)))

	err = cluster.allocateChannelsToQueryNode(ctx, watchDmChannelRequests, wait, excludeNodeIDs, includeNodeIDs, replicaID)
	if err != nil {
		log.Error("assignInternalTask: assign dmChannel to node failed", zap.Error(err))
		return nil, err
	}
	log.Info("assignInternalTask: assign dmChannel to node success", zap.Int("watch dmchannels", len(watchDmChannelRequests)))

	if len(loadSegmentRequests) > 0 {
		sort.Slice(loadSegmentRequests, func(i, j int) bool {
			return loadSegmentRequests[i].CollectionID < loadSegmentRequests[j].CollectionID ||
				loadSegmentRequests[i].CollectionID == loadSegmentRequests[j].CollectionID && loadSegmentRequests[i].DstNodeID < loadSegmentRequests[j].DstNodeID
		})

		batchReq := loadSegmentRequests[0]
		batchSize := proto.Size(batchReq)
		for _, req := range loadSegmentRequests[1:] {
			// Pack current batch, switch to new batch
			if req.CollectionID != batchReq.CollectionID || req.DstNodeID != batchReq.DstNodeID ||
				batchSize+proto.Size(req) > MaxSendSizeToEtcd {
				baseTask := newBaseTask(ctx, parentTask.getTriggerCondition())
				baseTask.setParentTask(parentTask)
				loadSegmentTask := &loadSegmentTask{
					baseTask:            baseTask,
					LoadSegmentsRequest: batchReq,
					meta:                meta,
					cluster:             cluster,
					excludeNodeIDs:      excludeNodeIDs,
				}
				internalTasks = append(internalTasks, loadSegmentTask)

				batchReq = req
				batchSize = proto.Size(batchReq)
			} else {
				batchReq.Infos = append(batchReq.Infos, req.Infos...)
				batchSize += proto.Size(req)
			}
		}

		// Pack the last batch
		baseTask := newBaseTask(ctx, parentTask.getTriggerCondition())
		baseTask.setParentTask(parentTask)
		loadSegmentTask := &loadSegmentTask{
			baseTask:            baseTask,
			LoadSegmentsRequest: batchReq,
			meta:                meta,
			cluster:             cluster,
			excludeNodeIDs:      excludeNodeIDs,
		}
		internalTasks = append(internalTasks, loadSegmentTask)
	}

	for _, req := range watchDmChannelRequests {
		baseTask := newBaseTask(ctx, parentTask.getTriggerCondition())
		baseTask.setParentTask(parentTask)
		watchDmChannelTask := &watchDmChannelTask{
			baseTask:               baseTask,
			WatchDmChannelsRequest: req,
			meta:                   meta,
			cluster:                cluster,
			excludeNodeIDs:         excludeNodeIDs,
		}
		internalTasks = append(internalTasks, watchDmChannelTask)
	}

	return internalTasks, nil
}

func generateWatchDeltaChannelInfo(info *datapb.VchannelInfo) (*datapb.VchannelInfo, error) {
	deltaChannelName, err := funcutil.ConvertChannelName(info.ChannelName, Params.CommonCfg.RootCoordDml, Params.CommonCfg.RootCoordDelta)
	if err != nil {
		return nil, err
	}
	deltaChannel := proto.Clone(info).(*datapb.VchannelInfo)
	deltaChannel.ChannelName = deltaChannelName
	deltaChannel.UnflushedSegments = nil
	deltaChannel.FlushedSegments = nil
	deltaChannel.DroppedSegments = nil
	return deltaChannel, nil
}

func mergeWatchDeltaChannelInfo(infos []*datapb.VchannelInfo) []*datapb.VchannelInfo {
	minPositions := make(map[string]int)
	for index, info := range infos {
		_, ok := minPositions[info.ChannelName]
		if !ok {
			minPositions[info.ChannelName] = index
		}
		minTimeStampIndex := minPositions[info.ChannelName]
		if info.SeekPosition.GetTimestamp() < infos[minTimeStampIndex].SeekPosition.GetTimestamp() {
			minPositions[info.ChannelName] = index
		}
	}
	var result []*datapb.VchannelInfo
	for _, index := range minPositions {
		result = append(result, infos[index])
	}
	log.Info("merge delta channels finished",
		zap.Any("origin info length", len(infos)),
		zap.Any("merged info length", len(result)),
	)
	return result
}

func mergeDmChannelInfo(infos []*datapb.VchannelInfo) map[string]*datapb.VchannelInfo {
	minPositions := make(map[string]*datapb.VchannelInfo)
	for _, info := range infos {
		if _, ok := minPositions[info.ChannelName]; !ok {
			minPositions[info.ChannelName] = info
			continue
		}
		minPositionInfo := minPositions[info.ChannelName]
		if info.SeekPosition.GetTimestamp() < minPositionInfo.SeekPosition.GetTimestamp() {
			minPositionInfo.SeekPosition = info.SeekPosition
		}
		minPositionInfo.DroppedSegments = append(minPositionInfo.DroppedSegments, info.DroppedSegments...)
		minPositionInfo.UnflushedSegments = append(minPositionInfo.UnflushedSegments, info.UnflushedSegments...)
		minPositionInfo.FlushedSegments = append(minPositionInfo.FlushedSegments, info.FlushedSegments...)
	}

	return minPositions
}
