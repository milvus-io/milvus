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
	"sort"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
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
	finishContext()
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

func newBaseTaskWithRetry(ctx context.Context, triggerType querypb.TriggerCondition, retryCount int) *baseTask {
	baseTask := newBaseTask(ctx, triggerType)
	baseTask.retryCount = retryCount
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
// Child task may be loadSegmentTask or watchDmChannelTask
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

	if err != nil {
		bt.setState(taskFailed)
	}

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

// finishContext calls the cancel function for the trace ctx.
func (bt *baseTask) finishContext() {
	if bt.cancel != nil {
		bt.cancel()
	}
}

type loadSegmentTask struct {
	*baseTask
	*querypb.LoadSegmentsRequest
	meta           Meta
	cluster        Cluster
	excludeNodeIDs []int64

	broker *globalMetaBroker
}

func (lst *loadSegmentTask) msgBase() *commonpb.MsgBase {
	return lst.Base
}

func (lst *loadSegmentTask) marshal() ([]byte, error) {
	return proto.Marshal(lst.LoadSegmentsRequest)
}

func (lst *loadSegmentTask) isValid() bool {
	online, err := lst.cluster.IsOnline(lst.DstNodeID)
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

func (lst *loadSegmentTask) preExecute(ctx context.Context) error {
	segmentIDs := make([]UniqueID, 0)
	for _, info := range lst.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	lst.setResultInfo(nil)
	log.Info("start do loadSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs),
		zap.Int64("loaded nodeID", lst.DstNodeID),
		zap.Int64("taskID", lst.getTaskID()))

	if err := lst.broker.acquireSegmentsReferLock(ctx, lst.taskID, segmentIDs); err != nil {
		log.Error("acquire reference lock on segments failed", zap.Int64s("segmentIDs", segmentIDs),
			zap.Error(err))
		return err
	}
	return nil
}

func (lst *loadSegmentTask) execute(ctx context.Context) error {
	defer lst.reduceRetryCount()

	err := lst.cluster.LoadSegments(ctx, lst.DstNodeID, lst.LoadSegmentsRequest)
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
	segmentIDs := make([]UniqueID, 0)
	for _, info := range lst.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	if err := lst.broker.releaseSegmentReferLock(lst.ctx, lst.taskID, segmentIDs); err != nil {
		panic(err)
	}

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

	reScheduledTasks, err := assignInternalTask(ctx, lst.getParentTask(), lst.meta, lst.cluster, loadSegmentReqs, nil, false, lst.excludeNodeIDs, nil, lst.ReplicaID, lst.broker)
	if err != nil {
		log.Error("loadSegment reschedule failed", zap.Int64s("excludeNodes", lst.excludeNodeIDs), zap.Int64("taskID", lst.getTaskID()), zap.Error(err))
		return nil, err
	}

	return reScheduledTasks, nil
}

type releaseSegmentTask struct {
	*baseTask
	*querypb.ReleaseSegmentsRequest
	cluster  Cluster
	leaderID UniqueID
}

func (rst *releaseSegmentTask) msgBase() *commonpb.MsgBase {
	return rst.Base
}

func (rst *releaseSegmentTask) marshal() ([]byte, error) {
	return proto.Marshal(rst.ReleaseSegmentsRequest)
}

func (rst *releaseSegmentTask) isValid() bool {
	online, err := rst.cluster.IsOnline(rst.NodeID)
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

	err := rst.cluster.ReleaseSegments(rst.ctx, rst.leaderID, rst.ReleaseSegmentsRequest)
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
	return proto.Marshal(thinWatchDmChannelsRequest(wdt.WatchDmChannelsRequest))
}

func (wdt *watchDmChannelTask) isValid() bool {
	online, err := wdt.cluster.IsOnline(wdt.NodeID)
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

	err := wdt.cluster.WatchDmChannels(wdt.ctx, wdt.NodeID, wdt.WatchDmChannelsRequest)
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
			SegmentInfos: wdt.SegmentInfos,
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
	reScheduledTasks, err := assignInternalTask(ctx, wdt.parentTask, wdt.meta, wdt.cluster, nil, watchDmChannelReqs, wait2AssignTaskSuccess, wdt.excludeNodeIDs, nil, wdt.ReplicaID, nil)
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
	online, err := wdt.cluster.IsOnline(wdt.NodeID)
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

	err := wdt.cluster.WatchDeltaChannels(wdt.ctx, wdt.NodeID, wdt.WatchDeltaChannelsRequest)
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

func assignInternalTask(ctx context.Context,
	parentTask task, meta Meta, cluster Cluster,
	loadSegmentRequests []*querypb.LoadSegmentsRequest,
	watchDmChannelRequests []*querypb.WatchDmChannelsRequest,
	wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64,
	broker *globalMetaBroker) ([]task, error) {

	internalTasks := make([]task, 0)
	err := cluster.AllocateSegmentsToQueryNode(ctx, loadSegmentRequests, wait, excludeNodeIDs, includeNodeIDs, replicaID)
	if err != nil {
		log.Error("assignInternalTask: assign segment to node failed", zap.Error(err))
		return nil, err
	}
	log.Info("assignInternalTask: assign segment to node success", zap.Int("load segments", len(loadSegmentRequests)))

	err = cluster.AllocateChannelsToQueryNode(ctx, watchDmChannelRequests, wait, excludeNodeIDs, includeNodeIDs, replicaID)
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
					broker:              broker,
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
			broker:              broker,
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
	deltaChannel.UnflushedSegmentIds = nil
	deltaChannel.FlushedSegmentIds = nil
	deltaChannel.DroppedSegmentIds = nil
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
		minPositionInfo.DroppedSegmentIds = append(minPositionInfo.DroppedSegmentIds, info.DroppedSegmentIds...)
		minPositionInfo.UnflushedSegmentIds = append(minPositionInfo.UnflushedSegmentIds, info.UnflushedSegmentIds...)
		minPositionInfo.FlushedSegmentIds = append(minPositionInfo.FlushedSegmentIds, info.FlushedSegmentIds...)
	}

	return minPositions
}
