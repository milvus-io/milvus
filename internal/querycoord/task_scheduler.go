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
	"container/list"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/trace"
	oplog "github.com/opentracing/opentracing-go/log"
)

// taskQueue is used to cache triggerTasks
type taskQueue struct {
	tasks *list.List

	maxTask  int64
	taskChan chan int // to block scheduler

	sync.Mutex
}

// Chan returns the taskChan of taskQueue
func (queue *taskQueue) Chan() <-chan int {
	return queue.taskChan
}

func (queue *taskQueue) taskEmpty() bool {
	queue.Lock()
	defer queue.Unlock()
	return queue.tasks.Len() == 0
}

func (queue *taskQueue) taskFull() bool {
	return int64(queue.tasks.Len()) >= queue.maxTask
}

func (queue *taskQueue) willLoadOrRelease(collectionID UniqueID) commonpb.MsgType {
	queue.Lock()
	defer queue.Unlock()
	// check the last task of this collection is load task or release task
	for e := queue.tasks.Back(); e != nil; e = e.Prev() {
		msgType := e.Value.(task).msgType()
		switch msgType {
		case commonpb.MsgType_LoadCollection:
			if e.Value.(task).(*loadCollectionTask).GetCollectionID() == collectionID {
				return msgType
			}
		case commonpb.MsgType_LoadPartitions:
			if e.Value.(task).(*loadPartitionTask).GetCollectionID() == collectionID {
				return msgType
			}
		case commonpb.MsgType_ReleaseCollection:
			if e.Value.(task).(*releaseCollectionTask).GetCollectionID() == collectionID {
				return msgType
			}
		case commonpb.MsgType_ReleasePartitions:
			if e.Value.(task).(*releasePartitionTask).GetCollectionID() == collectionID {
				return msgType
			}
		}
	}
	return commonpb.MsgType_Undefined
}

func (queue *taskQueue) addTask(t task) {
	queue.Lock()
	defer queue.Unlock()

	if queue.tasks.Len() == 0 {
		queue.taskChan <- 1
		queue.tasks.PushBack(t)
		metrics.QueryCoordNumParentTasks.WithLabelValues().Inc()
		return
	}

	for e := queue.tasks.Back(); e != nil; e = e.Prev() {
		if t.taskPriority() > e.Value.(task).taskPriority() {
			if e.Prev() == nil {
				queue.taskChan <- 1
				queue.tasks.InsertBefore(t, e)
				break
			}
			continue
		}
		//TODO:: take care of timestamp
		queue.taskChan <- 1
		queue.tasks.InsertAfter(t, e)
		break
	}
	metrics.QueryCoordNumParentTasks.WithLabelValues().Inc()
}

func (queue *taskQueue) addTaskToFront(t task) {
	queue.taskChan <- 1
	if queue.tasks.Len() == 0 {
		queue.tasks.PushBack(t)
	} else {
		queue.tasks.PushFront(t)
	}

	metrics.QueryCoordNumParentTasks.WithLabelValues().Inc()
}

// PopTask pops a trigger task from task list
func (queue *taskQueue) popTask() task {
	queue.Lock()
	defer queue.Unlock()

	if queue.tasks.Len() <= 0 {
		return nil
	}

	ft := queue.tasks.Front()
	queue.tasks.Remove(ft)

	metrics.QueryCoordNumParentTasks.WithLabelValues().Dec()

	return ft.Value.(task)
}

// NewTaskQueue creates a new task queue for scheduler to cache trigger tasks
func newTaskQueue() *taskQueue {
	return &taskQueue{
		tasks:    list.New(),
		maxTask:  1024,
		taskChan: make(chan int, 1024),
	}
}

// TaskScheduler controls the scheduling of trigger tasks and internal tasks
type TaskScheduler struct {
	triggerTaskQueue         *taskQueue
	activateTaskChan         chan task
	meta                     Meta
	cluster                  Cluster
	taskIDAllocator          func() (UniqueID, error)
	client                   *etcdkv.EtcdKV
	stopActivateTaskLoopChan chan int

	broker *globalMetaBroker

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewTaskScheduler reloads tasks from kv and returns a new taskScheduler
func newTaskScheduler(ctx context.Context,
	meta Meta,
	cluster Cluster,
	kv *etcdkv.EtcdKV,
	broker *globalMetaBroker,
	idAllocator func() (UniqueID, error)) (*TaskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	taskChan := make(chan task, 1024)
	stopTaskLoopChan := make(chan int, 1)
	s := &TaskScheduler{
		ctx:                      ctx1,
		cancel:                   cancel,
		meta:                     meta,
		cluster:                  cluster,
		taskIDAllocator:          idAllocator,
		activateTaskChan:         taskChan,
		client:                   kv,
		stopActivateTaskLoopChan: stopTaskLoopChan,
		broker:                   broker,
	}
	s.triggerTaskQueue = newTaskQueue()

	err := s.reloadFromKV()
	if err != nil {
		log.Error("reload task from kv failed", zap.Error(err))
		return nil, err
	}

	return s, nil
}

func (scheduler *TaskScheduler) reloadFromKV() error {
	triggerTaskIDKeys, triggerTaskValues, err := scheduler.client.LoadWithPrefix(triggerTaskPrefix)
	if err != nil {
		return err
	}
	activeTaskIDKeys, activeTaskValues, err := scheduler.client.LoadWithPrefix(activeTaskPrefix)
	if err != nil {
		return err
	}
	taskInfoKeys, taskInfoValues, err := scheduler.client.LoadWithPrefix(taskInfoPrefix)
	if err != nil {
		return err
	}

	triggerTasks := make(map[int64]task)
	for index := range triggerTaskIDKeys {
		taskID, err := strconv.ParseInt(filepath.Base(triggerTaskIDKeys[index]), 10, 64)
		if err != nil {
			return err
		}
		t, err := scheduler.unmarshalTask(taskID, triggerTaskValues[index])
		if err != nil {
			return err
		}
		triggerTasks[taskID] = t
	}

	metrics.QueryCoordNumParentTasks.WithLabelValues().Set(float64(len(triggerTasks)))

	activeTasks := make(map[int64]task)
	for index := range activeTaskIDKeys {
		taskID, err := strconv.ParseInt(filepath.Base(activeTaskIDKeys[index]), 10, 64)
		if err != nil {
			return err
		}
		t, err := scheduler.unmarshalTask(taskID, activeTaskValues[index])
		if err != nil {
			return err
		}
		activeTasks[taskID] = t
	}
	metrics.QueryCoordNumChildTasks.WithLabelValues().Set(float64(len(triggerTasks)))

	taskInfos := make(map[int64]taskState)
	for index := range taskInfoKeys {
		taskID, err := strconv.ParseInt(filepath.Base(taskInfoKeys[index]), 10, 64)
		if err != nil {
			return err
		}
		value, err := strconv.ParseInt(taskInfoValues[index], 10, 64)
		if err != nil {
			return err
		}
		state := taskState(value)
		taskInfos[taskID] = state
		if _, ok := triggerTasks[taskID]; !ok {
			log.Error("reloadFromKV: taskStateInfo and triggerTaskInfo are inconsistent")
			continue
		}
		triggerTasks[taskID].setState(state)
	}

	var doneTriggerTask task
	for _, t := range triggerTasks {
		if t.getState() == taskDone {
			doneTriggerTask = t
			for _, childTask := range activeTasks {
				childTask.setParentTask(t) //replace child task after reScheduler
				t.addChildTask(childTask)
			}
			t.setResultInfo(nil)
			continue
		}
		scheduler.triggerTaskQueue.addTask(t)
	}

	if doneTriggerTask != nil {
		scheduler.triggerTaskQueue.addTaskToFront(doneTriggerTask)
	}

	return nil
}

func (scheduler *TaskScheduler) unmarshalTask(taskID UniqueID, t string) (task, error) {
	header := commonpb.MsgHeader{}
	err := proto.Unmarshal([]byte(t), &header)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal message header, err %s ", err.Error())
	}
	var newTask task
	baseTask := newBaseTask(scheduler.ctx, querypb.TriggerCondition_GrpcRequest)
	switch header.Base.MsgType {
	case commonpb.MsgType_LoadCollection:
		loadReq := querypb.LoadCollectionRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		loadCollectionTask := &loadCollectionTask{
			baseTask:              baseTask,
			LoadCollectionRequest: &loadReq,
			broker:                scheduler.broker,
			cluster:               scheduler.cluster,
			meta:                  scheduler.meta,
		}
		newTask = loadCollectionTask
	case commonpb.MsgType_LoadPartitions:
		loadReq := querypb.LoadPartitionsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		loadPartitionTask := &loadPartitionTask{
			baseTask:              baseTask,
			LoadPartitionsRequest: &loadReq,
			broker:                scheduler.broker,
			cluster:               scheduler.cluster,
			meta:                  scheduler.meta,
		}
		newTask = loadPartitionTask
	case commonpb.MsgType_ReleaseCollection:
		loadReq := querypb.ReleaseCollectionRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		releaseCollectionTask := &releaseCollectionTask{
			baseTask:                 baseTask,
			ReleaseCollectionRequest: &loadReq,
			cluster:                  scheduler.cluster,
			meta:                     scheduler.meta,
			broker:                   scheduler.broker,
		}
		newTask = releaseCollectionTask
	case commonpb.MsgType_ReleasePartitions:
		loadReq := querypb.ReleasePartitionsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		releasePartitionTask := &releasePartitionTask{
			baseTask:                 baseTask,
			ReleasePartitionsRequest: &loadReq,
			cluster:                  scheduler.cluster,
			meta:                     scheduler.meta,
		}
		newTask = releasePartitionTask
	case commonpb.MsgType_LoadSegments:
		//TODO::trigger condition may be different
		loadReq := querypb.LoadSegmentsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		loadSegmentTask := &loadSegmentTask{
			baseTask:            baseTask,
			LoadSegmentsRequest: &loadReq,
			cluster:             scheduler.cluster,
			meta:                scheduler.meta,
			excludeNodeIDs:      []int64{},
			broker:              scheduler.broker,
		}
		newTask = loadSegmentTask
	case commonpb.MsgType_ReleaseSegments:
		//TODO::trigger condition may be different
		loadReq := querypb.ReleaseSegmentsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		releaseSegmentTask := &releaseSegmentTask{
			baseTask:               baseTask,
			ReleaseSegmentsRequest: &loadReq,
			cluster:                scheduler.cluster,
		}
		newTask = releaseSegmentTask
	case commonpb.MsgType_WatchDmChannels:
		//TODO::trigger condition may be different
		req := querypb.WatchDmChannelsRequest{}
		err = proto.Unmarshal([]byte(t), &req)
		if err != nil {
			return nil, err
		}
		fullReq, err := generateFullWatchDmChannelsRequest(baseTask.traceCtx(), scheduler.broker, &req)
		if err != nil {
			return nil, err
		}
		watchDmChannelTask := &watchDmChannelTask{
			baseTask:               baseTask,
			WatchDmChannelsRequest: fullReq,
			cluster:                scheduler.cluster,
			meta:                   scheduler.meta,
			excludeNodeIDs:         []int64{},
		}
		newTask = watchDmChannelTask
	case commonpb.MsgType_WatchDeltaChannels:
		log.Warn("legacy WatchDeltaChannels type found, ignore")
	case commonpb.MsgType_WatchQueryChannels:
		//Deprecated WatchQueryChannel
		log.Warn("legacy WatchQueryChannels type found, ignore")
	case commonpb.MsgType_LoadBalanceSegments:
		//TODO::trigger condition may be different
		loadReq := querypb.LoadBalanceRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		// if triggerCondition == nodeDown, and the queryNode resources are insufficient,
		// queryCoord will waits until queryNode can load the data, ensuring that the data is not lost
		baseTask = newBaseTaskWithRetry(scheduler.ctx, loadReq.BalanceReason, 0)
		baseTask.setTriggerCondition(loadReq.BalanceReason)
		loadBalanceTask := &loadBalanceTask{
			baseTask:           baseTask,
			LoadBalanceRequest: &loadReq,
			broker:             scheduler.broker,
			cluster:            scheduler.cluster,
			meta:               scheduler.meta,
		}
		newTask = loadBalanceTask
	case commonpb.MsgType_HandoffSegments:
		handoffReq := querypb.HandoffSegmentsRequest{}
		err = proto.Unmarshal([]byte(t), &handoffReq)
		if err != nil {
			return nil, err
		}
		handoffTask := &handoffTask{
			baseTask:               baseTask,
			HandoffSegmentsRequest: &handoffReq,
			broker:                 scheduler.broker,
			cluster:                scheduler.cluster,
			meta:                   scheduler.meta,
		}
		newTask = handoffTask

	default:
		err = errors.New("inValid msg type when unMarshal task")
		log.Error(err.Error())
		return nil, err
	}

	newTask.setTaskID(taskID)
	return newTask, nil
}

// Enqueue pushs a trigger task to triggerTaskQueue and assigns task id
func (scheduler *TaskScheduler) Enqueue(t task) error {
	// TODO, loadbalance, handoff and other task may not want to be persisted
	id, err := scheduler.taskIDAllocator()
	if err != nil {
		log.Error("allocator trigger taskID failed", zap.Error(err))
		return err
	}
	t.setTaskID(id)
	kvs := make(map[string]string)
	taskKey := fmt.Sprintf("%s/%d", triggerTaskPrefix, t.getTaskID())
	blobs, err := t.marshal()
	if err != nil {
		log.Error("error when save marshal task", zap.Int64("taskID", t.getTaskID()), zap.Error(err))
		return err
	}
	kvs[taskKey] = string(blobs)
	stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.getTaskID())
	kvs[stateKey] = strconv.Itoa(int(taskUndo))
	err = scheduler.client.MultiSave(kvs)
	if err != nil {
		//TODO::clean etcd meta
		log.Error("error when save trigger task to etcd", zap.Int64("taskID", t.getTaskID()), zap.Error(err))
		return err
	}
	t.setState(taskUndo)
	scheduler.triggerTaskQueue.addTask(t)
	log.Debug("EnQueue a triggerTask and save to etcd", zap.Int64("taskID", t.getTaskID()))

	return nil
}

func (scheduler *TaskScheduler) processTask(t task) error {
	log.Info("begin to process task", zap.Int64("taskID", t.getTaskID()), zap.String("task", reflect.TypeOf(t).String()))
	var taskInfoKey string
	// assign taskID for childTask and update triggerTask's childTask to etcd
	updateKVFn := func(parentTask task) error {
		// TODO:: if childTask.type == loadSegment, then only save segmentID to etcd instead of binlog paths
		// The binlog paths of each segment will be written into etcd in advance
		// The binlog paths will be filled in through etcd when load segment grpc is called
		for _, childTask := range parentTask.getChildTask() {
			kvs := make(map[string]string)
			id, err := scheduler.taskIDAllocator()
			if err != nil {
				return err
			}
			childTask.setTaskID(id)
			childTaskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, childTask.getTaskID())
			var protoSize int
			switch childTask.msgType() {
			case commonpb.MsgType_LoadSegments:
				protoSize = proto.Size(childTask.(*loadSegmentTask).LoadSegmentsRequest)
			case commonpb.MsgType_WatchDmChannels:
				protoSize = proto.Size(childTask.(*watchDmChannelTask).WatchDmChannelsRequest)
			default:
				//TODO::
			}
			log.Debug("updateKVFn: the size of internal request",
				zap.Int("size", protoSize),
				zap.Int64("taskID", childTask.getTaskID()),
				zap.String("type", childTask.msgType().String()))
			blobs, err := childTask.marshal()
			if err != nil {
				return err
			}
			kvs[childTaskKey] = string(blobs)
			stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, childTask.getTaskID())
			kvs[stateKey] = strconv.Itoa(int(taskUndo))
			err = scheduler.client.MultiSave(kvs)
			if err != nil {
				return err
			}
		}

		parentInfoKey := fmt.Sprintf("%s/%d", taskInfoPrefix, parentTask.getTaskID())
		err := scheduler.client.Save(parentInfoKey, strconv.Itoa(int(taskDone)))
		if err != nil {
			return err
		}

		return nil
	}

	span, ctx := trace.StartSpanFromContext(t.traceCtx(),
		opentracing.Tags{
			"Type": t.msgType(),
			"ID":   t.getTaskID(),
		})
	var err error
	defer span.Finish()

	defer func() {
		//task postExecute
		span.LogFields(oplog.Int64("processTask: scheduler process PostExecute", t.getTaskID()))
		t.postExecute(ctx)
	}()

	// bind schedular context with task context
	// cancel cannot be deferred here, since child task may rely on the parent context
	ctx, _ = scheduler.BindContext(ctx)

	// task preExecute
	span.LogFields(oplog.Int64("processTask: scheduler process PreExecute", t.getTaskID()))
	err = t.preExecute(ctx)
	if err != nil {
		log.Error("failed to preExecute task",
			zap.Error(err))
		t.setResultInfo(err)
		return err
	}
	taskInfoKey = fmt.Sprintf("%s/%d", taskInfoPrefix, t.getTaskID())
	err = scheduler.client.Save(taskInfoKey, strconv.Itoa(int(taskDoing)))
	if err != nil {
		trace.LogError(span, err)
		t.setResultInfo(err)
		return err
	}
	t.setState(taskDoing)

	// task execute
	span.LogFields(oplog.Int64("processTask: scheduler process Execute", t.getTaskID()))
	err = t.execute(ctx)
	if err != nil {
		log.Warn("failed to execute task", zap.Error(err))
		trace.LogError(span, err)
		return err
	}
	err = updateKVFn(t)
	if err != nil {
		log.Warn("failed to execute task", zap.Error(err))
		trace.LogError(span, err)
		t.setResultInfo(err)
		return err
	}
	log.Debug("processTask: update etcd success", zap.Int64("parent taskID", t.getTaskID()))
	if t.msgType() == commonpb.MsgType_LoadCollection || t.msgType() == commonpb.MsgType_LoadPartitions {
		t.notify(nil)
	}

	t.setState(taskDone)
	t.updateTaskProcess()

	return nil
}

func (scheduler *TaskScheduler) scheduleLoop() {
	defer scheduler.wg.Done()
	activeTaskWg := &sync.WaitGroup{}
	var triggerTask task

	processInternalTaskFn := func(activateTasks []task, triggerTask task) {
		log.Debug("scheduleLoop: num of child task",
			zap.Int("num child task", len(activateTasks)),
			zap.Int64("trigger task ID", triggerTask.getTaskID()),
		)
		for _, childTask := range activateTasks {
			if childTask != nil {
				log.Debug("scheduleLoop: add an activate task to activateChan", zap.Int64("taskID", childTask.getTaskID()))
				scheduler.activateTaskChan <- childTask
				activeTaskWg.Add(1)
				go scheduler.waitActivateTaskDone(activeTaskWg, childTask, triggerTask)
			}
		}
		activeTaskWg.Wait()
	}

	removeTaskFromKVFn := func(triggerTask task) error {
		childTasks := triggerTask.getChildTask()
		for _, t := range childTasks {
			childTaskKeys := make([]string, 0)
			taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, t.getTaskID())
			stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.getTaskID())
			childTaskKeys = append(childTaskKeys, taskKey)
			childTaskKeys = append(childTaskKeys, stateKey)
			err := scheduler.client.MultiRemove(childTaskKeys)
			// after recover, child Task's state will be TaskDone, will not be repeatedly executed
			if err != nil {
				panic(err)
			}
		}
		triggerTaskKeys := make([]string, 0)
		taskKey := fmt.Sprintf("%s/%d", triggerTaskPrefix, triggerTask.getTaskID())
		stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, triggerTask.getTaskID())
		triggerTaskKeys = append(triggerTaskKeys, taskKey)
		triggerTaskKeys = append(triggerTaskKeys, stateKey)
		err := scheduler.client.MultiRemove(triggerTaskKeys)
		if err != nil {
			panic(err)
		}
		return nil
	}

	for {
		var err error
		select {
		case <-scheduler.ctx.Done():
			scheduler.stopActivateTaskLoopChan <- 1
			return
		case <-scheduler.triggerTaskQueue.Chan():
			triggerTask = scheduler.triggerTaskQueue.popTask()
			if triggerTask == nil {
				break
			}
			log.Info("scheduleLoop: pop a triggerTask from triggerTaskQueue", zap.Int64("triggerTaskID", triggerTask.getTaskID()))
			alreadyNotify := true
			if triggerTask.getState() == taskUndo || triggerTask.getState() == taskDoing {
				err = scheduler.processTask(triggerTask)
				if err != nil {
					// Checks in preExecute not passed,
					// for only LoadCollection & LoadPartitions
					if errors.Is(err, ErrCollectionLoaded) ||
						errors.Is(err, ErrLoadParametersMismatch) {
						log.Warn("scheduleLoop: preExecute failed",
							zap.Int64("triggerTaskID", triggerTask.getTaskID()),
							zap.Error(err))

						triggerTask.setState(taskExpired)
						if errors.Is(err, ErrLoadParametersMismatch) {
							triggerTask.setState(taskFailed)
						}

						triggerTask.notify(err)

						err = removeTaskFromKVFn(triggerTask)
						if err != nil {
							log.Error("scheduleLoop: error when remove trigger and internal tasks from etcd", zap.Int64("triggerTaskID", triggerTask.getTaskID()), zap.Error(err))
							triggerTask.setResultInfo(err)
						} else {
							log.Info("scheduleLoop: trigger task done and delete from etcd", zap.Int64("triggerTaskID", triggerTask.getTaskID()))
						}

						triggerTask.finishContext()
						continue
					} else {
						log.Warn("scheduleLoop: process triggerTask failed", zap.Int64("triggerTaskID", triggerTask.getTaskID()), zap.Error(err))
						alreadyNotify = false
					}
				}
			}
			if triggerTask.msgType() != commonpb.MsgType_LoadCollection && triggerTask.msgType() != commonpb.MsgType_LoadPartitions {
				alreadyNotify = false
			}

			childTasks := triggerTask.getChildTask()
			if len(childTasks) != 0 {
				// include loadSegment, watchDmChannel, releaseCollection, releasePartition, releaseSegment
				processInternalTaskFn(childTasks, triggerTask)
			}

			// triggerTask may be LoadCollection, LoadPartitions, LoadBalance, Handoff
			if triggerTask.getResultInfo().ErrorCode == commonpb.ErrorCode_Success || triggerTask.getTriggerCondition() == querypb.TriggerCondition_NodeDown {
				err = updateSegmentInfoFromTask(triggerTask, scheduler.meta)
				if err != nil {
					triggerTask.setResultInfo(err)
				}
			}

			if triggerTask.getResultInfo().ErrorCode == commonpb.ErrorCode_Success {
				err = triggerTask.globalPostExecute(triggerTask.traceCtx())
				if err != nil {
					log.Error("scheduleLoop: failed to execute globalPostExecute() of task",
						zap.Int64("taskID", triggerTask.getTaskID()),
						zap.Error(err))
					triggerTask.setResultInfo(err)
				}
			}

			resultInfo := triggerTask.getResultInfo()
			if resultInfo.ErrorCode != commonpb.ErrorCode_Success {
				if !alreadyNotify {
					triggerTask.notify(errors.New(resultInfo.Reason))
					alreadyNotify = true
				}
				rollBackTasks := triggerTask.rollBack(triggerTask.traceCtx())
				log.Info("scheduleLoop: start rollBack after triggerTask failed",
					zap.Int64("triggerTaskID", triggerTask.getTaskID()),
					zap.Any("rollBackTasks", rollBackTasks),
					zap.String("error", resultInfo.Reason))
				// there is no need to save rollBacked internal task to etcd
				// After queryCoord recover, it will retry failed childTask
				// if childTask still execute failed, then reProduce rollBacked tasks
				processInternalTaskFn(rollBackTasks, triggerTask)
			}

			err = removeTaskFromKVFn(triggerTask)
			if err != nil {
				log.Error("scheduleLoop: error when remove trigger and internal tasks from etcd", zap.Int64("triggerTaskID", triggerTask.getTaskID()), zap.Error(err))
				triggerTask.setResultInfo(err)
			} else {
				log.Info("scheduleLoop: trigger task done and delete from etcd", zap.Int64("triggerTaskID", triggerTask.getTaskID()))
			}

			resultStatus := triggerTask.getResultInfo()
			if resultStatus.ErrorCode != commonpb.ErrorCode_Success {
				triggerTask.setState(taskFailed)
				if !alreadyNotify {
					triggerTask.notify(errors.New(resultStatus.Reason))
				}
			} else {
				triggerTask.updateTaskProcess()
				triggerTask.setState(taskExpired)
				if !alreadyNotify {
					triggerTask.notify(nil)
				}
			}
			// calling context cancel so that bind context goroutine may quit
			triggerTask.finishContext()
		}
	}
}

// waitActivateTaskDone function Synchronous wait internal task to be done
func (scheduler *TaskScheduler) waitActivateTaskDone(wg *sync.WaitGroup, t task, triggerTask task) {
	defer wg.Done()
	var err error
	redoFunc1 := func() {
		if !t.isValid() || !t.isRetryable() {
			log.Info("waitActivateTaskDone: reSchedule the activate task",
				zap.Int64("taskID", t.getTaskID()),
				zap.Int64("triggerTaskID", triggerTask.getTaskID()))
			reScheduledTasks, err := t.reschedule(triggerTask.traceCtx())
			if err != nil {
				log.Error("waitActivateTaskDone: reschedule task error",
					zap.Int64("taskID", t.getTaskID()),
					zap.Int64("triggerTaskID", triggerTask.getTaskID()),
					zap.Error(err))
				triggerTask.setResultInfo(err)
				return
			}
			removes := make([]string, 0)
			taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, t.getTaskID())
			removes = append(removes, taskKey)
			stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.getTaskID())
			removes = append(removes, stateKey)

			saves := make(map[string]string)
			for _, rt := range reScheduledTasks {
				if rt != nil {
					id, err := scheduler.taskIDAllocator()
					if err != nil {
						log.Error("waitActivateTaskDone: allocate id error",
							zap.Int64("triggerTaskID", triggerTask.getTaskID()),
							zap.Error(err))
						triggerTask.setResultInfo(err)
						return
					}
					rt.setTaskID(id)
					log.Info("waitActivateTaskDone: reScheduler set id", zap.Int64("id", rt.getTaskID()))
					taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, rt.getTaskID())
					blobs, err := rt.marshal()
					if err != nil {
						log.Error("waitActivateTaskDone: error when marshal active task",
							zap.Int64("triggerTaskID", triggerTask.getTaskID()),
							zap.Error(err))
						triggerTask.setResultInfo(err)
						return
					}
					saves[taskKey] = string(blobs)
					stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, rt.getTaskID())
					saves[stateKey] = strconv.Itoa(int(taskUndo))
				}
			}
			err = scheduler.client.MultiSaveAndRemove(saves, removes)
			if err != nil {
				log.Error("waitActivateTaskDone: error when save and remove task from etcd", zap.Int64("triggerTaskID", triggerTask.getTaskID()))
				triggerTask.setResultInfo(err)
				return
			}
			triggerTask.removeChildTaskByID(t.getTaskID())
			log.Info("waitActivateTaskDone: delete failed active task and save reScheduled task to etcd",
				zap.Int64("triggerTaskID", triggerTask.getTaskID()),
				zap.Int64("failed taskID", t.getTaskID()),
				zap.Any("reScheduled tasks", reScheduledTasks))

			for _, rt := range reScheduledTasks {
				if rt != nil {
					triggerTask.addChildTask(rt)
					log.Info("waitActivateTaskDone: add a reScheduled active task to activateChan", zap.Int64("taskID", rt.getTaskID()))
					scheduler.activateTaskChan <- rt
					wg.Add(1)
					go scheduler.waitActivateTaskDone(wg, rt, triggerTask)
				}
			}
			//delete task from etcd
		} else {
			log.Info("waitActivateTaskDone: retry the active task",
				zap.Int64("taskID", t.getTaskID()),
				zap.Int64("triggerTaskID", triggerTask.getTaskID()))
			scheduler.activateTaskChan <- t
			wg.Add(1)
			go scheduler.waitActivateTaskDone(wg, t, triggerTask)
		}
	}

	redoFunc2 := func(err error) {
		if t.isValid() {
			if !t.isRetryable() {
				log.Error("waitActivateTaskDone: activate task failed after retry",
					zap.Int64("taskID", t.getTaskID()),
					zap.Int64("triggerTaskID", triggerTask.getTaskID()))
				triggerTask.setResultInfo(err)
				return
			}
			log.Info("waitActivateTaskDone: retry the active task",
				zap.Int64("taskID", t.getTaskID()),
				zap.Int64("triggerTaskID", triggerTask.getTaskID()))
			scheduler.activateTaskChan <- t
			wg.Add(1)
			go scheduler.waitActivateTaskDone(wg, t, triggerTask)
		}
	}
	err = t.waitToFinish()
	if err != nil {
		log.Warn("waitActivateTaskDone: activate task return err",
			zap.Int64("taskID", t.getTaskID()),
			zap.Int64("triggerTaskID", triggerTask.getTaskID()),
			zap.Error(err))

		switch t.msgType() {
		case commonpb.MsgType_LoadSegments:
			redoFunc1()
		case commonpb.MsgType_WatchDmChannels:
			redoFunc1()
		case commonpb.MsgType_WatchDeltaChannels:
			redoFunc2(err)
		case commonpb.MsgType_WatchQueryChannels:
			redoFunc2(err)
		case commonpb.MsgType_ReleaseSegments:
			redoFunc2(err)
		case commonpb.MsgType_ReleaseCollection:
			redoFunc2(err)
		case commonpb.MsgType_ReleasePartitions:
			redoFunc2(err)
		default:
			//TODO:: case commonpb.MsgType_RemoveDmChannels:
		}
	} else {
		log.Info("waitActivateTaskDone: one activate task done",
			zap.Int64("taskID", t.getTaskID()),
			zap.Int64("triggerTaskID", triggerTask.getTaskID()))
		metrics.QueryCoordChildTaskLatency.WithLabelValues().Observe(float64(t.elapseSpan().Milliseconds()))
	}
}

// processActivateTaskLoop process internal task, such as loadSegment, watchQUeryChannel, watDmChannel
func (scheduler *TaskScheduler) processActivateTaskLoop() {
	defer scheduler.wg.Done()
	for {
		select {
		case <-scheduler.stopActivateTaskLoopChan:
			log.Info("processActivateTaskLoop, ctx done")
			return
		case t := <-scheduler.activateTaskChan:
			if t == nil {
				log.Error("processActivateTaskLoop: pop a nil active task", zap.Int64("taskID", t.getTaskID()))
				continue
			}

			if t.getState() != taskDone {
				go func() {
					err := scheduler.processTask(t)
					t.notify(err)
				}()
			}
		}
	}
}

// Start function start two goroutines to process trigger tasks and internal tasks
func (scheduler *TaskScheduler) Start() error {
	scheduler.wg.Add(2)
	go scheduler.scheduleLoop()
	go scheduler.processActivateTaskLoop()
	return nil
}

// Close function stops the scheduleLoop and the processActivateTaskLoop
func (scheduler *TaskScheduler) Close() {
	if scheduler.cancel != nil {
		scheduler.cancel()
	}
	scheduler.wg.Wait()
}

// BindContext binds input context with shceduler context.
// the result context will be canceled when either context is done.
func (scheduler *TaskScheduler) BindContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// use input context as parent
	nCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-scheduler.ctx.Done():
			// scheduler done
			cancel()
		case <-ctx.Done():
			// input ctx done
			cancel()
		case <-nCtx.Done():
			// result context done, cancel called to make goroutine quit
		}
	}()
	return nCtx, cancel
}

func updateSegmentInfoFromTask(triggerTask task, meta Meta) error {
	segmentInfosToSave := make(map[UniqueID][]*querypb.SegmentInfo)
	segmentInfosToRemove := make(map[UniqueID][]*querypb.SegmentInfo)
	var err error
	switch triggerTask.msgType() {
	case commonpb.MsgType_ReleaseCollection:
		// release all segmentInfo of the collection when release collection
		req := triggerTask.(*releaseCollectionTask).ReleaseCollectionRequest
		collectionID := req.CollectionID
		err = meta.removeGlobalSealedSegInfos(collectionID, nil)

	case commonpb.MsgType_ReleasePartitions:
		// release all segmentInfo of the partitions when release partitions
		req := triggerTask.(*releasePartitionTask).ReleasePartitionsRequest
		collectionID := req.CollectionID
		err = meta.removeGlobalSealedSegInfos(collectionID, req.PartitionIDs)

	case commonpb.MsgType_HandoffSegments:
		// remove released segments
		req := triggerTask.(*handoffTask).HandoffSegmentsRequest
		collectionID := req.SegmentInfos[0].CollectionID
		offlineInfos := make([]*querypb.SegmentInfo, 0)
		for _, releasedSegmentID := range req.ReleasedSegments {
			info, err := meta.getSegmentInfoByID(releasedSegmentID)
			if err != nil {
				// might be a retry, this is no correct but so far we will take it
				log.Warn("failed to find offline segment info while handoff, ignore it", zap.Int64("segmentID", releasedSegmentID), zap.Error(err))
			} else {
				offlineInfos = append(offlineInfos, info)
			}
		}
		segmentInfosToRemove[collectionID] = offlineInfos
		// still run default case to handle load segments
		fallthrough
	default:
		// save new segmentInfo when load segment
		segments := make(map[UniqueID]*querypb.SegmentInfo)
		for _, childTask := range triggerTask.getChildTask() {
			if childTask.msgType() == commonpb.MsgType_LoadSegments {
				req := childTask.(*loadSegmentTask).LoadSegmentsRequest
				dstNodeID := req.DstNodeID
				for _, loadInfo := range req.Infos {
					collectionID := loadInfo.CollectionID
					segmentID := loadInfo.SegmentID

					segment, saved := segments[segmentID]
					if !saved {
						segment, err = meta.getSegmentInfoByID(segmentID)
						if err != nil {
							segment = &querypb.SegmentInfo{
								SegmentID:      segmentID,
								CollectionID:   collectionID,
								PartitionID:    loadInfo.PartitionID,
								DmChannel:      loadInfo.InsertChannel,
								SegmentState:   commonpb.SegmentState_Sealed,
								CompactionFrom: loadInfo.CompactionFrom,
								ReplicaIds:     []UniqueID{},
								NodeIds:        []UniqueID{},
								NumRows:        loadInfo.NumOfRows,
							}
						}
					}
					segment.ReplicaIds = append(segment.ReplicaIds, req.ReplicaID)
					segment.ReplicaIds = uniqueSlice(segment.GetReplicaIds())
					replicas, err := meta.getReplicasByCollectionID(collectionID)
					if err != nil {
						return err
					}
					addNode2Segment(meta, dstNodeID, replicas, segment)

					segments[segmentID] = segment

					if _, ok := segmentInfosToSave[collectionID]; !ok {
						segmentInfosToSave[collectionID] = make([]*querypb.SegmentInfo, 0)
					}

					if !saved {
						segmentInfosToSave[collectionID] = append(segmentInfosToSave[collectionID], segment)
					}
				}
			}
		}

		log.Info("update segment info",
			zap.Int64("triggerTaskID", triggerTask.getTaskID()),
			zap.Any("segmentToSave", segmentInfosToSave),
			zap.Any("segmentToRemove", segmentInfosToRemove),
		)
		err = meta.saveGlobalSealedSegInfos(segmentInfosToSave, segmentInfosToRemove)
	}

	// no need to rollback since etcd meta is not changed
	if err != nil {
		return err
	}

	return nil
}
