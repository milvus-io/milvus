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
	"container/list"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	oplog "github.com/opentracing/opentracing-go/log"
)

// TaskQueue is used to cache triggerTasks
type TaskQueue struct {
	tasks *list.List

	maxTask  int64
	taskChan chan int // to block scheduler

	sync.Mutex
}

func (queue *TaskQueue) Chan() <-chan int {
	return queue.taskChan
}

func (queue *TaskQueue) taskEmpty() bool {
	queue.Lock()
	defer queue.Unlock()
	return queue.tasks.Len() == 0
}

func (queue *TaskQueue) taskFull() bool {
	return int64(queue.tasks.Len()) >= queue.maxTask
}

func (queue *TaskQueue) addTask(t task) {
	queue.Lock()
	defer queue.Unlock()

	if queue.tasks.Len() == 0 {
		queue.taskChan <- 1
		queue.tasks.PushBack(t)
		return
	}

	for e := queue.tasks.Back(); e != nil; e = e.Prev() {
		if t.TaskPriority() > e.Value.(task).TaskPriority() {
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
}

func (queue *TaskQueue) addTaskToFront(t task) {
	queue.taskChan <- 1
	if queue.tasks.Len() == 0 {
		queue.tasks.PushBack(t)
	} else {
		queue.tasks.PushFront(t)
	}
}

// PopTask pops a trigger task from task list
func (queue *TaskQueue) PopTask() task {
	queue.Lock()
	defer queue.Unlock()

	if queue.tasks.Len() <= 0 {
		log.Warn("sorry, but the unissued task list is empty!")
		return nil
	}

	ft := queue.tasks.Front()
	queue.tasks.Remove(ft)

	return ft.Value.(task)
}

// NewTaskQueue creates a new task queue for scheduler to cache trigger tasks
func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		tasks:    list.New(),
		maxTask:  1024,
		taskChan: make(chan int, 1024),
	}
}

// TaskScheduler controls the scheduling of trigger tasks and internal tasks
type TaskScheduler struct {
	triggerTaskQueue         *TaskQueue
	activateTaskChan         chan task
	meta                     Meta
	cluster                  Cluster
	taskIDAllocator          func() (UniqueID, error)
	client                   *etcdkv.EtcdKV
	stopActivateTaskLoopChan chan int

	rootCoord types.RootCoord
	dataCoord types.DataCoord

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTaskScheduler(ctx context.Context, meta Meta, cluster Cluster, kv *etcdkv.EtcdKV, rootCoord types.RootCoord, dataCoord types.DataCoord) (*TaskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	taskChan := make(chan task, 1024)
	stopTaskLoopChan := make(chan int, 1)
	s := &TaskScheduler{
		ctx:                      ctx1,
		cancel:                   cancel,
		meta:                     meta,
		cluster:                  cluster,
		activateTaskChan:         taskChan,
		client:                   kv,
		stopActivateTaskLoopChan: stopTaskLoopChan,
		rootCoord:                rootCoord,
		dataCoord:                dataCoord,
	}
	s.triggerTaskQueue = NewTaskQueue()
	//init id allocator
	etcdKV, err := tsoutil.NewTSOKVBase(Params.EtcdEndpoints, Params.KvRootPath, "queryCoordTaskID")
	if err != nil {
		return nil, err
	}
	idAllocator := allocator.NewGlobalIDAllocator("idTimestamp", etcdKV)
	if err := idAllocator.Initialize(); err != nil {
		log.Debug("query coordinator idAllocator initialize failed", zap.Error(err))
		return nil, err
	}
	s.taskIDAllocator = func() (UniqueID, error) {
		return idAllocator.AllocOne()
	}
	err = s.reloadFromKV()
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
		triggerTasks[taskID].SetState(state)
	}

	var doneTriggerTask task = nil
	for _, t := range triggerTasks {
		if t.State() == taskDone {
			doneTriggerTask = t
			for _, childTask := range activeTasks {
				childTask.SetParentTask(t) //replace child task after reScheduler
				t.AddChildTask(childTask)
			}
			t.SetResultInfo(nil)
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
		return nil, fmt.Errorf("Failed to unmarshal message header, err %s ", err.Error())
	}
	var newTask task
	baseTask := newBaseTask(scheduler.ctx, querypb.TriggerCondition_grpcRequest)
	switch header.Base.MsgType {
	case commonpb.MsgType_LoadCollection:
		loadReq := querypb.LoadCollectionRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		loadCollectionTask := &LoadCollectionTask{
			BaseTask:              baseTask,
			LoadCollectionRequest: &loadReq,
			rootCoord:             scheduler.rootCoord,
			dataCoord:             scheduler.dataCoord,
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
		loadPartitionTask := &LoadPartitionTask{
			BaseTask:              baseTask,
			LoadPartitionsRequest: &loadReq,
			dataCoord:             scheduler.dataCoord,
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
		releaseCollectionTask := &ReleaseCollectionTask{
			BaseTask:                 baseTask,
			ReleaseCollectionRequest: &loadReq,
			cluster:                  scheduler.cluster,
			meta:                     scheduler.meta,
			rootCoord:                scheduler.rootCoord,
		}
		newTask = releaseCollectionTask
	case commonpb.MsgType_ReleasePartitions:
		loadReq := querypb.ReleasePartitionsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		releasePartitionTask := &ReleasePartitionTask{
			BaseTask:                 baseTask,
			ReleasePartitionsRequest: &loadReq,
			cluster:                  scheduler.cluster,
		}
		newTask = releasePartitionTask
	case commonpb.MsgType_LoadSegments:
		//TODO::trigger condition may be different
		loadReq := querypb.LoadSegmentsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		loadSegmentTask := &LoadSegmentTask{
			BaseTask:            baseTask,
			LoadSegmentsRequest: &loadReq,
			cluster:             scheduler.cluster,
			meta:                scheduler.meta,
			excludeNodeIDs:      []int64{},
		}
		newTask = loadSegmentTask
	case commonpb.MsgType_ReleaseSegments:
		//TODO::trigger condition may be different
		loadReq := querypb.ReleaseSegmentsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		releaseSegmentTask := &ReleaseSegmentTask{
			BaseTask:               baseTask,
			ReleaseSegmentsRequest: &loadReq,
			cluster:                scheduler.cluster,
		}
		newTask = releaseSegmentTask
	case commonpb.MsgType_WatchDmChannels:
		//TODO::trigger condition may be different
		loadReq := querypb.WatchDmChannelsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		watchDmChannelTask := &WatchDmChannelTask{
			BaseTask:               baseTask,
			WatchDmChannelsRequest: &loadReq,
			cluster:                scheduler.cluster,
			meta:                   scheduler.meta,
			excludeNodeIDs:         []int64{},
		}
		newTask = watchDmChannelTask
	case commonpb.MsgType_WatchQueryChannels:
		//TODO::trigger condition may be different
		loadReq := querypb.AddQueryChannelRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		watchQueryChannelTask := &WatchQueryChannelTask{
			BaseTask:               baseTask,
			AddQueryChannelRequest: &loadReq,
			cluster:                scheduler.cluster,
		}
		newTask = watchQueryChannelTask
	case commonpb.MsgType_LoadBalanceSegments:
		//TODO::trigger condition may be different
		loadReq := querypb.LoadBalanceRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			return nil, err
		}
		loadBalanceTask := &LoadBalanceTask{
			BaseTask:           baseTask,
			LoadBalanceRequest: &loadReq,
			rootCoord:          scheduler.rootCoord,
			dataCoord:          scheduler.dataCoord,
			cluster:            scheduler.cluster,
			meta:               scheduler.meta,
		}
		newTask = loadBalanceTask
	default:
		err = errors.New("inValid msg type when unMarshal task")
		log.Error(err.Error())
		return nil, err
	}

	newTask.SetID(taskID)
	return newTask, nil
}

// Enqueue pushs a trigger task to triggerTaskQueue and assigns task id
func (scheduler *TaskScheduler) Enqueue(t task) error {
	id, err := scheduler.taskIDAllocator()
	if err != nil {
		log.Error("allocator trigger taskID failed", zap.Error(err))
		return err
	}
	t.SetID(id)
	kvs := make(map[string]string)
	taskKey := fmt.Sprintf("%s/%d", triggerTaskPrefix, t.ID())
	blobs, err := t.Marshal()
	if err != nil {
		log.Error("error when save marshal task", zap.Int64("taskID", t.ID()), zap.Error(err))
		return err
	}
	kvs[taskKey] = string(blobs)
	stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
	kvs[stateKey] = strconv.Itoa(int(taskUndo))
	err = scheduler.client.MultiSave(kvs)
	if err != nil {
		//TODO::clean etcd meta
		log.Error("error when save trigger task to etcd", zap.Int64("taskID", t.ID()), zap.Error(err))
		return err
	}
	t.SetState(taskUndo)
	scheduler.triggerTaskQueue.addTask(t)
	log.Debug("EnQueue a triggerTask and save to etcd", zap.Int64("taskID", t.ID()))

	return nil
}

func (scheduler *TaskScheduler) processTask(t task) error {
	var taskInfoKey string
	// assign taskID for childTask and update triggerTask's childTask to etcd
	updateKVFn := func(parentTask task) error {
		kvs := make(map[string]string)
		kvs[taskInfoKey] = strconv.Itoa(int(taskDone))
		for _, childTask := range parentTask.GetChildTask() {
			id, err := scheduler.taskIDAllocator()
			if err != nil {
				return err
			}
			childTask.SetID(id)
			childTaskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, childTask.ID())
			blobs, err := childTask.Marshal()
			if err != nil {
				return err
			}
			kvs[childTaskKey] = string(blobs)
			stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, childTask.ID())
			kvs[stateKey] = strconv.Itoa(int(taskUndo))
		}
		err := scheduler.client.MultiSave(kvs)
		if err != nil {
			return err
		}
		return nil
	}

	span, ctx := trace.StartSpanFromContext(t.TraceCtx(),
		opentracing.Tags{
			"Type": t.Type(),
			"ID":   t.ID(),
		})
	var err error
	defer span.Finish()

	defer func() {
		//task postExecute
		span.LogFields(oplog.Int64("processTask: scheduler process PostExecute", t.ID()))
		t.PostExecute(ctx)
	}()

	// task preExecute
	span.LogFields(oplog.Int64("processTask: scheduler process PreExecute", t.ID()))
	t.PreExecute(ctx)
	taskInfoKey = fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
	err = scheduler.client.Save(taskInfoKey, strconv.Itoa(int(taskDoing)))
	if err != nil {
		trace.LogError(span, err)
		t.SetResultInfo(err)
		return err
	}
	t.SetState(taskDoing)

	// task execute
	span.LogFields(oplog.Int64("processTask: scheduler process Execute", t.ID()))
	err = t.Execute(ctx)
	if err != nil {
		trace.LogError(span, err)
		return err
	}
	err = updateKVFn(t)
	if err != nil {
		trace.LogError(span, err)
		t.SetResultInfo(err)
		return err
	}
	log.Debug("processTask: update etcd success", zap.Int64("parent taskID", t.ID()))
	if t.Type() == commonpb.MsgType_LoadCollection || t.Type() == commonpb.MsgType_LoadPartitions {
		t.Notify(nil)
	}

	t.SetState(taskDone)
	t.UpdateTaskProcess()

	return nil
}

func (scheduler *TaskScheduler) scheduleLoop() {
	defer scheduler.wg.Done()
	activeTaskWg := &sync.WaitGroup{}
	var triggerTask task

	processInternalTaskFn := func(activateTasks []task, triggerTask task) {
		log.Debug("scheduleLoop: num of child task", zap.Int("num child task", len(activateTasks)))
		for _, childTask := range activateTasks {
			if childTask != nil {
				log.Debug("scheduleLoop: add a activate task to activateChan", zap.Int64("taskID", childTask.ID()))
				scheduler.activateTaskChan <- childTask
				activeTaskWg.Add(1)
				go scheduler.waitActivateTaskDone(activeTaskWg, childTask, triggerTask)
			}
		}
		activeTaskWg.Wait()
	}

	rollBackInterTaskFn := func(triggerTask task, originInternalTasks []task, rollBackTasks []task) error {
		saves := make(map[string]string)
		removes := make([]string, 0)
		childTaskIDs := make([]int64, 0)
		for _, t := range originInternalTasks {
			childTaskIDs = append(childTaskIDs, t.ID())
			taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
			removes = append(removes, taskKey)
			stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
			removes = append(removes, stateKey)
		}

		for _, t := range rollBackTasks {
			id, err := scheduler.taskIDAllocator()
			if err != nil {
				return err
			}
			t.SetID(id)
			taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
			blobs, err := t.Marshal()
			if err != nil {
				return err
			}
			saves[taskKey] = string(blobs)
			stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
			saves[stateKey] = strconv.Itoa(int(taskUndo))
		}

		err := scheduler.client.MultiSaveAndRemove(saves, removes)
		if err != nil {
			return err
		}
		for _, taskID := range childTaskIDs {
			triggerTask.RemoveChildTaskByID(taskID)
		}
		for _, t := range rollBackTasks {
			triggerTask.AddChildTask(t)
		}

		return nil
	}

	removeTaskFromKVFn := func(triggerTask task) error {
		keys := make([]string, 0)
		taskKey := fmt.Sprintf("%s/%d", triggerTaskPrefix, triggerTask.ID())
		stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, triggerTask.ID())
		keys = append(keys, taskKey)
		keys = append(keys, stateKey)
		childTasks := triggerTask.GetChildTask()
		for _, t := range childTasks {
			taskKey = fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
			stateKey = fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
			keys = append(keys, taskKey)
			keys = append(keys, stateKey)
		}
		err := scheduler.client.MultiRemove(keys)
		if err != nil {
			return err
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
			triggerTask = scheduler.triggerTaskQueue.PopTask()
			log.Debug("scheduleLoop: pop a triggerTask from triggerTaskQueue", zap.Int64("triggerTaskID", triggerTask.ID()))
			alreadyNotify := true
			if triggerTask.State() == taskUndo || triggerTask.State() == taskDoing {
				err = scheduler.processTask(triggerTask)
				if err != nil {
					log.Debug("scheduleLoop: process triggerTask failed", zap.Int64("triggerTaskID", triggerTask.ID()), zap.Error(err))
					alreadyNotify = false
				}
			}
			if triggerTask.Type() != commonpb.MsgType_LoadCollection && triggerTask.Type() != commonpb.MsgType_LoadPartitions {
				alreadyNotify = false
			}

			childTasks := triggerTask.GetChildTask()
			if len(childTasks) != 0 {
				activateTasks := make([]task, len(childTasks))
				copy(activateTasks, childTasks)
				processInternalTaskFn(activateTasks, triggerTask)
				resultStatus := triggerTask.GetResultInfo()
				if resultStatus.ErrorCode != commonpb.ErrorCode_Success {
					rollBackTasks := triggerTask.RollBack(scheduler.ctx)
					log.Debug("scheduleLoop: start rollBack after triggerTask failed",
						zap.Int64("triggerTaskID", triggerTask.ID()),
						zap.Any("rollBackTasks", rollBackTasks))
					err = rollBackInterTaskFn(triggerTask, childTasks, rollBackTasks)
					if err != nil {
						log.Error("scheduleLoop: rollBackInternalTask error",
							zap.Int64("triggerTaskID", triggerTask.ID()),
							zap.Error(err))
						triggerTask.SetResultInfo(err)
					} else {
						processInternalTaskFn(rollBackTasks, triggerTask)
					}
				}
			}

			err = removeTaskFromKVFn(triggerTask)
			if err != nil {
				log.Error("scheduleLoop: error when remove trigger and internal tasks from etcd", zap.Int64("triggerTaskID", triggerTask.ID()), zap.Error(err))
				triggerTask.SetResultInfo(err)
			} else {
				log.Debug("scheduleLoop: trigger task done and delete from etcd", zap.Int64("triggerTaskID", triggerTask.ID()))
			}

			resultStatus := triggerTask.GetResultInfo()
			if resultStatus.ErrorCode != commonpb.ErrorCode_Success {
				triggerTask.SetState(taskFailed)
				if !alreadyNotify {
					triggerTask.Notify(errors.New(resultStatus.Reason))
				}
			} else {
				triggerTask.UpdateTaskProcess()
				triggerTask.SetState(taskExpired)
				if !alreadyNotify {
					triggerTask.Notify(nil)
				}
			}
		}
	}
}

func (scheduler *TaskScheduler) waitActivateTaskDone(wg *sync.WaitGroup, t task, triggerTask task) {
	defer wg.Done()
	var err error
	redoFunc1 := func() {
		if !t.IsValid() || !t.IsRetryable() {
			log.Debug("waitActivateTaskDone: reSchedule the activate task",
				zap.Int64("taskID", t.ID()),
				zap.Int64("triggerTaskID", triggerTask.ID()))
			reScheduledTasks, err := t.Reschedule(scheduler.ctx)
			if err != nil {
				log.Error("waitActivateTaskDone: reschedule task error",
					zap.Int64("taskID", t.ID()),
					zap.Int64("triggerTaskID", triggerTask.ID()),
					zap.Error(err))
				triggerTask.SetResultInfo(err)
				return
			}
			removes := make([]string, 0)
			taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
			removes = append(removes, taskKey)
			stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
			removes = append(removes, stateKey)

			saves := make(map[string]string)
			for _, rt := range reScheduledTasks {
				if rt != nil {
					id, err := scheduler.taskIDAllocator()
					if err != nil {
						log.Error("waitActivateTaskDone: allocate id error",
							zap.Int64("triggerTaskID", triggerTask.ID()),
							zap.Error(err))
						triggerTask.SetResultInfo(err)
						return
					}
					rt.SetID(id)
					log.Debug("waitActivateTaskDone: reScheduler set id", zap.Int64("id", rt.ID()))
					taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, rt.ID())
					blobs, err := rt.Marshal()
					if err != nil {
						log.Error("waitActivateTaskDone: error when marshal active task",
							zap.Int64("triggerTaskID", triggerTask.ID()),
							zap.Error(err))
						triggerTask.SetResultInfo(err)
						return
					}
					saves[taskKey] = string(blobs)
					stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, rt.ID())
					saves[stateKey] = strconv.Itoa(int(taskUndo))
				}
			}
			//TODO::queryNode auto watch queryChannel, then update etcd use same id directly
			err = scheduler.client.MultiSaveAndRemove(saves, removes)
			if err != nil {
				log.Error("waitActivateTaskDone: error when save and remove task from etcd", zap.Int64("triggerTaskID", triggerTask.ID()))
				triggerTask.SetResultInfo(err)
				return
			}
			triggerTask.RemoveChildTaskByID(t.ID())
			log.Debug("waitActivateTaskDone: delete failed active task and save reScheduled task to etcd",
				zap.Int64("triggerTaskID", triggerTask.ID()),
				zap.Int64("failed taskID", t.ID()),
				zap.Any("reScheduled tasks", reScheduledTasks))

			for _, rt := range reScheduledTasks {
				if rt != nil {
					triggerTask.AddChildTask(rt)
					log.Debug("waitActivateTaskDone: add a reScheduled active task to activateChan", zap.Int64("taskID", rt.ID()))
					scheduler.activateTaskChan <- rt
					wg.Add(1)
					go scheduler.waitActivateTaskDone(wg, rt, triggerTask)
				}
			}
			//delete task from etcd
		} else {
			log.Debug("waitActivateTaskDone: retry the active task",
				zap.Int64("taskID", t.ID()),
				zap.Int64("triggerTaskID", triggerTask.ID()))
			scheduler.activateTaskChan <- t
			wg.Add(1)
			go scheduler.waitActivateTaskDone(wg, t, triggerTask)
		}
	}

	redoFunc2 := func(err error) {
		if t.IsValid() {
			if !t.IsRetryable() {
				log.Error("waitActivateTaskDone: activate task failed after retry",
					zap.Int64("taskID", t.ID()),
					zap.Int64("triggerTaskID", triggerTask.ID()))
				triggerTask.SetResultInfo(err)
				return
			}
			log.Debug("waitActivateTaskDone: retry the active task",
				zap.Int64("taskID", t.ID()),
				zap.Int64("triggerTaskID", triggerTask.ID()))
			scheduler.activateTaskChan <- t
			wg.Add(1)
			go scheduler.waitActivateTaskDone(wg, t, triggerTask)
		}
	}
	err = t.WaitToFinish()
	if err != nil {
		log.Debug("waitActivateTaskDone: activate task return err",
			zap.Int64("taskID", t.ID()),
			zap.Int64("triggerTaskID", triggerTask.ID()),
			zap.Error(err))

		switch t.Type() {
		case commonpb.MsgType_LoadSegments:
			redoFunc1()
		case commonpb.MsgType_WatchDmChannels:
			redoFunc1()
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
		log.Debug("waitActivateTaskDone: one activate task done",
			zap.Int64("taskID", t.ID()),
			zap.Int64("triggerTaskID", triggerTask.ID()))
	}
}

func (scheduler *TaskScheduler) processActivateTaskLoop() {
	defer scheduler.wg.Done()
	for {
		select {
		case <-scheduler.stopActivateTaskLoopChan:
			log.Debug("processActivateTaskLoop, ctx done")
			return

		case t := <-scheduler.activateTaskChan:
			if t == nil {
				log.Error("processActivateTaskLoop: pop a nil active task", zap.Int64("taskID", t.ID()))
				continue
			}

			log.Debug("processActivateTaskLoop: pop a active task from activateChan", zap.Int64("taskID", t.ID()))
			go func() {
				err := scheduler.processTask(t)
				t.Notify(err)
			}()
		}
	}
}

func (scheduler *TaskScheduler) Start() error {
	scheduler.wg.Add(2)
	go scheduler.scheduleLoop()
	go scheduler.processActivateTaskLoop()
	return nil
}

func (scheduler *TaskScheduler) Close() {
	scheduler.cancel()
	scheduler.wg.Wait()
}
