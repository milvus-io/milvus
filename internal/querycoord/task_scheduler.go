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

func (queue *TaskQueue) addTask(tasks []task) {
	queue.Lock()
	defer queue.Unlock()

	for _, t := range tasks {
		if queue.tasks.Len() == 0 {
			queue.taskChan <- 1
			queue.tasks.PushBack(t)
			continue
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
}

func (queue *TaskQueue) addTaskToFront(t task) {
	queue.taskChan <- 1
	if queue.tasks.Len() == 0 {
		queue.tasks.PushBack(t)
	} else {
		queue.tasks.PushFront(t)
	}
}

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

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		tasks:    list.New(),
		maxTask:  1024,
		taskChan: make(chan int, 1024),
	}
}

type TaskScheduler struct {
	triggerTaskQueue *TaskQueue
	activateTaskChan chan task
	meta             Meta
	cluster          Cluster
	taskIDAllocator  func() (UniqueID, error)
	client           *etcdkv.EtcdKV

	rootCoord types.RootCoord
	dataCoord types.DataCoord

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTaskScheduler(ctx context.Context, meta Meta, cluster Cluster, kv *etcdkv.EtcdKV, rootCoord types.RootCoord, dataCoord types.DataCoord) (*TaskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	taskChan := make(chan task, 1024)
	s := &TaskScheduler{
		ctx:              ctx1,
		cancel:           cancel,
		meta:             meta,
		cluster:          cluster,
		activateTaskChan: taskChan,
		client:           kv,
		rootCoord:        rootCoord,
		dataCoord:        dataCoord,
	}
	s.triggerTaskQueue = NewTaskQueue()
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
		t, err := scheduler.unmarshalTask(triggerTaskValues[index])
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
		t, err := scheduler.unmarshalTask(activeTaskValues[index])
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
	for id, t := range triggerTasks {
		if taskInfos[id] == taskDone {
			doneTriggerTask = t
			for _, childTask := range activeTasks {
				t.AddChildTask(childTask)
			}
			continue
		}
		scheduler.triggerTaskQueue.addTask([]task{t})
	}

	if doneTriggerTask != nil {
		scheduler.triggerTaskQueue.addTaskToFront(doneTriggerTask)
	}

	return nil
}

func (scheduler *TaskScheduler) unmarshalTask(t string) (task, error) {
	header := commonpb.MsgHeader{}
	err := proto.Unmarshal([]byte(t), &header)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal message header, err %s ", err.Error())
	}
	var newTask task
	switch header.Base.MsgType {
	case commonpb.MsgType_LoadCollection:
		loadReq := querypb.LoadCollectionRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		loadCollectionTask := &LoadCollectionTask{
			BaseTask: BaseTask{
				ctx:              scheduler.ctx,
				Condition:        NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
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
			log.Error(err.Error())
		}
		loadPartitionTask := &LoadPartitionTask{
			BaseTask: BaseTask{
				ctx:              scheduler.ctx,
				Condition:        NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
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
			log.Error(err.Error())
		}
		releaseCollectionTask := &ReleaseCollectionTask{
			BaseTask: BaseTask{
				ctx:              scheduler.ctx,
				Condition:        NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
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
			log.Error(err.Error())
		}
		releasePartitionTask := &ReleasePartitionTask{
			BaseTask: BaseTask{
				ctx:              scheduler.ctx,
				Condition:        NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			ReleasePartitionsRequest: &loadReq,
			cluster:                  scheduler.cluster,
		}
		newTask = releasePartitionTask
	case commonpb.MsgType_LoadSegments:
		loadReq := querypb.LoadSegmentsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		loadSegmentTask := &LoadSegmentTask{
			BaseTask: BaseTask{
				ctx:              scheduler.ctx,
				Condition:        NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			LoadSegmentsRequest: &loadReq,
			cluster:             scheduler.cluster,
			meta:                scheduler.meta,
		}
		newTask = loadSegmentTask
	case commonpb.MsgType_ReleaseSegments:
		loadReq := querypb.ReleaseSegmentsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		releaseSegmentTask := &ReleaseSegmentTask{
			BaseTask: BaseTask{
				ctx:              scheduler.ctx,
				Condition:        NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			ReleaseSegmentsRequest: &loadReq,
			cluster:                scheduler.cluster,
		}
		newTask = releaseSegmentTask
	case commonpb.MsgType_WatchDmChannels:
		loadReq := querypb.WatchDmChannelsRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		watchDmChannelTask := &WatchDmChannelTask{
			BaseTask: BaseTask{
				ctx:              scheduler.ctx,
				Condition:        NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			WatchDmChannelsRequest: &loadReq,
			cluster:                scheduler.cluster,
			meta:                   scheduler.meta,
		}
		newTask = watchDmChannelTask
	case commonpb.MsgType_WatchQueryChannels:
		loadReq := querypb.AddQueryChannelRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		watchQueryChannelTask := &WatchQueryChannelTask{
			BaseTask: BaseTask{
				ctx:              scheduler.ctx,
				Condition:        NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			AddQueryChannelRequest: &loadReq,
			cluster:                scheduler.cluster,
		}
		newTask = watchQueryChannelTask
	case commonpb.MsgType_LoadBalanceSegments:
		loadReq := querypb.LoadBalanceRequest{}
		err = proto.Unmarshal([]byte(t), &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		loadBalanceTask := &LoadBalanceTask{
			BaseTask: BaseTask{
				ctx:              scheduler.ctx,
				Condition:        NewTaskCondition(scheduler.ctx),
				triggerCondition: loadReq.BalanceReason,
			},
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

	return newTask, nil
}

func (scheduler *TaskScheduler) Enqueue(tasks []task) {
	for _, t := range tasks {
		id, err := scheduler.taskIDAllocator()
		if err != nil {
			log.Error(err.Error())
		}
		t.SetID(id)
		kvs := make(map[string]string)
		taskKey := fmt.Sprintf("%s/%d", triggerTaskPrefix, t.ID())
		blobs, err := t.Marshal()
		if err != nil {
			log.Error("error when save marshal task", zap.Int64("taskID", t.ID()), zap.String("error", err.Error()))
		}
		kvs[taskKey] = string(blobs)
		stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
		kvs[stateKey] = strconv.Itoa(int(taskUndo))
		err = scheduler.client.MultiSave(kvs)
		if err != nil {
			log.Error("error when save trigger task to etcd", zap.Int64("taskID", t.ID()), zap.String("error", err.Error()))
		}
		log.Debug("EnQueue a triggerTask and save to etcd", zap.Int64("taskID", t.ID()))
		t.SetState(taskUndo)
	}

	scheduler.triggerTaskQueue.addTask(tasks)
}

func (scheduler *TaskScheduler) processTask(t task) error {
	span, ctx := trace.StartSpanFromContext(t.TraceCtx(),
		opentracing.Tags{
			"Type": t.Type(),
			"ID":   t.ID(),
		})
	defer span.Finish()
	span.LogFields(oplog.Int64("processTask: scheduler process PreExecute", t.ID()))
	t.PreExecute(ctx)

	key := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
	err := scheduler.client.Save(key, strconv.Itoa(int(taskDoing)))
	if err != nil {
		log.Error("processTask: update task state err", zap.String("reason", err.Error()), zap.Int64("taskID", t.ID()))
		trace.LogError(span, err)
		return err
	}
	t.SetState(taskDoing)

	span.LogFields(oplog.Int64("processTask: scheduler process Execute", t.ID()))
	err = t.Execute(ctx)
	if err != nil {
		log.Debug("processTask: execute err", zap.String("reason", err.Error()), zap.Int64("taskID", t.ID()))
		trace.LogError(span, err)
		return err
	}

	for _, childTask := range t.GetChildTask() {
		if childTask == nil {
			log.Error("processTask: child task equal nil")
			continue
		}

		id, err := scheduler.taskIDAllocator()
		if err != nil {
			return err
		}
		childTask.SetID(id)
		kvs := make(map[string]string)
		taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, childTask.ID())
		blobs, err := childTask.Marshal()
		if err != nil {
			log.Error("processTask: marshal task err", zap.String("reason", err.Error()))
			trace.LogError(span, err)
			return err
		}
		kvs[taskKey] = string(blobs)
		stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, childTask.ID())
		kvs[stateKey] = strconv.Itoa(int(taskUndo))
		err = scheduler.client.MultiSave(kvs)
		if err != nil {
			log.Error("processTask: save active task info err", zap.String("reason", err.Error()))
			trace.LogError(span, err)
			return err
		}
		log.Debug("processTask: save active task to etcd", zap.Int64("parent taskID", t.ID()), zap.Int64("child taskID", childTask.ID()))
	}

	err = scheduler.client.Save(key, strconv.Itoa(int(taskDone)))
	if err != nil {
		log.Error("processTask: update task state err", zap.String("reason", err.Error()), zap.Int64("taskID", t.ID()))
		trace.LogError(span, err)
		return err
	}

	span.LogFields(oplog.Int64("processTask: scheduler process PostExecute", t.ID()))
	t.PostExecute(ctx)
	t.SetState(taskDone)

	return nil
}

func (scheduler *TaskScheduler) scheduleLoop() {
	defer scheduler.wg.Done()
	activeTaskWg := &sync.WaitGroup{}

	for {
		var err error = nil
		select {
		case <-scheduler.ctx.Done():
			return
		case <-scheduler.triggerTaskQueue.Chan():
			t := scheduler.triggerTaskQueue.PopTask()
			log.Debug("scheduleLoop: pop a triggerTask from triggerTaskQueue", zap.Int64("taskID", t.ID()))
			if t.State() < taskDone {
				err = scheduler.processTask(t)
				if err != nil {
					log.Error("scheduleLoop: process task error", zap.Any("error", err.Error()))
					t.Notify(err)
					t.PostExecute(scheduler.ctx)
				}
				if t.Type() == commonpb.MsgType_LoadCollection || t.Type() == commonpb.MsgType_LoadPartitions {
					t.Notify(err)
				}
			}
			log.Debug("scheduleLoop: num of child task", zap.Int("num child task", len(t.GetChildTask())))
			for _, childTask := range t.GetChildTask() {
				if childTask != nil {
					log.Debug("scheduleLoop: add a activate task to activateChan", zap.Int64("taskID", childTask.ID()))
					scheduler.activateTaskChan <- childTask
					activeTaskWg.Add(1)
					go scheduler.waitActivateTaskDone(activeTaskWg, childTask)
				}
			}
			activeTaskWg.Wait()
			if t.Type() == commonpb.MsgType_LoadCollection || t.Type() == commonpb.MsgType_LoadPartitions {
				t.PostExecute(scheduler.ctx)
			}

			keys := make([]string, 0)
			taskKey := fmt.Sprintf("%s/%d", triggerTaskPrefix, t.ID())
			stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
			keys = append(keys, taskKey)
			keys = append(keys, stateKey)
			err = scheduler.client.MultiRemove(keys)
			if err != nil {
				log.Error("scheduleLoop: error when remove trigger task to etcd", zap.Int64("taskID", t.ID()))
				t.Notify(err)
				continue
			}
			log.Debug("scheduleLoop: trigger task done and delete from etcd", zap.Int64("taskID", t.ID()))
			t.Notify(err)
		}
	}
}

func (scheduler *TaskScheduler) waitActivateTaskDone(wg *sync.WaitGroup, t task) {
	defer wg.Done()
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("waitActivateTaskDone: activate task return err", zap.Any("error", err.Error()), zap.Int64("taskID", t.ID()))
		redoFunc1 := func() {
			if !t.IsValid() {
				reScheduledTasks, err := t.Reschedule()
				if err != nil {
					log.Error(err.Error())
					return
				}
				removes := make([]string, 0)
				taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
				removes = append(removes, taskKey)
				stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
				removes = append(removes, stateKey)

				saves := make(map[string]string)
				reSchedID := make([]int64, 0)
				for _, rt := range reScheduledTasks {
					if rt != nil {
						id, err := scheduler.taskIDAllocator()
						if err != nil {
							log.Error(err.Error())
							continue
						}
						rt.SetID(id)
						taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, rt.ID())
						blobs, err := rt.Marshal()
						if err != nil {
							log.Error("waitActivateTaskDone: error when marshal active task")
							continue
							//TODO::xige-16 deal error when marshal task failed
						}
						saves[taskKey] = string(blobs)
						stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, rt.ID())
						saves[stateKey] = strconv.Itoa(int(taskUndo))
						reSchedID = append(reSchedID, rt.ID())
					}
				}
				err = scheduler.client.MultiSaveAndRemove(saves, removes)
				if err != nil {
					log.Error("waitActivateTaskDone: error when save and remove task from etcd")
					//TODO::xige-16 deal error when save meta failed
				}
				log.Debug("waitActivateTaskDone: delete failed active task and save reScheduled task to etcd", zap.Int64("failed taskID", t.ID()), zap.Int64s("reScheduled taskIDs", reSchedID))

				for _, rt := range reScheduledTasks {
					if rt != nil {
						log.Debug("waitActivateTaskDone: add a reScheduled active task to activateChan", zap.Int64("taskID", rt.ID()))
						scheduler.activateTaskChan <- rt
						wg.Add(1)
						go scheduler.waitActivateTaskDone(wg, rt)
					}
				}
				//delete task from etcd
			} else {
				log.Debug("waitActivateTaskDone: retry the active task", zap.Int64("taskID", t.ID()))
				scheduler.activateTaskChan <- t
				wg.Add(1)
				go scheduler.waitActivateTaskDone(wg, t)
			}
		}

		redoFunc2 := func() {
			if t.IsValid() {
				log.Debug("waitActivateTaskDone: retry the active task", zap.Int64("taskID", t.ID()))
				scheduler.activateTaskChan <- t
				wg.Add(1)
				go scheduler.waitActivateTaskDone(wg, t)
			} else {
				removes := make([]string, 0)
				taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
				removes = append(removes, taskKey)
				stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
				removes = append(removes, stateKey)
				err = scheduler.client.MultiRemove(removes)
				if err != nil {
					log.Error("waitActivateTaskDone: error when remove task from etcd", zap.Int64("taskID", t.ID()))
				}
			}
		}

		switch t.Type() {
		case commonpb.MsgType_LoadSegments:
			redoFunc1()
		case commonpb.MsgType_WatchDmChannels:
			redoFunc1()
		case commonpb.MsgType_WatchQueryChannels:
			redoFunc2()
		case commonpb.MsgType_ReleaseSegments:
			redoFunc2()
		case commonpb.MsgType_ReleaseCollection:
			redoFunc2()
		case commonpb.MsgType_ReleasePartitions:
			redoFunc2()
		default:
			//TODO:: case commonpb.MsgType_RemoveDmChannels:
		}
	} else {
		keys := make([]string, 0)
		taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
		stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
		keys = append(keys, taskKey)
		keys = append(keys, stateKey)
		err = scheduler.client.MultiRemove(keys)
		if err != nil {
			log.Error("waitActivateTaskDone: error when remove task from etcd", zap.Int64("taskID", t.ID()))
		}
		log.Debug("waitActivateTaskDone: delete activate task from etcd", zap.Int64("taskID", t.ID()))
	}
	log.Debug("waitActivateTaskDone: one activate task done", zap.Int64("taskID", t.ID()))
}

func (scheduler *TaskScheduler) processActivateTaskLoop() {
	defer scheduler.wg.Done()
	for {
		select {
		case <-scheduler.ctx.Done():
			return
		case t := <-scheduler.activateTaskChan:
			if t == nil {
				log.Error("processActivateTaskLoop: pop a nil active task", zap.Int64("taskID", t.ID()))
				continue
			}
			stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
			err := scheduler.client.Save(stateKey, strconv.Itoa(int(taskDoing)))
			if err != nil {
				t.Notify(err)
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
