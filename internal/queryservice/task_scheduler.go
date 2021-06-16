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
	"container/list"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"go.uber.org/zap"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	oplog "github.com/opentracing/opentracing-go/log"
)

const (
	triggerTaskPrefix = "queryService-triggerTask"
	activeTaskPrefix = "queryService-activeTask"
	taskInfoPrefix = "queryService-taskInfo"
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
	meta             *meta
	cluster *queryNodeCluster
	taskIDAllocator  func() (UniqueID, error)
	client           *etcdkv.EtcdKV

	master types.MasterService
	dataService types.DataService

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTaskScheduler(ctx context.Context, meta *meta, cluster *queryNodeCluster, kv *etcdkv.EtcdKV, master types.MasterService, dataService types.DataService) (*TaskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	taskChan := make(chan task, 1024)
	s := &TaskScheduler{
		ctx:              ctx1,
		cancel:           cancel,
		meta:             meta,
		cluster: cluster,
		activateTaskChan: taskChan,
		client:           kv,
		master: master,
		dataService: dataService,
	}
	s.triggerTaskQueue = NewTaskQueue()
	idAllocator := allocator.NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase(Params.EtcdEndpoints, Params.KvRootPath, "queryService task id"))
	if err := idAllocator.Initialize(); err != nil {
		log.Debug("QueryService idAllocator initialize failed", zap.Error(err))
		return nil, err
	}
	s.taskIDAllocator = func() (UniqueID, error) {
		return idAllocator.AllocOne()
	}
	err := s.reloadFromKV()
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
	for index := range triggerTaskIDKeys {
		taskID, err := strconv.ParseInt(filepath.Base(triggerTaskIDKeys[index]), 10, 64)
		if err != nil {
			return err
		}

	}
}

//todo:: taskID to nodeID save to etcd/ or save to proto releaseCollection
func (scheduler *TaskScheduler)unmarshalTask(t string) (task, error) {
	header := commonpb.MsgHeader{}
	err := proto.UnmarshalText(t, &header)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal message header, err %s ", err.Error())
	}
	var newTask task
	switch header.Base.MsgType {
	case commonpb.MsgType_LoadCollection:
		loadReq := querypb.LoadCollectionRequest{}
		err = proto.UnmarshalText(t, &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		loadCollectionTask := &LoadCollectionTask{
			BaseTask: BaseTask{
				ctx: scheduler.ctx,
				Condition: NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			LoadCollectionRequest: &loadReq,
			masterService: scheduler.master,
			dataService: scheduler.dataService,
			cluster: scheduler.cluster,
			meta: scheduler.meta,
		}
		newTask = loadCollectionTask
	case commonpb.MsgType_LoadPartitions:
		loadReq := querypb.LoadPartitionsRequest{}
		err = proto.UnmarshalText(t, &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		loadPartitionTask := &LoadPartitionTask{
			BaseTask: BaseTask{
				ctx: scheduler.ctx,
				Condition: NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			LoadPartitionsRequest: &loadReq,
			masterService: scheduler.master,
			dataService: scheduler.dataService,
			cluster: scheduler.cluster,
			meta: scheduler.meta,
		}
		newTask = loadPartitionTask
	case commonpb.MsgType_ReleaseCollection:
		loadReq := querypb.ReleaseCollectionRequest{}
		err = proto.UnmarshalText(t, &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		releaseCollectionTask := &ReleaseCollectionTask{
			BaseTask: BaseTask{
				ctx: scheduler.ctx,
				Condition: NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			ReleaseCollectionRequest: &loadReq,
			cluster: scheduler.cluster,
		}
		newTask = releaseCollectionTask
	case commonpb.MsgType_ReleasePartitions:
		loadReq := querypb.ReleasePartitionsRequest{}
		err = proto.UnmarshalText(t, &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		releasePartitionTask := &ReleasePartitionTask{
			BaseTask: BaseTask{
				ctx: scheduler.ctx,
				Condition: NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			ReleasePartitionsRequest: &loadReq,
			cluster: scheduler.cluster,
		}
		newTask = releasePartitionTask
	case commonpb.MsgType_LoadSegments:
		loadReq := querypb.LoadSegmentsRequest{}
		err = proto.UnmarshalText(t, &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		loadSegmentTask := &LoadSegmentTask{
			BaseTask: BaseTask{
				ctx: scheduler.ctx,
				Condition: NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			LoadSegmentsRequest: &loadReq,
			cluster: scheduler.cluster,
			meta: scheduler.meta,
		}
		newTask = loadSegmentTask
	case commonpb.MsgType_ReleaseSegments:
		loadReq := querypb.ReleaseSegmentsRequest{}
		err = proto.UnmarshalText(t, &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		releaseSegmentTask := &ReleaseSegmentTask{
			BaseTask: BaseTask{
				ctx: scheduler.ctx,
				Condition: NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			ReleaseSegmentsRequest: &loadReq,
			cluster: scheduler.cluster,
		}
		newTask = releaseSegmentTask
	case commonpb.MsgType_WatchDmChannels:
		loadReq := querypb.WatchDmChannelsRequest{}
		err = proto.UnmarshalText(t, &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		watchDmChannelTask := &WatchDmChannelTask{
			BaseTask: BaseTask{
				ctx: scheduler.ctx,
				Condition: NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			WatchDmChannelsRequest: &loadReq,
			cluster: scheduler.cluster,
			meta: scheduler.meta,
		}
		newTask = watchDmChannelTask
	case commonpb.MsgType_WatchQueryChannels:
		loadReq := querypb.AddQueryChannelRequest{}
		err = proto.UnmarshalText(t, &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		watchQueryChannelTask := &WatchQueryChannelTask{
			BaseTask: BaseTask{
				ctx: scheduler.ctx,
				Condition: NewTaskCondition(scheduler.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			AddQueryChannelRequest: &loadReq,
			cluster: scheduler.cluster,
		}
		newTask = watchQueryChannelTask
	case commonpb.MsgType_LoadBalanceSegments:
		loadReq := querypb.LoadBalanceRequest{}
		err = proto.UnmarshalText(t, &loadReq)
		if err != nil {
			log.Error(err.Error())
		}
		loadBalanceTask := &LoadBalanceTask{
			BaseTask: BaseTask{
				ctx: scheduler.ctx,
				Condition: NewTaskCondition(scheduler.ctx),
				triggerCondition: loadReq.BalanceReason,
			},
			LoadBalanceRequest: &loadReq,
			master: scheduler.master,
			dataService: scheduler.dataService,
			cluster: scheduler.cluster,
			meta: scheduler.meta,
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
		key := fmt.Sprintf("%s/%d", triggerTaskPrefix, t.ID())
		err = scheduler.client.Save(key, t.Marshal())
		if err != nil {
			log.Error("error when save trigger task to etcd")
		}
	}

	scheduler.triggerTaskQueue.addTask(tasks)
}

func (scheduler *TaskScheduler) AddActiveTask(tasks []task) {
	for _, t := range tasks {
		id, err := scheduler.taskIDAllocator()
		if err != nil {
			log.Error(err.Error())
		}
		t.SetID(id)
		key := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
		err = scheduler.client.Save(key, t.Marshal())
		if err != nil {
			log.Error("error when save active task to etcd")
		}

	}
}

func (scheduler *TaskScheduler) processTask(t task) {
	span, ctx := trace.StartSpanFromContext(t.TraceCtx(),
		opentracing.Tags{
			"Type": t.Type(),
			"ID":   t.ID(),
		})
	defer span.Finish()
	span.LogFields(oplog.Int64("scheduler process PreExecute", t.ID()))
	key := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
	err := scheduler.client.Save(key, string(assigning))
	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		log.Debug("update task state err", zap.String("reason", err.Error()))
		trace.LogError(span, err)
		return
	}

	err = t.PreExecute(ctx)
	if err != nil {
		log.Debug("preExecute err", zap.String("reason", err.Error()))
		trace.LogError(span, err)
		return
	}

	span.LogFields(oplog.Int64("scheduler process Execute", t.ID()))
	err = t.Execute(ctx)
	if err != nil {
		log.Debug("execute err", zap.String("reason", err.Error()))
		trace.LogError(span, err)
		return
	}
	span.LogFields(oplog.Int64("scheduler process PostExecute", t.ID()))
	err = t.PostExecute(ctx)
}

func (scheduler *TaskScheduler) scheduleLoop() {
	defer scheduler.wg.Done()
	var w sync.WaitGroup
	for {
		select {
		case <-scheduler.ctx.Done():
			return
		case <-scheduler.triggerTaskQueue.Chan():
			t := scheduler.triggerTaskQueue.PopTask()
			log.Debug("pop a triggerTask from triggerTaskQueue")
			go scheduler.processTask(t)
			err := t.WaitToFinish()
			if err != nil {
				log.Error("process task error", zap.Any("error", err.Error()))
				continue
			}
			err = scheduler.saveActiveTask(t)
			if err != nil {
				log.Error("save activate task error ")
			}
			//TODO::add active task to etcd
			w.Add(2)
			go scheduler.addActivateTask(&w, t)
			//TODO::handle active task return error, maybe node down...
			go scheduler.processActivateTask(&w)
			w.Wait()
			//TODO:: delete trigger task from etcd
		}
	}
}

func (scheduler *TaskScheduler) saveActiveTask(t task) error {
	for _, childTask := range t.GetChildTask() {
		if childTask != nil {
			id, err := scheduler.taskIDAllocator()
			if err != nil {
				return err
			}
			t.SetID(id)
			key := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
			err = scheduler.client.Save(key, t.Marshal())
			if err != nil {
				return err
			}
		}
	}

	key := fmt.Sprintf("%s/%d", taskInfoPrefix, t.ID())
	err := scheduler.client.Save(key, string(assigned))
	if err != nil {
		return err
	}

	return nil
}

func (scheduler *TaskScheduler) addActivateTask(wg *sync.WaitGroup, t task) {
	defer wg.Done()
	var activeTaskWg sync.WaitGroup
	log.Debug("num of child task", zap.Int("num child task", len(t.GetChildTask())))

	for _, childTask := range t.GetChildTask() {
		if childTask != nil {
			log.Debug("add a activate task to activateChan")
			scheduler.activateTaskChan <- childTask
			activeTaskWg.Add(1)
			go scheduler.waitActivateTaskDone(&activeTaskWg, childTask)
		}
	}
	scheduler.activateTaskChan <- nil
	activeTaskWg.Wait()
}

func (scheduler *TaskScheduler) waitActivateTaskDone(wg *sync.WaitGroup, t task) {
	defer wg.Done()
	err := t.WaitToFinish()
	if err != nil {
		log.Debug("waitActivateTaskDone: activate task return err", zap.Any("error", err.Error()))
		log.Debug("redo task")
		redoFunc1 := func() {
			if !t.IsValid() {
				err = t.Reschedule()
				if err != nil {
					log.Error(err.Error())
					return
				}
				removes := make([]string, 0)
				key := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
				removes = append(removes, key)

				saves := make(map[string]string)
				for _, childTask := range t.GetChildTask() {
					if childTask != nil {
						id, err := scheduler.taskIDAllocator()
						if err != nil {
							log.Error(err.Error())
							continue
						}
						t.SetID(id)
						taskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
						saves[taskKey] = t.Marshal()
					}
				}
				err = scheduler.client.MultiSaveAndRemove(saves, removes)
				if err != nil {
					log.Error("error when save and remove task from etcd")
				}

				for _, childTask := range t.GetChildTask() {
					if childTask != nil {
						log.Debug("add a activate task to activateChan")
						scheduler.activateTaskChan <- childTask
						wg.Add(1)
						go scheduler.waitActivateTaskDone(wg, childTask)
					}
				}
				//delete task from etcd
			} else {
				scheduler.activateTaskChan <- t
				wg.Add(1)
				go scheduler.waitActivateTaskDone(wg, t)
			}
		}
		redoFunc2 := func() {
			if t.IsValid() {
				scheduler.activateTaskChan <- t
				wg.Add(1)
				go scheduler.waitActivateTaskDone(wg, t)
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
		key := fmt.Sprintf("%s/%d", activeTaskPrefix, t.ID())
		err = scheduler.client.Remove(key)
		if err != nil {
			log.Error("error when remove task from etcd")
		}
	}
	log.Debug("one activate task done")
}

func (scheduler *TaskScheduler) processActivateTask(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-scheduler.ctx.Done():
			return
		case t := <-scheduler.activateTaskChan:
			if t == nil {
				return
			}
			log.Debug("pop a activate task from activateChan")
			scheduler.processTask(t)
		}
	}
}

func (scheduler *TaskScheduler) Start() error {
	scheduler.wg.Add(1)
	go scheduler.scheduleLoop()
	return nil
}

func (scheduler *TaskScheduler) Close() {
	scheduler.cancel()
	scheduler.wg.Wait()
}
