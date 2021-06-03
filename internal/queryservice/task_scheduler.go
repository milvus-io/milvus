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
	"sync"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/trace"
	oplog "github.com/opentracing/opentracing-go/log"
)

type TaskQueue struct {
	tasks *list.List

	maxTask  int64
	taskChan chan int // to block scheduler

	scheduler *TaskScheduler
	lock      sync.Mutex
}

func (queue *TaskQueue) Chan() <-chan int {
	return queue.taskChan
}

func (queue *TaskQueue) taskEmpty() bool {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.tasks.Len() == 0
}

func (queue *TaskQueue) taskFull() bool {
	return int64(queue.tasks.Len()) >= queue.maxTask
}

func (queue *TaskQueue) addTask(tasks []task) {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	for _, t := range tasks {
		for e := queue.tasks.Back(); e != nil; e = e.Prev() {
			if t.taskPriority() > e.Value.(task).taskPriority(){
				continue
			}
			//TODO:: take care of timestamp
			queue.taskChan <- 1
			if e != queue.tasks.Back() {
				queue.tasks.InsertAfter(t, e)
				continue
			}
			queue.tasks.PushBack(t)
		}
	}
}

func (queue *TaskQueue) PopTask() task {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	if queue.tasks.Len() <= 0 {
		log.Warn("sorry, but the unissued task list is empty!")
		return nil
	}

	ft := queue.tasks.Front()
	queue.tasks.Remove(ft)

	return ft.Value.(task)
}

//func (queue *TaskQueue) AddActiveTask(t task) {
//	queue.lock.Lock()
//	defer queue.lock.Unlock()
//
//	ts := t.Timestamp()
//	_, ok := queue.tasks[ts]
//	if ok {
//		log.Debug("queryService", zap.Uint64("task with timestamp ts already in active task list! ts:", ts))
//	}
//
//	queue.activeTasks[ts] = t
//}

//func (queue *TaskQueue) PopActiveTask(ts Timestamp) task {
//	queue.atLock.Lock()
//	defer queue.atLock.Unlock()
//
//	t, ok := queue.activeTasks[ts]
//	if ok {
//		log.Debug("queryService", zap.Uint64("task with timestamp ts has been deleted in active task list! ts:", ts))
//		delete(queue.activeTasks, ts)
//		return t
//	}
//
//	return nil
//}

func (queue *TaskQueue) Enqueue(t []task) {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	queue.addTask(t)
}

func NewTaskQueue(scheduler *TaskScheduler) *TaskQueue {
	return &TaskQueue{
		tasks:     list.New(),
		maxTask:   1024,
		taskChan:  make(chan int, 1024),
		scheduler: scheduler,
	}
}

type TaskScheduler struct {
	triggerTaskQueue *TaskQueue
	ActiveTaskQueue   *TaskQueue
	meta *meta

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTaskScheduler(ctx context.Context, meta *meta) *TaskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		ctx:    ctx1,
		cancel: cancel,
		meta: meta,
	}
	s.triggerTaskQueue = NewTaskQueue(s)
	s.ActiveTaskQueue = NewTaskQueue(s)

	return s
}

func (scheduler *TaskScheduler) scheduleTask() task {
	return scheduler.triggerTaskQueue.PopTask()
}

func (scheduler *TaskScheduler) processTask(t task, q *TaskQueue) {
	span, ctx := trace.StartSpanFromContext(t.TraceCtx(),
		opentracing.Tags{
			"Type": t.Name(),
			"ID":   t.ID(),
		})
	defer span.Finish()
	span.LogFields(oplog.Int64("scheduler process PreExecute", t.ID()))
	err := t.PreExecute(ctx)

	defer func() {
		t.Notify(err)
	}()
	if err != nil {
		log.Debug("preExecute err", zap.String("reason", err.Error()))
		trace.LogError(span, err)
		return
	}

	span.LogFields(oplog.Int64("scheduler process AddActiveTask", t.ID()))
	q.AddActiveTask(t)

	defer func() {
		span.LogFields(oplog.Int64("scheduler process PopActiveTask", t.ID()))
		q.PopActiveTask(t.Timestamp())
	}()
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

func (scheduler *TaskScheduler) taskScheduleLoop() {
	defer scheduler.wg.Done()
	for {
		select {
		case <-scheduler.ctx.Done():
			return
		case <-scheduler.triggerTaskQueue.Chan():
			t := scheduler.scheduleTask()
			scheduler.processTask(t, scheduler.triggerTaskQueue)
		}
	}
}

func (scheduler *TaskScheduler) Start() error {
	scheduler.wg.Add(1)
	go scheduler.taskScheduleLoop()

	return nil
}

func (scheduler *TaskScheduler) Close() {
	scheduler.cancel()
	scheduler.wg.Wait()
}
