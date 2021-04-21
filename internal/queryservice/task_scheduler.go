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
	"sync"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/trace"
	oplog "github.com/opentracing/opentracing-go/log"
)

type TaskQueue interface {
	utChan() <-chan int
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t task) error
	FrontUnissuedTask() task
	PopUnissuedTask() task
	AddActiveTask(t task)
	PopActiveTask(ts Timestamp) task
	Enqueue(t task) error
}

type BaseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[Timestamp]task
	utLock        sync.Mutex
	atLock        sync.Mutex

	maxTaskNum int64

	utBufChan chan int // to block scheduler

	sched *TaskScheduler
}

func (queue *BaseTaskQueue) utChan() <-chan int {
	return queue.utBufChan
}

func (queue *BaseTaskQueue) utEmpty() bool {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	return queue.unissuedTasks.Len() == 0
}

func (queue *BaseTaskQueue) utFull() bool {
	return int64(queue.unissuedTasks.Len()) >= queue.maxTaskNum
}

func (queue *BaseTaskQueue) addUnissuedTask(t task) error {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.utFull() {
		return errors.New("task queue is full")
	}
	if queue.unissuedTasks.Len() <= 0 {
		queue.unissuedTasks.PushBack(t)
		queue.utBufChan <- 1
		return nil
	}

	if t.Timestamp() >= queue.unissuedTasks.Back().Value.(task).Timestamp() {
		queue.unissuedTasks.PushBack(t)
		queue.utBufChan <- 1
		return nil
	}

	for e := queue.unissuedTasks.Front(); e != nil; e = e.Next() {
		if t.Timestamp() <= e.Value.(task).Timestamp() {
			queue.unissuedTasks.InsertBefore(t, e)
			queue.utBufChan <- 1
			return nil
		}
	}
	return errors.New("unexpected error in addUnissuedTask")

}

func (queue *BaseTaskQueue) FrontUnissuedTask() task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		log.Warn("sorry, but the unissued task list is empty!")
		return nil
	}

	return queue.unissuedTasks.Front().Value.(task)
}

func (queue *BaseTaskQueue) PopUnissuedTask() task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		log.Warn("sorry, but the unissued task list is empty!")
		return nil
	}

	ft := queue.unissuedTasks.Front()
	queue.unissuedTasks.Remove(ft)

	return ft.Value.(task)
}

func (queue *BaseTaskQueue) AddActiveTask(t task) {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	ts := t.Timestamp()
	_, ok := queue.activeTasks[ts]
	if ok {
		log.Debug("queryService", zap.Uint64("task with timestamp ts already in active task list! ts:", ts))
	}

	queue.activeTasks[ts] = t
}

func (queue *BaseTaskQueue) PopActiveTask(ts Timestamp) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	t, ok := queue.activeTasks[ts]
	if ok {
		log.Debug("queryService", zap.Uint64("task with timestamp ts has been deleted in active task list! ts:", ts))
		delete(queue.activeTasks, ts)
		return t
	}

	return nil
}

func (queue *BaseTaskQueue) Enqueue(t task) error {
	return queue.addUnissuedTask(t)
}

type DdTaskQueue struct {
	BaseTaskQueue
	lock sync.Mutex
}

func (queue *DdTaskQueue) Enqueue(t task) error {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.BaseTaskQueue.Enqueue(t)
}

func NewDdTaskQueue(sched *TaskScheduler) *DdTaskQueue {
	return &DdTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[Timestamp]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
			sched:         sched,
		},
	}
}

type TaskScheduler struct {
	DdQueue TaskQueue

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewTaskScheduler(ctx context.Context) *TaskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		ctx:    ctx1,
		cancel: cancel,
	}
	s.DdQueue = NewDdTaskQueue(s)

	return s
}

func (sched *TaskScheduler) scheduleDdTask() task {
	return sched.DdQueue.PopUnissuedTask()
}

func (sched *TaskScheduler) processTask(t task, q TaskQueue) {
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

func (sched *TaskScheduler) definitionLoop() {
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.DdQueue.utChan():
			if !sched.DdQueue.utEmpty() {
				t := sched.scheduleDdTask()
				sched.processTask(t, sched.DdQueue)
			}
		}
	}
}

func (sched *TaskScheduler) Start() error {
	sched.wg.Add(1)
	go sched.definitionLoop()

	return nil
}

func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}
