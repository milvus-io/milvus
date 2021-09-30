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

package indexnode

import (
	"container/list"
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"
)

// TaskQueue is a queue used to store tasks.
type TaskQueue interface {
	utChan() <-chan int
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t task) error
	//FrontUnissuedTask() task
	PopUnissuedTask() task
	AddActiveTask(t task)
	PopActiveTask(tID UniqueID) task
	Enqueue(t task) error
	//tryToRemoveUselessIndexBuildTask(indexID UniqueID) []UniqueID
}

// BaseTaskQueue is a basic instance of TaskQueue.
type BaseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[UniqueID]task
	utLock        sync.Mutex
	atLock        sync.Mutex

	// maxTaskNum should keep still
	maxTaskNum int64

	utBufChan chan int // to block scheduler

	sched *TaskScheduler
}

func (queue *BaseTaskQueue) utChan() <-chan int {
	return queue.utBufChan
}

func (queue *BaseTaskQueue) utEmpty() bool {
	return queue.unissuedTasks.Len() == 0
}

func (queue *BaseTaskQueue) utFull() bool {
	return int64(queue.unissuedTasks.Len()) >= queue.maxTaskNum
}

func (queue *BaseTaskQueue) addUnissuedTask(t task) error {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.utFull() {
		return errors.New("IndexNode task queue is full")
	}
	queue.unissuedTasks.PushBack(t)
	queue.utBufChan <- 1
	return nil
}

//func (queue *BaseTaskQueue) FrontUnissuedTask() task {
//	queue.utLock.Lock()
//	defer queue.utLock.Unlock()
//
//	if queue.unissuedTasks.Len() <= 0 {
//		log.Debug("IndexNode FrontUnissuedTask sorry, but the unissued task list is empty!")
//		return nil
//	}
//
//	return queue.unissuedTasks.Front().Value.(task)
//}

// PopUnissuedTask pops a task from tasks queue.
func (queue *BaseTaskQueue) PopUnissuedTask() task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		return nil
	}

	ft := queue.unissuedTasks.Front()
	queue.unissuedTasks.Remove(ft)

	return ft.Value.(task)
}

// AddActiveTask adds a task to activeTasks.
func (queue *BaseTaskQueue) AddActiveTask(t task) {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	tID := t.ID()
	_, ok := queue.activeTasks[tID]
	if ok {
		log.Debug("IndexNode task already in activate task list", zap.Any("TaskID", tID))
	}

	queue.activeTasks[tID] = t
}

// PopActiveTask tasks out a task from activateTask and the task will be executed.
func (queue *BaseTaskQueue) PopActiveTask(tID UniqueID) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	t, ok := queue.activeTasks[tID]
	if ok {
		delete(queue.activeTasks, tID)
		return t
	}
	log.Debug("IndexNode the task was not found in the active task list", zap.Any("TaskID", tID))
	return nil
}

//func (queue *BaseTaskQueue) tryToRemoveUselessIndexBuildTask(indexID UniqueID) []UniqueID {
//	queue.utLock.Lock()
//	defer queue.utLock.Unlock()
//
//	var next *list.Element
//	var indexBuildIDs []UniqueID
//	for e := queue.unissuedTasks.Front(); e != nil; e = next {
//		next = e.Next()
//		indexBuildTask, ok := e.Value.(*IndexBuildTask)
//		if !ok {
//			continue
//		}
//		if indexBuildTask.req.IndexID == indexID {
//			indexBuildIDs = append(indexBuildIDs, indexBuildTask.req.IndexBuildID)
//			queue.unissuedTasks.Remove(e)
//			indexBuildTask.Notify(nil)
//		}
//	}
//	return indexBuildIDs
//}

// Enqueue adds a task to TaskQueue.
func (queue *BaseTaskQueue) Enqueue(t task) error {
	err := t.OnEnqueue()
	if err != nil {
		return err
	}
	return queue.addUnissuedTask(t)
}

// IndexBuildTaskQueue is a task queue used to store building index tasks.
type IndexBuildTaskQueue struct {
	BaseTaskQueue
}

// NewIndexBuildTaskQueue creates a new IndexBuildTaskQueue.
func NewIndexBuildTaskQueue(sched *TaskScheduler) *IndexBuildTaskQueue {
	return &IndexBuildTaskQueue{
		BaseTaskQueue: BaseTaskQueue{
			unissuedTasks: list.New(),
			activeTasks:   make(map[UniqueID]task),
			maxTaskNum:    1024,
			utBufChan:     make(chan int, 1024),
			sched:         sched,
		},
	}
}

// TaskScheduler is a scheduler of indexing tasks.
type TaskScheduler struct {
	IndexBuildQueue TaskQueue

	buildParallel int
	kv            kv.BaseKV
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewTaskScheduler creates a new task scheduler of indexing tasks.
func NewTaskScheduler(ctx context.Context,
	kv kv.BaseKV) (*TaskScheduler, error) {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		kv:            kv,
		ctx:           ctx1,
		cancel:        cancel,
		buildParallel: 1, // default value
	}
	s.IndexBuildQueue = NewIndexBuildTaskQueue(s)

	return s, nil
}

//func (sched *TaskScheduler) setParallelism(parallel int) {
//	if parallel <= 0 {
//		log.Debug("IndexNode can not set parallelism to less than zero!")
//		return
//	}
//	sched.buildParallel = parallel
//}

func (sched *TaskScheduler) scheduleIndexBuildTask() []task {
	ret := make([]task, 0)
	for i := 0; i < sched.buildParallel; i++ {
		t := sched.IndexBuildQueue.PopUnissuedTask()
		if t == nil {
			return ret
		}
		ret = append(ret, t)
	}
	return ret
}

func (sched *TaskScheduler) processTask(t task, q TaskQueue) {
	span, ctx := trace.StartSpanFromContext(t.Ctx(),
		opentracing.Tags{
			"Type": t.Name(),
			"ID":   t.ID(),
		})

	defer span.Finish()
	span.LogFields(oplog.Int64("scheduler process PreExecute", t.ID()))
	err := t.PreExecute(ctx)
	t.SetError(err)

	defer func() {
		span.LogFields(oplog.Int64("scheduler process PostExecute", t.ID()))
		err := t.PostExecute(ctx)
		t.SetError(err)
	}()

	if err != nil {
		trace.LogError(span, err)
		return
	}

	span.LogFields(oplog.Int64("scheduler process AddActiveTask", t.ID()))
	q.AddActiveTask(t)

	// log.Printf("task add to active list ...")
	defer func() {
		span.LogFields(oplog.Int64("scheduler process PopActiveTask", t.ID()))
		q.PopActiveTask(t.ID())
		// log.Printf("pop from active list ...")
	}()

	span.LogFields(oplog.Int64("scheduler process Execute", t.ID()))
	err = t.Execute(ctx)
	t.SetError(err)
}

func (sched *TaskScheduler) indexBuildLoop() {
	log.Debug("IndexNode TaskScheduler start build loop ...")
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.IndexBuildQueue.utChan():
			if !sched.IndexBuildQueue.utEmpty() {
				tasks := sched.scheduleIndexBuildTask()
				var wg sync.WaitGroup
				for _, t := range tasks {
					wg.Add(1)
					go func(group *sync.WaitGroup, t task) {
						defer group.Done()
						sched.processTask(t, sched.IndexBuildQueue)
					}(&wg, t)
				}
				wg.Wait()
			}
		}
	}
}

// Start stats the task scheduler of indexing tasks.
func (sched *TaskScheduler) Start() error {

	sched.wg.Add(1)
	go sched.indexBuildLoop()
	return nil
}

// Close closes the task scheduler of indexing tasks.
func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}
