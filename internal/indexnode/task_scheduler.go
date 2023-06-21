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

package indexnode

import (
	"container/list"
	"context"
	"runtime/debug"
	"sync"

	"github.com/cockroachdb/errors"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
)

// TaskQueue is a queue used to store tasks.
type TaskQueue interface {
	utChan() <-chan int
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t task) error
	PopUnissuedTask() task
	AddActiveTask(t task)
	PopActiveTask(tName string) task
	Enqueue(t task) error
	GetTaskNum() (int, int)
}

// BaseTaskQueue is a basic instance of TaskQueue.
type IndexTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[string]task
	utLock        sync.Mutex
	atLock        sync.Mutex

	// maxTaskNum should keep still
	maxTaskNum int64

	utBufChan chan int // to block scheduler

	sched *TaskScheduler
}

func (queue *IndexTaskQueue) utChan() <-chan int {
	return queue.utBufChan
}

func (queue *IndexTaskQueue) utEmpty() bool {
	return queue.unissuedTasks.Len() == 0
}

func (queue *IndexTaskQueue) utFull() bool {
	return int64(queue.unissuedTasks.Len()) >= queue.maxTaskNum
}

func (queue *IndexTaskQueue) addUnissuedTask(t task) error {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.utFull() {
		return errors.New("IndexNode task queue is full")
	}
	queue.unissuedTasks.PushBack(t)
	queue.utBufChan <- 1
	return nil
}

// PopUnissuedTask pops a task from tasks queue.
func (queue *IndexTaskQueue) PopUnissuedTask() task {
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
func (queue *IndexTaskQueue) AddActiveTask(t task) {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	tName := t.Name()
	_, ok := queue.activeTasks[tName]
	if ok {
		log.Debug("IndexNode task already in active task list", zap.Any("TaskID", tName))
	}

	queue.activeTasks[tName] = t
}

// PopActiveTask pops a task from activateTask and the task will be executed.
func (queue *IndexTaskQueue) PopActiveTask(tName string) task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	t, ok := queue.activeTasks[tName]
	if ok {
		delete(queue.activeTasks, tName)
		return t
	}
	log.Debug("IndexNode task was not found in the active task list", zap.String("TaskName", tName))
	return nil
}

// Enqueue adds a task to TaskQueue.
func (queue *IndexTaskQueue) Enqueue(t task) error {
	err := t.OnEnqueue(t.Ctx())
	if err != nil {
		return err
	}
	return queue.addUnissuedTask(t)
}

func (queue *IndexTaskQueue) GetTaskNum() (int, int) {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	utNum := queue.unissuedTasks.Len()
	atNum := 0
	// remove the finished task
	for _, task := range queue.activeTasks {
		if task.GetState() != commonpb.IndexState_Finished && task.GetState() != commonpb.IndexState_Failed {
			atNum++
		}
	}
	return utNum, atNum
}

// NewIndexBuildTaskQueue creates a new IndexBuildTaskQueue.
func NewIndexBuildTaskQueue(sched *TaskScheduler) *IndexTaskQueue {
	return &IndexTaskQueue{
		unissuedTasks: list.New(),
		activeTasks:   make(map[string]task),
		maxTaskNum:    1024,
		utBufChan:     make(chan int, 1024),
		sched:         sched,
	}
}

// TaskScheduler is a scheduler of indexing tasks.
type TaskScheduler struct {
	IndexBuildQueue TaskQueue

	buildParallel int
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewTaskScheduler creates a new task scheduler of indexing tasks.
func NewTaskScheduler(ctx context.Context) *TaskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		ctx:           ctx1,
		cancel:        cancel,
		buildParallel: Params.IndexNodeCfg.BuildParallel.GetAsInt(),
	}
	s.IndexBuildQueue = NewIndexBuildTaskQueue(s)

	return s
}

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
	wrap := func(fn func(ctx context.Context) error) error {
		select {
		case <-t.Ctx().Done():
			return errCancel
		default:
			return fn(t.Ctx())
		}
	}

	defer func() {
		t.Reset()
		debug.FreeOSMemory()
	}()
	sched.IndexBuildQueue.AddActiveTask(t)
	defer sched.IndexBuildQueue.PopActiveTask(t.Name())
	log.Ctx(t.Ctx()).Debug("process task", zap.String("task", t.Name()))
	pipelines := []func(context.Context) error{t.Prepare, t.BuildIndex, t.SaveIndexFiles}
	for _, fn := range pipelines {
		if err := wrap(fn); err != nil {
			if err == errCancel {
				log.Ctx(t.Ctx()).Warn("index build task canceled", zap.String("task", t.Name()))
				t.SetState(commonpb.IndexState_Failed, err.Error())
			} else if errors.Is(err, ErrNoSuchKey) {
				t.SetState(commonpb.IndexState_Failed, err.Error())
			} else {
				t.SetState(commonpb.IndexState_Retry, err.Error())
			}
			return
		}
	}
	t.SetState(commonpb.IndexState_Finished, "")
}

func (sched *TaskScheduler) indexBuildLoop() {
	log.Debug("IndexNode TaskScheduler start build loop ...")
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.IndexBuildQueue.utChan():
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
