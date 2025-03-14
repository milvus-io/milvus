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

package index

import (
	"container/list"
	"context"
	"runtime/debug"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// TaskQueue is a queue used to store tasks.
type TaskQueue interface {
	utChan() <-chan struct{}
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t Task) error
	PopUnissuedTask() Task
	AddActiveTask(t Task)
	PopActiveTask(tName string) Task
	Enqueue(t Task) error
	GetTaskNum() (int, int)
}

// BaseTaskQueue is a basic instance of TaskQueue.
type IndexTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[string]Task
	utLock        sync.Mutex
	atLock        sync.Mutex

	// maxTaskNum should keep still
	maxTaskNum int64

	utBufChan chan struct{} // to block scheduler

	sched *TaskScheduler
}

func (queue *IndexTaskQueue) utChan() <-chan struct{} {
	return queue.utBufChan
}

func (queue *IndexTaskQueue) utEmpty() bool {
	return queue.unissuedTasks.Len() == 0
}

func (queue *IndexTaskQueue) utFull() bool {
	return int64(queue.unissuedTasks.Len()) >= queue.maxTaskNum
}

func (queue *IndexTaskQueue) addUnissuedTask(t Task) error {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.utFull() {
		return errors.New("index task queue is full")
	}
	queue.unissuedTasks.PushBack(t)
	select {
	case queue.utBufChan <- struct{}{}:
	default:
	}
	return nil
}

// PopUnissuedTask pops a task from tasks queue.
func (queue *IndexTaskQueue) PopUnissuedTask() Task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.unissuedTasks.Len() <= 0 {
		return nil
	}

	ft := queue.unissuedTasks.Front()
	queue.unissuedTasks.Remove(ft)

	return ft.Value.(Task)
}

// AddActiveTask adds a task to activeTasks.
func (queue *IndexTaskQueue) AddActiveTask(t Task) {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	tName := t.Name()
	_, ok := queue.activeTasks[tName]
	if ok {
		log.Ctx(context.TODO()).Debug("task already in active task list", zap.String("TaskID", tName))
	}

	queue.activeTasks[tName] = t
}

// PopActiveTask pops a task from activateTask and the task will be executed.
func (queue *IndexTaskQueue) PopActiveTask(tName string) Task {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	t, ok := queue.activeTasks[tName]
	if ok {
		delete(queue.activeTasks, tName)
		return t
	}
	log.Ctx(queue.sched.ctx).Debug("task was not found in the active task list", zap.String("TaskName", tName))
	return nil
}

// Enqueue adds a task to TaskQueue.
func (queue *IndexTaskQueue) Enqueue(t Task) error {
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
		if task.GetState() != indexpb.JobState_JobStateFinished && task.GetState() != indexpb.JobState_JobStateFailed {
			atNum++
		}
	}
	return utNum, atNum
}

// NewIndexBuildTaskQueue creates a new IndexBuildTaskQueue.
func NewIndexBuildTaskQueue(sched *TaskScheduler) *IndexTaskQueue {
	return &IndexTaskQueue{
		unissuedTasks: list.New(),
		activeTasks:   make(map[string]Task),
		maxTaskNum:    1024,

		utBufChan: make(chan struct{}, 1024),
		sched:     sched,
	}
}

// TaskScheduler is a scheduler of indexing tasks.
type TaskScheduler struct {
	TaskQueue TaskQueue

	BuildParallel int
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
		BuildParallel: paramtable.Get().DataNodeCfg.BuildParallel.GetAsInt(),
	}
	s.TaskQueue = NewIndexBuildTaskQueue(s)

	return s
}

func (sched *TaskScheduler) scheduleIndexBuildTask() []Task {
	ret := make([]Task, 0)
	for i := 0; i < sched.BuildParallel; i++ {
		t := sched.TaskQueue.PopUnissuedTask()
		if t == nil {
			return ret
		}
		ret = append(ret, t)
	}
	return ret
}

func getStateFromError(err error) indexpb.JobState {
	if errors.Is(err, errCancel) {
		return indexpb.JobState_JobStateRetry
	} else if errors.Is(err, merr.ErrIoKeyNotFound) || errors.Is(err, merr.ErrSegcoreUnsupported) {
		// NoSuchKey or unsupported error
		return indexpb.JobState_JobStateFailed
	} else if errors.Is(err, merr.ErrSegcorePretendFinished) {
		return indexpb.JobState_JobStateFinished
	}
	return indexpb.JobState_JobStateRetry
}

func (sched *TaskScheduler) processTask(t Task, q TaskQueue) {
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
	sched.TaskQueue.AddActiveTask(t)
	defer sched.TaskQueue.PopActiveTask(t.Name())
	log.Ctx(t.Ctx()).Debug("process task", zap.String("task", t.Name()))
	pipelines := []func(context.Context) error{t.PreExecute, t.Execute, t.PostExecute}
	for _, fn := range pipelines {
		if err := wrap(fn); err != nil {
			log.Ctx(t.Ctx()).Warn("process task failed", zap.Error(err))
			t.SetState(getStateFromError(err), err.Error())
			return
		}
	}
	t.SetState(indexpb.JobState_JobStateFinished, "")
}

func (sched *TaskScheduler) indexBuildLoop() {
	log.Ctx(sched.ctx).Debug("TaskScheduler start build loop ...")
	defer sched.wg.Done()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.TaskQueue.utChan():
			tasks := sched.scheduleIndexBuildTask()
			var wg sync.WaitGroup
			for _, t := range tasks {
				wg.Add(1)
				go func(group *sync.WaitGroup, t Task) {
					defer group.Done()
					sched.processTask(t, sched.TaskQueue)
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
