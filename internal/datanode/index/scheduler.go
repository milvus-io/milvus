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
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/datanode/taskcost"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	GetUsingSlot() int64
	GetActiveSlot() int64
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

	if queue.sched.ctx.Err() != nil {
		return errCancel
	}
	if queue.utFull() {
		return merr.Wrap(merr.ErrServiceResourceInsufficient, "index task queue is full")
	}
	// Vector index tasks acquire slots at each executable stage.
	if !queue.sched.usesStageSlots(t) {
		if !queue.sched.acquireSlots(t) {
			return merr.Wrap(merr.ErrServiceResourceInsufficient, "insufficient task slots")
		}
	}
	queue.unissuedTasks.PushBack(t)
	select {
	case queue.utBufChan <- struct{}{}:
	default:
	}
	return nil
}

func (queue *IndexTaskQueue) GetUsingSlot() int64 {
	return queue.sched.usingSlot.Load()
}

func (queue *IndexTaskQueue) GetActiveSlot() int64 {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	slots := int64(0)
	for name := range queue.activeTasks {
		slots += queue.sched.taskSlots(name)
	}
	return slots
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
		mlog.Debug(context.TODO(), "task already in active task list", mlog.String("TaskID", tName))
	}

	queue.activeTasks[tName] = t
}

// PopActiveTask pops a task from activateTask and the task will be executed.
func (queue *IndexTaskQueue) PopActiveTask(tName string) Task {
	queue.atLock.Lock()

	t, ok := queue.activeTasks[tName]
	if ok {
		delete(queue.activeTasks, tName)
		queue.atLock.Unlock()
		queue.sched.releaseSlots(t)
		return t
	}
	queue.atLock.Unlock()
	mlog.Debug(queue.sched.ctx, "task was not found in the active task list", mlog.String("TaskName", tName))
	return nil
}

// Enqueue adds a task to TaskQueue.
func (queue *IndexTaskQueue) Enqueue(t Task) error {
	err := t.OnEnqueue(t.Ctx())
	if err != nil {
		return err
	}
	if err = queue.addUnissuedTask(t); err != nil {
		return err
	}

	return nil
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

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	downloadTasks chan Task
	buildWakeup   chan struct{}
	slotMu        sync.Mutex
	heldSlots     map[string]int64
	usingSlot     atomic.Int64
	externalSlots func() int64
	totalSlots    func() int64
}

// NewTaskScheduler creates a new task scheduler of indexing tasks.
func NewTaskScheduler(ctx context.Context) *TaskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		ctx:           ctx1,
		cancel:        cancel,
		downloadTasks: make(chan Task, 1024),
		buildWakeup:   make(chan struct{}, 1),
		heldSlots:     make(map[string]int64),
		externalSlots: func() int64 { return 0 },
		totalSlots:    CalculateNodeSlots,
	}
	s.TaskQueue = NewIndexBuildTaskQueue(s)

	return s
}

func (sched *TaskScheduler) usesStageSlots(t Task) bool {
	_, ok := t.(PreparedTask)
	return ok && t.IsVectorIndex()
}

// SetExternalSlotUsage connects stage admission to the same node-wide slot
// accounting reported by QuerySlot. Call before starting the scheduler.
func (sched *TaskScheduler) SetExternalSlotUsage(usage func() int64) {
	sched.externalSlots = usage
}

// AdmitTask serializes non-index enqueueing with stage admission. The owner
// checks duplicate requests before applying the capacity error; a retry does not
// consume additional slots. enqueue must publish its slot usage before returning.
func (sched *TaskScheduler) AdmitTask(slots int64, enqueue func(error) error) error {
	sched.slotMu.Lock()
	defer sched.slotMu.Unlock()
	var admissionErr error
	if !sched.slotsAvailable(slots) {
		select {
		case sched.buildWakeup <- struct{}{}:
		default:
		}
		admissionErr = merr.Wrap(merr.ErrServiceResourceInsufficient, "insufficient task slots")
	}
	return enqueue(admissionErr)
}

func (sched *TaskScheduler) slotsAvailable(slots int64) bool {
	used := sched.usingSlot.Load() + sched.externalSlots()
	// Keep oversized tasks executable on an otherwise idle node, as in the
	// coordinator's existing best-effort admission. They run alone.
	return used == 0 || slots <= sched.totalSlots()-used
}

func (sched *TaskScheduler) acquireSlots(t Task) bool {
	sched.slotMu.Lock()
	defer sched.slotMu.Unlock()
	if _, ok := sched.heldSlots[t.Name()]; ok {
		return true
	}
	if !sched.slotsAvailable(t.GetSlot()) {
		return false
	}
	sched.heldSlots[t.Name()] = t.GetSlot()
	sched.usingSlot.Add(t.GetSlot())
	return true
}

func (sched *TaskScheduler) releaseSlots(t Task) {
	sched.slotMu.Lock()
	if slots, ok := sched.heldSlots[t.Name()]; ok {
		delete(sched.heldSlots, t.Name())
		sched.usingSlot.Sub(slots)
	}
	sched.slotMu.Unlock()
}

func (sched *TaskScheduler) taskSlots(name string) int64 {
	sched.slotMu.Lock()
	defer sched.slotMu.Unlock()
	return sched.heldSlots[name]
}

func getStateFromError(err error) indexpb.JobState {
	if errors.Is(err, errCancel) {
		return indexpb.JobState_JobStateRetry
	} else if errors.Is(err, merr.ErrIoKeyNotFound) || errors.Is(err, merr.ErrSegcoreUnsupported) ||
		errors.Is(err, merr.ErrDataIntegrity) || merr.IsSegcoreDataFormatBroken(err) {
		// NoSuchKey, unsupported, malformed persisted data, or meta that disagrees
		// with the data it points at cannot be fixed by retrying. ErrDataIntegrity
		// stays a system error on purpose: the request is well formed, it is Milvus
		// state that is inconsistent, so the blame must not move to the caller.
		return indexpb.JobState_JobStateFailed
	} else if errors.Is(err, merr.ErrSegcorePretendFinished) {
		return indexpb.JobState_JobStateFinished
	} else if merr.IsPermanentSegcoreErr(err) {
		// A segcore code the table marks permanent (corrupted data, a missing
		// object, a misconfigured bucket): every construction site of that code is
		// deterministic, so the task reproduces it on every worker.
		return indexpb.JobState_JobStateFailed
	} else if merr.GetErrorType(err) == merr.InputError {
		// The request or the source data is itself what fails the build, so the task
		// fails identically on every worker and on every attempt. Fail it once instead
		// of re-dispatching forever. This covers the segcore codes that
		// classForCode already tags as caller input (JsonKeyInvalid, ExprInvalid,
		// DimNotMatch, InvalidParameter, ...) as well as the ParameterInvalid
		// errors the task itself raises.
		return indexpb.JobState_JobStateFailed
	}
	return indexpb.JobState_JobStateRetry
}

type taskExecution struct {
	task       Task
	indexTask  *indexBuildTask
	started    time.Time
	usesGPU    bool
	stopCancel func() bool
}

func (sched *TaskScheduler) beginTask(t Task) *taskExecution {
	sched.TaskQueue.AddActiveTask(t)
	exec := &taskExecution{task: t, started: time.Now()}
	if ibt, ok := t.(*indexBuildTask); ok {
		exec.indexTask = ibt
		costCPUNum := taskcost.EstimateIndexBuildCPUNum(ibt.IsVectorIndex())
		ibt.manager.StoreIndexTaskExecutionStart(ibt.req.GetClusterID(), ibt.req.GetBuildID(), taskcost.NowMs(), costCPUNum)
	}
	return exec
}

func (sched *TaskScheduler) runStep(t Task, fn func(context.Context) error) error {
	select {
	case <-sched.ctx.Done():
		return errCancel
	case <-t.Ctx().Done():
		return errCancel
	default:
		return fn(t.Ctx())
	}
}

func (sched *TaskScheduler) finishTask(exec *taskExecution, err error) {
	t := exec.task
	if exec.stopCancel != nil {
		exec.stopCancel()
	}
	if _, ok := t.(PreparedTask); ok {
		// Index input owners must be released before returning memory slots.
		t.Reset()
		debug.FreeOSMemory()
	} else {
		defer func() {
			t.Reset()
			debug.FreeOSMemory()
		}()
	}
	state, reason := indexpb.JobState_JobStateFinished, ""
	if err != nil {
		state, reason = getStateFromError(err), err.Error()
		mlog.Warn(t.Ctx(), "process task failed", mlog.String("task", t.Name()), mlog.Err(err))
	}
	if exec.indexTask != nil {
		exec.indexTask.SetStateWithCost(state, reason, taskcost.NowMs(), taskcost.ElapsedMs(exec.started))
	} else {
		t.SetState(state, reason)
	}
	sched.TaskQueue.PopActiveTask(t.Name())
}

func (sched *TaskScheduler) processTask(t Task) {
	exec := sched.beginTask(t)
	var err error
	defer func() { sched.finishTask(exec, err) }()
	steps := []func(context.Context) error{t.PreExecute, t.Execute, t.PostExecute}
	for _, fn := range steps {
		if err = sched.runStep(t, fn); err != nil {
			return
		}
	}
}

func (sched *TaskScheduler) indexBuildLoop() {
	mlog.Debug(sched.ctx, "TaskScheduler start build loop ...")
	defer sched.wg.Done()
	defer close(sched.downloadTasks)
	defer func() {
		for t := sched.TaskQueue.PopUnissuedTask(); t != nil; t = sched.TaskQueue.PopUnissuedTask() {
			sched.finishTask(sched.beginTask(t), errCancel)
		}
	}()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.TaskQueue.utChan():
			t := sched.TaskQueue.PopUnissuedTask()
			if t == nil {
				continue
			}
			if _, ok := t.(PreparedTask); ok {
				select {
				case sched.downloadTasks <- t:
				case <-sched.ctx.Done():
					sched.finishTask(sched.beginTask(t), errCancel)
				}
				continue
			}
			sched.wg.Add(1)
			go func() {
				defer sched.wg.Done()
				sched.processTask(t)
			}()
		}
	}
}

// Start stats the task scheduler of indexing tasks.
func (sched *TaskScheduler) Start() error {
	sched.wg.Add(1)
	go sched.pipelineLoop()
	sched.wg.Add(1)
	go sched.indexBuildLoop()
	return nil
}

// Close closes the task scheduler of indexing tasks.
func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}
