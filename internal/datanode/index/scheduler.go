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
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/datanode/taskcost"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TaskQueue is a queue used to store tasks.
type TaskQueue interface {
	utChan() <-chan struct{}
	utEmpty() bool
	utFull() bool
	addUnissuedTask(t Task) error
	PopUnissuedTask(allowVector bool) Task
	AddActiveTask(t Task)
	PopActiveTask(tName string) Task
	Enqueue(t Task) error
	GetTaskNum() (int, int)
	GetUsingSlot() int64
	GetSlotUsage() (int64, int64)
	GetActiveSlot() int64
}

// BaseTaskQueue is a basic instance of TaskQueue.
type IndexTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[string]Task
	utLock        sync.Mutex
	atLock        sync.Mutex
	queuedSlot    int64 // protected by utLock; only unissued tasks

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
	slots := max(t.GetSlot(), 0)
	budget := max(queue.sched.totalSlots(), 0)
	// Bound queued work to one node's capacity without reserving execution slots.
	// An empty queue accepts one oversized task so it can eventually run alone.
	if queue.utFull() || (!queue.utEmpty() &&
		(queue.queuedSlot >= budget || slots > budget-queue.queuedSlot)) {
		return merr.Wrap(merr.ErrServiceResourceInsufficient, "index task queue is full")
	}
	queue.unissuedTasks.PushBack(t)
	queue.queuedSlot += slots
	select {
	case queue.utBufChan <- struct{}{}:
	default:
	}
	return nil
}

func (queue *IndexTaskQueue) GetUsingSlot() int64 {
	return queue.sched.usingSlot.Load()
}

// GetSlotUsage snapshots held and queued slots together, including the transfer
// from queued demand to execution slots during dispatch.
func (queue *IndexTaskQueue) GetSlotUsage() (int64, int64) {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	return queue.sched.usingSlot.Load(), queue.queuedSlot
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

// PopUnissuedTask preserves vector queue order while allowing non-vector tasks
// to use available slots when a vector waits for capacity or a task permit.
func (queue *IndexTaskQueue) PopUnissuedTask(allowVector bool) Task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	for item := queue.unissuedTasks.Front(); item != nil; item = item.Next() {
		t := item.Value.(Task)
		if queue.sched.ctx.Err() == nil && t.Ctx().Err() == nil {
			if t.IsVectorIndex() && !allowVector {
				continue
			}
			if !queue.sched.acquireSlots(t) {
				if t.IsVectorIndex() {
					// Later vectors cannot bypass the first blocked vector.
					allowVector = false
				}
				continue
			}
		}
		queue.unissuedTasks.Remove(item)
		queue.queuedSlot -= max(t.GetSlot(), 0)
		return t
	}
	return nil
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
		queue.sched.finishAdmission(tName)
		queue.sched.releaseSlots(tName)
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

type pendingTask struct {
	name string
	ctx  context.Context
}

// TaskScheduler is a scheduler of indexing tasks.
type TaskScheduler struct {
	TaskQueue TaskQueue

	wg     sync.WaitGroup
	taskWg sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	downloadTasks chan *taskExecution
	buildWakeup   chan struct{}
	slotMu        sync.Mutex
	heldSlots     map[string]int64
	pendingTasks  []pendingTask
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
		downloadTasks: make(chan *taskExecution, 1024),
		buildWakeup:   make(chan struct{}, 1),
		heldSlots:     make(map[string]int64),
		externalSlots: func() int64 { return 0 },
		totalSlots:    CalculateNodeSlots,
	}
	s.TaskQueue = NewIndexBuildTaskQueue(s)

	return s
}

// SetExternalSlotUsage connects task admission to the same node-wide slot
// accounting reported by QuerySlot. Call before starting the scheduler.
func (sched *TaskScheduler) SetExternalSlotUsage(usage func() int64) {
	sched.externalSlots = usage
}

func (sched *TaskScheduler) slotsAvailable(slots int64) bool {
	used := sched.usingSlot.Load() + sched.externalSlots()
	// Keep oversized tasks executable on an otherwise idle node, as in the
	// coordinator's existing best-effort admission. They run alone.
	return used == 0 || slots <= sched.totalSlots()-used
}

// removeCanceledTasks requires slotMu. Canceled tasks must not block
// admission while their task owners are still waiting to run cleanup.
func (sched *TaskScheduler) removeCanceledTasks() {
	sched.pendingTasks = slices.DeleteFunc(sched.pendingTasks, func(t pendingTask) bool {
		return t.ctx.Err() != nil
	})
}

// isBuildTurn preserves vector order across download completion and admission retries.
func (sched *TaskScheduler) isBuildTurn(t Task) bool {
	if !t.IsVectorIndex() {
		return true
	}
	sched.slotMu.Lock()
	defer sched.slotMu.Unlock()
	sched.removeCanceledTasks()
	return len(sched.pendingTasks) == 0 || sched.pendingTasks[0].name == t.Name()
}

func (sched *TaskScheduler) acquireSlots(t Task) bool {
	sched.slotMu.Lock()
	defer sched.slotMu.Unlock()
	if _, ok := sched.heldSlots[t.Name()]; ok {
		return true
	}
	sched.removeCanceledTasks()
	if t.Ctx().Err() != nil {
		return false
	}
	if t.IsVectorIndex() {
		if !slices.ContainsFunc(sched.pendingTasks, func(pending pendingTask) bool { return pending.name == t.Name() }) {
			sched.pendingTasks = append(sched.pendingTasks, pendingTask{name: t.Name(), ctx: t.Ctx()})
		}
	}
	if !sched.slotsAvailable(t.GetSlot()) {
		return false
	}
	sched.heldSlots[t.Name()] = t.GetSlot()
	sched.usingSlot.Add(t.GetSlot())
	return true
}

// finishAdmission removes a task only when its build is admitted or it terminates.
func (sched *TaskScheduler) finishAdmission(name string) {
	sched.slotMu.Lock()
	defer sched.slotMu.Unlock()
	sched.pendingTasks = slices.DeleteFunc(sched.pendingTasks, func(t pendingTask) bool { return t.name == name })
}

func (sched *TaskScheduler) releaseSlots(name string) {
	sched.slotMu.Lock()
	if slots, ok := sched.heldSlots[name]; ok {
		delete(sched.heldSlots, name)
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
	task        Task
	indexTask   *indexBuildTask
	buildEndMs  int64
	buildTimeMs int64
	stopCancel  func() bool
	onFinish    func()
}

func (sched *TaskScheduler) beginTask(t Task) *taskExecution {
	sched.TaskQueue.AddActiveTask(t)
	exec := &taskExecution{task: t}
	if ibt, ok := t.(*indexBuildTask); ok {
		exec.indexTask = ibt
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

// executeTask measures construction only; queueing, download and upload are
// excluded from the cost paired with the build's CPU count.
func (sched *TaskScheduler) executeTask(exec *taskExecution) error {
	return sched.runStep(exec.task, func(ctx context.Context) error {
		started := time.Now()
		if ibt := exec.indexTask; ibt != nil {
			costCPUNum := taskcost.EstimateIndexBuildCPUNum(ibt.IsVectorIndex())
			ibt.manager.StoreIndexTaskExecutionStart(ibt.req.GetClusterID(), ibt.req.GetBuildID(), started.UnixMilli(), costCPUNum)
		}
		err := exec.task.Execute(ctx)
		exec.buildTimeMs = taskcost.ElapsedMs(started)
		exec.buildEndMs = taskcost.NowMs()
		return err
	})
}

func (sched *TaskScheduler) finishTask(exec *taskExecution, err error) {
	if exec.onFinish != nil {
		defer exec.onFinish()
	}
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
		if exec.buildEndMs == 0 {
			exec.buildEndMs = taskcost.NowMs()
		}
		exec.indexTask.SetStateWithCost(state, reason, exec.buildEndMs, exec.buildTimeMs)
	} else {
		t.SetState(state, reason)
	}
	sched.TaskQueue.PopActiveTask(t.Name())
}

func (sched *TaskScheduler) processTask(exec *taskExecution) {
	t := exec.task
	var err error
	defer func() { sched.finishTask(exec, err) }()
	if err = sched.runStep(t, t.PreExecute); err != nil {
		return
	}
	if err = sched.executeTask(exec); err != nil {
		return
	}
	err = sched.runStep(t, t.PostExecute)
}

func (sched *TaskScheduler) indexBuildLoop() {
	mlog.Debug(sched.ctx, "TaskScheduler start build loop ...")
	defer sched.wg.Done()
	defer close(sched.downloadTasks)
	defer func() {
		for t := sched.TaskQueue.PopUnissuedTask(true); t != nil; t = sched.TaskQueue.PopUnissuedTask(true) {
			sched.finishTask(sched.beginTask(t), errCancel)
		}
	}()
	var runningVectors atomic.Int64
	finished := make(chan struct{}, 1)
	concurrency := max(1, paramtable.Get().DataNodeCfg.MaxVecIndexBuildConcurrency.GetAsInt())
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if sched.ctx.Err() != nil {
			return
		}
		if limit := paramtable.Get().DataNodeCfg.MaxVecIndexBuildConcurrency.GetAsInt(); limit > 0 {
			concurrency = limit
		}
		for sched.ctx.Err() == nil {
			t := sched.TaskQueue.PopUnissuedTask(runningVectors.Load() < int64(concurrency))
			if t == nil {
				break
			}
			exec := sched.beginTask(t)
			if t.Ctx().Err() != nil {
				sched.finishTask(exec, errCancel)
				continue
			}
			if t.IsVectorIndex() {
				// One permit covers preparation through upload and cleanup,
				// regardless of how the task's internal stages are scheduled.
				runningVectors.Inc()
				exec.onFinish = func() {
					runningVectors.Dec()
					select {
					case finished <- struct{}{}:
					default:
					}
				}
			}
			if _, ok := t.(PreparedTask); ok {
				select {
				case sched.downloadTasks <- exec:
				case <-sched.ctx.Done():
					sched.finishTask(exec, errCancel)
				}
				continue
			}
			sched.taskWg.Add(1)
			sched.finishAdmission(t.Name())
			go func() {
				defer sched.taskWg.Done()
				sched.processTask(exec)
			}()
		}
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.TaskQueue.utChan():
		case <-finished:
		case <-ticker.C:
		}
	}
}

// Start stats the task scheduler of indexing tasks.
func (sched *TaskScheduler) Start() error {
	sched.taskWg.Add(1)
	go sched.pipelineLoop()
	sched.wg.Add(1)
	go sched.indexBuildLoop()
	return nil
}

// Close stops dispatching new tasks. Running tasks retain their resources until
// they return; use Wait before destroying resources they may still access.
func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}

// Wait joins all tasks after Close, including native calls that cannot be interrupted.
func (sched *TaskScheduler) Wait() {
	sched.taskWg.Wait()
}
