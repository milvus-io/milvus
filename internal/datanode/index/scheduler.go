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
	"math"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/datanode/taskcost"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
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
	MarkInactive(t Task)
	Enqueue(t Task) error
	GetTaskNum() (int, int)
	GetUsingSlot() int64
	GetUsingResource() taskcommon.Resource
	GetSlotUsage() (int64, int64)
}

// BaseTaskQueue is a basic instance of TaskQueue.
type IndexTaskQueue struct {
	unissuedTasks *list.List
	utLock        sync.Mutex
	queuedSlot    int64 // protected by utLock; only unissued tasks
	queuedMemory  int64 // protected by utLock; not execution reservations

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
	res := t.GetResource()
	memory := queue.sched.capacity().Memory
	full := queue.queuedMemory >= memory || res.Memory > memory-queue.queuedMemory
	// Bound waiting work separately from execution. An empty queue accepts one
	// oversized task so it can eventually run alone.
	if queue.utFull() || (!queue.utEmpty() && full) {
		return merr.Wrap(merr.ErrServiceResourceInsufficient, "index task queue is full")
	}
	// Register the immutable price before making queued work visible. Its
	// execution owns this booking until cleanup finishes.
	queue.sched.resourceMu.Lock()
	queue.sched.executions[t] = &taskExecution{task: t, resource: res, slots: slots}
	queue.sched.resourceMu.Unlock()
	if index, ok := t.(*indexBuildTask); ok {
		index.queuedAt = time.Now()
	}
	queue.unissuedTasks.PushBack(t)
	queue.queuedSlot += slots
	queue.queuedMemory += res.Memory
	select {
	case queue.utBufChan <- struct{}{}:
	default:
	}
	return nil
}

func (queue *IndexTaskQueue) GetUsingSlot() int64 {
	return queue.sched.usingSlot.Load()
}

// GetUsingResource returns the cpu/memory booked by enqueued and active tasks.
func (queue *IndexTaskQueue) GetUsingResource() taskcommon.Resource {
	queue.sched.resourceMu.Lock()
	defer queue.sched.resourceMu.Unlock()
	var used taskcommon.Resource
	for _, exec := range queue.sched.executions {
		used = used.Add(exec.resource)
	}
	return used
}

// GetSlotUsage snapshots held and queued slots together, including the transfer
// from queued demand to execution slots during dispatch.
func (queue *IndexTaskQueue) GetSlotUsage() (int64, int64) {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	return queue.sched.usingSlot.Load(), queue.queuedSlot
}

// PopUnissuedTask preserves vector queue order while allowing non-vector tasks
// to use spare memory while a vector waits for capacity or a permit.
func (queue *IndexTaskQueue) PopUnissuedTask(allowVector bool) Task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	if queue.utEmpty() {
		return nil
	}
	queue.sched.resourceMu.Lock()
	defer queue.sched.resourceMu.Unlock()
	available := queue.sched.availableExecutionMemoryLocked()
	for item := queue.unissuedTasks.Front(); item != nil; item = item.Next() {
		t := item.Value.(Task)
		if queue.sched.ctx.Err() == nil && t.Ctx().Err() == nil {
			if t.IsVectorIndex() && !allowVector {
				continue
			}
			if !queue.sched.acquireResourcesLocked(t, available) {
				if t.IsVectorIndex() {
					// Later vectors cannot bypass the first blocked vector.
					allowVector = false
				}
				continue
			}
		}
		if index, ok := t.(*indexBuildTask); ok {
			outcome := "started"
			if queue.sched.ctx.Err() != nil || t.Ctx().Err() != nil {
				outcome = metrics.CancelLabel
			}
			wait := time.Since(index.queuedAt)
			metrics.DataNodeIndexTaskLatencyInQueue.WithLabelValues(paramtable.GetStringNodeID(), outcome).Observe(float64(wait) / float64(time.Millisecond))
		}
		queue.unissuedTasks.Remove(item)
		queue.queuedSlot -= max(t.GetSlot(), 0)
		queue.queuedMemory -= queue.sched.executions[t].resource.Memory
		return t
	}
	return nil
}

// AddActiveTask makes the execution visible to task-state queries.
func (queue *IndexTaskQueue) AddActiveTask(t Task) {
	queue.sched.resourceMu.Lock()
	defer queue.sched.resourceMu.Unlock()
	queue.sched.executions[t].active = true
}

// MarkInactive stops registry reads before cleanup resets task fields.
func (queue *IndexTaskQueue) MarkInactive(t Task) {
	queue.sched.resourceMu.Lock()
	defer queue.sched.resourceMu.Unlock()
	queue.sched.executions[t].active = false
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
	queue.sched.resourceMu.Lock()
	defer queue.sched.resourceMu.Unlock()

	utNum := queue.unissuedTasks.Len()
	atNum := 0
	// remove the finished task
	for task, exec := range queue.sched.executions {
		if exec.active && task.GetState() != indexpb.JobState_JobStateFinished && task.GetState() != indexpb.JobState_JobStateFailed {
			atNum++
		}
	}
	return utNum, atNum
}

// NewIndexBuildTaskQueue creates a new IndexBuildTaskQueue.
func NewIndexBuildTaskQueue(sched *TaskScheduler) *IndexTaskQueue {
	return &IndexTaskQueue{
		unissuedTasks: list.New(),
		maxTaskNum:    1024,

		utBufChan: make(chan struct{}, 1), // coalesced; the loop drains admissible work
		sched:     sched,
	}
}

type pendingTask struct {
	ctx  context.Context
	task Task
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
	resourceMu    sync.Mutex
	executions    map[Task]*taskExecution // accepted work, including queued tasks
	pendingTasks  []pendingTask
	usingSlot     atomic.Int64
	capacity      func() taskcommon.Resource
}

// NewTaskScheduler creates a new task scheduler of indexing tasks.
func NewTaskScheduler(ctx context.Context, capacity func() taskcommon.Resource) *TaskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		capacity:      capacity,
		ctx:           ctx1,
		cancel:        cancel,
		downloadTasks: make(chan *taskExecution, 1024),
		buildWakeup:   make(chan struct{}, 1),
		executions:    make(map[Task]*taskExecution),
	}
	s.TaskQueue = NewIndexBuildTaskQueue(s)

	return s
}

// removeCanceledTasks requires resourceMu. Canceled tasks must not block
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
	sched.resourceMu.Lock()
	defer sched.resourceMu.Unlock()
	sched.removeCanceledTasks()
	return len(sched.pendingTasks) == 0 || sched.pendingTasks[0].task == t
}

// An idle executor admits one oversized task. Otherwise a scan shares the
// remaining memory of the admitted executions until it selects its first task.
func (sched *TaskScheduler) availableExecutionMemoryLocked() int64 {
	var used int64
	for _, exec := range sched.executions {
		if exec.admitted {
			used += exec.resource.Memory
		}
	}
	if used == 0 {
		return math.MaxInt64
	}
	return sched.capacity().Memory - used
}

func (sched *TaskScheduler) acquireResourcesLocked(t Task, available int64) bool {
	sched.removeCanceledTasks()
	if t.Ctx().Err() != nil {
		return false
	}
	if t.IsVectorIndex() {
		if !slices.ContainsFunc(sched.pendingTasks, func(pending pendingTask) bool { return pending.task == t }) {
			sched.pendingTasks = append(sched.pendingTasks, pendingTask{ctx: t.Ctx(), task: t})
		}
	}

	exec := sched.executions[t]
	if exec.resource.Memory > available {
		return false
	}
	exec.admitted = true
	sched.usingSlot.Add(exec.slots)
	return true
}

// finishAdmission removes a task only when its build is admitted or it terminates.
func (sched *TaskScheduler) finishAdmission(task Task) {
	sched.resourceMu.Lock()
	defer sched.resourceMu.Unlock()
	sched.pendingTasks = slices.DeleteFunc(sched.pendingTasks, func(t pendingTask) bool { return t.task == task })
}

// releaseResources retires the execution after cleanup, returning its accepted
// resource and execution slots together with its pending build-order entry.
func (sched *TaskScheduler) releaseResources(task Task) {
	sched.resourceMu.Lock()
	defer sched.resourceMu.Unlock()
	exec := sched.executions[task]
	delete(sched.executions, task)
	if exec.admitted {
		sched.usingSlot.Sub(exec.slots)
	}
	sched.pendingTasks = slices.DeleteFunc(sched.pendingTasks, func(t pendingTask) bool { return t.task == task })
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
		// segcoreCodeTable already tags as caller input (JsonKeyInvalid, ExprInvalid,
		// DimNotMatch, InvalidParameter, ...) as well as the ParameterInvalid
		// errors the task itself raises.
		return indexpb.JobState_JobStateFailed
	}
	return indexpb.JobState_JobStateRetry
}

// indexAttemptOutcome preserves the scheduler's retry/failure decision and exposes cancellation separately.
func indexAttemptOutcome(err error, state indexpb.JobState) string {
	switch {
	case errors.Is(err, errCancel), errors.Is(err, context.Canceled), errors.Is(err, merr.ErrSegcoreFollyCancel):
		return metrics.CancelLabel
	case state == indexpb.JobState_JobStateRetry:
		return "retry"
	case state == indexpb.JobState_JobStateFailed:
		return metrics.FailLabel
	default:
		return metrics.SuccessLabel
	}
}

type taskExecution struct {
	resource    taskcommon.Resource // immutable accepted booking
	slots       int64
	admitted    bool // protected by resourceMu
	active      bool // protected by resourceMu
	task        Task
	indexTask   *indexBuildTask
	buildEndMs  int64
	buildTimeMs int64
	stopCancel  func() bool
	onFinish    func()
}

func (sched *TaskScheduler) beginTask(t Task) *taskExecution {
	sched.TaskQueue.AddActiveTask(t)
	sched.resourceMu.Lock()
	exec := sched.executions[t]
	sched.resourceMu.Unlock()
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
			ibt.manager.StoreIndexTaskExecutionStart(ibt.req.GetClusterID(), ibt.req.GetBuildID(), ibt.info, started.UnixMilli(), costCPUNum)
		}
		buildStart := time.Now()
		err := exec.task.Execute(ctx)
		elapsed := time.Since(buildStart)
		exec.buildTimeMs = taskcost.ElapsedMs(started)
		exec.buildEndMs = taskcost.NowMs()
		if exec.indexTask != nil {
			state := indexpb.JobState_JobStateFinished
			if err != nil {
				state = getStateFromError(err)
			}
			metrics.DataNodeBuildIndexExecutionSeconds.WithLabelValues(paramtable.GetStringNodeID(), indexAttemptOutcome(err, state)).Observe(elapsed.Seconds())
		}
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
	_, prepared := t.(PreparedTask)
	if prepared {
		// Index input owners must be released before returning memory slots.
		t.Reset()
		debug.FreeOSMemory()
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
		metrics.DataNodeBuildIndexLatency.WithLabelValues(paramtable.GetStringNodeID(), indexAttemptOutcome(err, state)).Observe(exec.indexTask.tr.ElapseSpan().Seconds())
		exec.indexTask.SetStateWithCost(state, reason, exec.buildEndMs, exec.buildTimeMs)
	} else {
		t.SetState(state, reason)
	}
	// Stop registry readers before stats/analyze reset the fields GetState
	// needs. Removing the registry entry does not return its execution budget.
	sched.TaskQueue.MarkInactive(t)
	if !prepared {
		// Stats/analyze need their request to publish the result, but their
		// input owners must still be cleaned up before execution memory is free.
		t.Reset()
		debug.FreeOSMemory()
	}
	sched.releaseResources(t)
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
			sched.finishAdmission(t)
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
