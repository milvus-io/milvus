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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/datanode/taskcost"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	queue.sched.executions[t] = &taskExecution{resource: res, slots: slots}
	queue.sched.resourceMu.Unlock()
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

// TaskScheduler admits index, stats and analyze work before preparation.
// Accepted bookings include waiting tasks; execution reservations do not.
type TaskScheduler struct {
	TaskQueue  TaskQueue
	wg         sync.WaitGroup
	taskWg     sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	wake       chan struct{}
	resourceMu sync.Mutex
	executions map[Task]*taskExecution
	usingSlot  atomic.Int64
	capacity   func() taskcommon.Resource
}

// NewTaskScheduler creates the index, stats and analyze scheduler.
func NewTaskScheduler(ctx context.Context, capacity func() taskcommon.Resource) *TaskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		capacity:   capacity,
		ctx:        ctx1,
		cancel:     cancel,
		wake:       make(chan struct{}, 1),
		executions: make(map[Task]*taskExecution),
	}
	s.TaskQueue = NewIndexBuildTaskQueue(s)
	return s
}

type taskExecution struct {
	resource taskcommon.Resource
	slots    int64
	admitted bool
	active   bool
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
	if t.Ctx().Err() != nil {
		return false
	}
	exec := sched.executions[t]
	if exec.resource.Memory > available {
		return false
	}
	exec.admitted = true
	sched.usingSlot.Add(exec.slots)
	return true
}

func (sched *TaskScheduler) notify() {
	select {
	case sched.wake <- struct{}{}:
	default:
	}
}

// releaseResources retires the execution after cleanup, returning its accepted
// resource and execution slots together.
func (sched *TaskScheduler) releaseResources(task Task) {
	sched.resourceMu.Lock()
	defer sched.resourceMu.Unlock()
	exec := sched.executions[task]
	delete(sched.executions, task)
	if exec.admitted {
		sched.usingSlot.Sub(exec.slots)
	}
	sched.notify()
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

// finishTask releases native owners before making execution memory reusable.
// Registry readers must stop before Reset invalidates the task's state fields.
func (sched *TaskScheduler) finishTask(t Task) {
	sched.TaskQueue.MarkInactive(t)
	t.Reset()
	debug.FreeOSMemory()
	sched.releaseResources(t)
}

func (sched *TaskScheduler) processTask(t Task) {
	wrap := func(fn func(ctx context.Context) error) error {
		select {
		case <-sched.ctx.Done():
			return errCancel
		case <-t.Ctx().Done():
			return errCancel
		default:
			return fn(t.Ctx())
		}
	}

	sched.TaskQueue.AddActiveTask(t)
	defer sched.finishTask(t)
	var (
		indexTask  *indexBuildTask
		costCPUNum int64
		execStart  time.Time
	)
	if ibt, ok := t.(*indexBuildTask); ok {
		indexTask = ibt
		costCPUNum = taskcost.EstimateIndexBuildCPUNum(indexTask.IsVectorIndex())
		// execStart carries a monotonic clock reading; CostTimeMs derived from
		// it is immune to wall-clock steps. ExecStartMs/ExecEndMs stay wall-clock
		// timestamps for external exposure.
		execStart = time.Now()
		indexTask.manager.StoreIndexTaskExecutionStart(indexTask.req.GetClusterID(), indexTask.req.GetBuildID(), taskcost.NowMs(), costCPUNum)
		mlog.Debug(t.Ctx(), "process task", mlog.String("task", t.Name()), mlog.Int64("costCPUNum", costCPUNum))
	} else {
		mlog.Debug(t.Ctx(), "process task", mlog.String("task", t.Name()))
	}

	pipelines := []func(context.Context) error{t.PreExecute, t.Execute, t.PostExecute}
	for _, fn := range pipelines {
		if err := wrap(fn); err != nil {
			if indexTask != nil {
				costTimeMs := taskcost.ElapsedMs(execStart)
				// End bookkeeping and final state must land in one critical
				// section, so a concurrent QueryTask never sees a final cost
				// paired with an in-progress state.
				indexTask.SetStateWithCost(getStateFromError(err), err.Error(), taskcost.NowMs(), costTimeMs)
				mlog.Warn(t.Ctx(), "process task failed", mlog.Err(err), mlog.Int64("costTimeMs", costTimeMs), mlog.Int64("costCPUNum", costCPUNum))
			} else {
				t.SetState(getStateFromError(err), err.Error())
				mlog.Warn(t.Ctx(), "process task failed", mlog.Err(err))
			}
			return
		}
	}
	if indexTask != nil {
		costTimeMs := taskcost.ElapsedMs(execStart)
		indexTask.SetStateWithCost(indexpb.JobState_JobStateFinished, "", taskcost.NowMs(), costTimeMs)
		mlog.Debug(t.Ctx(), "process task completed", mlog.String("task", t.Name()), mlog.Int64("costTimeMs", costTimeMs), mlog.Int64("costCPUNum", costCPUNum))
	} else {
		t.SetState(indexpb.JobState_JobStateFinished, "")
		mlog.Debug(t.Ctx(), "process task completed", mlog.String("task", t.Name()))
	}
}

func (sched *TaskScheduler) indexBuildLoop() {
	mlog.Debug(sched.ctx, "TaskScheduler start build loop ...")
	defer sched.wg.Done()
	defer func() {
		// Cancellation bypasses admission so every accepted queued task is
		// retired and returns its booking even if it never started.
		for t := sched.TaskQueue.PopUnissuedTask(true); t != nil; t = sched.TaskQueue.PopUnissuedTask(true) {
			sched.processTask(t)
		}
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var runningVectors atomic.Int64
	for {
		for sched.ctx.Err() == nil {
			limit := GetVecIndexBuildPool().Cap()
			task := sched.TaskQueue.PopUnissuedTask(runningVectors.Load() < int64(limit))
			if task == nil {
				break
			}
			if sched.ctx.Err() != nil || task.Ctx().Err() != nil {
				// A canceled queued vector owns no native work. Retire it
				// immediately instead of waiting for a full vector pool.
				sched.processTask(task)
				continue
			}
			vector := task.IsVectorIndex()
			if vector {
				runningVectors.Inc()
			}
			sched.taskWg.Add(1)
			go func() {
				defer sched.taskWg.Done()
				if !vector {
					sched.processTask(task)
					return
				}
				defer func() { runningVectors.Dec(); sched.notify() }()
				GetVecIndexBuildPool().Submit(func() (any, error) {
					sched.processTask(task)
					return nil, nil
				}).Await()
			}()
		}
		select {
		case <-sched.ctx.Done():
			return
		case <-sched.TaskQueue.utChan():
		case <-sched.wake:
		case <-ticker.C:
		}
	}
}

// Start starts the index, stats and analyze scheduler.
func (sched *TaskScheduler) Start() error {
	sched.wg.Add(1)
	go sched.indexBuildLoop()
	return nil
}

// Close waits for cancellation, native execution and cleanup to finish.
func (sched *TaskScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
	sched.taskWg.Wait()
}
