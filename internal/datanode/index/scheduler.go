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
	PopActiveTask(t Task) Task
	ReleaseResource(t Task)
	Enqueue(t Task) error
	GetTaskNum() (int, int)
	GetUsingSlot() int64
	GetUsingResource() taskcommon.Resource
	GetSlotUsage() (int64, int64)
	GetActiveSlot() int64
}

// BaseTaskQueue is a basic instance of TaskQueue.
type IndexTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[Task]struct{}
	bookings      map[Task]taskcommon.Resource // protected by utLock
	utLock        sync.Mutex
	atLock        sync.Mutex
	queuedSlot    int64 // protected by utLock; only unissued tasks
	queuedMemory  int64 // protected by utLock; not execution reservations

	// maxTaskNum should keep still
	maxTaskNum int64

	utBufChan chan struct{} // to block scheduler

	// Cpu/memory booked for enqueued and active tasks, the two-dimensional
	// counterpart of the slot accounting above. DataCoord estimates both and
	// QuerySlot reports all accepted work, including the queue. Execution
	// admission uses a separate ledger so waiting tasks cannot block themselves.
	usingCPU    atomic.Int64
	usingMemory atomic.Int64

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
	res := queue.sched.taskResource(t)
	var full bool
	if memory := queue.sched.memoryCapacity(); memory > 0 {
		full = queue.queuedMemory >= memory || res.Memory > memory-queue.queuedMemory
	}
	// Bound waiting work separately from execution. An empty queue accepts one
	// oversized task so it can eventually run alone.
	if queue.utFull() || (!queue.utEmpty() && full) {
		return merr.Wrap(merr.ErrServiceResourceInsufficient, "index task queue is full")
	}
	// Book cpu/memory before the task becomes visible to the scheduler: once
	// it is in unissuedTasks it can be popped, run and released by
	// ReleaseResource at any time, and a release that ran ahead of its booking
	// would leave the ledger briefly below zero -- reported as an idle node.
	queue.bookings[t] = res
	queue.usingCPU.Add(res.CPU)
	queue.usingMemory.Add(res.Memory)
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
//
// A task is booked before it can be released (addUnissuedTask), so the
// counters do not go negative in normal operation. They are plain atomics,
// though, where the other two executors clamp at zero
// (taskcommon.Resource.Sub), and a negative figure here is the unsafe
// direction: QuerySlot computes available = total - used, so a negative "used"
// reports more free than the node has. It is clamped at zero on the way out so
// the three ledgers behave the same.
func (queue *IndexTaskQueue) GetUsingResource() taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(queue.usingCPU.Load(), 0),
		Memory: max(queue.usingMemory.Load(), 0),
	}
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
	for task := range queue.activeTasks {
		slots += queue.sched.taskSlots(task)
	}
	return slots
}

// PopUnissuedTask preserves vector queue order while allowing non-vector tasks
// to use spare memory (legacy: slots) while a vector waits for capacity or a permit.
func (queue *IndexTaskQueue) PopUnissuedTask(allowVector bool) Task {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()

	for item := queue.unissuedTasks.Front(); item != nil; item = item.Next() {
		t := item.Value.(Task)
		if queue.sched.ctx.Err() == nil && t.Ctx().Err() == nil {
			if t.IsVectorIndex() && !allowVector {
				continue
			}
			if !queue.sched.acquireResources(t, queue.bookings[t].Memory) {
				if t.IsVectorIndex() {
					// Later vectors cannot bypass the first blocked vector.
					allowVector = false
				}
				continue
			}
		}
		queue.unissuedTasks.Remove(item)
		queue.queuedSlot -= max(t.GetSlot(), 0)
		queue.queuedMemory -= queue.bookings[t].Memory
		return t
	}
	return nil
}

// AddActiveTask adds a task to activeTasks.
func (queue *IndexTaskQueue) AddActiveTask(t Task) {
	queue.atLock.Lock()
	defer queue.atLock.Unlock()

	_, ok := queue.activeTasks[t]
	if ok {
		mlog.Debug(t.Ctx(), "task already in active task list", mlog.String("TaskID", t.Name()))
	}

	queue.activeTasks[t] = struct{}{}
}

// PopActiveTask removes the execution from the registry. Its owner returns
// reservations separately, after task cleanup has actually finished.
func (queue *IndexTaskQueue) PopActiveTask(t Task) Task {
	queue.atLock.Lock()

	_, ok := queue.activeTasks[t]
	if ok {
		delete(queue.activeTasks, t)
		queue.atLock.Unlock()
		return t
	}
	queue.atLock.Unlock()
	mlog.Debug(queue.sched.ctx, "task was not found in the active task list", mlog.String("TaskName", t.Name()))
	return nil
}

// ReleaseResource returns the immutable booking for this execution. Cleanup
// may reset task fields, and a canceled native call may overlap a retry with
// the same name; neither may release another execution's budget.
func (queue *IndexTaskQueue) ReleaseResource(t Task) {
	queue.utLock.Lock()
	defer queue.utLock.Unlock()
	if res, ok := queue.bookings[t]; ok {
		delete(queue.bookings, t)
		queue.usingCPU.Sub(res.CPU)
		queue.usingMemory.Sub(res.Memory)
	}
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
	for task := range queue.activeTasks {
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
		activeTasks:   make(map[Task]struct{}),
		bookings:      make(map[Task]taskcommon.Resource),
		maxTaskNum:    1024,

		utBufChan: make(chan struct{}, 1), // coalesced; the loop drains admissible work
		sched:     sched,
	}
}

// TaskScheduler admits index, stats and analyze work before preparation.
// Accepted bookings include waiting tasks; execution reservations do not.
type TaskScheduler struct {
	TaskQueue       TaskQueue
	wg              sync.WaitGroup
	taskWg          sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	wake            chan struct{}
	resourceMu      sync.Mutex
	heldSlots       map[Task]int64
	heldMemory      map[Task]int64
	usingSlot       atomic.Int64
	usingMemory     atomic.Int64
	totalSlots      func() int64
	totalMemory     func() int64
	memoryAdmission *taskcost.MemoryAdmission
	heldReleases    map[Task]func()
}

// NewTaskScheduler creates the index, stats and analyze scheduler.
func NewTaskScheduler(ctx context.Context) *TaskScheduler {
	ctx1, cancel := context.WithCancel(ctx)
	s := &TaskScheduler{
		ctx:          ctx1,
		cancel:       cancel,
		wake:         make(chan struct{}, 1),
		heldSlots:    make(map[Task]int64),
		heldMemory:   make(map[Task]int64),
		heldReleases: make(map[Task]func()),
		totalSlots:   CalculateNodeSlots,
	}
	s.TaskQueue = NewIndexBuildTaskQueue(s)
	return s
}

// SetMemoryAdmission connects local execution to the node's memory capacity.
// Call before Start. Other executors' accepted work shares this budget; this
// scheduler's waiting bookings are excluded so they cannot block themselves.
// When memory capacity is unknown, retain 3.0's existing execution-pool behavior.
// The returned budget can also admit import V3 without double-booking memory.
func (sched *TaskScheduler) SetMemoryAdmission(capacity, externalUsage func() int64) *taskcost.MemoryAdmission {
	sched.totalMemory = capacity
	sched.memoryAdmission = taskcost.NewMemoryAdmission(capacity, externalUsage)
	return sched.memoryAdmission
}

func (sched *TaskScheduler) memoryCapacity() int64 {
	if sched.totalMemory == nil {
		return 0
	}
	return max(sched.totalMemory(), 0)
}

func (sched *TaskScheduler) taskResource(t Task) taskcommon.Resource {
	res := t.GetResource()
	if res.Memory > 0 {
		return res
	}
	if capacity := sched.memoryCapacity(); capacity > 0 {
		// Direct legacy index RPCs carry no estimate. Preserve their slot share
		// instead of letting zero-priced work bypass memory admission.
		res.Memory = taskcost.ResourceFromSlot(t.GetSlot(), sched.totalSlots(),
			taskcommon.Resource{Memory: capacity}).Memory
		if res.Memory <= 0 {
			res.Memory = max(paramtable.Get().DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize(), 1)
		}
	}
	return res
}

func (sched *TaskScheduler) acquireResources(t Task, memory int64) bool {
	sched.resourceMu.Lock()
	defer sched.resourceMu.Unlock()
	if _, ok := sched.heldSlots[t]; ok {
		return true
	}
	if t.Ctx().Err() != nil {
		return false
	}

	if sched.memoryAdmission != nil {
		release, ok := sched.memoryAdmission.TryAcquire(memory)
		if !ok {
			return false
		}
		sched.heldReleases[t] = release
	}

	slots := max(t.GetSlot(), 0)
	sched.heldSlots[t] = slots
	sched.heldMemory[t] = memory
	sched.usingSlot.Add(slots)
	sched.usingMemory.Add(memory)
	return true
}

func (sched *TaskScheduler) notify() {
	select {
	case sched.wake <- struct{}{}:
	default:
	}
}

func (sched *TaskScheduler) releaseResources(t Task) {
	sched.resourceMu.Lock()
	if slots, ok := sched.heldSlots[t]; ok {
		delete(sched.heldSlots, t)
		sched.usingSlot.Sub(slots)
		sched.usingMemory.Sub(sched.heldMemory[t])
		delete(sched.heldMemory, t)
		if release := sched.heldReleases[t]; release != nil {
			release()
			delete(sched.heldReleases, t)
		}
	}
	sched.resourceMu.Unlock()
	sched.notify()
}

func (sched *TaskScheduler) taskSlots(t Task) int64 {
	sched.resourceMu.Lock()
	defer sched.resourceMu.Unlock()
	return sched.heldSlots[t]
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
	sched.TaskQueue.PopActiveTask(t)
	t.Reset()
	debug.FreeOSMemory()
	sched.releaseResources(t)
	sched.TaskQueue.ReleaseResource(t)
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
			limit := max(GetVecIndexBuildPool().Cap(), 1)
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
				started := false
				_, err := GetVecIndexBuildPool().Submit(func() (any, error) {
					started = true
					sched.processTask(task)
					return nil, nil
				}).Await()
				if err != nil && !started {
					// A rejected pool submission never runs processTask's
					// cleanup. Retire its accepted booking and reservation here.
					task.SetState(getStateFromError(err), err.Error())
					sched.finishTask(task)
				}
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
