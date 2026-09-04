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

package task

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const NullNodeID = -1

type GlobalScheduler interface {
	Enqueue(task Task)
	// AbortAndRemoveTask drops a task on its worker before releasing scheduler
	// ownership. It waits for an in-flight callback on that task, bounded by
	// dataCoord.requestTimeoutSeconds, and the drop itself has the same bound.
	AbortAndRemoveTask(taskID int64)
	// GetPendingTaskCount returns the number of queued tasks of the given type.
	// The queue is shared by every task type, so callers that gate admission for
	// one kind of work must scope the count to that kind, otherwise an unrelated
	// backlog starves them.
	GetPendingTaskCount(taskType taskcommon.Type) int

	// Finalize hands ownership of a task back to its owner. The task is removed
	// from dispatch first -- so no further worker callback can be issued for it
	// -- and fn then runs under the same per-task lock that guards those
	// callbacks, waiting for any in-flight one to drain. Use it to run terminal
	// work (cleanup) that must not interleave with the worker callbacks.
	Finalize(taskID int64, fn func())

	// Update runs fn under the per-task lock, so it cannot interleave with a
	// worker callback for the same task. It waits for an in-flight callback to
	// finish. That wait is bounded by the callback's own work -- normally one
	// worker RPC (dataCoord.requestTimeoutSeconds), but a callback that runs
	// terminal follow-up inline (refresh finishing its job) can hold the lock
	// longer.
	Update(taskID int64, fn func())

	Start()
	Stop()
}

var _ GlobalScheduler = (*globalTaskScheduler)(nil)

type globalTaskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu *lock.KeyLock[int64]
	// tasks holds every task the scheduler owns. There is no
	// separate pending/running container: what the scheduler should do follows
	// from the task's own state and is derived once per round, so a state
	// change never has to be mirrored by moving the task somewhere else. That
	// is what keeps ownership unambiguous -- "the scheduler owns this task" is
	// a single map lookup, not an agreement between two containers.
	tasks *typeutil.ConcurrentMap[int64, Task]
	// checking guards the detached check pass so at most one is ever in flight.
	// See round().
	checking  atomic.Bool
	execPool  *conc.Pool[struct{}]
	checkPool *conc.Pool[struct{}]
	cluster   session.Cluster
}

// Enqueue hands a task to the scheduler, ignoring one it already owns.
//
// Ownership is a single map entry: GetOrInsert answers "is this already ours?"
// and claims it in one atomic step, with no second scheduler state to keep in
// sync.
func (s *globalTaskScheduler) Enqueue(task Task) {
	// Read the state once, so the branch that decides is the branch that records.
	state := task.GetTaskState()
	if state != taskcommon.Init && state != taskcommon.InProgress {
		// Nothing to schedule. Claiming it would leave an entry only the next
		// round could collect.
		return
	}
	if _, loaded := s.tasks.GetOrInsert(task.GetTaskID(), task); loaded {
		return
	}
	if state == taskcommon.InProgress {
		task.SetTaskTime(taskcommon.TimeStart, time.Now())
	} else {
		task.SetTaskTime(taskcommon.TimeQueue, time.Now())
	}
	mlog.Info(s.ctx, "task enqueued", WrapTaskLog(task)...)
}

// GetPendingTaskCount returns the number of queued tasks of the given type. The
// queue is shared by every task type, so callers that gate admission for one
// kind of work must scope the count to that kind, otherwise an unrelated backlog
// starves them.
func (s *globalTaskScheduler) GetPendingTaskCount(taskType taskcommon.Type) int {
	count := 0
	for _, task := range s.tasks.Values() {
		if task.GetTaskState() == taskcommon.Init && task.GetTaskType() == taskType {
			count++
		}
	}
	return count
}

// releaseUnderTaskLock drops scheduler ownership of a task. The caller must
// hold the task's key lock, which is what stops it from racing a callback that
// is in the middle of acting on the same task. When worker cleanup is needed,
// callers send it before this release so a same-ID retry cannot be admitted
// ahead of the old attempt's drop.
//
// The returned task is nil when the scheduler did not own it -- the common case
// for an idempotent abort.
func (s *globalTaskScheduler) releaseUnderTaskLock(taskID int64) Task {
	task, ok := s.tasks.GetAndRemove(taskID)
	if !ok {
		return nil
	}
	return task
}

// ownsUnderTaskLock reports whether task is still the scheduler's copy of its
// ID. Both loops pick their work from an unlocked snapshot, so an abort or a
// Finalize may have taken the task before the callback got the lock. Identity
// rather than presence: an abort followed by an Enqueue of a rebuilt task leaves
// a different object under the same ID, and the stale copy must not act on it.
func (s *globalTaskScheduler) ownsUnderTaskLock(task Task) bool {
	owned, ok := s.tasks.Get(task.GetTaskID())
	return ok && owned == task
}

func (s *globalTaskScheduler) AbortAndRemoveTask(taskID int64) {
	s.mu.Lock(taskID)
	defer s.mu.Unlock(taskID)
	if task, ok := s.tasks.Get(taskID); ok {
		mlog.Info(s.ctx, "task aborted, releasing it and dropping it on its worker",
			WrapTaskLog(task, mlog.String("state", task.GetTaskState().String()))...)
		// Keep the map entry until Drop returns. A business inspector may offer a
		// same-ID retry meanwhile; Enqueue sees the old ownership and leaves it
		// alone instead of letting this drop delete the new worker attempt.
		task.DropTaskOnWorker(s.cluster)
		s.releaseUnderTaskLock(taskID)
	}
}

func (s *globalTaskScheduler) Finalize(taskID int64, fn func()) {
	s.mu.Lock(taskID)
	defer s.mu.Unlock(taskID)
	// Remove under the lock, never before it. Holding the lock means no worker
	// callback is in flight for this task, and one still waiting behind us finds
	// the task gone and gives up (ownsUnderTaskLock).
	if released := s.releaseUnderTaskLock(taskID); released != nil {
		// Only when something was held: an owner may finalize a task the
		// scheduler already let go, and that is not an event.
		mlog.Info(s.ctx, "scheduler ownership released to the task owner",
			WrapTaskLog(released, mlog.String("state", released.GetTaskState().String()))...)
	}
	fn()
}

func (s *globalTaskScheduler) Update(taskID int64, fn func()) {
	s.mu.Lock(taskID)
	defer s.mu.Unlock(taskID)
	fn()
}

// createUnderTaskLock dispatches one task. Nothing has to be moved on success:
// the task stays the scheduler's, and its new state alone puts it in check()'s
// hands.
func (s *globalTaskScheduler) createUnderTaskLock(task Task, nodeID int64) {
	taskID := task.GetTaskID()
	// Exclusive, not shared: this is the only thing serializing the mutations a
	// task's callbacks make to its own state.
	s.mu.Lock(taskID)
	defer s.mu.Unlock(taskID)
	if !s.ownsUnderTaskLock(task) {
		mlog.Info(s.ctx, "task is no longer owned by this dispatch, skip it", WrapTaskLog(task)...)
		return
	}
	if task.GetTaskState() != taskcommon.Init {
		// Its state changed between the snapshot and this lock. Do nothing --
		// the next round classifies it again.
		return
	}
	mlog.Info(s.ctx, "processing task...", WrapTaskLog(task)...)
	task.CreateTaskOnWorker(nodeID, s.cluster)
	switch task.GetTaskState() {
	case taskcommon.InProgress:
		// Accepted by the worker.
		task.SetTaskTime(taskcommon.TimeStart, time.Now())
	case taskcommon.None, taskcommon.Init, taskcommon.Retry:
		// One Enqueue buys exactly one Create call. The business callback owns
		// cleanup for a failed attempt before it exposes Init/Retry. A generic
		// Drop here would be both redundant and dangerous: after ownership is
		// released, a delayed Drop can delete the owner's same-ID retry.
		mlog.Info(s.ctx, "task dispatch attempt ended, releasing scheduler ownership",
			WrapTaskLog(task, mlog.String("state", task.GetTaskState().String()))...)
		task.SetTaskTime(taskcommon.TimeEnd, time.Now())
		s.releaseUnderTaskLock(taskID)
	default:
		// Terminal tasks are no longer useful on the worker.
		mlog.Info(s.ctx, "task ended during dispatch, releasing scheduler ownership",
			WrapTaskLog(task, mlog.String("state", task.GetTaskState().String()))...)
		task.SetTaskTime(taskcommon.TimeEnd, time.Now())
		task.DropTaskOnWorker(s.cluster)
		s.releaseUnderTaskLock(taskID)
	}
}

// queryUnderTaskLock polls one running task.
func (s *globalTaskScheduler) queryUnderTaskLock(task Task) {
	taskID := task.GetTaskID()
	s.mu.Lock(taskID)
	defer s.mu.Unlock(taskID)
	if !s.ownsUnderTaskLock(task) {
		return
	}
	if task.GetTaskState() != taskcommon.InProgress {
		// Its state changed between the snapshot and this lock -- the owner
		// ended the attempt in meta, or a callback reset it to Init. Polling it
		// anyway would hand a terminal task one more worker round-trip, and an
		// error on that round-trip can push a Failed task back to Pending
		// (import's QueryTaskOnWorker does exactly that on a query error).
		// Leave it for the next round to classify.
		return
	}
	task.QueryTaskOnWorker(s.cluster)
	switch task.GetTaskState() {
	case taskcommon.None:
		// The worker does not know this task. Nothing to drop -- but this is a
		// task disappearing on us, so it is never something to pass over in
		// silence.
		mlog.Info(s.ctx, "worker does not know this task, releasing scheduler ownership", WrapTaskLog(task)...)
		s.releaseUnderTaskLock(taskID)
	case taskcommon.InProgress:
		// Still running.
	case taskcommon.Init, taskcommon.Retry:
		// The business callback already handled this attempt and returned it to
		// its owner. Do not issue a second Drop that can race a same-ID retry.
		mlog.Info(s.ctx, "task attempt ended, releasing scheduler ownership",
			WrapTaskLog(task, mlog.String("state", task.GetTaskState().String()))...)
		task.SetTaskTime(taskcommon.TimeEnd, time.Now())
		s.releaseUnderTaskLock(taskID)
	default:
		// Terminal tasks are no longer useful on the worker.
		mlog.Info(s.ctx, "task ended on its worker, releasing scheduler ownership",
			WrapTaskLog(task, mlog.String("state", task.GetTaskState().String()))...)
		task.SetTaskTime(taskcommon.TimeEnd, time.Now())
		task.DropTaskOnWorker(s.cluster)
		s.releaseUnderTaskLock(taskID)
	}
}

func (s *globalTaskScheduler) Start() {
	dur := paramtable.Get().DataCoordCfg.TaskScheduleInterval.GetAsDuration(time.Millisecond)
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(dur)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.round()
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.updateTaskTimeMetrics()
			}
		}
	}()
}

func (s *globalTaskScheduler) Stop() {
	s.cancel()
	s.wg.Wait()
	s.execPool.Release()
	s.checkPool.Release()
}

// pickNode selects the least-loaded node (the one with the most available slots)
// for a task requiring taskSlot slots, instead of the first node that happens to
// fit. Always assigning to the most-available node spreads tasks evenly across
// DataNodes (water-filling on available slots) rather than packing them onto
// whichever node is iterated first.
//
// It returns NullNodeID when no node has any available slot for a positive-slot
// task. Non-positive-slot tasks are scheduled on the most-available node without
// consuming slots. When even the most-available node cannot fully satisfy
// taskSlot, it falls back to that node on a best-effort basis and drains its
// slots.
//
// The picked node's slots are decremented in place, so a scheduling round that
// reuses the same map across all its picks sees each task's cost.
func (s *globalTaskScheduler) pickNode(nodeSlots map[int64]*session.WorkerSlots, taskSlot int64) int64 {
	// A linear scan, not a heap: this runs once per pending task against the
	// DataNode count, which is small enough that a few integer comparisons beat
	// the bookkeeping a heap needs to stay ordered while its elements are
	// mutated.
	bestNode := int64(NullNodeID)
	var best *session.WorkerSlots
	for nodeID, slots := range nodeSlots {
		if best == nil || slots.AvailableSlots > best.AvailableSlots {
			bestNode, best = nodeID, slots
		}
	}
	if best == nil {
		return NullNodeID
	}
	if taskSlot <= 0 {
		return bestNode
	}
	if best.AvailableSlots <= 0 {
		// The most-available compatible node has no slot, so neither does any
		// other compatible node.
		return NullNodeID
	}
	if best.AvailableSlots >= taskSlot {
		best.AvailableSlots -= taskSlot
	} else {
		// No compatible node can fully satisfy the request; assign to the
		// most-available compatible node on a best-effort basis.
		best.AvailableSlots = 0
	}
	return bestNode
}

// partition buckets every owned task by its current state in a single pass, in
// no particular order within a bucket.
//
// A single scan answers all three questions at once, so a round cannot disagree
// with itself about where a task belongs. The buckets are only a snapshot --
// every callback re-checks ownership and state under the key lock.
func (s *globalTaskScheduler) partition() (pending, running, ended []Task) {
	for _, task := range s.tasks.Values() {
		switch task.GetTaskState() {
		case taskcommon.Init:
			pending = append(pending, task)
		case taskcommon.InProgress:
			running = append(running, task)
		default:
			ended = append(ended, task)
		}
	}
	return pending, running, ended
}

// round is one scheduling pass over everything the scheduler owns: release
// ended attempts, dispatch what needs a worker, and poll what a worker may still
// hold.
//
// Checking is detached rather than awaited. Each poll is a worker RPC bounded
// only by dataCoord.requestTimeoutSeconds, and a node that has stopped
// answering must not hold up the dispatch of unrelated tasks for that long. The
// atomic guard allows at most one check pass in flight, so later rounds skip a
// slow pass instead of stacking more worker queries behind it.
func (s *globalTaskScheduler) round() {
	pending, running, ended := s.partition()

	if len(running) > 0 && s.checking.CompareAndSwap(false, true) {
		// Counted in the same WaitGroup as the loops, so Stop() still waits for
		// the worker polls it started. Adding is safe here because the caller of
		// round() is itself a WaitGroup member, so the counter is never zero at
		// this point.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.checking.Store(false)
			s.check(running)
		}()
	}

	// Release before dispatching, not after: dropping an ended attempt on its
	// worker frees the slot, and the QuerySlot inside schedule must see it.
	s.releaseEndedTaskOwnership(ended)
	s.schedule(pending)
}

// releaseEndedTaskOwnership releases tasks whose persisted state leaves no more
// scheduler work. Retry cleanup belongs to the business callback that exposed
// Retry; the scheduler must not issue a second, potentially delayed same-ID
// Drop. Terminal tasks still get best-effort worker cleanup here because an
// owner may end them directly in metadata without going through a callback.
//
// Ownership is not something an owner has to hand back explicitly: ending an
// attempt in meta and nothing else is a normal operation.
// importChecker.tryFailingTasks does exactly that: when a job fails it marks
// every one of its tasks Failed directly, including the ones the scheduler is
// holding. Without this, those tasks would never be looked at again and their
// IDs would stay claimed against every future Enqueue.
func (s *globalTaskScheduler) releaseEndedTaskOwnership(ended []Task) {
	for _, task := range ended {
		taskID := task.GetTaskID()
		s.mu.Lock(taskID)
		// Re-read under the lock: the snapshot is unlocked, so this may be a
		// different object by now, or the same one brought back to life.
		if s.ownsUnderTaskLock(task) {
			switch task.GetTaskState() {
			case taskcommon.Init, taskcommon.InProgress:
			case taskcommon.None, taskcommon.Retry:
				mlog.Info(s.ctx, "task owner ended the attempt, releasing scheduler ownership",
					WrapTaskLog(task, mlog.String("state", task.GetTaskState().String()))...)
				s.releaseUnderTaskLock(taskID)
			default:
				// Nobody told the scheduler; it derived this from the task's own
				// state. Whoever ended the attempt in meta logged their own reason
				// at best, and never mentioned that the scheduler was still holding
				// it -- so record the release itself.
				mlog.Info(s.ctx, "task owner ended the attempt, releasing scheduler ownership",
					WrapTaskLog(task, mlog.String("state", task.GetTaskState().String()))...)
				// Preserve ownership until terminal best-effort cleanup returns.
				task.DropTaskOnWorker(s.cluster)
				s.releaseUnderTaskLock(taskID)
			}
		}
		s.mu.Unlock(taskID)
	}
}

func (s *globalTaskScheduler) schedule(pending []Task) {
	if len(pending) == 0 {
		return
	}
	// Smaller task IDs first, so a task cannot be starved by later arrivals.
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].GetTaskID() < pending[j].GetTaskID()
	})
	nodeSlots := s.cluster.QuerySlot()
	// Rated, not plain Info: a round fires every TaskScheduleInterval (100ms by
	// default) and this line carries the whole node-slot map, so as long as any
	// task is waiting it alone produces ten log lines a second. State changes
	// stay at Info; the periodic sweep does not.
	mlog.RatedInfo(s.ctx, rate.Limit(1), "scheduling pending tasks...",
		mlog.Int("num", len(pending)), mlog.Any("nodeSlots", nodeSlots))

	// nodeSlots is reused across every pick in this round, so each task is placed
	// on the node that is least loaded after the earlier picks were charged.
	futures := make([]*conc.Future[struct{}], 0, len(pending))
	for _, task := range pending {
		taskSlot := task.GetTaskSlot()
		nodeID := s.pickNode(nodeSlots, taskSlot)
		if nodeID == NullNodeID {
			break
		}
		future := s.execPool.Submit(func() (struct{}, error) {
			s.createUnderTaskLock(task, nodeID)
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	// Await before returning: a task whose dispatch is still in flight is still
	// Init, so the next round would send it out a second time.
	_ = conc.AwaitAll(futures...)
}

func (s *globalTaskScheduler) check(running []Task) {
	if len(running) == 0 {
		return
	}

	futures := make([]*conc.Future[struct{}], 0, len(running))
	for _, task := range running {
		future := s.checkPool.Submit(func() (struct{}, error) {
			s.queryUnderTaskLock(task)
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	_ = conc.AwaitAll(futures...)
}

func (s *globalTaskScheduler) updateTaskTimeMetrics() {
	var (
		taskNumByTypeAndState = make(map[string]map[string]int64) // taskType => [taskState => taskNum]
		maxTaskQueueingTime   = make(map[string]int64)
		maxTaskRunningTime    = make(map[string]int64)
	)

	for _, taskType := range taskcommon.TypeList {
		taskNumByTypeAndState[taskType] = make(map[string]int64)
	}

	collectPendingMetricsFunc := func(task Task) {
		taskID := task.GetTaskID()
		taskType := task.GetTaskType()

		queueingTime := time.Since(task.GetTaskTime(taskcommon.TimeQueue))
		if queueingTime > paramtable.Get().DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
			mlog.Warn(s.ctx, "task queueing time is too long", mlog.FieldTaskID(taskID),
				mlog.Int64("queueing time(ms)", queueingTime.Milliseconds()))
		}

		maxQueueingTime, ok := maxTaskQueueingTime[taskType]
		if !ok || maxQueueingTime < queueingTime.Milliseconds() {
			maxTaskQueueingTime[taskType] = queueingTime.Milliseconds()
		}

		taskNumByTypeAndState[taskType][task.GetTaskState().String()]++
		metrics.TaskVersion.WithLabelValues(taskType).Observe(float64(task.GetTaskVersion()))
	}

	collectRunningMetricsFunc := func(task Task) {
		taskType := task.GetTaskType()

		runningTime := time.Since(task.GetTaskTime(taskcommon.TimeStart))
		if runningTime > paramtable.Get().DataCoordCfg.TaskSlowThreshold.GetAsDuration(time.Second) {
			mlog.Warn(s.ctx, "task running time is too long", mlog.FieldTaskID(task.GetTaskID()),
				mlog.Int64("running time(ms)", runningTime.Milliseconds()))
		}

		maxRunningTime, ok := maxTaskRunningTime[taskType]
		if !ok || maxRunningTime < runningTime.Milliseconds() {
			maxTaskRunningTime[taskType] = runningTime.Milliseconds()
		}

		taskNumByTypeAndState[taskType][task.GetTaskState().String()]++
	}

	// No per-task lock here. These are the same unlocked state reads partition
	// already makes, and a sample that catches a task mid-transition is
	// harmless. Taking the key lock instead would queue this pass behind
	// whatever worker RPC each task's callback is holding it across -- bounded
	// only by dataCoord.requestTimeoutSeconds, one task after another, and
	// contending with dispatch for the same keys.
	pending, running, _ := s.partition()

	for _, task := range pending {
		collectPendingMetricsFunc(task)
	}

	for _, task := range running {
		collectRunningMetricsFunc(task)
	}

	for taskType, queueingTime := range maxTaskQueueingTime {
		metrics.DataCoordTaskExecuteLatency.
			WithLabelValues(taskType, metrics.Pending).Observe(float64(queueingTime))
	}

	for taskType, runningTime := range maxTaskRunningTime {
		metrics.DataCoordTaskExecuteLatency.
			WithLabelValues(taskType, metrics.Executing).Observe(float64(runningTime))
	}

	metrics.TaskNumInGlobalScheduler.Reset()
	for taskType, taskNumByState := range taskNumByTypeAndState {
		for taskState, taskNum := range taskNumByState {
			metrics.TaskNumInGlobalScheduler.WithLabelValues(taskType, taskState).Set(float64(taskNum))
		}
	}
}

func NewGlobalTaskScheduler(ctx context.Context, cluster session.Cluster) GlobalScheduler {
	execPool := conc.NewPool[struct{}](128)
	checkPool := conc.NewPool[struct{}](128)
	ctx1, cancel := context.WithCancel(ctx)
	return &globalTaskScheduler{
		ctx:       ctx1,
		cancel:    cancel,
		wg:        sync.WaitGroup{},
		mu:        lock.NewKeyLock[int64](),
		tasks:     typeutil.NewConcurrentMap[int64, Task](),
		execPool:  execPool,
		checkPool: checkPool,
		cluster:   cluster,
	}
}
