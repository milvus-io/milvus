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
	"sync"
	"time"

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

// maxDelayedPerRound caps how many tasks one scheduling round may set aside
// (because they are in failure backoff, or because they alone do not fit the
// cluster right now) before it stops popping. Two reasons for a cap:
//
//   - it bounds the work of a round. Under memory pressure with slots still
//     free every pick misses, and without the cap the round pops and prices the
//     entire queue only to push all of it back;
//   - it bounds the window in which a set-aside task is invisible. Between the
//     pop and the re-queue at the end of the round the task is in neither
//     pendingTasks nor runningTasks, so GetPendingTaskCount does not count it
//     and AbortAndRemoveTask cannot find it.
//
// Backoff and does-not-fit share the one slice and therefore the one budget.
// That is deliberate: both are "set aside and retried next round", and one
// counter is simpler than two. The value is a round-trip budget, not a fairness
// guarantee - a task beyond the cap is simply looked at in a later round.
const maxDelayedPerRound = 64

type GlobalScheduler interface {
	Enqueue(task Task)
	AbortAndRemoveTask(taskID int64)
	// GetPendingTaskCount returns the number of queued tasks of the given type.
	// The queue is shared by every task type, so callers that gate admission for
	// one kind of work must scope the count to that kind, otherwise an unrelated
	// backlog starves them. Tasks waiting on a retry backoff deadline ARE counted:
	// they still occupy queue depth, and excluding them would let a worker-side
	// failure storm silently disable the caller's admission gate.
	GetPendingTaskCount(taskType taskcommon.Type) int

	Start()
	Stop()
}

var _ GlobalScheduler = (*globalTaskScheduler)(nil)

type globalTaskScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu           *lock.KeyLock[int64]
	pendingTasks PriorityQueue
	runningTasks *typeutil.ConcurrentMap[int64, Task]
	execPool     *conc.Pool[struct{}]
	checkPool    *conc.Pool[struct{}]
	cluster      session.Cluster
	// backoffs delays re-dispatch of tasks that failed on a worker. Without
	// it a task that keeps failing (e.g. its object-storage reads are being
	// throttled) is re-sent every TaskScheduleInterval (~100ms), which turns
	// one bad task into a dispatch storm that keeps the store throttled.
	backoffs *typeutil.ConcurrentMap[int64, *taskBackoff]
}

// taskBackoff records how often a task failed on a worker and when it may be
// dispatched again. Entries are replaced wholesale (copy-on-write) so readers
// never observe a partially updated value.
type taskBackoff struct {
	failures  int
	notBefore time.Time
}

// recordTaskFailure schedules the next dispatch of a failed task with
// exponential backoff: interval * 2^(failures-1), capped at maxInterval.
func (s *globalTaskScheduler) recordTaskFailure(task Task) {
	interval := paramtable.Get().DataCoordCfg.TaskRetryBackoffInterval.GetAsDuration(time.Second)
	if interval <= 0 {
		return
	}
	maxInterval := paramtable.Get().DataCoordCfg.TaskRetryBackoffMaxInterval.GetAsDuration(time.Second)

	failures := 1
	if old, ok := s.backoffs.Get(task.GetTaskID()); ok {
		failures = old.failures + 1
	}
	// cap the shift to keep the doubling far away from overflow
	if shift := failures - 1; shift < 30 {
		interval <<= shift
	} else {
		interval = maxInterval
	}
	if maxInterval > 0 && interval > maxInterval {
		interval = maxInterval
	}
	s.backoffs.Insert(task.GetTaskID(), &taskBackoff{
		failures:  failures,
		notBefore: time.Now().Add(interval),
	})
	mlog.Info(s.ctx, "task failed on worker, backing off before retry",
		WrapTaskLog(task, mlog.Int("failures", failures), mlog.Duration("backoff", interval))...)
}

// taskInBackoff reports whether the task's next dispatch is still delayed.
func (s *globalTaskScheduler) taskInBackoff(task Task) bool {
	bo, ok := s.backoffs.Get(task.GetTaskID())
	return ok && time.Now().Before(bo.notBefore)
}

func (s *globalTaskScheduler) Enqueue(task Task) {
	if s.pendingTasks.Get(task.GetTaskID()) != nil {
		return
	}
	if s.runningTasks.Contain(task.GetTaskID()) {
		return
	}
	switch task.GetTaskState() {
	case taskcommon.Init:
		task.SetTaskTime(taskcommon.TimeQueue, time.Now())
		s.pendingTasks.Push(task)
	case taskcommon.InProgress, taskcommon.Retry:
		task.SetTaskTime(taskcommon.TimeStart, time.Now())
		s.runningTasks.Insert(task.GetTaskID(), task)
	}
	mlog.Info(s.ctx, "task enqueued", WrapTaskLog(task)...)
}

func (s *globalTaskScheduler) GetPendingTaskCount(taskType taskcommon.Type) int {
	return s.pendingTasks.TaskCountBy(func(task Task) bool {
		return task.GetTaskType() == taskType
	})
}

func (s *globalTaskScheduler) AbortAndRemoveTask(taskID int64) {
	s.mu.Lock(taskID)
	defer s.mu.Unlock(taskID)
	if task, ok := s.runningTasks.GetAndRemove(taskID); ok {
		task.DropTaskOnWorker(s.cluster)
	}
	if task := s.pendingTasks.Get(taskID); task != nil {
		task.DropTaskOnWorker(s.cluster)
		s.pendingTasks.Remove(taskID)
	}
	s.backoffs.Remove(taskID)
}

func (s *globalTaskScheduler) Start() {
	dur := paramtable.Get().DataCoordCfg.TaskScheduleInterval.GetAsDuration(time.Millisecond)
	s.wg.Add(3)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(dur)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.schedule()
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(dur)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.check()
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
}

type nodeSlotEntry struct {
	nodeID int64
	slots  *session.WorkerSlots
}

// newNodeSlotHeap builds a max-heap of worker nodes ordered by their available
// slots, so the most-available (least-loaded) node always sits at the top.
func newNodeSlotHeap(workerSlots map[int64]*session.WorkerSlots) typeutil.Heap[*nodeSlotEntry] {
	slots := make([]*nodeSlotEntry, 0, len(workerSlots))
	for nodeID, ws := range workerSlots {
		slots = append(slots, &nodeSlotEntry{
			nodeID: nodeID,
			slots:  ws,
		})
	}
	return typeutil.NewObjectArrayBasedMaximumHeap(slots, func(entry *nodeSlotEntry) int64 {
		return entry.slots.AvailableSlots
	})
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
// slots, preserving the previous behavior.
//
// The picked node's slots are updated in place; the caller reuses the same heap
// across all tasks in a scheduling round so later picks observe the decremented
// slots.
//
// The method is kept for its callers and tests; the placement itself lives in
// pickNodeFromHeap so nodePicker can share it as its fallback tier.
func (s *globalTaskScheduler) pickNode(slotHeap typeutil.Heap[*nodeSlotEntry], taskSlot int64) int64 {
	return pickNodeFromHeap(slotHeap, taskSlot)
}

func pickNodeFromHeap(slotHeap typeutil.Heap[*nodeSlotEntry], taskSlot int64) int64 {
	if slotHeap.Len() == 0 {
		return NullNodeID
	}
	// Pop the most-available node, mutate its slots, then push it back. An element
	// must not be mutated while it stays in the heap, or the heap order breaks.
	entry := slotHeap.Pop()
	if taskSlot <= 0 {
		slotHeap.Push(entry)
		return entry.nodeID
	}
	if entry.slots.AvailableSlots <= 0 {
		// The most-available node has no slot, so neither does any other node.
		slotHeap.Push(entry)
		return NullNodeID
	}
	if entry.slots.AvailableSlots >= taskSlot {
		entry.slots.AvailableSlots -= taskSlot
	} else {
		// No node can fully satisfy the request; assign to the most-available
		// node on a best-effort basis and drain its slots.
		entry.slots.AvailableSlots = 0
	}
	slotHeap.Push(entry)
	return entry.nodeID
}

func (s *globalTaskScheduler) schedule() {
	pendingNum := s.pendingTasks.TaskCount()
	if pendingNum == 0 {
		return
	}
	nodeSlots := s.cluster.QuerySlot()
	mlog.Info(s.ctx, "scheduling pending tasks...", mlog.Int("num", pendingNum), mlog.Any("nodeSlots", nodeSlots))

	// Build the picker once per round and reuse it across all picks, so each
	// task is placed on the currently least-loaded node.
	picker := newNodePicker(nodeSlots)
	futures := make([]*conc.Future[struct{}], 0)
	var delayed []Task
	for {
		task := s.pendingTasks.Pop()
		if task == nil {
			break
		}
		// A task in failure backoff gives way: it re-enters the queue after
		// this round and is dispatched once its delay elapses, so one
		// persistently failing task cannot occupy the scheduler.
		if s.taskInBackoff(task) {
			delayed = append(delayed, task)
			continue
		}
		taskSlot := task.GetTaskSlot()
		// Price once per round and reuse: the placement decision and the log
		// below must agree, and a family that walks meta on a cache miss would
		// otherwise pay for the walk twice.
		resource := task.GetTaskResource()
		nodeID := picker.Pick(taskSlot, resource)
		if nodeID == NullNodeID {
			if picker.exhausted() {
				// No worker of either tier has room left, so nothing behind this
				// task can be placed either: end the round.
				s.pendingTasks.Push(task)
				break
			}
			if len(delayed) >= maxDelayedPerRound {
				// Enough set aside for one round: stop popping rather than walk
				// the rest of the queue only to push it back. See
				// maxDelayedPerRound.
				s.pendingTasks.Push(task)
				break
			}
			// The cluster still has room; this task alone does not fit it. Give
			// way like a backoff task does, so one oversized task at the head of
			// the queue cannot stall every smaller task behind it (the queue is
			// ordered by task ID, so "head" means "oldest", not "biggest").
			//
			// Trade-off: ending the round used to reserve the cluster for this
			// task implicitly. Without that reservation a steady stream of small
			// tasks can keep delaying it; an explicit reservation or aging
			// mechanism is a follow-up.
			delayed = append(delayed, task)
			continue
		}
		future := s.execPool.Submit(func() (struct{}, error) {
			s.mu.RLock(task.GetTaskID())
			defer s.mu.RUnlock(task.GetTaskID())
			mlog.Info(s.ctx, "processing task...",
				WrapTaskLog(task, mlog.Stringer("resource", resource))...)
			if task.GetTaskState() == taskcommon.Init {
				task.CreateTaskOnWorker(nodeID, s.cluster)
				switch task.GetTaskState() {
				case taskcommon.Init, taskcommon.Retry:
					s.recordTaskFailure(task)
					s.pendingTasks.Push(task)
				case taskcommon.InProgress:
					// The task was accepted by the worker and is now in flight.
					// Any accumulated failure count is intentionally kept: reaching
					// InProgress only means a slot happened to be free, not that the
					// cause of earlier failures is gone. If the task fails again the
					// backoff must keep escalating rather than restart from scratch.
					// The entry is cleared only on a terminal state (here and in
					// check()).
					task.SetTaskTime(taskcommon.TimeStart, time.Now())
					s.runningTasks.Insert(task.GetTaskID(), task)
				case taskcommon.None, taskcommon.Finished, taskcommon.Failed:
					// CreateTaskOnWorker can drive a task straight to a terminal
					// state (e.g. missing meta, unhealthy segment, estimation
					// failure). Such a task leaves the scheduler without ever
					// entering runningTasks, so check()'s terminal-state cleanup
					// never runs. Drop the backoff entry here; otherwise it would
					// leak until datacoord restarts and grow without bound under
					// the very failure storms this backoff exists to relieve.
					s.backoffs.Remove(task.GetTaskID())
				}
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	for _, task := range delayed {
		s.pendingTasks.Push(task)
	}
	_ = conc.AwaitAll(futures...)
}

func (s *globalTaskScheduler) check() {
	if s.runningTasks.Len() <= 0 {
		return
	}
	mlog.Info(s.ctx, "check running tasks", mlog.Int("num", s.runningTasks.Len()))

	tasks := s.runningTasks.Values()
	futures := make([]*conc.Future[struct{}], 0, len(tasks))
	for _, task := range tasks {
		future := s.checkPool.Submit(func() (struct{}, error) {
			s.mu.RLock(task.GetTaskID())
			defer s.mu.RUnlock(task.GetTaskID())
			task.QueryTaskOnWorker(s.cluster)
			switch task.GetTaskState() {
			case taskcommon.None:
				s.runningTasks.Remove(task.GetTaskID())
				s.backoffs.Remove(task.GetTaskID())
			case taskcommon.Init, taskcommon.Retry:
				s.recordTaskFailure(task)
				s.runningTasks.Remove(task.GetTaskID())
				s.pendingTasks.Push(task)
			case taskcommon.Finished, taskcommon.Failed:
				task.SetTaskTime(taskcommon.TimeEnd, time.Now())
				task.DropTaskOnWorker(s.cluster)
				s.runningTasks.Remove(task.GetTaskID())
				s.backoffs.Remove(task.GetTaskID())
			}
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

	collectPendingMetricsFunc := func(taskID int64) {
		task := s.pendingTasks.Get(taskID)
		if task == nil {
			return
		}

		s.mu.Lock(taskID)
		defer s.mu.Unlock(taskID)

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
		s.mu.Lock(task.GetTaskID())
		defer s.mu.Unlock(task.GetTaskID())

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

	taskIDs := s.pendingTasks.TaskIDs()

	for _, taskID := range taskIDs {
		collectPendingMetricsFunc(taskID)
	}

	allRunningTasks := s.runningTasks.Values()
	for _, task := range allRunningTasks {
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
		ctx:          ctx1,
		cancel:       cancel,
		wg:           sync.WaitGroup{},
		mu:           lock.NewKeyLock[int64](),
		pendingTasks: NewPriorityQueuePolicy(),
		runningTasks: typeutil.NewConcurrentMap[int64, Task](),
		execPool:     execPool,
		checkPool:    checkPool,
		cluster:      cluster,
		backoffs:     typeutil.NewConcurrentMap[int64, *taskBackoff](),
	}
}
