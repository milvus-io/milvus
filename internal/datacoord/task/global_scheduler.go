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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blang/semver/v4"
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
	// AbortAndRemoveTask releases a task and drops it on its worker. It waits
	// for an in-flight callback on that task, bounded by
	// dataCoord.requestTimeoutSeconds.
	AbortAndRemoveTask(taskID int64)
	// GetPendingTaskCount returns the number of queued tasks of the given type.
	// The queue is shared by every task type, so callers that gate admission for
	// one kind of work must scope the count to that kind, otherwise an unrelated
	// backlog starves them. Tasks waiting on a retry backoff deadline ARE counted:
	// they still occupy queue depth, and excluding them would let a worker-side
	// failure storm silently disable the caller's admission gate.
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
	// tasks holds every task the scheduler owns, one entry each. There is no
	// separate pending/running container: which phase a task is in follows from
	// its own state and is derived once per round (see taskPhase), so a state
	// change never has to be mirrored by moving the task somewhere else. That
	// is what keeps ownership unambiguous -- "the scheduler owns this task" is
	// a single map lookup, not an agreement between two containers.
	tasks *typeutil.ConcurrentMap[int64, *taskEntry]
	// checking guards the detached check pass so at most one is ever in flight.
	// See round().
	checking  atomic.Bool
	execPool  *conc.Pool[struct{}]
	checkPool *conc.Pool[struct{}]
	cluster   session.Cluster
}

// taskEntry is the scheduler's ownership record for one task: the task itself
// plus the delay its failures on a worker have earned. Without that delay a
// task that keeps failing (its object-storage reads are being throttled, say)
// is re-sent every TaskScheduleInterval (~100ms), which turns one bad task into
// a dispatch storm that keeps the store throttled.
//
// The delay lives in the ownership entry rather than in a map of its own so it
// cannot outlive, or fall behind, the ownership it describes: releasing a task
// is one removal, not two that have to be kept in step.
//
// Entries are replaced wholesale (copy-on-write) rather than mutated, so a
// reader never observes a partially updated value.
type taskEntry struct {
	task      Task
	failures  int
	notBefore time.Time
}

// dispatchDelayed reports whether this task's next dispatch is still held back
// by the backoff its earlier failures earned.
func (e *taskEntry) dispatchDelayed() bool {
	return time.Now().Before(e.notBefore)
}

// recordFailureUnderTaskLock delays the next dispatch of a task that failed on
// a worker, with exponential backoff: interval * 2^(failures-1), capped at
// maxInterval. The caller must hold the task's key lock, which is what makes
// the read-modify-write of its entry safe.
func (s *globalTaskScheduler) recordFailureUnderTaskLock(task Task) {
	interval := paramtable.Get().DataCoordCfg.TaskRetryBackoffInterval.GetAsDuration(time.Second)
	if interval <= 0 {
		return
	}
	maxInterval := paramtable.Get().DataCoordCfg.TaskRetryBackoffMaxInterval.GetAsDuration(time.Second)

	taskID := task.GetTaskID()
	old, ok := s.tasks.Get(taskID)
	if !ok || old.task != task {
		// Released, or already replaced by a rebuilt task under the same ID.
		// Recording a failure against it would put back an entry nobody owns.
		return
	}
	failures := old.failures + 1
	// cap the shift to keep the doubling far away from overflow
	if shift := failures - 1; shift < 30 {
		interval <<= shift
	} else {
		interval = maxInterval
	}
	if maxInterval > 0 && interval > maxInterval {
		interval = maxInterval
	}
	s.tasks.Insert(taskID, &taskEntry{
		task:      task,
		failures:  failures,
		notBefore: time.Now().Add(interval),
	})
	mlog.Info(s.ctx, "task failed on worker, backing off before retry",
		WrapTaskLog(task, mlog.Int("failures", failures), mlog.Duration("backoff", interval))...)
}

// phase is what the scheduler has to do with a task next. It is derived from
// the task's own state instead of being tracked, so a task whose state changes
// under the scheduler simply gets classified differently on the next round --
// there is nothing to keep in sync.
type phase int

const (
	// phaseDispatch: never reached a worker, schedule() must send it to one.
	phaseDispatch phase = iota
	// phaseCheck: a worker may be holding it, check() must poll it. Retry
	// belongs here rather than with phaseDispatch: it means an attempt was made
	// and did not stick, and no CreateTaskOnWorker implementation drops the
	// previous attempt before sending a new one. Re-dispatching straight away
	// would leave the earlier attempt running on its old node, burning a slot
	// nobody will ever collect. Polling gets there first -- the task's own
	// QueryTaskOnWorker drops the stale copy and resets the state to Init,
	// which puts it back in phaseDispatch.
	phaseCheck
	// phaseDone: the attempt is over. Whoever owns the task drives it from
	// here, and any rebuild arrives under a fresh task ID, so the scheduler
	// releases it.
	phaseDone
)

func taskPhase(task Task) phase {
	switch task.GetTaskState() {
	case taskcommon.Init:
		return phaseDispatch
	case taskcommon.InProgress, taskcommon.Retry:
		return phaseCheck
	default:
		return phaseDone
	}
}

// Enqueue hands a task to the scheduler, ignoring one it already owns.
//
// It runs without the task's key lock on purpose. Inspectors call it while
// holding their own locks -- externalCollectionRefreshManager.enqueueTask holds
// schedulerOwnershipMu, compactionInspector holds executingGuard -- and a worker
// callback running under the key lock reaches back for those same locks
// (refreshExternalCollectionTask takes its manager lease from inside
// CreateTaskOnWorker and QueryTaskOnWorker). Waiting for the key lock here would
// close that loop into a deadlock rather than a stall.
//
// Staying lock-free costs nothing because ownership is a single map entry:
// GetOrInsert answers "is this already ours?" and claims it in one atomic step,
// with no second container that could disagree.
func (s *globalTaskScheduler) Enqueue(task Task) {
	// Read the phase once, so the branch that decides is the branch that records.
	p := taskPhase(task)
	if p == phaseDone {
		// Nothing to schedule. Claiming it would leave an entry only the next
		// round could collect.
		return
	}
	if _, loaded := s.tasks.GetOrInsert(task.GetTaskID(), &taskEntry{task: task}); loaded {
		return
	}
	if p == phaseCheck {
		task.SetTaskTime(taskcommon.TimeStart, time.Now())
	} else {
		task.SetTaskTime(taskcommon.TimeQueue, time.Now())
	}
	mlog.Info(s.ctx, "task enqueued", WrapTaskLog(task)...)
}

// GetPendingTaskCount returns the number of queued tasks of the given type. The
// queue is shared by every task type, so callers that gate admission for one
// kind of work must scope the count to that kind, otherwise an unrelated backlog
// starves them. Tasks waiting on a retry backoff deadline ARE counted: they
// still occupy queue depth, and excluding them would let a worker-side failure
// storm silently disable the caller's admission gate.
func (s *globalTaskScheduler) GetPendingTaskCount(taskType taskcommon.Type) int {
	count := 0
	for _, entry := range s.tasks.Values() {
		if taskPhase(entry.task) == phaseDispatch && entry.task.GetTaskType() == taskType {
			count++
		}
	}
	return count
}

// releaseUnderTaskLock drops scheduler ownership of a task and returns it so the
// caller can drop it on its worker after unlocking. The caller must hold the
// task's key lock, which is what stops it from racing a callback that is in the
// middle of acting on the same task.
//
// Removing the entry is the whole release: the accumulated backoff lives in it,
// so there is nothing left behind that a later Enqueue of the same ID could
// inherit.
//
// The returned task is nil when the scheduler did not own it -- the common case
// for an idempotent abort.
func (s *globalTaskScheduler) releaseUnderTaskLock(taskID int64) Task {
	entry, ok := s.tasks.GetAndRemove(taskID)
	if !ok {
		return nil
	}
	return entry.task
}

// ownsUnderTaskLock reports whether task is still the scheduler's copy of its
// ID. Both loops pick their work from an unlocked snapshot, so an abort or a
// Finalize may have taken the task before the callback got the lock. Identity
// rather than presence: an abort followed by an Enqueue of a rebuilt task leaves
// a different object under the same ID, and the stale copy must not act on it.
func (s *globalTaskScheduler) ownsUnderTaskLock(task Task) bool {
	owned, ok := s.tasks.Get(task.GetTaskID())
	return ok && owned.task == task
}

func (s *globalTaskScheduler) AbortAndRemoveTask(taskID int64) {
	// Ownership under the lock, the worker RPC after it: a node that has stopped
	// answering must not hold this key for a full request timeout while another
	// caller waits on Finalize or check().
	s.mu.Lock(taskID)
	removed := s.releaseUnderTaskLock(taskID)
	s.mu.Unlock(taskID)

	// Only when something was actually held: an abort is idempotent, and most
	// calls are the second one.
	if removed != nil {
		mlog.Info(s.ctx, "task aborted, releasing it and dropping it on its worker",
			WrapTaskLog(removed, mlog.String("state", removed.GetTaskState().String()))...)
		removed.DropTaskOnWorker(s.cluster)
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
		mlog.Info(s.ctx, "ownership handed back to the task owner",
			WrapTaskLog(released, mlog.String("state", released.GetTaskState().String()))...)
	}
	fn()
}

func (s *globalTaskScheduler) Update(taskID int64, fn func()) {
	s.mu.Lock(taskID)
	defer s.mu.Unlock(taskID)
	fn()
}

// The two callbacks below report the worker drop back to their caller rather
// than sending it, so ownership is released under the key lock but the RPC goes
// out after the unlock. That RPC is bounded only by
// dataCoord.requestTimeoutSeconds; sending it inside the critical section would
// let an unresponsive node hold the key for a second full timeout and stall the
// Finalize that compaction cleanup runs under. The aborts follow the same rule.

// createUnderTaskLock dispatches one task and reports whether the caller must
// drop it on the worker afterwards. Nothing has to be moved on success: the task
// stays the scheduler's, and its new state alone puts it in check()'s hands.
func (s *globalTaskScheduler) createUnderTaskLock(task Task, nodeID int64) bool {
	taskID := task.GetTaskID()
	// Exclusive, not shared: this is the only thing serializing the mutations a
	// task's callbacks make to its own state.
	s.mu.Lock(taskID)
	defer s.mu.Unlock(taskID)
	if !s.ownsUnderTaskLock(task) {
		mlog.Info(s.ctx, "task is no longer owned by this dispatch, skip it", WrapTaskLog(task)...)
		return false
	}
	if taskPhase(task) != phaseDispatch {
		// Its state changed between the snapshot and this lock. Do nothing --
		// the next round classifies it again.
		return false
	}
	mlog.Info(s.ctx, "processing task...", WrapTaskLog(task)...)
	task.CreateTaskOnWorker(nodeID, s.cluster)
	switch task.GetTaskState() {
	case taskcommon.Init, taskcommon.Retry:
		// The worker did not take it. It stays ours; back off before the next
		// attempt so one persistently failing task cannot flood the cluster.
		s.recordFailureUnderTaskLock(task)
	case taskcommon.InProgress:
		// Accepted by the worker. Any accumulated failure count is intentionally
		// kept: reaching InProgress only means a slot happened to be free, not
		// that the cause of earlier failures is gone. If the task fails again the
		// backoff must keep escalating rather than restart from scratch. The
		// entry is cleared only when ownership is released.
		task.SetTaskTime(taskcommon.TimeStart, time.Now())
	default:
		// CreateTaskOnWorker can drive a task straight to a terminal state (e.g.
		// missing meta, unhealthy segment, estimation failure, or an accepted
		// create whose response was lost). The drop below matters most for that
		// last case, where the worker may be running a task nobody will collect.
		//
		// Say so. This is where the task leaves the scheduler for good, and
		// whether anything explained why is up to a CreateTaskOnWorker
		// implementation the scheduler does not control -- so the release point
		// records it unconditionally.
		mlog.Info(s.ctx, "task reached a terminal state on dispatch, releasing it",
			WrapTaskLog(task, mlog.String("state", task.GetTaskState().String()))...)
		task.SetTaskTime(taskcommon.TimeEnd, time.Now())
		s.releaseUnderTaskLock(taskID)
		return true
	}
	return false
}

// queryUnderTaskLock polls one running task and reports whether the caller must
// drop it on the worker afterwards.
func (s *globalTaskScheduler) queryUnderTaskLock(task Task) bool {
	taskID := task.GetTaskID()
	s.mu.Lock(taskID)
	defer s.mu.Unlock(taskID)
	if !s.ownsUnderTaskLock(task) {
		return false
	}
	if taskPhase(task) != phaseCheck {
		// Its state changed between the snapshot and this lock -- the owner
		// ended the attempt in meta, or a callback reset it to Init. Polling it
		// anyway would hand a terminal task one more worker round-trip, and an
		// error on that round-trip can push a Failed task back to Pending
		// (import's QueryTaskOnWorker does exactly that on a query error).
		// Leave it for the next round to classify.
		return false
	}
	task.QueryTaskOnWorker(s.cluster)
	switch task.GetTaskState() {
	case taskcommon.None:
		// The worker does not know this task. Nothing to drop -- but this is a
		// task disappearing on us, so it is never something to pass over in
		// silence.
		mlog.Info(s.ctx, "worker does not know this task, releasing it", WrapTaskLog(task)...)
		s.releaseUnderTaskLock(taskID)
	case taskcommon.Init, taskcommon.Retry:
		// Back to needing a worker; it stays ours and schedule() picks it up.
		s.recordFailureUnderTaskLock(task)
	case taskcommon.Finished, taskcommon.Failed:
		mlog.Info(s.ctx, "task ended on its worker, releasing it",
			WrapTaskLog(task, mlog.String("state", task.GetTaskState().String()))...)
		task.SetTaskTime(taskcommon.TimeEnd, time.Now())
		s.releaseUnderTaskLock(taskID)
		return true
	}
	return false
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
	return s.pickNodeWithMinimumVersion(nodeSlots, taskSlot, semver.Version{})
}

func (s *globalTaskScheduler) pickNodeWithMinimumVersion(
	nodeSlots map[int64]*session.WorkerSlots,
	taskSlot int64,
	minimumVersion semver.Version,
) int64 {
	// A linear scan, not a heap: this runs once per pending task against the
	// DataNode count, which is small enough that a few integer comparisons beat
	// the bookkeeping a heap needs to stay ordered while its elements are
	// mutated.
	bestNode := int64(NullNodeID)
	var best *session.WorkerSlots
	for nodeID, slots := range nodeSlots {
		if !workerSupportsMinimumVersion(slots.Version, minimumVersion) {
			continue
		}
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

func workerSupportsMinimumVersion(workerVersion string, minimumVersion semver.Version) bool {
	if minimumVersion.Equals(semver.Version{}) {
		return true
	}
	workerVersion = strings.TrimSpace(workerVersion)
	if workerVersion == "" {
		return false
	}
	version, err := semver.ParseTolerant(workerVersion)
	if err != nil {
		// Development builds report branch-date-commit strings. A non-empty
		// version still proves the worker understands the version response field;
		// old DataNodes decode this new field as empty.
		return true
	}
	version.Pre = nil
	version.Build = nil
	minimumVersion.Pre = nil
	minimumVersion.Build = nil
	return version.GTE(minimumVersion)
}

func minimumWorkerVersion(task Task) (semver.Version, bool) {
	constraint, ok := task.(WorkerVersionConstraint)
	if !ok {
		return semver.Version{}, false
	}
	minimumVersion := constraint.MinimumWorkerVersion()
	return minimumVersion, !minimumVersion.Equals(semver.Version{})
}

// partition derives every owned task's phase in a single pass and buckets it
// accordingly, in no particular order within a bucket.
//
// One pass, not one per phase: the phase is a function of the task's own state,
// so a single scan answers all three questions at once and a round cannot
// disagree with itself about where a task belongs. The buckets are only a
// snapshot -- every callback re-checks ownership, and the dispatch path
// re-checks the phase, under the key lock.
func (s *globalTaskScheduler) partition() (pending, running, done []*taskEntry) {
	for _, entry := range s.tasks.Values() {
		switch taskPhase(entry.task) {
		case phaseDispatch:
			pending = append(pending, entry)
		case phaseCheck:
			running = append(running, entry)
		default:
			done = append(done, entry)
		}
	}
	return pending, running, done
}

// round is one scheduling pass over everything the scheduler owns. Every owned
// task is in exactly one phase and a round drives all three: release what is
// done, dispatch what needs a worker, poll what a worker may still hold.
//
// Checking is detached rather than awaited. Each poll is a worker RPC bounded
// only by dataCoord.requestTimeoutSeconds, and a node that has stopped
// answering must not hold up the dispatch of unrelated tasks for that long. The
// guard is what a separate check loop used to get for free from its own ticker:
// at most one check pass is ever in flight, so a slow pass is skipped by the
// next round instead of being stacked on top of itself.
func (s *globalTaskScheduler) round() {
	pending, running, done := s.partition()

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

	// Release before dispatching, not after: dropping a finished task on its
	// worker frees the slot, and the QuerySlot inside schedule must see it.
	s.releaseDoneTasks(done)
	s.schedule(pending)
}

// releaseDoneTasks handles the third phase. Ownership is not something an owner
// has to hand back: the scheduler derives what to do with a task from its state,
// and "nothing left to do" is one of the three answers, so it must be acted on
// like the other two.
//
// It is not a safety net for owners that forget to call Finalize or an abort --
// ending an attempt in meta and nothing else is a normal thing to do.
// importChecker.tryFailingTasks does exactly that: when a job fails it marks
// every one of its tasks Failed directly, including the ones the scheduler is
// holding. Without this, those tasks would never be looked at again and their
// IDs would stay claimed against every future Enqueue.
func (s *globalTaskScheduler) releaseDoneTasks(done []*taskEntry) {
	var released []Task
	for _, entry := range done {
		task := entry.task
		taskID := task.GetTaskID()
		s.mu.Lock(taskID)
		// Re-read under the lock: the snapshot is unlocked, so this may be a
		// different object by now, or the same one brought back to life.
		if s.ownsUnderTaskLock(task) && taskPhase(task) == phaseDone {
			// Nobody told the scheduler; it derived this from the task's own
			// state. Whoever ended the attempt in meta logged their own reason
			// at best, and never mentioned that the scheduler was still holding
			// it -- so record the release itself.
			mlog.Info(s.ctx, "owner ended this task without releasing it, releasing it now",
				WrapTaskLog(task, mlog.String("state", task.GetTaskState().String()))...)
			s.releaseUnderTaskLock(taskID)
			released = append(released, task)
		}
		s.mu.Unlock(taskID)
	}
	if len(released) == 0 {
		return
	}

	// Drop them on their workers, the same as the callbacks do on a terminal
	// state and for the same reason: whoever ended the attempt in meta did not
	// touch the worker, which may still be running the task and holding its
	// slot. Exactly one drop is sent -- a callback that ends a task releases it
	// itself, is skipped above, and the task is out of the map, so no later
	// round can re-send.
	//
	// Detached, not awaited. Each drop is an RPC bounded only by
	// dataCoord.requestTimeoutSeconds, and this runs on the round goroutine
	// ahead of dispatch: awaiting used to let one hung node hold up every
	// unrelated pending task for a full request timeout. The only thing the
	// await bought was that QuerySlot saw the freed slots in the same round;
	// detached, they show up a round or two later, which costs nothing. Counted
	// in wg so Stop still waits the drops out; the Add is safe because the
	// caller is itself wg-counted, so the counter is never zero here.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		futures := make([]*conc.Future[struct{}], 0, len(released))
		for _, task := range released {
			futures = append(futures, s.execPool.Submit(func() (struct{}, error) {
				task.DropTaskOnWorker(s.cluster)
				return struct{}{}, nil
			}))
		}
		_ = conc.AwaitAll(futures...)
	}()
}

func (s *globalTaskScheduler) schedule(pending []*taskEntry) {
	if len(pending) == 0 {
		return
	}
	// Smaller task IDs first, so a task cannot be starved by later arrivals.
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].task.GetTaskID() < pending[j].task.GetTaskID()
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
	for _, entry := range pending {
		// A task in failure backoff gives way: it stays owned and is dispatched
		// by a later round once its delay elapses, so one persistently failing
		// task cannot occupy the scheduler.
		if entry.dispatchDelayed() {
			continue
		}
		task := entry.task
		taskSlot := task.GetTaskSlot()
		minimumVersion, versionConstrained := minimumWorkerVersion(task)
		nodeID := s.pickNodeWithMinimumVersion(nodeSlots, taskSlot, minimumVersion)
		if nodeID == NullNodeID {
			if versionConstrained {
				// Only the version-compatible nodes are exhausted; a task
				// without that constraint may still find a node.
				continue
			}
			break
		}
		future := s.execPool.Submit(func() (struct{}, error) {
			if s.createUnderTaskLock(task, nodeID) {
				task.DropTaskOnWorker(s.cluster)
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	// Await before returning: a task whose dispatch is still in flight is still
	// in phaseDispatch, so the next round would send it out a second time.
	_ = conc.AwaitAll(futures...)
}

func (s *globalTaskScheduler) check(running []*taskEntry) {
	if len(running) == 0 {
		return
	}

	futures := make([]*conc.Future[struct{}], 0, len(running))
	for _, entry := range running {
		task := entry.task
		future := s.checkPool.Submit(func() (struct{}, error) {
			if s.queryUnderTaskLock(task) {
				task.DropTaskOnWorker(s.cluster)
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

	for _, entry := range pending {
		collectPendingMetricsFunc(entry.task)
	}

	for _, entry := range running {
		collectRunningMetricsFunc(entry.task)
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
		tasks:     typeutil.NewConcurrentMap[int64, *taskEntry](),
		execPool:  execPool,
		checkPool: checkPool,
		cluster:   cluster,
	}
}
