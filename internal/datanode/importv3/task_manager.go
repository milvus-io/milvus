// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The manager's lifecycle state is the import task state proto enum directly,
// the same choice importv2 makes: it is the wire vocabulary DataCoord already
// speaks, so no translation layer is needed for the typed responses. The
// generic QueryTask projection still uses taskcommon.FromImportState at the
// RPC boundary.
type Snapshot struct {
	TaskID   int64
	RunID    int64
	State    datapb.ImportTaskStateV2
	Reason   string
	Result   any
	Progress TaskProgress
}

// Task is one fenced import V3-family worker task (reshard, import, count-only
// preimport). DataCoord drives it through Create/Query/Drop; the manager owns
// its lifecycle: the state machine, run fencing and cancellation.
type Task interface {
	TaskID() int64
	RunID() int64
	// Kind labels the task for metrics and logs ("reshard", "import",
	// "preimport").
	Kind() string
	// Slot is the task's slot cost on this node.
	Slot() int64
	// Execute runs the task to completion (or error) on the manager's
	// goroutine. A canceled ctx is the only cancellation mechanism.
	Execute(ctx context.Context) (any, error)
}

type taskEntry struct {
	mu     sync.RWMutex
	worker Task
	state  datapb.ImportTaskStateV2
	reason string
	result any
	cancel context.CancelFunc
	ctx    context.Context

	// started is true once the scheduler admitted the task into the pool. A
	// queued task (started == false) stays in the FIFO queue and reserves its
	// slots for backpressure, but is invisible to Stats -- exactly the tasks
	// the old scheduler-owned queue held.
	started bool
	// finished is true once the pool closure exited. It is guarded by the
	// manager lock (not mu): retireLocked uses it to decide whether a removed
	// run still needs draining, and finishRun sets it.
	finished bool
	// enqueued is the Submit time, used for the queue-phase latency metric.
	enqueued time.Time
}

// TaskManager owns the whole process-local task lifecycle: the durable state
// machine, run fencing, cancellation, the admission queue and execution.
// Durable task/run fencing remains in DataCoord; this manager's run check
// prevents late Query or completion callbacks from mutating a newer run on the
// same DataNode.
//
// It is the single source of truth for a task's state: a task is inserted once
// (Submit) as queued, the Scheduler admits it (Start), and Query/Drop always
// observe it here whether it is queued or running.
type TaskManager struct {
	ctx     context.Context
	cancel  context.CancelFunc
	pool    *conc.Pool[any]
	metrics *Metrics
	mu      sync.RWMutex
	tasks   map[int64]*taskEntry
	queue   []*taskEntry // FIFO of queued (not yet started) tasks
	// draining holds started runs whose entry was removed (Dropped or
	// superseded) while their pool closure is still executing. They keep
	// counting toward Slots until the closure exits, so admit() cannot start a
	// queued task on slots a canceled run still uses.
	draining map[*taskEntry]struct{}
	// wg tracks in-flight pool closures so Close can wait for every run before
	// releasing the pool.
	wg     sync.WaitGroup
	closed bool
	// notify wakes the scheduler's admission loop. It is set once at startup,
	// before any task is submitted, and is nil in tests that drive Start
	// directly.
	notify func()
}

func NewTaskManager(parent context.Context, metrics *Metrics) *TaskManager {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	// Sized exactly like the import V2 execution pool: the slot scheduler
	// bounds how many tasks ever run, so this pool only tracks their futures
	// and never blocks Submit in practice.
	poolSize := hardware.GetCPUNum() * paramtable.Get().DataNodeCfg.ImportConcurrencyPerCPUCore.GetAsInt()
	return &TaskManager{
		ctx:      ctx,
		cancel:   cancel,
		pool:     conc.NewPool[any](poolSize),
		metrics:  metrics,
		tasks:    make(map[int64]*taskEntry),
		draining: make(map[*taskEntry]struct{}),
	}
}

// SetNotify installs the wakeup callback the manager fires whenever the set of
// runnable tasks may have changed (a queued task arrives, a slot frees, a task
// is dropped). It must be called before Start.
func (m *TaskManager) SetNotify(notify func()) {
	if m != nil {
		m.notify = notify
	}
}

func (m *TaskManager) notifyDone() {
	if m != nil && m.notify != nil {
		m.notify()
	}
}

// Submit validates a task and queues it for admission. It does not execute the
// task: the Scheduler calls Start once the node has free slots. Submit is
// idempotent for the current run and a no-op for a stale run, the same run
// fencing the old Add enforced.
func (m *TaskManager) Submit(worker Task) error {
	if m == nil || worker == nil || worker.TaskID() == 0 || worker.RunID() == 0 || worker.Slot() <= 0 {
		return merr.WrapErrImportSysFailedMsg("invalid import V3 task create request")
	}
	taskID, runID := worker.TaskID(), worker.RunID()
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return merr.WrapErrServiceNotReadyMsg("import V3 task manager is closed")
	}
	if existing, ok := m.tasks[taskID]; ok {
		existing.mu.RLock()
		existingRun, existingState := existing.worker.RunID(), existing.state
		existing.mu.RUnlock()
		switch {
		case runID == existingRun && existingState == datapb.ImportTaskStateV2_Retry:
			// A task kind without its own run fencing (the count-only
			// preimport) is re-created by DataCoord with the same run id
			// after a retry: replace the finished attempt. reshard/import
			// retries always arrive as a fresh run id, so this never
			// re-executes fenced work. The retried goroutine has already
			// ended, so there is nothing to cancel and nothing to drain.
			m.retireLocked(existing, taskID)
		case runID == existingRun:
			m.mu.Unlock()
			return nil // Create is idempotent for the same fenced run.
		case runID < existingRun:
			m.mu.Unlock()
			return nil // Older run is stale and must not replace current work.
		default:
			if existing.cancel != nil {
				existing.cancel()
			}
			m.retireLocked(existing, taskID)
		}
	}
	ctx, cancel := context.WithCancel(m.ctx) //nolint:gosec // G118: cancel is stored in task and called by task termination or manager Close.
	rec := &taskEntry{worker: worker, state: datapb.ImportTaskStateV2_Pending, cancel: cancel, ctx: ctx, enqueued: time.Now()}
	m.tasks[taskID] = rec
	m.queue = append(m.queue, rec)
	m.mu.Unlock()

	m.notifyDone()
	return nil
}

// Start admits one queued task into the execution pool. It is the scheduler's
// action; the scheduler is its only caller and invokes it sequentially, so no
// other Start runs concurrently. It is idempotent for a run that already
// started and returns an error, without mutating state, when the task is gone
// or its run was superseded.
//
// Start sets the started flag under the manager lock but submits to the pool
// outside it: pool.Submit blocks when every worker is busy, and that must not
// hold the manager lock against Query/Slots/Drop.
func (m *TaskManager) Start(taskID, runID int64) error {
	if m == nil {
		return merr.WrapErrServiceNotReadyMsg("import V3 task manager is not initialized")
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return merr.WrapErrServiceNotReadyMsg("import V3 task manager is closed")
	}
	rec := m.tasks[taskID]
	if rec == nil {
		m.mu.Unlock()
		return merr.WrapErrImportSysFailedMsg("import V3 task %d is gone before it could start", taskID)
	}
	if rec.worker.RunID() != runID {
		m.mu.Unlock()
		return merr.WrapErrImportSysFailedMsg("import V3 task %d run %d is stale", taskID, runID)
	}
	if rec.started {
		m.mu.Unlock()
		return nil
	}
	rec.started = true
	m.removeQueuedLocked(rec)
	m.wg.Add(1)
	ctx := rec.ctx
	m.mu.Unlock()

	m.pool.Submit(func() (any, error) {
		defer m.finishRun(rec)
		rec.mu.Lock()
		if rec.state != datapb.ImportTaskStateV2_Pending {
			rec.mu.Unlock()
			return nil, nil
		}
		rec.state = datapb.ImportTaskStateV2_InProgress
		rec.mu.Unlock()
		runStart := time.Now()
		result, err := rec.worker.Execute(ctx)
		m.metrics.ObserveRunLatency(rec.worker.Kind(), time.Since(runStart))
		rec.mu.Lock()
		if err != nil {
			// A canceled run must never record Failed: DataCoord's stale query
			// would fail the whole job for a run it already superseded or
			// dropped.
			if ctx.Err() != nil {
				rec.state = datapb.ImportTaskStateV2_Retry
				rec.reason = ctx.Err().Error()
			} else if common.IsTerminalImportV3Err(err) {
				rec.state = datapb.ImportTaskStateV2_Failed
				rec.reason = err.Error()
			} else {
				// Same denylist as DataCoord's checker, shared via
				// common.IsTerminalImportV3Err: only provably permanent errors
				// fail the task. Everything else -- typed ErrIoFailed from an
				// unmapped object-store 5xx, raw manifest-write errors, Loon
				// transients, ID exhaustion -- retries until the job timeout.
				// Loon transients and ID exhaustion need no special cases:
				// neither carries a terminal milvus code.
				rec.state = datapb.ImportTaskStateV2_Retry
				rec.reason = err.Error()
			}
		} else {
			rec.result = result
			rec.state = datapb.ImportTaskStateV2_Completed
		}
		rec.mu.Unlock()
		return nil, nil
	})
	return nil
}

// finishRun is the pool closure's exit path. It marks the run finished, drops
// it from the draining set (freeing its slot if it was dropped or superseded
// mid-run), and wakes the scheduler. It runs for every admitted run, whether it
// completed normally or was retired.
func (m *TaskManager) finishRun(rec *taskEntry) {
	m.mu.Lock()
	delete(m.draining, rec)
	rec.finished = true
	m.mu.Unlock()
	m.wg.Done()
	// A finished run frees its slot: wake the scheduler so a queued task can
	// take it without waiting for the resync ticker.
	m.notifyDone()
}

// retireLocked removes a task entry from m.tasks. A started run whose closure
// is still executing moves to m.draining so its slot keeps counting toward
// Slots until finishRun drops it; a queued or already-finished run has no slot
// to preserve. The caller holds m.mu.
func (m *TaskManager) retireLocked(rec *taskEntry, taskID int64) {
	m.removeQueuedLocked(rec)
	if rec.started && !rec.finished {
		m.draining[rec] = struct{}{}
	}
	delete(m.tasks, taskID)
}

// removeQueuedLocked removes rec from the FIFO queue. The caller holds m.mu.
func (m *TaskManager) removeQueuedLocked(rec *taskEntry) {
	for i, e := range m.queue {
		if e == rec {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return
		}
	}
}

// PendingTasks returns the queued tasks in FIFO admission order. It is the
// scheduler's candidate list.
func (m *TaskManager) PendingTasks() []Candidate {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Candidate, 0, len(m.queue))
	for _, rec := range m.queue {
		out = append(out, Candidate{
			TaskID:   rec.worker.TaskID(),
			RunID:    rec.worker.RunID(),
			Kind:     rec.worker.Kind(),
			Slot:     rec.worker.Slot(),
			Enqueued: rec.enqueued,
		})
	}
	return out
}

// Queued reports the number and slot cost of tasks still waiting to start.
// Queued slots count as used in QuerySlot so DataCoord sees the backlog.
func (m *TaskManager) Queued() (int, int64) {
	if m == nil {
		return 0, 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var slots int64
	for _, rec := range m.queue {
		slots += rec.worker.Slot()
	}
	return len(m.queue), slots
}

// QueuedSlots returns the slots reserved by queued tasks. They count as used
// in QuerySlot so DataCoord sees the backlog.
func (m *TaskManager) QueuedSlots() int64 {
	_, slots := m.Queued()
	return slots
}

// Slots returns slots currently occupied by started tasks that are pending or
// running. Completed, failed, retryable and still-queued runs have stopped
// consuming DataNode work. Dropped or superseded runs keep counting until their
// pool closure exits (m.draining), so admission never reuses the slots of a run
// that is still executing.
func (m *TaskManager) Slots() int64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var slots int64
	count := func(rec *taskEntry) {
		if !rec.started {
			return
		}
		rec.mu.RLock()
		if rec.state == datapb.ImportTaskStateV2_Pending || rec.state == datapb.ImportTaskStateV2_InProgress {
			slots += rec.worker.Slot()
		}
		rec.mu.RUnlock()
	}
	for _, rec := range m.tasks {
		count(rec)
	}
	for rec := range m.draining {
		count(rec)
	}
	return slots
}

func (m *TaskManager) Query(taskID, runID int64) (Snapshot, bool) {
	m.mu.RLock()
	rec := m.tasks[taskID]
	m.mu.RUnlock()
	if rec == nil {
		return Snapshot{}, false
	}
	rec.mu.RLock()
	defer rec.mu.RUnlock()
	if runID != 0 && runID != rec.worker.RunID() {
		// A stale Query is deliberately a no-op.  DataCoord will query the
		// persisted current run again instead of treating an old worker reply
		// as a task failure.
		return Snapshot{}, false
	}
	snapshot := Snapshot{
		TaskID: rec.worker.TaskID(),
		RunID:  rec.worker.RunID(),
		State:  rec.state,
		Reason: rec.reason,
		Result: rec.result,
	}
	if reporter, ok := rec.worker.(interface{ Progress() TaskProgress }); ok {
		snapshot.Progress = reporter.Progress()
	}
	return snapshot, true
}

func (m *TaskManager) Drop(taskID, runID int64) bool {
	m.mu.Lock()
	rec := m.tasks[taskID]
	if rec == nil {
		m.mu.Unlock()
		return false
	}
	rec.mu.RLock()
	matched := runID == 0 || rec.worker.RunID() == runID
	cancel := rec.cancel
	rec.mu.RUnlock()
	if !matched {
		m.mu.Unlock()
		return false
	}
	wasQueued := !rec.started
	m.retireLocked(rec, taskID)
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if wasQueued {
		mlog.Info(context.TODO(), "slot scheduler discards queued task",
			mlog.FieldTaskID(taskID), mlog.Int64("runID", rec.worker.RunID()), mlog.String("kind", rec.worker.Kind()))
		// Refresh the queue gauge promptly; no slot is freed.
		m.notifyDone()
	}
	// A dropped started run keeps its slot until its closure exits; finishRun
	// wakes the scheduler then, so a queued task can take the freed slot.
	return true
}

// Stats returns the per-kind state distribution of started tasks, feeding the
// datanode_import_v3_tasks gauge and the state log. Queued tasks are excluded:
// their backlog is reported by the scheduler queue gauge instead.
func (m *TaskManager) Stats() map[string]map[datapb.ImportTaskStateV2]int {
	out := make(map[string]map[datapb.ImportTaskStateV2]int)
	if m == nil {
		return out
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, rec := range m.tasks {
		if !rec.started {
			continue
		}
		rec.mu.RLock()
		kind := rec.worker.Kind()
		if out[kind] == nil {
			out[kind] = make(map[datapb.ImportTaskStateV2]int)
		}
		out[kind][rec.state]++
		rec.mu.RUnlock()
	}
	return out
}

// Close stops accepting useful work, cancels every process-local V3 task, and
// waits for the callbacks visible to this DataNode process.  It is a local
// shutdown guarantee only; it does not create a cross-node DropAndWait RPC.
func (m *TaskManager) Close() {
	if m == nil {
		return
	}
	m.cancel()
	m.mu.Lock()
	m.closed = true
	for _, rec := range m.tasks {
		if rec.cancel != nil {
			rec.cancel()
		}
	}
	m.tasks = make(map[int64]*taskEntry)
	m.queue = nil
	m.draining = make(map[*taskEntry]struct{})
	m.mu.Unlock()
	// Wait for every admitted closure to exit before releasing the pool, so no
	// run executes after shutdown and no pool goroutine leaks. New Starts are
	// already excluded by m.closed under the lock above.
	m.wg.Wait()
	m.pool.Release()
}
