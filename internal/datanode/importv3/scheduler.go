// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// The scheduler owns both the admission policy and the task registry for import
// V3 on one DataNode: the registry is the per-run entry plus the lock-guarded
// lookups and lifecycle helpers over the composite-key OrderedMap, and the
// admission policy decides which pending task starts next. DataCoord drives it
// through Submit/Query/Drop, and the admission loop decides what runs. Every
// registry helper except the exported slot reports is "Unsafe": the caller holds
// s.mu.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The scheduler's lifecycle state is the import task state proto enum
// directly, the same choice importv2 makes: it is the wire vocabulary DataCoord
// already speaks, so no translation layer is needed for the typed responses.
// The generic QueryTask projection still uses taskcommon.FromImportState at the
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
// preimport). DataCoord drives it through Create/Query/Drop; the scheduler owns
// its lifecycle: the state machine, run fencing and cancellation.
type Task interface {
	TaskID() int64
	RunID() int64
	// Kind labels the task for metrics and logs ("reshard", "import",
	// "preimport").
	Kind() string
	// Slot is the task's slot cost on this node.
	Slot() int64
	// Execute runs the task to completion (or error) on the pool's
	// goroutine. A canceled ctx is the only cancellation mechanism.
	Execute(ctx context.Context) (any, error)
}

// taskEntryKey identifies a task run in the scheduler registry. It is the
// composite (taskID, runID) pair, so an exact-entry lookup is an O(1) map hit
// instead of a registry scan. A replacement run gets a distinct key, so it
// never evicts the still-running run it supersedes.
type taskEntryKey struct{ taskID, runID int64 }

// taskEntry is one task run in the scheduler registry. Its result state (state,
// reason, result) is guarded by the entry's own mu; the lifecycle flags
// (started, retired, finished) and the key are guarded by the scheduler's mu.
type taskEntry struct {
	mu     sync.RWMutex
	worker Task
	state  datapb.ImportTaskStateV2
	reason string
	result any
	cancel context.CancelFunc
	ctx    context.Context
	// key is the entry's immutable key in the scheduler registry: its
	// (taskID, runID). It is assigned in addUnsafe when the entry is inserted.
	key taskEntryKey

	// started is true once the scheduler admitted the task into the exec pool
	// (selectUnsafe). A pending task (started == false) is still backed up: it
	// reserves its slots for backpressure, but is invisible to the state gauge.
	started bool
	// retired is true once the run is removed from the registry view -- a Drop,
	// or a newer run superseding it -- while its pool closure is still alive. A
	// retired entry is invisible to Query/the state gauge/the queued backlog but
	// still counts its slot until the closure exits.
	retired bool
	// finished is true once the pool closure exited.
	finished bool
	// enqueued is the Submit time, used for the queue-phase latency metric.
	enqueued time.Time
}

// Scheduler owns the whole process-local import V3 slot story in one type: the
// admission policy (which pending task starts next within the node's total slot
// budget) and the task registry plus execution pool that runs the admitted
// tasks. Merging the two lets one admission pass take a single lock over the
// registry, instead of a snapshot of pending candidates followed by a per-run
// start that could race with a concurrent Submit/Drop.
//
// Its view of the node is deliberately narrow: only import V3 tasks flow
// through it, and only their own slots count against the node's total slot
// capacity. Index-build and compaction usage is not consulted here; DataCoord's
// global water-filling already accounts for every executor through QuerySlot.
//
// Backpressure works in both directions:
//   - admission: a task starts only when capacity - usage covers its slot;
//   - reporting: queued slots count as used in QuerySlot, so DataCoord stops
//     water-filling new tasks onto this node instead of over-assigning.
//
// The backlog is unbounded by design: DataCoord's own slot accounting bounds
// how many tasks ever reach this node, so it cannot grow without limit.
// A pending task answers Pending to Query (see Query), never "not found", so
// DataCoord waits for its slot instead of retrying the task elsewhere.
//
// Admission is edge-triggered with a periodic resync, the same shape as a
// controller: a Submit, a Drop or a task completion wakes the loop (wake), and
// decisions are always recomputed from the registry's current state. The
// one-second ticker is a safety net for a missed wakeup, not the primary
// trigger.
//
// The registry is the single source of truth for a task's state: a task is
// inserted once (Submit) as pending, admit starts it, and Query/Drop always
// observe it here. Durable task/run fencing remains in DataCoord; this
// scheduler's run check prevents late Query or completion callbacks from
// mutating a newer run on the same DataNode.
type Scheduler struct {
	// capacity reports the node's total slot budget. Reading it through a
	// function keeps the scheduler decoupled from the slot policy.
	capacity func() int64
	metrics  *Metrics

	ctx    context.Context
	cancel context.CancelFunc
	exec   *conc.Pool[any]

	mu     sync.RWMutex
	tasks  *typeutil.OrderedMap[taskEntryKey, *taskEntry]
	wg     sync.WaitGroup
	closed bool

	wakeChan  chan struct{}
	closeChan chan struct{}
	closeOnce sync.Once
}

func NewScheduler(parent context.Context, capacity func() int64, metrics *Metrics) *Scheduler {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent) //nolint:gosec // G118: cancel is stored on the scheduler and called by Close.
	// Sized exactly like the import V2 execution pool: the admission policy
	// bounds how many tasks ever run, so this pool only tracks their futures
	// and never blocks admit in practice.
	poolSize := hardware.GetCPUNum() * paramtable.Get().DataNodeCfg.ImportConcurrencyPerCPUCore.GetAsInt()
	return &Scheduler{
		capacity:  capacity,
		metrics:   metrics,
		ctx:       ctx,
		cancel:    cancel,
		exec:      conc.NewPool[any](poolSize),
		tasks:     typeutil.NewOrderedMap[taskEntryKey, *taskEntry](),
		wakeChan:  make(chan struct{}, 1),
		closeChan: make(chan struct{}),
	}
}

// Submit validates a task and queues it for admission. It does not execute the
// task: admit runs it once the node has free slots. Submit is idempotent for
// the current run and a no-op for a stale run, the same run fencing the old Add
// enforced.
func (s *Scheduler) Submit(worker Task) error {
	if s == nil || worker == nil || worker.TaskID() == 0 || worker.RunID() == 0 || worker.Slot() <= 0 {
		return merr.WrapErrImportSysFailedMsg("invalid import V3 task create request")
	}
	taskID, runID := worker.TaskID(), worker.RunID()
	// added reports whether this Submit registered a new run: only then is the
	// admission loop worth waking, and the wake happens outside the lock.
	added, err := func() (bool, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.closed {
			return false, merr.WrapErrServiceNotReadyMsg("import V3 scheduler is closed")
		}
		if existing := s.getUnsafe(taskID); existing != nil {
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
				s.retireUnsafe(existing)
			case runID <= existingRun:
				// Same fenced run: Create is idempotent. Older run: stale, and
				// it must not replace the current work.
				return false, nil
			default:
				if existing.cancel != nil {
					existing.cancel()
				}
				s.retireUnsafe(existing)
			}
		}
		ctx, cancel := context.WithCancel(s.ctx) //nolint:gosec // G118: cancel is stored in the task entry and called on task termination or scheduler Close.
		s.addUnsafe(&taskEntry{
			worker:   worker,
			state:    datapb.ImportTaskStateV2_Pending,
			cancel:   cancel,
			ctx:      ctx,
			enqueued: time.Now(),
		})
		return true, nil
	}()
	if err != nil {
		return err
	}
	if added {
		s.wake()
	}
	return nil
}

// Query returns the current state of a task run. A runID of 0 means "the
// current run for this task".
func (s *Scheduler) Query(taskID, runID int64) (Snapshot, bool) {
	s.mu.RLock()
	var rec *taskEntry
	if runID != 0 {
		// Exact-entry lookup through the composite key. A retired run (dropped
		// or superseded) keeps answering not-found even while its closure still
		// holds its slot, so a stale Query stays a deliberate no-op: DataCoord
		// will query the persisted current run again instead of treating an old
		// worker reply as a task failure.
		if e, ok := s.tasks.Get(taskEntryKey{taskID: taskID, runID: runID}); ok && !e.retired {
			rec = e
		}
	} else {
		rec = s.getUnsafe(taskID)
	}
	s.mu.RUnlock()
	if rec == nil {
		return Snapshot{}, false
	}
	rec.mu.RLock()
	defer rec.mu.RUnlock()
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

// Drop removes and cancels a task run (a runID of 0 means the current run). It
// returns whether a droppable run existed. A run superseded by a newer run id
// is already retired and answers false, so a stale Drop never cancels the
// current work.
func (s *Scheduler) Drop(taskID, runID int64) bool {
	// rec is nil when there is nothing droppable; wasQueued drives the queue
	// gauge refresh below. Both side effects happen outside the lock.
	rec, wasQueued := func() (*taskEntry, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		var rec *taskEntry
		if runID != 0 {
			// Exact-entry lookup through the composite key; a retired run is
			// never droppable again.
			if e, ok := s.tasks.Get(taskEntryKey{taskID: taskID, runID: runID}); ok && !e.retired {
				rec = e
			}
		} else {
			rec = s.getUnsafe(taskID)
		}
		if rec == nil {
			return nil, false
		}
		wasQueued := !rec.started
		s.retireUnsafe(rec)
		return rec, wasQueued
	}()
	if rec == nil {
		return false
	}
	if rec.cancel != nil {
		rec.cancel()
	}
	if wasQueued {
		mlog.Info(context.TODO(), "slot scheduler discards queued task",
			mlog.FieldTaskID(taskID), mlog.Int64("runID", rec.worker.RunID()), mlog.String("kind", rec.worker.Kind()))
		// Refresh the queue gauge promptly; no slot is freed.
		s.wake()
	}
	// A dropped started run keeps its slot until its closure exits; finishRun
	// wakes the scheduler then, so a pending task can take the freed slot.
	return true
}

// Slots returns the slots that started runs still hold: every admitted run
// whose pool closure has not exited. Pending tasks are reported through
// QueuedSlots instead. A dropped or superseded run keeps counting until its
// closure exits, so admission never reuses the slots of a run that is still
// executing.
func (s *Scheduler) Slots() int64 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.slotsUnsafe()
}

// QueuedSlots returns the slots reserved by queued tasks. They count as used in
// QuerySlot so DataCoord sees the backlog.
func (s *Scheduler) QueuedSlots() int64 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, slots := s.queuedUnsafe()
	return slots
}

// Start runs the admission loop until Close. It is a singleton loop like the
// other datanode schedulers, so a plain ticker is used instead of a pool.
func (s *Scheduler) Start() {
	mlog.Info(context.TODO(), "start slot scheduler")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeChan:
			mlog.Info(context.TODO(), "slot scheduler exited")
			return
		case <-s.wakeChan:
			s.admit()
		case <-ticker.C:
			s.admit()
		}
	}
}

// Close stops the admission loop, cancels every process-local V3 task, and
// waits for the callbacks visible to this DataNode process before releasing the
// execution pool. It is a local shutdown guarantee only; it does not create a
// cross-node DropAndWait RPC.
func (s *Scheduler) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		close(s.closeChan)
		s.cancel()
		s.mu.Lock()
		s.closed = true
		for _, key := range s.tasks.Keys() {
			if rec, ok := s.tasks.Get(key); ok && rec.cancel != nil {
				rec.cancel()
			}
		}
		s.tasks = typeutil.NewOrderedMap[taskEntryKey, *taskEntry]()
		s.mu.Unlock()
		// Wait for every admitted closure to exit before releasing the pool, so
		// no run executes after shutdown and no pool goroutine leaks. New runs
		// are already excluded by s.closed under the lock above.
		s.wg.Wait()
		s.exec.Release()
	})
}

// wake wakes the admission loop. It never blocks: a pending wakeup already
// covers the new one, so concurrent Submits, Drops and completions coalesce
// into a single admission pass.
func (s *Scheduler) wake() {
	if s == nil {
		return
	}
	select {
	case s.wakeChan <- struct{}{}:
	default:
	}
}

// admit starts every pending task that fits the node's free slots, in Submit
// order. A task that does not fit is skipped and retried on the next cycle;
// smaller tasks behind it may still start (no head-of-line blocking).
//
// A task whose own slot exceeds the node's total capacity can never satisfy
// usage+slot <= capacity while anything else runs, so once the node has no
// import V3 task running it is admitted on its own instead of being queued
// forever: DataCoord assigned it here, so the node owes it a run.
//
// The pass is level-triggered: it reads the current used slots and the current
// pending set and recomputes every decision, so a missed or duplicated wakeup
// cannot corrupt the outcome. Selection and marking happen under one lock
// (selectUnsafe); the runs are submitted to the exec pool outside it.
func (s *Scheduler) admit() {
	s.mu.Lock()
	batch, queued, queuedSlots, states := s.selectUnsafe(s.capacity())
	s.mu.Unlock()
	for _, rec := range batch {
		// Submit runs outside the scheduler lock: the exec pool may block when
		// every worker is busy, and that backpressure must never stall the
		// admission loop or the RPC callers that woke it.
		s.exec.Submit(s.runClosure(rec))
		s.metrics.ObserveQueueLatency(rec.worker.Kind(), time.Since(rec.enqueued))
		mlog.Info(context.TODO(), "slot scheduler started task",
			mlog.FieldTaskID(rec.worker.TaskID()), mlog.Int64("runID", rec.worker.RunID()),
			mlog.String("kind", rec.worker.Kind()), mlog.Int64("slot", rec.worker.Slot()),
			mlog.Duration("queueWait", time.Since(rec.enqueued)))
	}
	s.metrics.SetQueue(queued, queuedSlots)
	s.metrics.SetTaskStates(states)
}

// selectUnsafe is one admission pass. The caller holds s.mu, so the used-slot
// read, the admission decision and the started marking all happen atomically:
// there is no snapshot-then-start race with a concurrent Submit or Drop.
//
// It walks the not-yet-started entries in Submit order, skipping one when it
// cannot fit alongside what already runs, and marks the admitted ones started.
// In the same pass it computes the queued backlog (the entries it left alone)
// and the per-kind state distribution of started, non-retired entries.
func (s *Scheduler) selectUnsafe(capacity int64) (batch []*taskEntry, queued int, queuedSlots int64, states map[string]map[datapb.ImportTaskStateV2]int) {
	used := s.slotsUnsafe()
	states = make(map[string]map[datapb.ImportTaskStateV2]int)
	for _, key := range s.tasks.Keys() {
		rec, ok := s.tasks.Get(key)
		if !ok || rec.retired {
			continue
		}
		if !rec.started {
			slot := rec.worker.Slot()
			if used > 0 && used+slot > capacity {
				// Does not fit alongside what already runs: keep it queued and
				// try again on the next cycle.
				queued++
				queuedSlots += slot
				continue
			}
			rec.started = true
			s.wg.Add(1)
			batch = append(batch, rec)
			used += slot
		}
		rec.mu.RLock()
		kind := rec.worker.Kind()
		if states[kind] == nil {
			states[kind] = make(map[datapb.ImportTaskStateV2]int)
		}
		states[kind][rec.state]++
		rec.mu.RUnlock()
	}
	return batch, queued, queuedSlots, states
}

// runClosure builds the pool closure that runs one admitted task to completion
// (or error) and records its terminal state. It runs for every admitted run,
// whether it completed normally, was canceled, or was retired.
func (s *Scheduler) runClosure(rec *taskEntry) func() (any, error) {
	return func() (any, error) {
		defer s.finishRun(rec)
		// A panic inside the worker must terminate the run instead of leaving it
		// InProgress forever: conc re-panics after recording the future error, so
		// without this recover the state write below never runs, the slot stays
		// charged and DataCoord keeps querying a run that never turns terminal.
		defer func() {
			x := recover()
			if x == nil {
				return
			}
			mlog.Error(rec.ctx, "import V3 worker panicked",
				mlog.Int64("taskID", rec.worker.TaskID()),
				mlog.String("kind", rec.worker.Kind()),
				mlog.Any("panic", x))
			rec.mu.Lock()
			if rec.state == datapb.ImportTaskStateV2_Pending || rec.state == datapb.ImportTaskStateV2_InProgress {
				rec.state = datapb.ImportTaskStateV2_Failed
				rec.reason = fmt.Sprintf("worker panicked: %v", x)
			}
			rec.mu.Unlock()
		}()
		rec.mu.Lock()
		if rec.state != datapb.ImportTaskStateV2_Pending {
			rec.mu.Unlock()
			return nil, nil
		}
		rec.state = datapb.ImportTaskStateV2_InProgress
		rec.mu.Unlock()
		runStart := time.Now()
		result, err := rec.worker.Execute(rec.ctx)
		s.metrics.ObserveRunLatency(rec.worker.Kind(), time.Since(runStart))
		rec.mu.Lock()
		if err != nil {
			// A canceled run must never record Failed: DataCoord's stale query
			// would fail the whole job for a run it already superseded or
			// dropped.
			if rec.ctx.Err() != nil {
				rec.state = datapb.ImportTaskStateV2_Retry
				rec.reason = rec.ctx.Err().Error()
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
	}
}

// finishRun is the pool closure's exit path. It marks the run finished and, if
// the run was retired (dropped or superseded mid-run), deletes it from the
// registry now, freeing its slot; otherwise the entry stays observable so a
// late Query still sees its terminal state. It runs for every admitted run,
// whether it completed normally or was retired.
func (s *Scheduler) finishRun(rec *taskEntry) {
	s.mu.Lock()
	rec.finished = true
	if rec.retired {
		s.deleteUnsafe(rec)
	}
	s.mu.Unlock()
	s.wg.Done()
	// A finished run frees its slot: wake the scheduler so a pending task can
	// take it without waiting for the resync ticker.
	s.wake()
}

// getUnsafe returns the current (non-retired) entry for taskID, or nil if the
// task is absent or its current run was retired. At most one entry per task id
// is non-retired: Submit retires the previous run before inserting a
// replacement. This is the by-taskID scan; callers that already know the run
// use s.tasks.Get with the composite key instead. The caller holds s.mu.
func (s *Scheduler) getUnsafe(taskID int64) *taskEntry {
	for _, key := range s.tasks.Keys() {
		rec, ok := s.tasks.Get(key)
		if ok && !rec.retired && rec.worker.TaskID() == taskID {
			return rec
		}
	}
	return nil
}

// addUnsafe inserts rec into the registry in Submit order, assigning it its
// immutable (taskID, runID) key. The caller holds s.mu.
func (s *Scheduler) addUnsafe(rec *taskEntry) {
	rec.key = taskEntryKey{taskID: rec.worker.TaskID(), runID: rec.worker.RunID()}
	s.tasks.Set(rec.key, rec)
}

// retireUnsafe removes rec from the registry view. A started run whose closure
// is still executing is only marked retired so its slot keeps counting toward
// Slots until finishRun deletes it; a pending or already-finished run has no
// slot to preserve and is deleted now (identity-checked, since a same-run Retry
// may already share the key with its replacement). The caller holds s.mu.
func (s *Scheduler) retireUnsafe(rec *taskEntry) {
	if !rec.started || rec.finished {
		s.deleteUnsafe(rec)
		return
	}
	rec.retired = true
}

// deleteUnsafe removes rec from the registry, but only if rec still owns its
// key. The composite key can transiently be shared: Submit's same-run Retry
// path retires a predecessor whose state is already Retry but whose closure has
// not yet set finished, so Set overwrites the predecessor in the map; when that
// predecessor's finishRun later runs it must not delete the replacement now
// sitting at the same key. The caller holds s.mu.
func (s *Scheduler) deleteUnsafe(rec *taskEntry) {
	if cur, ok := s.tasks.Get(rec.key); ok && cur == rec {
		s.tasks.Delete(rec.key)
	}
}

// slotsUnsafe reports the slots that started runs still hold: every admitted run
// whose pool closure has not exited, retired or not. Pending tasks are reported
// through the queued backlog instead. A dropped or superseded run keeps counting
// until its closure exits, so admission never reuses the slots of a run that is
// still executing. The caller holds s.mu.
func (s *Scheduler) slotsUnsafe() int64 {
	var slots int64
	for _, key := range s.tasks.Keys() {
		if rec, ok := s.tasks.Get(key); ok && rec.started && !rec.finished {
			slots += rec.worker.Slot()
		}
	}
	return slots
}

// queuedUnsafe reports the number and slot cost of tasks still waiting to start.
// The caller holds s.mu.
func (s *Scheduler) queuedUnsafe() (int, int64) {
	var (
		tasks int
		slots int64
	)
	for _, key := range s.tasks.Keys() {
		if rec, ok := s.tasks.Get(key); ok && !rec.started {
			tasks++
			slots += rec.worker.Slot()
		}
	}
	return tasks, slots
}
