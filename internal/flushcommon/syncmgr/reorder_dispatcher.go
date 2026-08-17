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

package syncmgr

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// reorderEntry is one submitted task and everything the dispatcher owns on its
// behalf. All mutable fields are guarded by reorderDispatcher.mu, except
// finishOnce which is the completion funnel itself.
type reorderEntry struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	stopCloseHook        func() bool
	task                 Task
	callbacks            []func(error) error
	resultCh             chan error
	releaseAdmission     bool      // dispatcher owns and releases the admission token
	admittedAt           time.Time // immutable: when admission succeeded
	preparedAt           time.Time // when Prepare returned; start of the wait for Commit
	prepareQueueDuration time.Duration
	prepareDuration      time.Duration
	commitWaitDuration   time.Duration
	commitDuration       time.Duration
	prepareRan           bool // false when an already-aborted key skipped all phases
	queueObserved        bool // legacy aggregate queue metric was emitted at Commit start

	queued     bool // still occupies a slot in its key's FIFO
	prepared   bool // Prepare returned; owns no worker or native handle anymore
	finishing  bool // a drain owns this entry's completion; others must not touch it
	finishOnce sync.Once
}

// reorderKeyState is the FIFO state of one key (segment).
type reorderKeyState struct {
	entries  []*reorderEntry
	aborted  bool
	abortErr error
	// committing means entries[0] has been handed to the commit pool; only
	// commitDone may clear it. It is always the head that is committing:
	// tryCommitLocked only dispatches entries[0], and while the flag is set no
	// path removes an entry (drainAborted refuses a committing head, commitDone
	// clears the flag before removeEntryLocked) and entries are only appended.
	committing bool
}

// reorderDispatcher executes sync tasks. It runs Prepare concurrently across the
// whole node but publishes Commit — and every completion callback — in
// submission order per key.
//
// It is ONLY an executor and does not retry. Whole-task retry belongs to the
// task owner. Admission is node-wide and bounded, however: callers must not be
// able to materialize an unbounded number of task payloads while the worker
// pools are saturated. Submit blocks before the task becomes visible to the
// dispatcher, and that wait is cancellation-aware.
//
// When a task fails, the whole suffix of its key is aborted with it. Ordering is
// the reason: the tasks behind it were built against state its Commit was
// supposed to publish. They all go back to the write buffer, which re-submits
// from the oldest.
//
// Locking rules, enforced throughout this file:
//   - mu guards every dispatcher and entry field.
//   - a *Locked helper requires mu held and returns with mu still held.
//   - every other method acquires and releases mu itself, and never calls user
//     code (task methods, callbacks) while holding it.
type reorderDispatcher[K comparable] struct {
	mu        sync.Mutex
	completed *syncutil.ContextCond // an accepted task finished

	keys map[K]*reorderKeyState

	closed      bool
	closeCtx    context.Context
	closeCancel context.CancelFunc

	preparePool *conc.Pool[struct{}]
	commitPool  *conc.Pool[struct{}]
	admission   *syncutil.Semaphore
	poolMu      sync.Mutex

	accepted int
}

func dispatcherAdmissionCapacity(maxParallel int) int {
	capacity := maxParallel * 2
	if capacity < 4 {
		capacity = 4
	}
	return capacity
}

func newReorderDispatcher[K comparable](maxParallel int) *reorderDispatcher[K] {
	if maxParallel <= 0 {
		maxParallel = 1
	}
	// closeCancel is retained by the dispatcher and invoked exactly once by
	// beginClose; it cannot be deferred in this constructor.
	//nolint:gosec
	closeCtx, closeCancel := context.WithCancel(context.Background())
	d := &reorderDispatcher[K]{
		keys:        make(map[K]*reorderKeyState),
		closeCtx:    closeCtx,
		closeCancel: closeCancel,
		preparePool: conc.NewPool[struct{}](maxParallel, conc.WithPreAlloc(false)),
		commitPool:  conc.NewPool[struct{}](maxParallel, conc.WithPreAlloc(false)),
		admission:   syncutil.NewSemaphore(dispatcherAdmissionCapacity(maxParallel)),
	}
	d.completed = syncutil.NewContextCond(&d.mu)
	return d
}

// Submit waits for node-wide admission, queues a task at the tail of its key,
// and returns a Future that completes after the task's callbacks have run.
func (d *reorderDispatcher[K]) Submit(ctx context.Context, key K, task Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
	return d.submit(ctx, key, task, nil, callbacks...)
}

// submit is Submit with an acceptance hook. SyncManager uses the hook to make a
// task visible in its diagnostics only after admission succeeds, but before any
// phase or callback can observe it.
func (d *reorderDispatcher[K]) submit(
	ctx context.Context,
	key K,
	task Task,
	onAccepted func(),
	callbacks ...func(error) error,
) (*conc.Future[struct{}], error) {
	if err := d.acquireAdmission(ctx); err != nil {
		return nil, err
	}
	return d.submitAcquired(ctx, key, task, onAccepted, true, callbacks...)
}

// submitReserved accepts an attempt using an admission token retained by its
// caller across the task payload's whole lifetime (including retries).
func (d *reorderDispatcher[K]) submitReserved(
	ctx context.Context,
	key K,
	task Task,
	onAccepted func(),
	callbacks ...func(error) error,
) (*conc.Future[struct{}], error) {
	return d.submitAcquired(ctx, key, task, onAccepted, false, callbacks...)
}

// submitAcquired accepts a task using one admission token already held. When
// releaseAdmission is true the dispatcher owns the token for this attempt;
// otherwise the caller keeps it across attempts and releases it only after the
// materialized payload reaches a terminal state.
func (d *reorderDispatcher[K]) submitAcquired(
	ctx context.Context,
	key K,
	task Task,
	onAccepted func(),
	releaseAdmission bool,
	callbacks ...func(error) error,
) (*conc.Future[struct{}], error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		if releaseAdmission {
			d.admission.Release()
		}
		return nil, context.Canceled
	}
	entry, future := d.newEntry(ctx, task, releaseAdmission, callbacks)
	if onAccepted != nil {
		onAccepted()
	}
	// From here on finish() is the sole completion and admission-release path.
	d.accepted++
	state := d.keys[key]
	if state == nil {
		state = &reorderKeyState{}
		d.keys[key] = state
	}
	if state.aborted {
		// The key is still draining a failure; everything behind it belongs to
		// the same retry round and must not start. It still joins the FIFO:
		// completing it inline here would run its callbacks ahead of earlier
		// entries whose drain is blocked on a running phase, breaking the
		// per-key completion order the class promises. Marked prepared because
		// Prepare never runs — the entry owns no worker and no native handle,
		// so drainAborted may complete it as soon as it reaches the head.
		entry.queued = true
		entry.prepared = true
		state.entries = append(state.entries, entry)
		metrics.WALFlusherSyncDispatcherTaskTotal.WithLabelValues(paramtable.GetStringNodeID()).Inc()
		metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(paramtable.GetStringNodeID()).Inc()
		d.mu.Unlock()
		d.drainAborted(key)
		return future, nil
	}
	entry.queued = true
	state.entries = append(state.entries, entry)
	// Increment while the entry is still protected by mu. A concurrent aborted-key
	// drain cannot remove the entry and decrement the gauge before this increment.
	metrics.WALFlusherSyncDispatcherTaskTotal.WithLabelValues(paramtable.GetStringNodeID()).Inc()
	metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(paramtable.GetStringNodeID()).Inc()
	d.mu.Unlock()

	d.dispatchPrepare(key, entry)
	return future, nil
}

func (d *reorderDispatcher[K]) acquireAdmission(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.closeCtx.Err(); err != nil {
		return err
	}
	admissionCtx, cancel := context.WithCancel(ctx)
	stopCloseHook := context.AfterFunc(d.closeCtx, cancel)
	err := d.admission.Acquire(admissionCtx)
	if err == nil {
		switch {
		case ctx.Err() != nil:
			d.admission.Release()
			err = ctx.Err()
		case d.closeCtx.Err() != nil:
			d.admission.Release()
			err = d.closeCtx.Err()
		case admissionCtx.Err() != nil:
			d.admission.Release()
			err = context.Cause(admissionCtx)
		}
	}
	stopCloseHook()
	cancel()
	return err
}

func (d *reorderDispatcher[K]) newEntry(
	ctx context.Context,
	task Task,
	releaseAdmission bool,
	callbacks []func(error) error,
) (*reorderEntry, *conc.Future[struct{}]) {
	entryCtx, cancel := context.WithCancel(ctx)
	entry := &reorderEntry{
		ctx:              entryCtx,
		cancel:           cancel,
		stopCloseHook:    context.AfterFunc(d.closeCtx, cancel),
		task:             task,
		callbacks:        callbacks,
		resultCh:         make(chan error, 1),
		releaseAdmission: releaseAdmission,
		admittedAt:       time.Now(),
	}
	future := conc.Go(func() (struct{}, error) {
		return struct{}{}, <-entry.resultCh
	})
	return entry, future
}

func (d *reorderDispatcher[K]) dispatchPrepare(key K, entry *reorderEntry) {
	// The pool is submitted from a fresh goroutine because this can be reached
	// from a pool worker's completion path, which has not released its slot yet.
	go func() {
		nodeID := paramtable.GetStringNodeID()
		future := d.preparePool.Submit(func() (struct{}, error) {
			startedAt := time.Now()
			prepareQueue := startedAt.Sub(entry.admittedAt)
			d.mu.Lock()
			entry.prepareRan = true
			entry.prepareQueueDuration = prepareQueue
			d.mu.Unlock()

			metrics.WALFlusherSyncDispatcherPrepareQueueDuration.WithLabelValues(nodeID).
				Observe(prepareQueue.Seconds())
			start := time.Now()
			err := runPhase(entry.ctx, entry.task.Prepare)
			finishedAt := time.Now()
			prepareDuration := finishedAt.Sub(start)
			d.mu.Lock()
			entry.prepareDuration = prepareDuration
			if err == nil {
				// Record the phase boundary at the worker, immediately after
				// Prepare returns. prepareDone runs on another goroutine and may be
				// scheduled later under load; using its wall clock would leave that
				// delay out of both PrepareDuration and CommitWaitDuration.
				entry.preparedAt = finishedAt
			}
			d.mu.Unlock()
			metrics.WALFlusherSyncDispatcherPrepareDuration.WithLabelValues(nodeID).
				Observe(prepareDuration.Seconds())
			return struct{}{}, err
		})
		<-future.Inner()
		d.prepareDone(key, entry, future.Err())
	}()
}

func (d *reorderDispatcher[K]) prepareDone(key K, entry *reorderEntry, err error) {
	d.mu.Lock()
	state := d.keys[key]
	if state == nil || !entry.queued {
		d.mu.Unlock()
		return
	}
	// prepared means Prepare has returned and no longer owns a worker or a
	// native handle, so abort cleanup may now complete this entry.
	entry.prepared = true
	if err != nil {
		d.abortKeyLocked(state, err)
		d.mu.Unlock()
		d.drainAborted(key)
		return
	}
	// preparedAt means the task is READY to Commit, not merely that Prepare no
	// longer owns a worker. Prepare failures use prepared only for safe cleanup
	// and must not be charged as waiting for Commit.
	if state.aborted {
		d.mu.Unlock()
		d.drainAborted(key)
		return
	}
	d.tryCommitLocked(key, state)
	d.mu.Unlock()
}

// tryCommitLocked hands the key's head to the commit pool if it is ready and no
// other commit for this key is running.
func (d *reorderDispatcher[K]) tryCommitLocked(key K, state *reorderKeyState) {
	if state.aborted || state.committing || len(state.entries) == 0 {
		return
	}
	head := state.entries[0]
	if !head.prepared {
		return
	}
	state.committing = true
	d.dispatchCommit(key, head)
}

func (d *reorderDispatcher[K]) dispatchCommit(key K, entry *reorderEntry) {
	// dispatchCommit is called with d.mu held, so capture the protected timestamp
	// before handing the work to another goroutine.
	preparedAt := entry.preparedAt
	go func() {
		nodeID := paramtable.GetStringNodeID()
		future := d.commitPool.Submit(func() (struct{}, error) {
			// The wait covers everything between Prepare returning and Commit
			// starting: earlier commits of this key, their callbacks, and the
			// commit pool queue. "Prepare fast but checkpoint slow" shows up
			// here.
			commitWait := time.Since(preparedAt)
			d.mu.Lock()
			entry.commitWaitDuration = commitWait
			queueDuration := entry.prepareQueueDuration + commitWait
			entry.queueObserved = true
			d.mu.Unlock()
			metrics.WALFlusherSyncDispatcherCommitWaitDuration.WithLabelValues(nodeID).
				Observe(commitWait.Seconds())
			// Preserve the legacy aggregate as queueing only: wait for a Prepare
			// worker plus wait for this key/Commit worker. Admission and phase
			// execution are deliberately excluded.
			metrics.WALFlusherSyncDispatcherQueueDuration.WithLabelValues(nodeID).
				Observe(queueDuration.Seconds())
			start := time.Now()
			err := runPhase(entry.ctx, entry.task.Commit)
			commitDuration := time.Since(start)
			d.mu.Lock()
			entry.commitDuration = commitDuration
			d.mu.Unlock()
			metrics.WALFlusherSyncDispatcherCommitDuration.WithLabelValues(nodeID).
				Observe(commitDuration.Seconds())
			return struct{}{}, err
		})
		<-future.Inner()
		d.commitDone(key, entry, future.Err())
	}()
}

func (d *reorderDispatcher[K]) commitDone(key K, entry *reorderEntry, err error) {
	if err != nil {
		d.mu.Lock()
		if state := d.keys[key]; state != nil {
			state.committing = false
			d.abortKeyLocked(state, err)
		}
		d.mu.Unlock()
		d.drainAborted(key)
		return
	}

	// The entry keeps its head position and its key's committing flag for the whole
	// ACK: the next Commit on this key must not overtake this task's callbacks,
	// which are what publish the checkpoint and segment-state cleanup this
	// commit depends on. The release below is the single point where the key
	// moves on.
	d.finish(entry, nil, func() {
		d.mu.Lock()
		state := d.keys[key]
		if state != nil {
			state.committing = false
		}
		d.removeEntryLocked(key, state, entry)
		if state != nil && !state.aborted {
			d.tryCommitLocked(key, state)
		}
		d.mu.Unlock()
	})
	// A head timeout or a sibling failure may have aborted the key while the ACK
	// was running; the suffix it left behind is drained here.
	d.drainAborted(key)
}

// abortKeyLocked marks the whole key failed and cancels its entries. Entries are
// completed by drainAborted, in FIFO order, once each of them is safe to touch.
func (d *reorderDispatcher[K]) abortKeyLocked(state *reorderKeyState, err error) {
	if state.aborted {
		return
	}
	state.aborted = true
	state.abortErr = err
	for _, entry := range state.entries {
		entry.cancel()
	}
}

// drainAborted completes the entries of an aborted key that no longer belong to
// a running phase, keeping the FIFO completion order intact.
//
// The head entry keeps its queue slot until its callbacks have run — same
// contract as commitDone's release. Removing it first would let a concurrent
// Submit re-create the key and Commit against state the failed entry's
// callbacks have not reset yet. The finishing flag is what stops a second
// concurrent drain from picking the same head and spinning on its no-op
// finishOnce.
func (d *reorderDispatcher[K]) drainAborted(key K) {
	for {
		d.mu.Lock()
		state := d.keys[key]
		if state == nil || !state.aborted || len(state.entries) == 0 {
			d.mu.Unlock()
			return
		}
		head := state.entries[0]
		// A committing entry is completed by commitDone. A Prepare that has not
		// returned still owns native resources and must reach prepareDone first.
		// A finishing entry already belongs to another drain.
		if state.committing || !head.prepared || head.finishing {
			d.mu.Unlock()
			return
		}
		head.finishing = true
		err := state.abortErr
		d.mu.Unlock()

		d.finish(head, err, func() {
			d.mu.Lock()
			d.removeEntryLocked(key, d.keys[key], head)
			d.mu.Unlock()
		})
	}
}

func (d *reorderDispatcher[K]) removeEntryLocked(key K, state *reorderKeyState, entry *reorderEntry) {
	if !entry.queued {
		return
	}
	entry.queued = false
	metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(paramtable.GetStringNodeID()).Dec()

	if state != nil {
		for i, candidate := range state.entries {
			if candidate == entry {
				state.entries = append(state.entries[:i], state.entries[i+1:]...)
				break
			}
		}
		d.dropEmptyKeyLocked(key)
	}
}

// dropEmptyKeyLocked forgets a key that holds no entries.
func (d *reorderDispatcher[K]) dropEmptyKeyLocked(key K) {
	if state := d.keys[key]; state != nil && len(state.entries) == 0 {
		delete(d.keys, key)
	}
}

// finish is the single completion funnel: it runs the callbacks, hands the key
// on via release, publishes the Future, and drops the entry from close
// accounting — exactly once, and never with mu held.
//
// release, when set, runs after the callbacks and is where the entry gives up
// its position in the key's FIFO. Ordering matters: removing that position
// before callbacks finish lets the next task commit against state this one has
// not published yet. The node-wide admission slot is independent and is
// released only after callbacks so failed/canceled tasks remain inside the
// bound until lifecycle callbacks release their payload or source. A callback
// must not synchronously Submit follow-up work, Await a same-key future, or call
// Close; follow-up submission must be detached and begin after callback return.
func (d *reorderDispatcher[K]) finish(entry *reorderEntry, err error, release func()) {
	entry.finishOnce.Do(func() {
		d.mu.Lock()
		prepareRan := entry.prepareRan
		queueObserved := entry.queueObserved
		commitReady := !entry.preparedAt.IsZero()
		commitWaitDuration := entry.commitWaitDuration
		if !queueObserved && commitReady {
			// Prepare succeeded, but the entry was aborted before Commit could
			// start. Account the real time it spent ready behind the key/Commit
			// pool instead of silently reporting a zero wait.
			commitWaitDuration = time.Since(entry.preparedAt)
		}
		queueDuration := entry.prepareQueueDuration + commitWaitDuration
		executeDuration := entry.prepareDuration + entry.commitDuration
		d.mu.Unlock()
		if prepareRan && !queueObserved {
			if commitReady {
				metrics.WALFlusherSyncDispatcherCommitWaitDuration.WithLabelValues(paramtable.GetStringNodeID()).
					Observe(commitWaitDuration.Seconds())
			}
			// Prepare failed, or a successfully prepared entry was aborted before
			// Commit. The latter includes its real ready-to-Commit wait above.
			metrics.WALFlusherSyncDispatcherQueueDuration.WithLabelValues(paramtable.GetStringNodeID()).
				Observe(queueDuration.Seconds())
		}

		callbackStart := time.Now()
		panicValue, finalErr := runCallbacks(err, entry.callbacks)
		executeDuration += time.Since(callbackStart)
		// Preserve the legacy aggregate as actual work only: Prepare, Commit,
		// and completion callbacks. Admission and both queue waits are excluded.
		// An entry admitted onto an already-aborted key runs callbacks without a
		// phase; that callback work is still execution and must be observable.
		metrics.WALFlusherSyncDispatcherExecuteDuration.WithLabelValues(paramtable.GetStringNodeID()).
			Observe(executeDuration.Seconds())
		if release != nil {
			release()
		}
		entry.stopCloseHook()
		entry.cancel()
		if entry.releaseAdmission {
			d.admission.Release()
		}

		// Publish the result BEFORE dropping the accepted count. waitClosed
		// releases on accepted reaching zero, so anything published after the
		// decrement can be missed by a Close that has already returned. The
		// channel is buffered with capacity one and has exactly one sender, so
		// this cannot block while mu is held.
		entry.resultCh <- finalErr
		close(entry.resultCh)

		// LockAndBroadcast RETURNS WITH d.mu HELD — the accounting below runs
		// under it, and the bare Unlock is its pair. The "never with mu held"
		// rule above is about the callbacks, which already ran.
		d.completed.LockAndBroadcast()
		d.accepted--
		d.mu.Unlock()

		repanicAsync(panicValue)
	})
}

// beginClose fences new submissions and aborts every key. Entries already
// running in a pool are completed by their own path; waitClosed is the fence
// that covers all of them.
func (d *reorderDispatcher[K]) beginClose() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	d.closeCancel()
	keys := make([]K, 0, len(d.keys))
	for key, state := range d.keys {
		keys = append(keys, key)
		d.abortKeyLocked(state, context.Canceled)
	}
	d.mu.Unlock()

	// Callbacks are external code and may re-enter the dispatcher, so drain
	// outside the lock.
	for _, key := range keys {
		d.drainAborted(key)
	}
}

// waitClosed waits until every task the dispatcher accepted has run its
// callbacks, published its result, and been accounted for. ctx is the shutdown
// bound.
//
// "Published its result" means the value is in the entry's result channel, not
// that Future.Done() is already true: the Future is a conc.Go relay reading
// that channel, and conc exposes no way to complete a Future directly, so the
// relay goroutine may still be unscheduled. What this does guarantee is that
// the Future needs nothing further FROM THE DISPATCHER — Await returns without
// any dispatcher work remaining. Callers that need Done() must Await.
func (d *reorderDispatcher[K]) waitClosed(ctx context.Context) error {
	d.mu.Lock()
	for d.accepted > 0 {
		// Wait only reacquires mu when it succeeds.
		if err := d.completed.Wait(ctx); err != nil {
			return err
		}
	}
	d.mu.Unlock()
	return nil
}

// Close is the dispatcher-only fence used by tests; SyncManager runs the full
// bounded shutdown sequence.
func (d *reorderDispatcher[K]) Close() {
	d.beginClose()
}

func (d *reorderDispatcher[K]) releasePools(timeout time.Duration) error {
	d.poolMu.Lock()
	defer d.poolMu.Unlock()

	deadline := time.Now().Add(timeout)
	prepareErr := d.preparePool.ReleaseTimeout(timeout)
	remaining := time.Until(deadline)
	if remaining <= 0 {
		// ReleaseTimeout closes a pool before waiting. Do the same for Commit
		// even when Prepare consumed the entire shutdown budget; otherwise its
		// workers remain live forever after Close returns an error.
		d.commitPool.Release()
		return errors.CombineErrors(prepareErr, context.DeadlineExceeded)
	}
	commitErr := d.commitPool.ReleaseTimeout(remaining)
	return errors.CombineErrors(prepareErr, commitErr)
}

func (d *reorderDispatcher[K]) resize(size int) error {
	d.poolMu.Lock()
	defer d.poolMu.Unlock()

	if err := d.preparePool.Resize(size); err != nil {
		return err
	}
	if err := d.commitPool.Resize(size); err != nil {
		return err
	}
	d.admission.SetCapacity(dispatcherAdmissionCapacity(size))
	return nil
}

// Pending is the number of accepted tasks that have not completed.
func (d *reorderDispatcher[K]) Pending() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.accepted
}

// runPhase runs one phase exactly once. Cancellation is checked first because a
// task's context can expire after admission while it waits for a worker. A phase
// that has already started cannot be preempted by the dispatcher.
func runPhase(ctx context.Context, phase func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return phase(ctx)
}

// runCallbacks is deliberately panic-tolerant per callback. HandleError is the
// first callback installed by SyncManager and may be fatal; write-buffer
// lifecycle callbacks after it still have to release segment state and payload
// ownership before that panic is propagated.
func runCallbacks(initialErr error, callbacks []func(error) error) (any, error) {
	finalErr := initialErr
	var firstPanic any
	var panicResultErr error
	for _, callback := range callbacks {
		if firstPanic != nil {
			// Once a callback panics, every remaining lifecycle callback must see
			// the preserved failure even if an earlier cleanup callback returned nil.
			finalErr = panicResultErr
		}
		func() {
			defer func() {
				if panicValue := recover(); panicValue != nil {
					if firstPanic == nil {
						firstPanic = panicValue
						// Preserve the error being handled when the first panic happened
						// so downstream lifecycle callbacks and the Future retain its
						// errors.Is/merr identity. A panic on an otherwise successful
						// attempt becomes a System error instead of reported success.
						panicResultErr = finalErr
						if panicResultErr == nil {
							panicResultErr = merr.WrapErrServiceInternalMsg("sync task callback panicked: %v", panicValue)
						}
					}
					finalErr = panicResultErr
				}
			}()
			finalErr = callback(finalErr)
		}()
	}
	if firstPanic != nil {
		finalErr = panicResultErr
	}
	return firstPanic, finalErr
}

func repanicAsync(panicValue any) {
	if panicValue == nil {
		return
	}
	go func() {
		panic(panicValue)
	}()
}
