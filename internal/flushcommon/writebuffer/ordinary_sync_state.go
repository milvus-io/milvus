package writebuffer

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

type ordinarySyncPhase uint8

const (
	ordinarySyncAttemptInFlight ordinarySyncPhase = iota
	ordinarySyncRetryWait
)

type retryTimer interface {
	Stop() bool
}

type retryAfterFunc func(time.Duration, func()) retryTimer

type ordinarySyncState struct {
	task            *syncmgr.SyncTask
	phase           ordinarySyncPhase
	attempt         uint64
	retryTimer      retryTimer
	retryCtx        context.Context
	payloadReleased chan struct{}
	done            chan struct{}
	doneOnce        sync.Once
	terminalErr     error
	pending         bool
}

type ordinarySyncOutcome struct {
	attemptErr  error
	terminalErr error
	fatalErr    error
	terminal    bool
}

func (s *ordinarySyncState) complete(err error) {
	s.doneOnce.Do(func() {
		s.terminalErr = err
		close(s.done)
	})
}

func (wb *writeBufferBase) scheduleOrdinaryRetryLocked(state *ordinarySyncState) {
	if state == nil || state.phase != ordinarySyncRetryWait || state.retryTimer != nil || wb.closed {
		return
	}
	delay := wb.growingSourceRetryInterval
	if delay < 0 {
		if !wb.dropping {
			return
		}
		delay = defaultGrowingSourceRetryInterval
	}
	segmentID := state.task.SegmentID()
	expectedAttempt := state.attempt
	state.retryTimer = wb.ordinaryRetryAfterFunc(delay, func() {
		wb.retryOrdinarySync(segmentID, state, expectedAttempt)
	})
}

// beginOrdinaryDropLocked transfers every current logical task to the Drop
// context and snapshots its completion before any callback can remove it from
// ordinarySyncs. The caller must set wb.dropping and hold wb.mut.
func (wb *writeBufferBase) beginOrdinaryDropLocked(ctx context.Context) ([]syncmgr.Task, []*ordinarySyncState) {
	tasks := make([]syncmgr.Task, 0)
	waiters := make([]*ordinarySyncState, 0, len(wb.ordinarySyncs))
	for _, state := range wb.ordinarySyncs {
		state.retryCtx = ctx
		if state.phase == ordinarySyncRetryWait {
			if state.retryTimer != nil {
				state.retryTimer.Stop()
				state.retryTimer = nil
			}
			state.phase = ordinarySyncAttemptInFlight
			state.attempt++
			tasks = append(tasks, state.task)
		}
		waiters = append(waiters, state)
	}
	return tasks, waiters
}

func (wb *writeBufferBase) waitOrdinarySyncs(
	ctx context.Context,
	cancel context.CancelFunc,
	tasks []syncmgr.Task,
	waiters []*ordinarySyncState,
) error {
	if err := ctx.Err(); err != nil {
		remaining := wb.cancelOutstandingOrdinarySyncs(err, cancel)
		for _, waiter := range remaining {
			<-waiter.done
		}
		return err
	}

	if len(tasks) > 0 {
		wb.submitSyncTasks(ctx, tasks)
	}
	return wb.waitOrdinarySyncWaiters(ctx, cancel, waiters)
}

func (wb *writeBufferBase) waitOrdinarySyncWaiters(ctx context.Context, cancel context.CancelFunc, waiters []*ordinarySyncState) error {
	if len(waiters) == 0 {
		return ctx.Err()
	}

	// A sequential drain can hide a terminal failure behind an earlier task
	// that is still retrying. Fan every completion into one buffered channel so
	// the first terminal signal cancels the Drop context immediately, then keep
	// draining until every logical owner has completed its cleanup.
	results := make(chan error, len(waiters))
	for _, state := range waiters {
		go func(state *ordinarySyncState) {
			<-state.done
			results <- state.terminalErr
		}(state)
	}

	var firstErr error
	ctxDone := ctx.Done()
	for range waiters {
		select {
		case err := <-results:
			if err != nil && firstErr == nil {
				firstErr = err
				wb.cancelOutstandingOrdinarySyncs(err, cancel)
				ctxDone = nil
			}
		case <-ctxDone:
			if firstErr == nil {
				firstErr = ctx.Err()
				wb.cancelOutstandingOrdinarySyncs(firstErr, cancel)
				ctxDone = nil
			}
		}
	}
	return firstErr
}

// cancelOutstandingOrdinarySyncs aborts a Drop drain without returning while
// another logical task still owns its payload. Retry-wait tasks can be
// discarded immediately; queued/running attempts are canceled and their
// callbacks remain the sole owner of terminal cleanup.
func (wb *writeBufferBase) cancelOutstandingOrdinarySyncs(terminalErr error, cancel context.CancelFunc) []*ordinarySyncState {
	if cancel != nil {
		cancel()
	}
	wb.syncCancel()

	wb.mut.Lock()
	waiters := make([]*ordinarySyncState, 0, len(wb.ordinarySyncs))
	completed := make([]*ordinarySyncState, 0)
	for _, state := range wb.ordinarySyncs {
		waiters = append(waiters, state)
		if state.phase == ordinarySyncRetryWait {
			wb.discardOrdinarySyncLocked(state, false)
			completed = append(completed, state)
		}
	}
	wb.mut.Unlock()

	for _, state := range completed {
		state.complete(terminalErr)
	}
	return waiters
}

func (wb *writeBufferBase) retryOrdinarySync(segmentID int64, expectedState *ordinarySyncState, expectedAttempt uint64) {
	wb.mut.Lock()
	state, ok := wb.ordinarySyncs[segmentID]
	if !ok || state != expectedState || state.phase != ordinarySyncRetryWait || state.attempt != expectedAttempt || wb.closed {
		wb.mut.Unlock()
		return
	}
	state.retryTimer = nil
	retryCtx := state.retryCtx
	if retryCtx == nil {
		retryCtx = wb.syncCtx
	}
	if err := retryCtx.Err(); err != nil {
		wb.discardOrdinarySyncLocked(state, false)
		wb.mut.Unlock()
		state.complete(err)
		return
	}
	state.phase = ordinarySyncAttemptInFlight
	state.attempt++
	task := state.task
	wb.mut.Unlock()

	wb.submitSyncTasks(retryCtx, []syncmgr.Task{task})
}

func (wb *writeBufferBase) discardOrdinarySyncLocked(state *ordinarySyncState, removeCheckpoint bool) {
	if state == nil {
		return
	}
	segmentID := state.task.SegmentID()
	if state.retryTimer != nil {
		state.retryTimer.Stop()
		state.retryTimer = nil
	}
	if state.task != nil {
		wb.metaCache.UpdateSegments(
			metacache.DiscardSyncing(state.task.BatchRows()),
			metacache.WithSegmentIDs(segmentID),
		)
		if removeCheckpoint && state.task.StartPosition() != nil {
			wb.syncCheckpoint.Remove(segmentID, state.task.StartPosition().GetTimestamp())
		}
		state.task.ReleasePayload()
	}
	if current, ok := wb.ordinarySyncs[segmentID]; ok && current == state {
		delete(wb.ordinarySyncs, segmentID)
	}
}

func (wb *writeBufferBase) handleOrdinarySyncResult(ctx context.Context, task *syncmgr.SyncTask, attempt uint64, attemptErr error) ordinarySyncOutcome {
	segmentID := task.SegmentID()
	var followup bool
	var followupTasks []syncmgr.Task

	wb.mut.Lock()
	state, ok := wb.ordinarySyncs[segmentID]
	if !ok || state.task != task || state.attempt != attempt || state.phase != ordinarySyncAttemptInFlight {
		wb.mut.Unlock()
		return ordinarySyncOutcome{attemptErr: attemptErr}
	}
	if attemptErr != nil {
		outcome := ordinarySyncOutcome{attemptErr: attemptErr}
		if wb.closed {
			wb.discardOrdinarySyncLocked(state, true)
			outcome.terminal = true
			outcome.terminalErr = attemptErr
		} else if wb.dropping && (ctx.Err() != nil || errors.Is(attemptErr, context.Canceled) || errors.Is(attemptErr, context.DeadlineExceeded)) {
			// Drop is aborting before metadata commit. Keep the checkpoint pinned
			// for WAL replay, but do not leave a detached retry loop owning the
			// pack after Close returns.
			wb.discardOrdinarySyncLocked(state, false)
			outcome.terminal = true
			outcome.terminalErr = attemptErr
		} else if reason := ordinarySyncFatal(attemptErr); reason != "" {
			fatalErr := errors.Wrapf(attemptErr,
				"sync unrecoverable (%s), segmentID=%d", reason, segmentID)
			// Keep the checkpoint candidate pinned for WAL replay, but release
			// every in-memory ownership before invoking the fatal handler. A
			// custom handler that returns must not leak the payload.
			wb.discardOrdinarySyncLocked(state, false)
			outcome.terminal = true
			outcome.terminalErr = fatalErr
			outcome.fatalErr = fatalErr
		} else {
			state.phase = ordinarySyncRetryWait
			wb.growingSourceRatedLogger.RatedWarn(ctx, rate.Limit(1),
				"sync task failed, retaining logical task for retry",
				mlog.Int64("segmentID", segmentID),
				mlog.Uint64("attempt", attempt),
				mlog.Err(attemptErr))
			wb.scheduleOrdinaryRetryLocked(state)
		}
		wb.mut.Unlock()
		return outcome
	}

	if state.retryTimer != nil {
		state.retryTimer.Stop()
		state.retryTimer = nil
	}
	_, hasBufferedPayload := wb.buffers[segmentID]
	followup = state.pending && hasBufferedPayload
	if segment, exists := wb.metaCache.GetSegmentByID(segmentID); exists {
		switch segment.State() {
		case commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing:
			followup = followup || !task.IsFlush()
		case commonpb.SegmentState_Dropped:
			followup = followup || !task.IsDrop()
		}
	}
	if task.StartPosition() != nil {
		wb.syncCheckpoint.Remove(segmentID, task.StartPosition().GetTimestamp())
	}
	if task.IsFlush() {
		wb.metaCache.RemoveSegments(metacache.WithSegmentIDs(segmentID))
	}
	delete(wb.ordinarySyncs, segmentID)
	followupAllowed := !wb.closed && !wb.dropping
	if followup && followupAllowed {
		// Build the next logical owner before state.done is published. Otherwise a
		// BufferData waiter can observe a full tail with no owner during the gap
		// between this callback and an asynchronous syncSegments call.
		followupTasks = wb.getSyncTasksLocked(wb.syncCtx, []int64{segmentID})
	}
	wb.mut.Unlock()

	if len(followupTasks) > 0 {
		// Submission can block on dispatcher admission. Keep it asynchronous so
		// the current callback can finish and release its own semaphore slot.
		go wb.submitSyncTasks(wb.syncCtx, followupTasks)
	}
	return ordinarySyncOutcome{terminal: true}
}

// finishOrdinarySyncOutcome publishes logical-task completion only after all
// user-visible callbacks have finished. The defer also preserves the completion
// fence when an observer or fatal handler panics.
func (wb *writeBufferBase) finishOrdinarySyncOutcome(state *ordinarySyncState, task syncmgr.Task, outcome ordinarySyncOutcome) error {
	if outcome.terminal {
		defer state.complete(outcome.terminalErr)
	}
	var firstPanic any
	run := func(callback func()) {
		defer func() {
			if panicValue := recover(); panicValue != nil && firstPanic == nil {
				firstPanic = panicValue
			}
		}()
		callback()
	}
	if wb.taskObserverCallback != nil {
		run(func() {
			wb.taskObserverCallback(task, outcome.attemptErr)
		})
	}
	if outcome.fatalErr != nil {
		run(func() {
			wb.errHandler(outcome.fatalErr)
		})
	}
	if firstPanic != nil {
		panic(firstPanic)
	}
	return outcome.attemptErr
}
