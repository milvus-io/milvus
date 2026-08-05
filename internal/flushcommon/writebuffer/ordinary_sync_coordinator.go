package writebuffer

import (
	"context"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// ordinarySyncEntry is one flush task and the write buffer's ownership of it.
// The entry — and the payload its task holds — survives a failed attempt: the
// task stays at its place in the queue and is submitted again by the retry
// drive. Only success or a terminal failure removes it.
type ordinarySyncEntry struct {
	task            *syncmgr.SyncTask
	payloadReleased chan struct{}
	done            chan struct{}
	terminalErr     error
	submitted       bool
	// lastAttempt is when this entry was last handed to the sync manager. The
	// retry drive uses the OLDEST entry's stamp, so one segment retries as a
	// unit rather than per task.
	lastAttempt time.Time
	failed      bool
	// everFailed stays set across re-drives (failed flips back on submit).
	// It marks the segment as flush-impaired: backpressure then waits on task
	// COMPLETION, not just payload release — a Commit stuck in retry holds no
	// payload, and without this its buffered tail grows with no bound at all.
	everFailed bool
}

type ordinarySyncQueue struct {
	entries []*ordinarySyncEntry
	pending bool
}

// needsRetryLocked reports whether this segment has a failed attempt that is due
// for another one.
func (q *ordinarySyncQueue) needsRetryLocked(now time.Time, interval time.Duration) bool {
	if len(q.entries) == 0 {
		return false
	}
	head := q.entries[0]
	return head.failed && now.Sub(head.lastAttempt) >= interval
}

// driveRetries re-submits segments whose last flush attempt failed. It is
// called from the flowgraph on every timetick, which is why the sync path needs
// no timer of its own: timeticks arrive continuously while a channel is alive,
// and the only place that survives without them — Drop — drives this itself.
//
// Retry always restarts from the OLDEST pending task of the segment. The tasks
// behind it were built against state that task's commit was supposed to
// publish, so they can only be replayed in the same order.
func (wb *writeBufferBase) driveRetries(ctx context.Context) {
	interval := wb.retryInterval()
	if interval < 0 {
		return
	}
	now := time.Now()

	wb.mut.Lock()
	if wb.closed || wb.dropping {
		wb.mut.Unlock()
		return
	}
	due := wb.dueOrdinaryRetriesLocked(now, interval)
	wb.mut.Unlock()

	for _, entry := range due {
		wb.resubmitOrdinarySync(ctx, entry)
	}
	wb.driveGrowingSourceRetries(ctx, now, interval)
}

// dueOrdinaryRetriesLocked collects and re-arms every entry of every segment
// whose oldest failed attempt is due for another one. Shared by the timetick
// drive (driveRetries) and the synchronous drop wait (waitOrdinarySyncs),
// which must drive its own retries because timeticks stop once the flowgraph
// delivers DropChannel.
func (wb *writeBufferBase) dueOrdinaryRetriesLocked(now time.Time, interval time.Duration) []*ordinarySyncEntry {
	var due []*ordinarySyncEntry
	for _, queue := range wb.ordinarySyncQueues {
		if !queue.needsRetryLocked(now, interval) {
			continue
		}
		for _, entry := range queue.entries {
			entry.failed = false
			entry.submitted = false
			entry.lastAttempt = now
			due = append(due, entry)
		}
	}
	return due
}

func (wb *writeBufferBase) registerOrdinarySyncLocked(entry *ordinarySyncEntry) {
	segmentID := entry.task.SegmentID()
	queue := wb.ordinarySyncQueues[segmentID]
	if queue == nil {
		queue = &ordinarySyncQueue{}
		wb.ordinarySyncQueues[segmentID] = queue
	}
	queue.entries = append(queue.entries, entry)
}

func (wb *writeBufferBase) ordinarySyncEntryLocked(task *syncmgr.SyncTask) *ordinarySyncEntry {
	queue := wb.ordinarySyncQueues[task.SegmentID()]
	if queue == nil {
		return nil
	}
	for _, entry := range queue.entries {
		if entry.task == task {
			return entry
		}
	}
	return nil
}

func (wb *writeBufferBase) removeOrdinarySyncLocked(entry *ordinarySyncEntry) {
	segmentID := entry.task.SegmentID()
	queue := wb.ordinarySyncQueues[segmentID]
	if queue == nil {
		return
	}
	for i, candidate := range queue.entries {
		if candidate == entry {
			queue.entries = append(queue.entries[:i], queue.entries[i+1:]...)
			break
		}
	}
	if len(queue.entries) == 0 {
		delete(wb.ordinarySyncQueues, segmentID)
	}
}

// segmentReorderWindow is how many tasks of one segment may be outstanding at
// once. They prepare in parallel and commit in order; the window is what turns
// that parallelism into a bounded amount of resident payload. It lives here
// because the write buffer is the only owner of the queue — the sync manager
// just executes what it is given.
const segmentReorderWindow = 5

// ordinarySyncBlockedLocked reports whether segmentID may not produce a new sync
// task right now. It is read-only; callers that intend to build a task use
// deferOrdinarySyncLocked instead.
//
// A segment is blocked when it still holds a task the dispatcher has not
// accepted yet (its payload accounting is not settled), when it has filled the
// reorder window, or when it is sealed/flushing/dropped — the final task of a
// segment must be built from committed counters, not from a snapshot that an
// uncommitted predecessor is about to change.
func (wb *writeBufferBase) ordinarySyncBlockedLocked(segmentID int64) bool {
	queue := wb.ordinarySyncQueues[segmentID]
	if queue == nil || len(queue.entries) == 0 {
		return false
	}
	if len(queue.entries) >= segmentReorderWindow {
		return true
	}
	for _, entry := range queue.entries {
		if !entry.submitted {
			return true
		}
	}
	if segment, exists := wb.metaCache.GetSegmentByID(segmentID); exists {
		switch segment.State() {
		case commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing, commonpb.SegmentState_Dropped:
			return true
		}
	}
	return false
}

// deferOrdinarySyncLocked is ordinarySyncBlockedLocked plus the note that this
// segment wants a task as soon as its queue drains.
func (wb *writeBufferBase) deferOrdinarySyncLocked(segmentID int64) bool {
	if !wb.ordinarySyncBlockedLocked(segmentID) {
		return false
	}
	wb.ordinarySyncQueues[segmentID].pending = true
	return true
}

func (wb *writeBufferBase) allOrdinarySyncEntriesLocked() []*ordinarySyncEntry {
	var entries []*ordinarySyncEntry
	for _, queue := range wb.ordinarySyncQueues {
		entries = append(entries, queue.entries...)
	}
	return entries
}

// takeParkedOrdinarySyncsLocked removes and returns every entry that is parked
// waiting for a retry re-drive (failed, not in flight). Once nothing will ever
// drive retries again — Close, or an aborted Drop — these entries would hold
// their payload forever; the caller must hand them to
// abandonParkedOrdinarySyncs outside the lock. In-flight entries are left
// alone: their own completion callback settles them.
func (wb *writeBufferBase) takeParkedOrdinarySyncsLocked() []*ordinarySyncEntry {
	var parked []*ordinarySyncEntry
	for _, queue := range wb.ordinarySyncQueues {
		for _, entry := range queue.entries {
			if entry.failed && !entry.submitted {
				parked = append(parked, entry)
			}
		}
	}
	for _, entry := range parked {
		wb.removeOrdinarySyncLocked(entry)
	}
	return parked
}

// abandonParkedOrdinarySyncs releases everything a parked entry still owns and
// completes it, mirroring the terminal branch of finishOrdinarySync. Runs
// WITHOUT wb.mut held: Abandon releases the payload, whose accounting callback
// may take locks of its own.
func (wb *writeBufferBase) abandonParkedOrdinarySyncs(parked []*ordinarySyncEntry, err error) {
	for _, entry := range parked {
		wb.metaCache.UpdateSegments(
			metacache.DiscardSyncing(entry.task.BatchRows()),
			metacache.WithSegmentIDs(entry.task.SegmentID()),
		)
		entry.task.Abandon()
		entry.complete(err)
	}
}

func (entry *ordinarySyncEntry) complete(err error) {
	entry.terminalErr = err
	close(entry.done)
}

func (wb *writeBufferBase) waitOrdinarySyncs(ctx context.Context, cancel context.CancelFunc, waiters []*ordinarySyncEntry) error {
	if len(waiters) == 0 {
		return ctx.Err()
	}

	results := make(chan error, len(waiters))
	for _, entry := range waiters {
		go func(entry *ordinarySyncEntry) {
			<-entry.done
			results <- entry.terminalErr
		}(entry)
	}

	var firstErr error
	ctxDone := ctx.Done()
	// After cancellation the wait becomes bounded. Cancelling makes whatever is
	// cancellable unwind, but an already-started native write takes no
	// cancellation token, so waiting past the grace only turns the caller's
	// timeout into a hang — and a DropChannel that never returns is strictly
	// worse than one that reports failure and lets WAL replay redo the work.
	var grace <-chan time.Time
	var graceTimer *time.Timer
	defer func() {
		if graceTimer != nil {
			graceTimer.Stop()
		}
	}()
	abort := func(err error) {
		firstErr = err
		if cancel != nil {
			cancel()
		}
		wb.syncCancel()
		ctxDone = nil
		graceTimer = time.NewTimer(growingFlushCancelGrace)
		grace = graceTimer.C
	}
	// This wait runs during Drop, after the flowgraph stopped delivering
	// timeticks, so driveRetries can no longer re-drive a failed attempt.
	// Retryable failures keep their entry pending (finishOrdinarySync leaves
	// them queued while dropping) and this loop re-submits them on the same
	// flushRetryInterval cadence — until the caller's context cancels, which
	// turns every further attempt terminal.
	retryEvery := wb.retryInterval()
	var retryTick <-chan time.Time
	if retryEvery >= 0 {
		tick := retryEvery
		if tick < 100*time.Millisecond {
			tick = 100 * time.Millisecond
		}
		retryTicker := time.NewTicker(tick)
		defer retryTicker.Stop()
		retryTick = retryTicker.C
	}
	for completed := 0; completed < len(waiters); {
		select {
		case err := <-results:
			completed++
			if err != nil && firstErr == nil {
				abort(err)
			}
		case <-ctxDone:
			if firstErr == nil {
				abort(ctx.Err())
			}
		case <-grace:
			wb.logger.Warn(wb.syncCtx, "giving up the wait for cancelled sync tasks; "+
				"they cannot be preempted and are left to finish in the background",
				mlog.Err(firstErr))
			return firstErr
		case now := <-retryTick:
			if firstErr != nil {
				continue
			}
			wb.mut.Lock()
			due := wb.dueOrdinaryRetriesLocked(now, retryEvery)
			wb.mut.Unlock()
			for _, entry := range due {
				wb.resubmitOrdinarySync(ctx, entry)
			}
		}
	}
	return firstErr
}

// finishOrdinarySync owns the write-buffer side of task completion. The sync
// manager already retries Prepare and Commit in place, so any error reaching
// this callback terminates the segment stream and leaves its checkpoint pinned
// for WAL replay.
func (wb *writeBufferBase) finishOrdinarySync(ctx context.Context, entry *ordinarySyncEntry, task *syncmgr.SyncTask, taskErr error) error {
	segmentID := task.SegmentID()
	var followupTasks []syncmgr.Task
	var fatalErr error

	wb.mut.Lock()
	if wb.ordinarySyncEntryLocked(task) != entry {
		wb.mut.Unlock()
		return taskErr
	}

	if taskErr != nil {
		// Retryable failures stay queued during Drop too: Drop is a synchronous
		// wait, and waitOrdinarySyncs drives these retries itself (timeticks
		// stop once DropChannel is delivered). Only Close makes retrying
		// pointless — nothing will ever drive it again.
		if syncmgr.ClassifySyncError(taskErr) == syncmgr.SyncRetry && !wb.closed {
			// Keep everything: the payload, the metacache syncing counters and
			// the queue position. driveRetries submits this exact task object
			// again, and its Prepare will not rewrite what it already wrote.
			// HandleError is deliberately NOT called — it destroys the prepared
			// storage handle this retry is going to reuse.
			entry.failed = true
			entry.everFailed = true
			entry.submitted = false
			// Stamp the attempt so flushRetryInterval is honoured from the
			// FIRST failure; a zero lastAttempt made the very next timetick
			// retry immediately.
			entry.lastAttempt = time.Now()
			wb.mut.Unlock()
			wb.retryRatedLogger.RatedWarn(ctx, rate.Limit(1), "flush attempt failed, will retry",
				mlog.Int64("segmentID", segmentID), mlog.Err(taskErr))
			return taskErr
		}
		wb.metaCache.UpdateSegments(
			metacache.DiscardSyncing(task.BatchRows()),
			metacache.WithSegmentIDs(segmentID),
		)
		// Terminal: this task will never run again, so release everything it
		// holds — payload and any prepared storage handle.
		task.Abandon()
		wb.removeOrdinarySyncLocked(entry)
		// During Drop the terminal error is not swallowed — it surfaces
		// through entry.terminalErr to waitOrdinarySyncs, whose caller (Close)
		// already escalates a failed Drop. Feeding it to the fatal errHandler
		// as well would just crash the process twice for one failure.
		if !wb.closed && !wb.dropping {
			fatalErr = taskErr
		}
	} else {
		queue := wb.ordinarySyncQueues[segmentID]
		followup := queue != nil && queue.pending && wb.buffers[segmentID] != nil
		if queue != nil {
			queue.pending = false
		}
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
		wb.removeOrdinarySyncLocked(entry)
		if followup && !wb.closed && !wb.dropping {
			followupTasks = wb.getSyncTasksLocked(wb.syncCtx, []int64{segmentID})
		}
	}
	wb.mut.Unlock()

	if len(followupTasks) > 0 {
		go wb.submitSyncTasks(wb.syncCtx, followupTasks)
	}

	defer entry.complete(taskErr)
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
		run(func() { wb.taskObserverCallback(task, taskErr) })
	}
	if fatalErr != nil {
		run(func() { wb.errHandler(fatalErr) })
	}
	if firstPanic != nil {
		panic(firstPanic)
	}
	return taskErr
}
