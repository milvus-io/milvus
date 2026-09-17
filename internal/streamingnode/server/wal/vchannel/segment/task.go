package segment

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// retryLogRate throttles the retry log of a task. The limiter is shared
// process-wide per call site (pkg/mlog keys on the caller's PC), so this is
// the maximum rate for the whole streamingnode, not per task — the first
// failure of any given task is not guaranteed to be logged; the _suppressed
// field reports how many lines were dropped. 1/s keeps a busy process at one
// line per second without flooding.
const retryLogRate = rate.Limit(1)

type segmentTask interface {
	nodescheduler.Task
	Done() bool
	Submitted() bool
	markSubmitted()
}

type segmentTaskBase struct {
	segment   *SegmentView
	taskName  string
	done      atomic.Bool
	submitted atomic.Bool
}

func (t *segmentTaskBase) Done() bool {
	return t.done.Load()
}

func (t *segmentTaskBase) Submitted() bool {
	return t.submitted.Load()
}

func (t *segmentTaskBase) markSubmitted() {
	t.submitted.Store(true)
}

// markRetryable wraps a task error so the node scheduler requeues it after a
// backoff instead of finishing the task. It is used for RPC failures whose
// retryability is only known at the call site, and as the fallback for errors
// that are not explicitly marked unrecoverable.
func (t *segmentTaskBase) markRetryable(err error) error {
	return errors.Mark(err, nodescheduler.ErrDelay)
}

// execute runs fn once the task is actually submitted. Segment tasks are
// serialized by the view: only the head of the pending queue is submitted, and
// finishWith pops it and submits the next one, so no predecessor bookkeeping is
// needed. Only errors explicitly marked unrecoverable fail the segment;
// everything else is retried.
func (t *segmentTaskBase) execute(ctx context.Context, fn func(context.Context) error) error {
	if ferr := t.segment.unrecoverableErr(); ferr != nil {
		return t.finishWith(ferr)
	}
	err := fn(ctx)
	if err == nil {
		t.done.Store(true)
		return t.finishWith(nil)
	}
	if !retry.IsRecoverable(err) {
		// Explicitly marked unrecoverable (input/invariant error or permanent
		// coordinator rejection): fail the segment so later tasks short-circuit
		// instead of retrying a failure that can never succeed.
		t.segment.markUnrecoverable(ctx, err)
		return t.finishWith(err)
	}
	// Retryable failure: stay submitted, the scheduler requeues after a
	// backoff. execute is the single place that marks ErrDelay; producers
	// (lifecycle, pack writer) return raw errors. Rate-limit the retry log so
	// a permanently failing task stays observable without spamming.
	err = t.markRetryable(err)
	mlog.RatedWarn(ctx, retryLogRate, "segment task failed, will retry",
		mlog.String("taskType", t.taskName),
		mlog.Int64("segmentID", t.segment.segmentID),
		mlog.String("vchannel", t.segment.vchannel),
		mlog.Err(err))
	return err
}

// finishWith pops the task from the segment's pending queue and submits the
// next queued task, keeping execution strictly serial. It is called on both
// success and terminal failure. The task being finished is always the queue
// head, because tasks are submitted strictly one at a time.
func (t *segmentTaskBase) finishWith(err error) error {
	t.segment.finishTask()
	return err
}

type ensureGrowingSegmentTask struct {
	segmentTaskBase
	timetick uint64
}

func (t *ensureGrowingSegmentTask) Execute(ctx context.Context) error {
	return t.execute(ctx, func(ctx context.Context) error {
		segment := t.segment
		meta := segment.AssignmentMeta()
		if err := segment.lifecycle.EnsureGrowingSegment(ctx, meta); err != nil {
			// Classification is execute's job: it marks retryable errors with
			// ErrDelay and fails the segment on unrecoverable ones. Marking
			// ErrDelay here would also tag unrecoverable errors, and the
			// scheduler would requeue an already-finished task.
			return err
		}

		segment.mu.Lock()
		handles := segment.markPendingDataDurableLocked(t.timetick)
		segment.mu.Unlock()
		segment.NotifyDataUpdated()
		releaseMessages(handles)
		return nil
	})
}

type flushL1BufferTask struct {
	segmentTaskBase
	timetick uint64
}

func (t *flushL1BufferTask) Execute(ctx context.Context) error {
	return t.execute(ctx, func(ctx context.Context) error {
		return t.segment.FlushInsertChunk(ctx, t.timetick)
	})
}

type commitL1SegmentTask struct {
	segmentTaskBase
	timetick      uint64
	flushTimeTick uint64
}

func (t *commitL1SegmentTask) Execute(ctx context.Context) error {
	return t.execute(ctx, func(ctx context.Context) error {
		segment := t.segment
		segment.mu.Lock()
		finalCommitDone := segment.finalCommitDone.Load()
		segment.mu.Unlock()
		if finalCommitDone {
			return nil
		}
		if err := segment.FlushInsertChunk(ctx, t.flushTimeTick); err != nil {
			return err
		}
		meta := segment.AssignmentMeta()
		if err := segment.lifecycle.CommitL1Segment(ctx, meta); err != nil {
			// Same as ensure: classification is execute's job, marking ErrDelay
			// here would requeue a terminally-failed task.
			return err
		}

		segment.mu.Lock()
		handles := segment.markPendingDataDurableLocked(t.timetick)
		segment.finalCommitDone.Store(true)
		segment.meta.L1CommitDone = true
		segment.durableMeta.State = segment.meta.State
		segment.durableMeta.L1CommitDone = true
		if stat := segment.meta.GetStat(); stat != nil && segment.durableMeta.GetStat() != nil {
			segment.durableMeta.Stat.LastModifiedTimestamp = stat.GetLastModifiedTimestamp()
		}
		segment.dirty = true
		// After the final commit no flush task can ever cover the buffer again.
		// A non-empty buffer here means out-of-timetick-order inserts slipped
		// past the replay guards: their rows can never be persisted anymore. Do
		// not plain-release them (that would make the loss invisible and
		// overturn the "release = durable" contract); poison them instead, so a
		// consumer can enumerate and handle them separately, then warn loudly.
		var abandoned []message.RetainedImmutableMessage
		if len(segment.pending.entries) > 0 {
			mlog.Warn(ctx, "final commit leaves pending insert data unpersisted",
				mlog.Int64("segmentID", segment.segmentID),
				mlog.String("vchannel", segment.vchannel),
				mlog.Int("pendingEntries", len(segment.pending.entries)),
				mlog.Uint64("pendingFromTimeTick", segment.pending.fromTimeTick),
				mlog.Uint64("commitTimeTick", t.timetick))
			abandoned = segment.pending.entries
			segment.pending.reset()
		}
		segment.mu.Unlock()
		segment.NotifyDataUpdated()
		releaseMessages(handles)
		for _, handle := range abandoned {
			handle.PoisonedRelease()
		}
		return nil
	})
}

func (s *SegmentView) newEnsureGrowingSegmentTaskLocked(timetick uint64) segmentTask {
	task := &ensureGrowingSegmentTask{
		segmentTaskBase: s.newSegmentTaskBaseLocked("ensureGrowingSegmentTask"),
		timetick:        timetick,
	}
	s.pendingTasks = append(s.pendingTasks, task)
	return task
}

func (s *SegmentView) newFlushL1BufferTaskLocked() segmentTask {
	task := &flushL1BufferTask{
		segmentTaskBase: s.newSegmentTaskBaseLocked("flushL1BufferTask"),
		timetick:        s.enqueuePendingFlushChunkLocked(),
	}
	s.pendingTasks = append(s.pendingTasks, task)
	return task
}

func (s *SegmentView) newCommitL1SegmentTaskLocked(timetick uint64) segmentTask {
	if !s.canScheduleFinalCommitLocked() {
		return nil
	}
	return s.newCommitL1SegmentTaskWithFlushTimeTickLocked(timetick, s.enqueuePendingFlushChunkLocked())
}

func (s *SegmentView) newRecoveredCommitL1SegmentTaskLocked(timetick uint64) segmentTask {
	if !s.canScheduleFinalCommitLocked() {
		return nil
	}
	return s.newCommitL1SegmentTaskWithFlushTimeTickLocked(timetick, 0)
}

func (s *SegmentView) canScheduleFinalCommitLocked() bool {
	return !s.finalCommitDone.Load() &&
		s.unrecoverableErr() == nil &&
		(s.pendingFinalCommit == nil || s.pendingFinalCommit.Done())
}

func (s *SegmentView) newCommitL1SegmentTaskWithFlushTimeTickLocked(timetick, flushTimeTick uint64) segmentTask {
	task := &commitL1SegmentTask{
		segmentTaskBase: s.newSegmentTaskBaseLocked("commitL1SegmentTask"),
		timetick:        timetick,
		flushTimeTick:   flushTimeTick,
	}
	s.pendingFinalCommit = task
	s.pendingTasks = append(s.pendingTasks, task)
	return task
}

func (s *SegmentView) newSegmentTaskBaseLocked(taskName string) segmentTaskBase {
	return segmentTaskBase{
		segment:  s,
		taskName: taskName,
	}
}

// maybeSubmitNextLocked submits the head of the pending queue if it is not
// already submitted. Only the head is ever submitted: a segment executes its
// tasks strictly one at a time, in creation order.
//
// Precondition: s.mu is held. The scheduler must be non-nil — ViewConfig
// callers must provide a runtime with a scheduler (see ViewConfig.Runtime);
// a nil scheduler is a caller bug that panics here at the first submission.
// An empty queue is a normal state after the last task finished.
func (s *SegmentView) maybeSubmitNextLocked() {
	if len(s.pendingTasks) == 0 {
		return
	}
	head := s.pendingTasks[0]
	if head.Submitted() {
		return
	}
	head.markSubmitted()
	s.runtime.Scheduler.Submit(head)
}

// finishTask pops the finished (success or terminal failure) task from the
// head of the pending queue and submits the next one, keeping execution
// strictly serial: a task is submitted only after the previous one finished,
// so a failure cannot strand later tasks behind a never-done predecessor.
// The finished task is always the queue head, because tasks are submitted
// strictly one at a time. Callers must not hold s.mu: finishTask takes it.
func (s *SegmentView) finishTask() {
	s.mu.Lock()
	defer s.mu.Unlock()
	finished := s.pendingTasks[0]
	// Nil the popped slot so the backing array does not keep the finished task
	// (and, through its segment back-pointer, the view) reachable.
	s.pendingTasks[0] = nil
	s.pendingTasks = s.pendingTasks[1:]
	// Drop the pending final-commit reference when the finished task is that
	// commit, so it is not kept reachable after the pop either.
	if s.pendingFinalCommit == finished {
		s.pendingFinalCommit = nil
	}
	s.maybeSubmitNextLocked()
}

// markUnrecoverable records a terminal unrecoverable task error on the segment. All subsequently
// created tasks fail fast with the same error instead of executing, and observations are poisoned
// instead of buffered (see ObserveInsert / ObserveCreateSegmentMessageV2 / Flush).
//
// Every retained message in the three pending structures is poisoned and released, so a failed
// segment pins nothing in memory and leaves no message silently dropped: each poisoned message
// carries a marker a consumer can observe and handle separately (reassign / replay), instead of
// the failure becoming invisible. The poison is message-level and survives in the shared core, so
// any handle to the same message can observe it. Reclaiming the view itself is the concern of the
// future owner that wires this package into the vchannel module; until then the view stays, which
// is the intended fail-safe. The failure is logged loudly for upper-layer accounting.
func (s *SegmentView) markUnrecoverable(ctx context.Context, err error) {
	// Record the terminal error and snapshot the retained handles under the
	// same lock, so the logged count is an exact snapshot of the terminal
	// state: no observer can be appending to the pending structures while the
	// Store is in flight, because both happen inside s.mu.
	s.mu.Lock()
	// reentrant records whether the segment was already unrecoverable before
	// this sweep: a second markUnrecoverable (today prevented by the
	// serial-task invariant plus the fail-fast check, but kept structural by
	// the empty-structures no-op) collects nothing and re-reports the same
	// terminal state, so the log must not read as a fresh sweep. Read before
	// Store: the stored error is this very sweep, not a prior one.
	reentrant := s.unrecoverableErr() != nil
	s.unrecoverableError.Store(&err)
	// Poison every retained message: mark it unrecoverable and release the
	// handle so nothing is pinned in memory, yet nothing is silently dropped.
	poisoned := s.collectAllPendingLocked()
	s.mu.Unlock()
	for _, handle := range poisoned {
		handle.PoisonedRelease()
	}
	mlog.Error(ctx, "segment failed unrecoverably, poisoned all pending messages",
		mlog.Int64("segmentID", s.segmentID),
		mlog.String("vchannel", s.vchannel),
		mlog.Int("poisonedHandles", len(poisoned)),
		mlog.Bool("reentrant", reentrant),
		mlog.Err(err))
}

// unrecoverableErr returns the segment's terminal unrecoverable task error, or nil if the segment has
// not been marked unrecoverable.
func (s *SegmentView) unrecoverableErr() error {
	if v := s.unrecoverableError.Load(); v != nil {
		return *v
	}
	return nil
}
