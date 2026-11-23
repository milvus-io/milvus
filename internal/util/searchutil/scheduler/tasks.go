package scheduler

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	schedulePolicyNameFIFO            = "fifo"
	schedulePolicyNameUserTaskPolling = "user-task-polling"
)

// NewScheduler create a scheduler by policyName.
func NewScheduler(policyName string) Scheduler {
	switch policyName {
	case "":
		fallthrough
	case schedulePolicyNameFIFO:
		return newScheduler(
			newFIFOPolicy(),
		)
	case schedulePolicyNameUserTaskPolling:
		return newScheduler(
			newUserTaskPollingPolicy(),
		)
	default:
		panic("invalid schedule task policy")
	}
}

// tryIntoMergeTask convert inner task into MergeTask,
// Return nil if inner task is not a MergeTask.
func tryIntoMergeTask(t Task) MergeTask {
	if mt, ok := t.(MergeTask); ok {
		return mt
	}
	return nil
}

type Scheduler interface {
	// Add a new task into scheduler, follow some constraints.
	// 1. Error will be returned if scheduler reaches some limit.
	// 2. Error will be returned if task context is canceled while waiting to be accepted.
	// 3. Concurrent safe.
	Add(task Task) error

	// ClearQueued removes queued tasks matched by filter and notifies waiters.
	ClearQueued(ctx context.Context, filter TaskFilter, reason string) (ClearResult, error)

	// Start schedule the owned task asynchronously and continuously.
	// Shall be called only once
	Start()

	// Stop make scheduler deny all incoming tasks
	// and cleans up all related resources
	Stop()

	// GetWaitingTaskTotalNQ
	GetWaitingTaskTotalNQ() int64

	// GetWaitingTaskTotal
	GetWaitingTaskTotal() int64
}

type TaskFilter func(Task) bool

type ClearResult struct {
	QueuedCleared   int64
	QueuedNQCleared int64
}

// schedulePolicy is the policy of scheduler.
type schedulePolicy interface {
	// Cleanup removes queued tasks whose context deadline has been reached.
	// Removed tasks are returned to scheduler for error notification.
	Cleanup(now time.Time) []*queuedTask

	// Remove removes queued tasks matched by filter.
	Remove(filter TaskFilter, now time.Time) []*queuedTask

	// Push add a new task into scheduler.
	// Return the count of new task added (task may be chunked or merged)
	// 0 and an error will be returned if scheduler reaches some limit.
	Push(task *queuedTask) (int, error)

	// Pop get the task next ready to run.
	Pop(now time.Time) *queuedTask

	Len() int
}

type queuedTask struct {
	Task

	enqueueTime time.Time
	// accountedNQ is the NQ this task carried when it was popped, before
	// PruneCanceled may have shrunk it. The waiting counters were credited
	// with that value at push time, so they must be debited with the same
	// value regardless of how many members survive.
	accountedNQ int64
	// maxWait is how long the task may wait in the queue before the node
	// counts as overloaded for it. Zero means no limit.
	maxWait time.Duration
}

// countedNQ returns the NQ to debit from the waiting counters for this task.
func (t *queuedTask) countedNQ() int64 {
	if !t.valid() {
		return 0
	}
	if t.accountedNQ > 0 {
		return t.accountedNQ
	}
	return t.NQ()
}

func newQueuedTask(task Task, enqueueTime time.Time) *queuedTask {
	return &queuedTask{
		Task:        task,
		enqueueTime: enqueueTime,
		maxWait:     maxQueueWait(task, enqueueTime),
	}
}

// maxQueueWait returns how long task may wait in the queue: maxQueueWaitRatio
// of the time it has left when it is queued, at most maxQueueWait. A task
// without a deadline gets maxQueueWait. Zero means no limit, which is the case
// when maxQueueWaitRatio is not positive.
func maxQueueWait(task Task, now time.Time) time.Duration {
	cfg := &paramtable.Get().QueryNodeCfg
	ratio := cfg.MaxQueueWaitRatio.GetAsFloat()
	if ratio <= 0 {
		return 0
	}
	limit := cfg.MaxQueueWait.GetAsDurationByParse()
	if deadline, ok := task.Context().Deadline(); ok {
		// A task about to expire still gets a positive limit: zero would mean
		// no limit. One that has expired is not queued, see
		// handleAddTaskRequest.
		byDeadline := max(time.Duration(float64(deadline.Sub(now))*ratio), time.Nanosecond)
		if limit <= 0 || byDeadline < limit {
			limit = byDeadline
		}
	}
	return max(limit, 0)
}

// queueWaitAdmitShare is the share of its queue wait limit that the task at
// the head of the queue may have waited for a new task to be let in behind
// it. With the whole limit the head would wait for about the limit all the
// time, and nearly every new task would be let in only to be refused when it
// leaves the queue, having waited for nothing.
const queueWaitAdmitShare = 0.5

// mustWaitTooLong reports whether a task queued at now behind head, the task
// that leaves the queue next, would wait for longer than it may. It is judged
// by how long head has waited so far: a measured time, where an estimate from
// the queue length and the pace of the workers would multiply the errors of
// both.
func (t *queuedTask) mustWaitTooLong(head *queuedTask, now time.Time) bool {
	return t.valid() && t.maxWait > 0 && head.valid() &&
		head.queueDuration(now) > time.Duration(float64(t.maxWait)*queueWaitAdmitShare)
}

// overloaded reports whether the task has waited in the queue for longer than
// it may.
func (t *queuedTask) overloaded(now time.Time) bool {
	return t.valid() && t.maxWait > 0 && t.queueDuration(now) > t.maxWait
}

func (t *queuedTask) queueDuration(now time.Time) time.Duration {
	if !t.valid() || t.enqueueTime.IsZero() {
		return 0
	}
	return now.Sub(t.enqueueTime)
}

func (t *queuedTask) valid() bool {
	return t != nil && t.Task != nil
}

func (t *queuedTask) cleanupReady(now time.Time) bool {
	if !t.valid() {
		return false
	}
	// A task standing for a merged group answers for all of it. Its own
	// context belongs to the owner alone, so reading that would end the whole
	// group as soon as the owner was canceled or near its deadline.
	if g, ok := t.Task.(ExpirableGroup); ok {
		return g.ExpiryReady(now)
	}
	if t.Context().Err() != nil {
		return true
	}
	deadline, ok := t.Context().Deadline()
	return ok && !now.Before(deadline)
}

func cleanupTaskError(task *queuedTask) error {
	if err := task.Context().Err(); err != nil {
		return err
	}
	return context.DeadlineExceeded
}

// MergeTask is a Task which can be merged with other task
type MergeTask interface {
	Task

	// MergeWith other task, return true if merge success.
	// After success, the task merged should be dropped.
	MergeWith(Task) bool

	// MinNQ returns the minimum NQ among the original tasks in this merged task.
	MinNQ() int64
}

// PrunableTask is a Task that may stand for a group of merged requests and can
// drop the members whose context is already canceled before the group
// executes. It keeps one member's cancellation from ending the others.
type PrunableTask interface {
	Task

	// PruneCanceled completes every member whose context is canceled with
	// that member's own context error and returns the task to execute for the
	// remaining members: the receiver when nothing was pruned, nil when no
	// member remains. It also reports how many members it dropped and the
	// context error of the first of them, so that the caller can say why.
	PruneCanceled() (survivor Task, dropped int, cause error)
}

// ExpirableGroup is a Task that may stand for a group of merged requests and
// answers the queue's expiry sweep for the whole group. The sweep runs when the
// queue is full and takes out the tasks that are done; for a group that must
// mean every request in it, not only the owner whose context the queue holds.
type ExpirableGroup interface {
	Task

	// ExpiryReady reports whether every request in the group is canceled or
	// has a deadline no later than cleanupTime.
	ExpiryReady(cleanupTime time.Time) bool

	// FinishExpired completes every request in the group with its own
	// reason: its context error if it has one, otherwise
	// context.DeadlineExceeded, for a request the sweep takes out shortly
	// before its deadline.
	FinishExpired()
}

// UnrunTask is a Task that can be completed without having run. Done is how
// the executor finishes a task and records what it ran; a task the queue gives
// up on, cleared by an administrator for instance, never reached the executor
// and must not be counted as if it had.
type UnrunTask interface {
	Task

	// FinishUnrun completes every request the task stands for with that
	// request's own context error if it has one, otherwise with err.
	FinishUnrun(err error)
}

// pruneCanceled drops a task whose context is canceled, or the canceled
// members of a prunable group. It returns the task to execute, or nil when
// there is nothing left to run, together with how many requests it dropped and
// why the first of them was dropped. A dropped task has been completed with its
// own context error.
func pruneCanceled(t Task) (Task, int, error) {
	if p, ok := t.(PrunableTask); ok {
		return p.PruneCanceled()
	}
	if err := t.Context().Err(); err != nil {
		t.Done(err)
		return nil, 1, err
	}
	return t, 0, nil
}

// A task is execute unit of scheduler.
type Task interface {
	Context() context.Context

	// Return the username which task is belong to.
	// Return "" if the task do not contain any user info.
	Username() string

	// Return whether the task would be running on GPU.
	IsGpuIndex() bool

	// PreExecute the task, only call once.
	PreExecute() error

	// Execute the task, only call once.
	Execute() error

	// Done notify the task finished.
	Done(err error)

	// Wait for task finish.
	// Concurrent safe.
	Wait() error

	// Return the NQ of task.
	NQ() int64

	SearchResult() *internalpb.SearchResults
}
