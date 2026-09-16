package scheduler

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
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
	// PruneCancelled may have shrunk it. The waiting counters were credited
	// with that value at push time, so they must be debited with the same
	// value regardless of how many members survive.
	accountedNQ int64
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
	}
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
// drop the members whose context is already cancelled before the group
// executes. It keeps one member's cancellation from ending the others.
type PrunableTask interface {
	Task

	// PruneCancelled completes every member whose context is cancelled with
	// that member's own context error and returns the task to execute for the
	// remaining members. It returns the receiver when nothing was pruned and
	// nil when no member remains.
	PruneCancelled() Task
}

// pruneCancelled drops a task whose context is cancelled, or the cancelled
// members of a prunable group. It returns the task to execute, or nil when
// there is nothing left to run. A dropped task has been completed with its own
// context error.
func pruneCancelled(t Task) Task {
	if p, ok := t.(PrunableTask); ok {
		return p.PruneCancelled()
	}
	if err := t.Context().Err(); err != nil {
		t.Done(err)
		return nil
	}
	return t
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
