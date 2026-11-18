package scheduler

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

// schedulePolicy is the policy of scheduler.
type schedulePolicy interface {
	// Cleanup removes queued tasks whose context deadline has been reached.
	// Removed tasks are returned to scheduler for error notification.
	Cleanup(now time.Time) []*queuedTask

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
	// maxWait is how long the task may wait in the queue before the node
	// counts as overloaded for it. Zero means no limit.
	maxWait time.Duration
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
