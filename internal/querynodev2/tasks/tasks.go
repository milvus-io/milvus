package tasks

import (
	"context"
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
	// 1. It's a non-block operation.
	// 2. Error will be returned if scheduler reaches some limit.
	// 3. Concurrent safe.
	Add(task Task) error

	// Start schedule the owned task asynchronously and continuously.
	// 1. Stop processing until ctx.Cancel() is called.
	// 2. Only call once.
	Start(ctx context.Context)

	// GetWaitingTaskTotalNQ
	GetWaitingTaskTotalNQ() int64

	// GetWaitingTaskTotal
	GetWaitingTaskTotal() int64
}

// schedulePolicy is the policy of scheduler.
type schedulePolicy interface {
	// Push add a new task into scheduler.
	// Return the count of new task added (task may be chunked, merged or dropped)
	// 0 and an error will be returned if scheduler reaches some limit.
	Push(task Task) (int, error)

	// Pop get the task next ready to run.
	Pop() Task

	Len() int
}

// MergeTask is a Task which can be merged with other task
type MergeTask interface {
	Task

	// MergeWith other task, return true if merge success.
	// After success, the task merged should be dropped.
	MergeWith(Task) bool
}

// A task is execute unit of scheduler.
type Task interface {
	// Return the username which task is belong to.
	// Return "" if the task do not contain any user info.
	Username() string

	// PreExecute the task, only call once.
	PreExecute() error

	// Execute the task, only call once.
	Execute() error

	// Done notify the task finished.
	Done(err error)

	// Check if the Task is canceled.
	// Concurrent safe.
	Canceled() error

	// Wait for task finish.
	// Concurrent safe.
	Wait() error

	// Return the NQ of task.
	NQ() int64
}
