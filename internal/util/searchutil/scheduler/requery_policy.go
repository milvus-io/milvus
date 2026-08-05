package scheduler

import (
	"fmt"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const requeryPriorityBaseCredit = 3

// laneClassifier lets the scheduler classify a task once before admission.
// The result is stamped on queuedTask and remains immutable afterwards.
type laneClassifier interface {
	UseLane(task Task) bool
}

var (
	_ schedulePolicy = (*requeryPriorityPolicy)(nil)
	_ laneClassifier = (*requeryPriorityPolicy)(nil)
)

// requeryPriorityPolicy adds a bounded priority lane to an existing policy.
// It is owned by the scheduler goroutine, so lane and credit need no locks.
type requeryPriorityPolicy struct {
	inner schedulePolicy
	lane  *mergeTaskQueue
	// requeryCredit is the number of lane tasks that may be selected before
	// another live regular task is required. A live regular Pop refreshes it to
	// max(base credit, task.originalRequestCount).
	requeryCredit int
}

func newRequeryPriorityPolicy(inner schedulePolicy) *requeryPriorityPolicy {
	return &requeryPriorityPolicy{
		inner:         inner,
		lane:          newMergeTaskQueue("requery"),
		requeryCredit: requeryPriorityBaseCredit,
	}
}

func (p *requeryPriorityPolicy) UseLane(task Task) bool {
	return task != nil &&
		contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel &&
		requeryLaneCapacity() > 0
}

func (p *requeryPriorityPolicy) Cleanup(now time.Time) []*queuedTask {
	removed := p.lane.cleanup(now)
	return append(removed, p.inner.Cleanup(now)...)
}

func (p *requeryPriorityPolicy) Remove(filter TaskFilter, now time.Time) []*queuedTask {
	removed := p.lane.remove(filter, now)
	return append(removed, p.inner.Remove(filter, now)...)
}

func (p *requeryPriorityPolicy) Push(task *queuedTask) (int, error) {
	if !task.requery {
		return p.inner.Push(task)
	}

	// The scheduler owns capacity admission because only it can account for
	// both the physical lane and a scheduler-local staged requery task.
	p.lane.push(task)
	return 1, nil
}

func (p *requeryPriorityPolicy) Pop(now time.Time) *queuedTask {
	if p.requeryCredit > 0 {
		if task := p.lane.pop(); task.valid() {
			p.requeryCredit--
			return task
		}
	}

	if task := p.inner.Pop(now); task.valid() {
		// Credit is a backlog-aware weighted burst, not a reservation for a
		// specific parent request. A live regular task refreshes the current
		// window, while a task already canceled at Pop does not count as regular
		// service.
		if task.Context().Err() == nil {
			p.requeryCredit = max(requeryPriorityBaseCredit, task.originalRequestCount)
		}
		return task
	}

	// Do not return nil while the lane still has work. This also lets an
	// existing lane drain after the feature is disabled dynamically. Count the
	// fallback task as the first task in a new base-credit window so a regular
	// task arriving while it is waiting for handoff still observes the bound.
	task := p.lane.pop()
	if task.valid() {
		p.requeryCredit = requeryPriorityBaseCredit - 1
		return task
	}
	p.requeryCredit = requeryPriorityBaseCredit
	return nil
}

func (p *requeryPriorityPolicy) Len() int {
	return p.lane.len() + p.inner.Len()
}

func requeryLaneCapacityError(capacity int64) error {
	return merr.WrapErrTooManyRequests(
		int32(capacity),
		fmt.Sprintf("limit by %s", paramtable.Get().QueryNodeCfg.RequeryUnsolvedQueueSize.Key),
	)
}

// requeryLaneCapacity uses ParamItem's typed cache, so steady-state scheduler
// reads do not repeat string normalization or parsing.
func requeryLaneCapacity() int64 {
	return paramtable.Get().QueryNodeCfg.RequeryUnsolvedQueueSize.GetAsInt64()
}
