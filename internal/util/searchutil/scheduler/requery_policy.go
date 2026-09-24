package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// taskServedObserver is probed by the scheduler to report a task that actually
// left scheduler ownership through execChan handoff. Tasks dropped by
// expiration or clear are never reported.
type taskServedObserver interface {
	onTaskServed(task *queuedTask)
}

// taskFinishedObserver is probed by the scheduler for terminal task results.
// Implementations must be safe for concurrent calls from execution workers.
type taskFinishedObserver interface {
	onTaskFinished(task Task, err error)
}

const (
	requeryFailureWindow = 10 * time.Second

	requeryCreditLevel1 = 1
	requeryCreditLevel2 = 2
	requeryCreditLevel3 = 3

	requeryPromoteLevel1 = 0.20
	requeryPromoteLevel2 = 0.30
	requeryPromoteLevel3 = 0.40

	requeryDemoteLevel0 = 0.15
	requeryDemoteLevel1 = 0.25
	requeryDemoteLevel2 = 0.35
)

var (
	_ schedulePolicy       = (*requeryPriorityPolicy)(nil)
	_ taskServedObserver   = (*requeryPriorityPolicy)(nil)
	_ taskFinishedObserver = (*requeryPriorityPolicy)(nil)
)

// requeryPriorityPolicy enables a bounded priority lane only when recent local
// requery timeouts justify it. Lane and credit state are owned by the scheduler
// goroutine; execution workers only update atomic outcome counters.
type requeryPriorityPolicy struct {
	inner schedulePolicy
	lane  *mergeTaskQueue

	windowStart    time.Time
	requerySuccess atomic.Uint64
	requeryTimeout atomic.Uint64
	priorityCredit int

	// requeryCredit is the number of lane tasks that may be selected before
	// another live regular task is required. A successful regular execChan
	// handoff refreshes it to the active adaptive credit.
	requeryCredit int
}

func newRequeryPriorityPolicy(inner schedulePolicy) *requeryPriorityPolicy {
	return &requeryPriorityPolicy{
		inner:       inner,
		lane:        newMergeTaskQueue("requery"),
		windowStart: time.Now(),
	}
}

func (p *requeryPriorityPolicy) Classify(task Task) taskClass {
	p.refreshPriority(time.Now())
	if p.priorityCredit > 0 && isRequeryTask(task) &&
		requeryLaneCapacity() > 0 {
		return taskClassPriorityLane
	}
	return p.inner.Classify(task)
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
	if task.class != taskClassPriorityLane {
		return p.inner.Push(task)
	}

	// The scheduler owns capacity admission because only it can account for
	// both the physical lane and a scheduler-local staged requery task.
	p.lane.push(task)
	return 1, nil
}

func (p *requeryPriorityPolicy) Pop(now time.Time) *queuedTask {
	p.refreshPriority(now)
	if p.requeryCredit > 0 {
		if task := p.lane.pop(); task.valid() {
			p.requeryCredit--
			return task
		}
	}

	// Credit is refreshed in onTaskServed, not here: a popped regular task can
	// still be dropped by expiration cleanup or clear while staged for handoff,
	// and such a task must not open a new lane window.
	if task := p.inner.Pop(now); task.valid() {
		return task
	}

	// Do not return nil while the lane still has work. This also lets an
	// existing lane drain after the feature is disabled dynamically. Count the
	// fallback task as the first task in a new active-credit window so a regular
	// task arriving while it is waiting for handoff still observes the bound.
	burstCredit := p.burstCredit()
	task := p.lane.pop()
	p.requeryCredit = burstCredit
	if task.valid() {
		p.requeryCredit--
	}
	return task
}

func (p *requeryPriorityPolicy) Len() int {
	return p.lane.len() + p.inner.Len()
}

// onTaskServed refreshes the requery credit window only when a regular task is
// actually handed to execution.
func (p *requeryPriorityPolicy) onTaskServed(task *queuedTask) {
	if task.class == taskClassPriorityLane {
		return
	}
	p.requeryCredit = p.burstCredit()
}

// onTaskFinished records only outcomes useful to priority adaptation. Explicit
// cancellation and non-timeout errors are excluded from both numerator and
// denominator because reordering cannot reliably improve them.
func (p *requeryPriorityPolicy) onTaskFinished(task Task, err error) {
	if !isRequeryTask(task) {
		return
	}
	ctxErr := task.Context().Err()
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctxErr, context.DeadlineExceeded) {
		p.requeryTimeout.Inc()
		return
	}
	if err == nil && ctxErr == nil {
		p.requerySuccess.Inc()
	}
}

// refreshPriority maps the last complete observation window to one adaptive
// credit step. Separate promotion and demotion thresholds provide hysteresis.
func (p *requeryPriorityPolicy) refreshPriority(now time.Time) {
	if now.Sub(p.windowStart) < requeryFailureWindow {
		return
	}

	timeouts := p.requeryTimeout.Swap(0)
	successes := p.requerySuccess.Swap(0)
	p.windowStart = now
	if timeouts+successes == 0 {
		return
	}

	timeoutRate := float64(timeouts) / float64(timeouts+successes)
	oldCredit := p.priorityCredit
	switch p.priorityCredit {
	case 0:
		if timeoutRate >= requeryPromoteLevel1 {
			p.priorityCredit = requeryCreditLevel1
		}
	case requeryCreditLevel1:
		if timeoutRate >= requeryPromoteLevel2 {
			p.priorityCredit = requeryCreditLevel2
		} else if timeoutRate < requeryDemoteLevel0 {
			p.priorityCredit = 0
		}
	case requeryCreditLevel2:
		if timeoutRate >= requeryPromoteLevel3 {
			p.priorityCredit = requeryCreditLevel3
		} else if timeoutRate < requeryDemoteLevel1 {
			p.priorityCredit = requeryCreditLevel1
		}
	case requeryCreditLevel3:
		if timeoutRate < requeryDemoteLevel2 {
			p.priorityCredit = requeryCreditLevel2
		}
	}

	maxCredit := min(requeryCreditLevel3, requeryPriorityBaseCredit())
	p.priorityCredit = min(p.priorityCredit, maxCredit)
	if p.priorityCredit != oldCredit {
		p.requeryCredit = p.burstCredit()
	}
}

// burstCredit keeps one lane slot while draining after priority is disabled;
// Classify already routes all new requery tasks back to the regular FIFO.
func (p *requeryPriorityPolicy) burstCredit() int {
	if p.priorityCredit > 0 {
		return p.priorityCredit
	}
	if p.lane.len() > 0 {
		return requeryCreditLevel1
	}
	return 0
}

func isRequeryTask(task Task) bool {
	return task != nil && contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel
}

func requeryPriorityBaseCredit() int {
	return paramtable.Get().QueryNodeCfg.RequeryPriorityBaseCredit.GetAsInt()
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
