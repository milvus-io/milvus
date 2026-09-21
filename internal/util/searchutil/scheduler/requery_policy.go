package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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

var (
	_ schedulePolicy     = (*requeryPriorityPolicy)(nil)
	_ taskServedObserver = (*requeryPriorityPolicy)(nil)
)

// requeryPriorityPolicy adds a bounded priority lane to an existing policy.
// It is owned by the scheduler goroutine, so lane and credit need no locks.
type requeryPriorityPolicy struct {
	inner schedulePolicy
	lane  *mergeTaskQueue
	// requeryCredit is the number of lane tasks that may be selected before
	// another live regular task is required. A successful regular execChan
	// handoff refreshes it to max(base credit, task.originalRequestCount).
	requeryCredit int
	logStats      requeryPolicyLogStats
}

type requeryPolicyLogStats struct {
	lastLog                 time.Time
	priorityPops            int
	fallbackPops            int
	regularPops             int
	regularHandoffs         int
	requeryAheadOfRegular   int
	regularAheadOfRequery   int
	creditExhaustedWithBoth int
	mergeBoostedRefreshes   int
	maxEffectiveCredit      int
	minCreditLeft           int
	maxRequeryQueued        int
	maxRegularQueued        int
}

func newRequeryPriorityPolicy(inner schedulePolicy) *requeryPriorityPolicy {
	return &requeryPriorityPolicy{
		inner:         inner,
		lane:          newMergeTaskQueue("requery"),
		requeryCredit: requeryPriorityBaseCredit(),
	}
}

func (p *requeryPriorityPolicy) Classify(task Task) taskClass {
	if task != nil &&
		contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel &&
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
		added, err := p.inner.Push(task)
		p.recordQueueDepths()
		return added, err
	}

	// The scheduler owns capacity admission because only it can account for
	// both the physical lane and a scheduler-local staged requery task.
	p.lane.push(task)
	p.recordQueueDepths()
	return 1, nil
}

func (p *requeryPriorityPolicy) Pop(now time.Time) *queuedTask {
	p.logCreditStats(now)
	regularQueued := p.inner.Len() > 0
	requeryQueued := p.lane.len() > 0
	p.logStats.maxEffectiveCredit = max(p.logStats.maxEffectiveCredit, p.requeryCredit)
	if regularQueued && requeryQueued && p.requeryCredit == 0 {
		p.logStats.creditExhaustedWithBoth++
	}
	if p.requeryCredit > 0 {
		if task := p.lane.pop(); task.valid() {
			p.requeryCredit--
			p.logStats.minCreditLeft = min(p.logStats.minCreditLeft, p.requeryCredit)
			p.logStats.priorityPops++
			if regularQueued {
				p.logStats.requeryAheadOfRegular++
			}
			return task
		}
	}

	// Credit is refreshed in onTaskServed, not here: a popped regular task can
	// still be dropped by expiration cleanup or clear while staged for handoff,
	// and such a task must not open a new lane window.
	if task := p.inner.Pop(now); task.valid() {
		p.logStats.regularPops++
		if requeryQueued {
			p.logStats.regularAheadOfRequery++
		}
		return task
	}

	// Do not return nil while the lane still has work. This also lets an
	// existing lane drain after the feature is disabled dynamically. Count the
	// fallback task as the first task in a new base-credit window so a regular
	// task arriving while it is waiting for handoff still observes the bound.
	task := p.lane.pop()
	if task.valid() {
		p.requeryCredit = requeryPriorityBaseCredit() - 1
		p.logStats.minCreditLeft = min(p.logStats.minCreditLeft, p.requeryCredit)
		p.logStats.fallbackPops++
		return task
	}
	p.requeryCredit = requeryPriorityBaseCredit()
	p.logStats.minCreditLeft = min(p.logStats.minCreditLeft, p.requeryCredit)
	return nil
}

func (p *requeryPriorityPolicy) Len() int {
	return p.lane.len() + p.inner.Len()
}

// onTaskServed refreshes the requery credit window only when a regular task is
// actually handed to execution. Credit is a backlog-aware weighted burst, not a
// reservation for a specific parent request; a served regular task refreshes
// the current window to max(base credit, its merged original request count).
func (p *requeryPriorityPolicy) onTaskServed(task *queuedTask) {
	if task.class == taskClassPriorityLane {
		return
	}
	baseCredit := requeryPriorityBaseCredit()
	p.requeryCredit = max(baseCredit, task.originalRequestCount)
	p.logStats.minCreditLeft = min(p.logStats.minCreditLeft, p.requeryCredit)
	p.logStats.regularHandoffs++
	p.logStats.maxEffectiveCredit = max(p.logStats.maxEffectiveCredit, p.requeryCredit)
	if task.originalRequestCount > baseCredit {
		p.logStats.mergeBoostedRefreshes++
	}
}

func (p *requeryPriorityPolicy) recordQueueDepths() {
	p.logStats.maxRequeryQueued = max(p.logStats.maxRequeryQueued, p.lane.len())
	p.logStats.maxRegularQueued = max(p.logStats.maxRegularQueued, p.inner.Len())
}

// logCreditStats reports policy decisions, not execChan handoffs; queue depths
// exclude the scheduler's staged task.
func (p *requeryPriorityPolicy) logCreditStats(now time.Time) {
	if p.logStats.lastLog.IsZero() {
		p.logStats.lastLog = now
		p.logStats.minCreditLeft = p.requeryCredit
		return
	}
	if now.Sub(p.logStats.lastLog) < 5*time.Second {
		return
	}
	mlog.RatedInfo(context.TODO(), 1, "requery priority policy stats",
		mlog.Duration("window", now.Sub(p.logStats.lastLog)),
		mlog.Int("configuredCredit", requeryPriorityBaseCredit()),
		mlog.Int("remainingCredit", p.requeryCredit),
		mlog.Int("minCreditLeft", p.logStats.minCreditLeft),
		mlog.Int("maxEffectiveCredit", p.logStats.maxEffectiveCredit),
		mlog.Int("mergeBoostedRefreshes", p.logStats.mergeBoostedRefreshes),
		mlog.Int("priorityRequeryPops", p.logStats.priorityPops),
		mlog.Int("fallbackRequeryPops", p.logStats.fallbackPops),
		mlog.Int("regularPops", p.logStats.regularPops),
		mlog.Int("regularHandoffs", p.logStats.regularHandoffs),
		mlog.Int("requeryAheadOfRegular", p.logStats.requeryAheadOfRegular),
		mlog.Int("regularAheadOfRequery", p.logStats.regularAheadOfRequery),
		mlog.Int("creditExhaustedWithBothQueued", p.logStats.creditExhaustedWithBoth),
		mlog.Int("maxRequeryQueued", p.logStats.maxRequeryQueued),
		mlog.Int("maxRegularQueued", p.logStats.maxRegularQueued))
	p.logStats = requeryPolicyLogStats{lastLog: now, minCreditLeft: p.requeryCredit}
	p.recordQueueDepths()
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
