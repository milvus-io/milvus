package scheduler

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var (
	_ schedulePolicy     = (*requeryPriorityPolicy)(nil)
	_ taskServedObserver = (*requeryPriorityPolicy)(nil)
)

// requeryPriorityPolicy gives requery a bounded burst while both lanes are
// backlogged. Credit counts tasks, not estimated CPU or I/O cost.
type requeryPriorityPolicy struct {
	*requeryLanes
	configuredCredit int64
	remainingCredit  int64
}

func newRequeryPriorityPolicy() *requeryPriorityPolicy {
	cfg := &paramtable.Get().QueryNodeCfg
	credit := cfg.RequeryPriorityBaseCredit.GetAsInt64()
	lanes := newRequeryLanes()
	mlog.Info(context.TODO(), "requery priority policy enabled",
		mlog.FieldNodeID(paramtable.GetNodeID()),
		mlog.Int64("regularCapacity", cfg.MaxUnsolvedQueueSize.GetAsInt64()),
		mlog.Int64("requeryCapacity", lanes.requeryCapacity),
		mlog.Int64("requeryCredit", credit))
	return &requeryPriorityPolicy{
		requeryLanes:     lanes,
		configuredCredit: credit,
		remainingCredit:  credit,
	}
}

func (p *requeryPriorityPolicy) refreshCredit() {
	next := paramtable.Get().QueryNodeCfg.RequeryPriorityBaseCredit.GetAsInt64()
	if next == p.configuredCredit {
		return
	}
	used := max(int64(0), p.configuredCredit-p.remainingCredit)
	p.configuredCredit = next
	p.remainingCredit = max(int64(0), next-used)
}

func (p *requeryPriorityPolicy) Pop(now time.Time) *queuedTask {
	p.refreshCredit()
	regular, requery := p.regular.queue.front(), p.requery.front()
	if regular.cleanupReady(now) {
		return p.regular.queue.pop()
	}
	if requery.cleanupReady(now) {
		return p.requery.pop()
	}
	switch {
	case regular.valid() && requery.valid() && p.remainingCredit > 0:
		p.remainingCredit--
		return p.requery.pop()
	case regular.valid():
		return p.regular.queue.pop()
	case requery.valid():
		return p.requery.pop()
	default:
		return nil
	}
}

func (p *requeryPriorityPolicy) onTaskServed(task *queuedTask) {
	if !task.valid() || isRequeryTask(task.Task) {
		return
	}
	credit := paramtable.Get().QueryNodeCfg.RequeryPriorityBaseCredit.GetAsInt64()
	p.configuredCredit = credit
	p.remainingCredit = credit
}
