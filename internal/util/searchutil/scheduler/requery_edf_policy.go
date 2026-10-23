package scheduler

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ schedulePolicy = (*requeryEDFPolicy)(nil)

// requeryEDFPolicy compares the heads of two FIFO lanes, not every queued task.
// Queue ownership and capacity accounting stay on the scheduling goroutine.
type requeryEDFPolicy struct {
	*requeryLanes
}

func newRequeryEDFPolicy() *requeryEDFPolicy {
	cfg := &paramtable.Get().QueryNodeCfg
	lanes := newRequeryLanes()
	mlog.Info(context.TODO(), "requery EDF policy enabled",
		mlog.FieldNodeID(paramtable.GetNodeID()),
		mlog.Int64("regularCapacity", cfg.MaxUnsolvedQueueSize.GetAsInt64()),
		mlog.Int64("requeryCapacity", lanes.requeryCapacity),
		mlog.String("selection", "FIFO lane heads at Pop; finite deadline ties favor requery"))
	return &requeryEDFPolicy{requeryLanes: lanes}
}

func (p *requeryEDFPolicy) Push(task *queuedTask) (int, error) {
	task.schedulingDeadline, _ = task.Context().Deadline()
	return p.requeryLanes.Push(task)
}

// Pop compares lane heads at selection time. The generic scheduler may stage
// the selected task before execution; later arrivals do not replace it.
func (p *requeryEDFPolicy) Pop(now time.Time) *queuedTask {
	regular, requery := p.regular.queue.front(), p.requery.front()
	if regular.cleanupReady(now) {
		return p.regular.queue.pop()
	}
	if requery.cleanupReady(now) {
		return p.requery.pop()
	}
	switch {
	case regular.valid() && requery.valid():
		return p.earlierDeadlineQueue(regular, requery).pop()
	case regular.valid():
		return p.regular.queue.pop()
	case requery.valid():
		return p.requery.pop()
	default:
		return nil
	}
}

func (p *requeryEDFPolicy) earlierDeadlineQueue(regular, requery *queuedTask) *mergeTaskQueue {
	regularDeadline, requeryDeadline := regular.schedulingDeadline, requery.schedulingDeadline
	if regularDeadline.IsZero() && requeryDeadline.IsZero() {
		// Without deadlines, preserve arrival order across the two lanes.
		if regular.enqueueTime.Before(requery.enqueueTime) || regular.enqueueTime.Equal(requery.enqueueTime) {
			return p.regular.queue
		}
		return p.requery
	}
	if regularDeadline.IsZero() {
		return p.requery
	}
	if requeryDeadline.IsZero() {
		return p.regular.queue
	}
	if regularDeadline.Before(requeryDeadline) {
		return p.regular.queue
	}
	// Equal finite deadlines favor completion of an existing search request.
	if regularDeadline.Equal(requeryDeadline) {
		return p.requery
	}
	return p.requery
}
