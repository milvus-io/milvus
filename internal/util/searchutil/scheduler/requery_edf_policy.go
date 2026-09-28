package scheduler

import (
	"fmt"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ schedulePolicy = (*requeryEDFPolicy)(nil)

// requeryEDFPolicy compares the heads of two FIFO lanes, not every queued task.
// Queue ownership and capacity accounting stay on the scheduling goroutine.
type requeryEDFPolicy struct {
	regular         *fifoPolicy
	requery         *mergeTaskQueue
	requeryCapacity int64
}

func newRequeryEDFPolicy() *requeryEDFPolicy {
	return &requeryEDFPolicy{
		regular:         &fifoPolicy{queue: newMergeTaskQueue("")},
		requery:         newMergeTaskQueue(metrics.ReQueryLabel),
		requeryCapacity: paramtable.Get().QueryNodeCfg.RequeryUnsolvedQueueSize.GetAsInt64(),
	}
}

func (p *requeryEDFPolicy) CheckAdmission(task Task, _ int64) error {
	cfg := &paramtable.Get().QueryNodeCfg
	if contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel {
		if p.requeryCapacity > 0 && int64(p.requery.len()) >= p.requeryCapacity {
			return merr.WrapErrTooManyRequests(
				int32(p.requeryCapacity),
				fmt.Sprintf("limit by %s", cfg.RequeryUnsolvedQueueSize.Key),
			)
		}
		return nil
	}
	return p.regular.CheckAdmission(task, int64(p.regular.Len()))
}

func (p *requeryEDFPolicy) Push(task *queuedTask) (int, error) {
	task.schedulingDeadline, _ = task.Context().Deadline()
	if contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel {
		p.requery.push(task)
		return 1, nil
	}
	return p.regular.Push(task)
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
	if requeryDeadline.IsZero() || regularDeadline.Before(requeryDeadline) {
		return p.regular.queue
	}
	// Equal finite deadlines favor completion of an existing search request.
	return p.requery
}

func (p *requeryEDFPolicy) Cleanup(now time.Time) []*queuedTask {
	// EDF only reclaims tasks whose actual deadline has passed.
	return append(p.regular.queue.cleanup(now), p.requery.cleanup(now)...)
}

func (p *requeryEDFPolicy) Remove(filter TaskFilter, now time.Time) []*queuedTask {
	return append(p.regular.Remove(filter, now), p.requery.remove(filter, now)...)
}

func (p *requeryEDFPolicy) Len() int {
	return p.regular.Len() + p.requery.len()
}
