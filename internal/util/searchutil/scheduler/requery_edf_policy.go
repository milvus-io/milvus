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

func (p *requeryEDFPolicy) Push(task *queuedTask) (int, error) {
	cfg := &paramtable.Get().QueryNodeCfg
	if contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel {
		if p.requeryCapacity > 0 && int64(p.requery.len()) >= p.requeryCapacity {
			return 0, merr.WrapErrTooManyRequests(
				int32(p.requeryCapacity),
				fmt.Sprintf("limit by %s", cfg.RequeryUnsolvedQueueSize.Key),
			)
		}
		p.requery.push(task)
		return 1, nil
	}

	regularCapacity := cfg.MaxUnsolvedQueueSize.GetAsInt64()
	if regularCapacity > 0 && int64(p.regular.Len()) >= regularCapacity {
		return 0, merr.WrapErrTooManyRequests(
			int32(regularCapacity),
			fmt.Sprintf("limit by %s", cfg.MaxUnsolvedQueueSize.Key),
		)
	}
	return p.regular.Push(task)
}

func (p *requeryEDFPolicy) Pop(now time.Time) *queuedTask {
	return p.popReady(now, true, true)
}

// popReady never stages a live task for a busy pool. Expired heads are returned
// regardless of pool capacity so the scheduler can settle counters and Done.
func (p *requeryEDFPolicy) popReady(now time.Time, cpuReady, gpuReady bool) *queuedTask {
	regular, requery := p.regular.queue.front(), p.requery.front()
	if regular.cleanupReady(now) {
		return p.regular.queue.pop()
	}
	if requery.cleanupReady(now) {
		return p.requery.pop()
	}
	ready := func(task *queuedTask) bool {
		if !task.valid() {
			return false
		}
		if task.IsGpuIndex() {
			return gpuReady
		}
		return cpuReady
	}
	regularReady, requeryReady := ready(regular), ready(requery)
	switch {
	case regularReady && requeryReady:
		if edfRegularFirst(regular, requery) {
			return p.regular.queue.pop()
		}
		return p.requery.pop()
	case regularReady:
		return p.regular.queue.pop()
	case requeryReady:
		return p.requery.pop()
	default:
		return nil
	}
}

func edfRegularFirst(regular, requery *queuedTask) bool {
	regularDeadline, requeryDeadline := regular.schedulingDeadline, requery.schedulingDeadline
	if regularDeadline.IsZero() && requeryDeadline.IsZero() {
		// Without deadlines, preserve arrival order across the two lanes.
		return regular.enqueueTime.Before(requery.enqueueTime) || regular.enqueueTime.Equal(requery.enqueueTime)
	}
	if regularDeadline.IsZero() {
		return false
	}
	if requeryDeadline.IsZero() {
		return true
	}
	// Equal finite deadlines favor completion of an existing search request.
	return regularDeadline.Before(requeryDeadline)
}

func (p *requeryEDFPolicy) Cleanup(now time.Time) []*queuedTask {
	return append(p.regular.Cleanup(now), p.requery.cleanup(now)...)
}

func (p *requeryEDFPolicy) Remove(filter TaskFilter, now time.Time) []*queuedTask {
	return append(p.regular.Remove(filter, now), p.requery.remove(filter, now)...)
}

func (p *requeryEDFPolicy) Len() int {
	return p.regular.Len() + p.requery.len()
}
