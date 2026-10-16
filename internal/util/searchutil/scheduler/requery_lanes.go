package scheduler

import (
	"fmt"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// requeryLanes owns the lane-neutral queue mechanics shared by policies that
// separate requery work from regular read work. Selection stays in each policy.
type requeryLanes struct {
	regular         *fifoPolicy
	requery         *mergeTaskQueue
	requeryCapacity int64
}

func newRequeryLanes() *requeryLanes {
	return &requeryLanes{
		regular:         &fifoPolicy{queue: newMergeTaskQueue("")},
		requery:         newMergeTaskQueue(metrics.ReQueryLabel),
		requeryCapacity: paramtable.Get().QueryNodeCfg.RequeryUnsolvedQueueSize.GetAsInt64(),
	}
}

func isRequeryTask(task Task) bool {
	return contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel
}

func (p *requeryLanes) CheckAdmission(task Task, _ int64) error {
	cfg := &paramtable.Get().QueryNodeCfg
	if isRequeryTask(task) {
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

func (p *requeryLanes) Push(task *queuedTask) (int, error) {
	if isRequeryTask(task.Task) {
		p.requery.push(task)
		return 1, nil
	}
	return p.regular.Push(task)
}

func (p *requeryLanes) Cleanup(now time.Time) []*queuedTask {
	return append(p.regular.queue.cleanup(now), p.requery.cleanup(now)...)
}

func (p *requeryLanes) Remove(filter TaskFilter, now time.Time) []*queuedTask {
	return append(p.regular.Remove(filter, now), p.requery.remove(filter, now)...)
}

func (p *requeryLanes) Len() int {
	return p.regular.Len() + p.requery.len()
}
