package scheduler

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ schedulePolicy = &fifoPolicy{}

// newFIFOPolicy create a new fifo schedule policy.
func newFIFOPolicy() schedulePolicy {
	return &fifoPolicy{
		queue: newMergeTaskQueue(""),
	}
}

// fifoPolicy is a fifo policy with merge queue.
type fifoPolicy struct {
	queue *mergeTaskQueue
}

func (p *fifoPolicy) Cleanup(now time.Time) []*queuedTask {
	return p.queue.cleanup(now)
}

func (p *fifoPolicy) Remove(filter TaskFilter, now time.Time) []*queuedTask {
	return p.queue.remove(filter, now)
}

// Push add a new task into scheduler, an error will be returned if scheduler reaches some limit.
func (p *fifoPolicy) Push(task *queuedTask) (int, error) {
	pt := paramtable.Get()

	// Try to merge task if task can merge.
	if t := tryIntoMergeTask(task.Task); t != nil {
		maxNQ := pt.QueryNodeCfg.MaxGroupNQ.GetAsInt64()
		nqMergeRatio := pt.QueryNodeCfg.NQMergeRatio.GetAsFloat()
		maxDeadlineMergeGap := pt.QueryNodeCfg.MaxDeadlineMergeGap.GetAsDurationByParse()
		if p.queue.tryMerge(task, maxNQ, nqMergeRatio, maxDeadlineMergeGap) {
			return 0, nil
		}
	}

	// Add a new task into queue.
	p.queue.push(task)
	return 1, nil
}

// Pop get the task next ready to run.
func (p *fifoPolicy) Pop(now time.Time) *queuedTask {
	return p.queue.pop()
}

// Len get ready task counts.
func (p *fifoPolicy) Len() int {
	return p.queue.len()
}
