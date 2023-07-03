package tasks

import (
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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

// Push add a new task into scheduler, an error will be returned if scheduler reaches some limit.
func (p *fifoPolicy) Push(task Task) (int, error) {
	pt := paramtable.Get()

	// Try to merge task if task can merge.
	if t := tryIntoMergeTask(task); t != nil {
		maxNQ := pt.QueryNodeCfg.MaxGroupNQ.GetAsInt64()
		if p.queue.tryMerge(t, maxNQ) {
			return 0, nil
		}
	}

	// Add a new task into queue.
	p.queue.push(task)
	return 1, nil
}

// Pop get the task next ready to run.
func (p *fifoPolicy) Pop() Task {
	task := p.queue.front()
	p.queue.pop()
	return task
}

// Len get ready task counts.
func (p *fifoPolicy) Len() int {
	return p.queue.len()
}
