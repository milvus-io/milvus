package moduleapi

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

const (
	PriorityData             int32 = -5
	PriorityRecoveryPressure int32 = 5
)

// DataTaskPriority carries a sticky recovery-tail promotion for one task,
// including retries. Its zero value represents ordinary data work. Keep it with
// the task, not the component: later unrelated work must start at PriorityData.
type DataTaskPriority struct {
	mu     sync.Mutex
	urgent bool
	handle nodescheduler.PriorityTaskHandle
}

// Submit publishes the handle atomically with respect to Promote. The scheduler
// must execute asynchronously, as required by AsyncTaskScheduler.
func (p *DataTaskPriority) Submit(scheduler AsyncTaskScheduler, task nodescheduler.Task) {
	p.mu.Lock()
	defer p.mu.Unlock()
	priority := PriorityData
	if p.urgent {
		priority = PriorityRecoveryPressure
	}
	p.handle, _ = scheduler.Submit(task, nodescheduler.WithPriority(priority)).(nodescheduler.PriorityTaskHandle)
}

// Promote raises pending work without interrupting a running attempt or
// shortening a delayed retry. Legacy schedulers still receive the priority
// option at submission, but cannot update an already submitted task.
func (p *DataTaskPriority) Promote() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.urgent = true
	if p.handle != nil {
		p.handle.SetPriority(PriorityRecoveryPressure)
	}
}
