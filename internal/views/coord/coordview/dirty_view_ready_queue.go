package coordview

import (
	"container/list"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A ready shard has one heap entry. Its FIFO position survives score updates.
type dirtyViewReadyQueue struct {
	heap     typeutil.IndexedHeap[*pendingDirtyViewEvent]
	oldest   list.List
	sequence uint64
	burst    int
}

const flushPriorityBurst = 8

func newDirtyViewReadyQueue() dirtyViewReadyQueue {
	return dirtyViewReadyQueue{heap: typeutil.NewIndexedHeap(func(a, b *pendingDirtyViewEvent) bool {
		if a.priority != b.priority {
			return a.priority > b.priority
		}
		return a.sequence < b.sequence
	}, func(event *pendingDirtyViewEvent, index int) { event.index = index })}
}

func (q *dirtyViewReadyQueue) upsert(event *pendingDirtyViewEvent) {
	priority := event.preparationPriority()
	if event.age != nil {
		if event.priority != priority {
			event.priority = priority
			q.heap.Fix(event.index)
		}
		return
	}
	q.sequence++
	event.sequence = q.sequence
	event.priority = priority
	event.age = q.oldest.PushBack(event)
	q.heap.Push(event)
}

// Take keeps a shard whole. If it cannot fit, leave both it and the fairness
// turn for the next batch, whose first shard may exceed the transaction budget.
func (q *dirtyViewReadyQueue) take(remaining int, first bool) *pendingDirtyViewEvent {
	if q.heap.Len() == 0 {
		return nil
	}
	event := q.heap.Peek()
	if q.burst == flushPriorityBurst {
		event = q.oldest.Front().Value.(*pendingDirtyViewEvent)
	}
	if !first && event.operationCount() > remaining {
		return nil
	}
	q.remove(event)
	if q.heap.Len() == 0 || q.burst == flushPriorityBurst {
		q.burst = 0
	} else {
		q.burst++
	}
	return event
}

func (q *dirtyViewReadyQueue) remove(event *pendingDirtyViewEvent) {
	q.heap.Remove(event.index)
	q.oldest.Remove(event.age)
	event.age = nil
}
