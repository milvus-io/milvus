package tasks

import (
	"container/ring"
	"time"
)

func newMergeTaskQueue(group string) *mergeTaskQueue {
	return &mergeTaskQueue{
		name:             group,
		tasks:            make([]Task, 0),
		cleanupTimestamp: time.Now(),
	}
}

type mergeTaskQueue struct {
	name             string
	tasks            []Task
	cleanupTimestamp time.Time
}

// len returns the length of taskQueue.
func (q *mergeTaskQueue) len() int {
	return len(q.tasks)
}

// push add a new task to the end of taskQueue.
func (q *mergeTaskQueue) push(t Task) {
	q.tasks = append(q.tasks, t)
}

// front returns the first element of taskQueue,
// returns nil if task queue is empty.
func (q *mergeTaskQueue) front() Task {
	if q.len() > 0 {
		return q.tasks[0]
	}
	return nil
}

// pop pops the first element of taskQueue,
func (q *mergeTaskQueue) pop() {
	if q.len() > 0 {
		q.tasks = q.tasks[1:]
		if q.len() == 0 {
			q.cleanupTimestamp = time.Now()
		}
	}
}

// Return true if user based task is empty and empty for d time.
func (q *mergeTaskQueue) expire(d time.Duration) bool {
	if q.len() != 0 {
		return false
	}
	if time.Since(q.cleanupTimestamp) > d {
		return true
	}
	return false
}

// tryMerge try to a new task to any task in queue.
func (q *mergeTaskQueue) tryMerge(task MergeTask, maxNQ int64) bool {
	nqRest := maxNQ - task.NQ()
	// No need to perform any merge if task.nq is greater than maxNQ.
	if nqRest <= 0 {
		return false
	}
	for i := q.len() - 1; i >= 0; i-- {
		if taskInQueue := tryIntoMergeTask(q.tasks[i]); taskInQueue != nil {
			// Try to merge it if limit of nq is enough.
			if taskInQueue.NQ() <= nqRest && taskInQueue.MergeWith(task) {
				return true
			}
		}
	}
	return false
}

// newFairPollingTaskQueue create a fair polling task queue.
func newFairPollingTaskQueue() *fairPollingTaskQueue {
	return &fairPollingTaskQueue{
		count:      0,
		route:      make(map[string]*ring.Ring),
		checkpoint: nil,
	}
}

// fairPollingTaskQueue is a fairly polling queue.
type fairPollingTaskQueue struct {
	count      int
	route      map[string]*ring.Ring
	checkpoint *ring.Ring
}

// len returns the item count in FairPollingQueue.
func (q *fairPollingTaskQueue) len() int {
	return q.count
}

// groupLen returns the length of a group.
func (q *fairPollingTaskQueue) groupLen(group string) int {
	if r, ok := q.route[group]; ok {
		return r.Value.(*mergeTaskQueue).len()
	}
	return 0
}

// tryMergeWithOtherGroup try to merge given task into exists tasks in the other group.
func (q *fairPollingTaskQueue) tryMergeWithOtherGroup(group string, task MergeTask, maxNQ int64) bool {
	if q.count == 0 {
		return false
	}
	// Try to merge task into other group before checkpoint.
	node := q.checkpoint.Prev()
	queuesLen := q.checkpoint.Len()
	for i := 0; i < queuesLen; i++ {
		prev := node.Prev()
		queue := node.Value.(*mergeTaskQueue)
		if queue.len() == 0 || queue.name == group {
			continue
		}
		if queue.tryMerge(task, maxNQ) {
			return true
		}
		node = prev
	}
	return false
}

// tryMergeWithSameGroup try to merge given task into exists tasks in the same group.
func (q *fairPollingTaskQueue) tryMergeWithSameGroup(group string, task MergeTask, maxNQ int64) bool {
	if q.count == 0 {
		return false
	}
	// Applied to task with same group first.
	if r, ok := q.route[group]; ok {
		// Try to merge task into queue.
		if r.Value.(*mergeTaskQueue).tryMerge(task, maxNQ) {
			return true
		}
	}
	return false
}

// push add a new task into queue, try merge first.
func (q *fairPollingTaskQueue) push(group string, task Task) {
	// Add a new task.
	if r, ok := q.route[group]; ok {
		// Add new task to the back of queue if queue exist.
		r.Value.(*mergeTaskQueue).push(task)
	} else {
		// Create a new task queue, and add it to the route and queues.
		newQueue := newMergeTaskQueue(group)
		newQueue.push(task)
		newRing := ring.New(1)
		newRing.Value = newQueue
		q.route[group] = newRing
		if q.checkpoint == nil {
			// Create new ring if not exist.
			q.checkpoint = newRing
		} else {
			// Add the new ring before the checkpoint.
			q.checkpoint.Prev().Link(newRing)
		}
	}
	q.count++
}

// pop pop next ready task.
func (q *fairPollingTaskQueue) pop(queueExpire time.Duration) (task Task) {
	// Return directly if there's no task exists.
	if q.count == 0 {
		return
	}
	checkpoint := q.checkpoint
	queuesLen := q.checkpoint.Len()

	for i := 0; i < queuesLen; i++ {
		next := checkpoint.Next()
		// Find task in this queue.
		queue := checkpoint.Value.(*mergeTaskQueue)

		// empty task queue for this user.
		if queue.len() == 0 {
			// expire the queue.
			if queue.expire(queueExpire) {
				delete(q.route, queue.name)
				if checkpoint.Len() == 1 {
					checkpoint = nil
					break
				} else {
					checkpoint.Prev().Unlink(1)
				}
			}
			checkpoint = next
			continue
		}
		task = queue.front()
		queue.pop()
		q.count--
		checkpoint = next
		break
	}

	// Update checkpoint.
	q.checkpoint = checkpoint
	return
}
