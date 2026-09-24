package scheduler

import (
	"container/ring"
	"time"
)

func newMergeTaskQueue(group string) *mergeTaskQueue {
	return &mergeTaskQueue{
		name:             group,
		tasks:            make([]*queuedTask, 0),
		cleanupTimestamp: time.Now(),
	}
}

type mergeTaskQueue struct {
	name             string
	tasks            []*queuedTask
	count            int
	cleanupTimestamp time.Time
}

// len returns the length of taskQueue.
func (q *mergeTaskQueue) len() int {
	return q.count
}

// push add a new task to the end of taskQueue.
func (q *mergeTaskQueue) push(task *queuedTask) {
	q.tasks = append(q.tasks, task)
	q.count++
}

// front returns the first element of taskQueue.
func (q *mergeTaskQueue) front() *queuedTask {
	q.dropRemovedPrefix()
	if len(q.tasks) > 0 {
		return q.tasks[0]
	}
	return nil
}

// pop pops the first element of taskQueue.
func (q *mergeTaskQueue) pop() *queuedTask {
	for {
		q.dropRemovedPrefix()
		if len(q.tasks) == 0 {
			return nil
		}

		task := q.tasks[0]
		q.tasks = q.tasks[1:]
		if !task.valid() {
			continue
		}

		removed := q.markRemoved(task, time.Now())
		if q.len() == 0 {
			clear(q.tasks)
			q.tasks = nil
		}
		return removed
	}
}

func (q *mergeTaskQueue) cleanup(now time.Time) []*queuedTask {
	if q.len() == 0 {
		return nil
	}

	removed := make([]*queuedTask, 0)
	for _, task := range q.tasks {
		if !task.cleanupReady(now) {
			continue
		}
		removed = append(removed, q.markRemoved(task, now))
	}

	if q.len() == 0 {
		clear(q.tasks)
		q.tasks = nil
	}
	return removed
}

func (q *mergeTaskQueue) remove(filter TaskFilter, now time.Time) []*queuedTask {
	if q.len() == 0 {
		return nil
	}

	removed := make([]*queuedTask, 0)
	for _, task := range q.tasks {
		if !task.valid() || filter != nil && !filter(task.Task) {
			continue
		}
		removed = append(removed, q.markRemoved(task, now))
	}

	if q.len() == 0 {
		clear(q.tasks)
		q.tasks = nil
	}
	return removed
}

func (q *mergeTaskQueue) markRemoved(task *queuedTask, now time.Time) *queuedTask {
	if !task.valid() {
		return nil
	}
	removed := *task
	task.Task = nil
	q.count--
	if q.count == 0 {
		q.cleanupTimestamp = now
	}
	return &removed
}

func (q *mergeTaskQueue) dropRemovedPrefix() {
	for len(q.tasks) > 0 {
		task := q.tasks[0]
		if task.valid() {
			return
		}
		q.tasks[0] = nil
		q.tasks = q.tasks[1:]
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

func canMergeNQ(task MergeTask, other MergeTask, maxNQ int64, nqMergeRatio float64) bool {
	totalNQ := task.NQ() + other.NQ()
	if totalNQ > maxNQ {
		return false
	}
	if nqMergeRatio <= 0 {
		return true
	}
	minNQ := task.MinNQ()
	if otherMinNQ := other.MinNQ(); otherMinNQ < minNQ {
		minNQ = otherMinNQ
	}
	return minNQ > 0 && float64(totalNQ)/float64(minNQ) <= nqMergeRatio
}

func canMergeDeadline(task *queuedTask, other *queuedTask, maxDeadlineMergeGap time.Duration) bool {
	if maxDeadlineMergeGap < 0 {
		return true
	}
	deadline, ok := task.Context().Deadline()
	otherDeadline, otherOk := other.Context().Deadline()
	if !ok && !otherOk {
		return true
	}
	if ok != otherOk {
		return false
	}
	if deadline.After(otherDeadline) {
		deadline, otherDeadline = otherDeadline, deadline
	}
	return otherDeadline.Sub(deadline) <= maxDeadlineMergeGap
}

// tryMerge try to a new task to any task in queue.
func (q *mergeTaskQueue) tryMerge(task *queuedTask, maxNQ int64, nqMergeRatio float64, maxDeadlineMergeGap time.Duration) bool {
	mergeTask := tryIntoMergeTask(task.Task)
	if mergeTask == nil {
		return false
	}
	// No need to perform any merge if task.nq is greater than maxNQ.
	if mergeTask.NQ() >= maxNQ {
		return false
	}
	for i := len(q.tasks) - 1; i >= 0; i-- {
		taskInQueue := q.tasks[i]
		if !taskInQueue.valid() {
			continue
		}
		if taskInQueue := tryIntoMergeTask(taskInQueue.Task); taskInQueue != nil {
			// Try to merge it if limit of nq is enough.
			if (canMergeNQ(taskInQueue, mergeTask, maxNQ, nqMergeRatio) &&
				canMergeDeadline(q.tasks[i], task, maxDeadlineMergeGap)) &&
				taskInQueue.MergeWith(mergeTask) {
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
func (q *fairPollingTaskQueue) tryMergeWithOtherGroup(group string, task *queuedTask, maxNQ int64, nqMergeRatio float64, maxDeadlineMergeGap time.Duration) bool {
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
		if queue.tryMerge(task, maxNQ, nqMergeRatio, maxDeadlineMergeGap) {
			return true
		}
		node = prev
	}
	return false
}

// tryMergeWithSameGroup try to merge given task into exists tasks in the same group.
func (q *fairPollingTaskQueue) tryMergeWithSameGroup(group string, task *queuedTask, maxNQ int64, nqMergeRatio float64, maxDeadlineMergeGap time.Duration) bool {
	if q.count == 0 {
		return false
	}
	// Applied to task with same group first.
	if r, ok := q.route[group]; ok {
		// Try to merge task into queue.
		if r.Value.(*mergeTaskQueue).tryMerge(task, maxNQ, nqMergeRatio, maxDeadlineMergeGap) {
			return true
		}
	}
	return false
}

// push add a new task into queue, try merge first.
func (q *fairPollingTaskQueue) push(group string, task *queuedTask) {
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

func (q *fairPollingTaskQueue) cleanup(now time.Time) []*queuedTask {
	if q.count == 0 || q.checkpoint == nil {
		return nil
	}
	removed := make([]*queuedTask, 0)
	checkpoint := q.checkpoint
	queuesLen := q.checkpoint.Len()
	for i := 0; i < queuesLen; i++ {
		queue := checkpoint.Value.(*mergeTaskQueue)
		tasks := queue.cleanup(now)
		if len(tasks) > 0 {
			q.count -= len(tasks)
			removed = append(removed, tasks...)
		}
		checkpoint = checkpoint.Next()
	}
	return removed
}

func (q *fairPollingTaskQueue) remove(filter TaskFilter, now time.Time) []*queuedTask {
	if q.count == 0 || q.checkpoint == nil {
		return nil
	}
	removed := make([]*queuedTask, 0)
	checkpoint := q.checkpoint
	queuesLen := q.checkpoint.Len()
	for i := 0; i < queuesLen; i++ {
		queue := checkpoint.Value.(*mergeTaskQueue)
		tasks := queue.remove(filter, now)
		if len(tasks) > 0 {
			q.count -= len(tasks)
			removed = append(removed, tasks...)
		}
		checkpoint = checkpoint.Next()
	}
	return removed
}

// pop pop next ready task.
func (q *fairPollingTaskQueue) pop(queueExpire time.Duration) *queuedTask {
	// Return directly if there's no task exists.
	if q.count == 0 {
		return nil
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
		task := queue.pop()
		if task.valid() {
			q.count--
		}
		if !task.valid() {
			checkpoint = next
			continue
		}
		checkpoint = next
		q.checkpoint = checkpoint
		return task
	}

	// Update checkpoint.
	q.checkpoint = checkpoint
	return nil
}
