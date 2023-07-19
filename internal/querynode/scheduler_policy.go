package querynode

import (
	"container/list"
	"container/ring"
	"time"

	"github.com/milvus-io/milvus/internal/util/contextutil"
)

const (
	scheduleReadPolicyNameFIFO            = "fifo"
	scheduleReadPolicyNameUserTaskPolling = "user-task-polling"
)

var (
	_ scheduleReadPolicy = &fifoScheduleReadPolicy{}
	_ scheduleReadPolicy = &userTaskPollingScheduleReadPolicy{}
)

type scheduleReadPolicy interface {
	// Read task count inside.
	len() int

	// Add a new task into ready task.
	addTask(rt readTask)

	// Merge a new task into exist ready task.
	// Return true if merge success.
	mergeTask(rt readTask) bool

	// Schedule new task to run.
	schedule(targetCPUUsage int32, maxNum int32, minNum int32) ([]readTask, int32)
}

// Create a new schedule policy.
func newReadScheduleTaskPolicy(policyName string) scheduleReadPolicy {
	switch policyName {
	case "":
		fallthrough
	case scheduleReadPolicyNameFIFO:
		return newFIFOScheduleReadPolicy()
	case scheduleReadPolicyNameUserTaskPolling:
		return newUserTaskPollingScheduleReadPolicy(
			Params.QueryNodeCfg.ScheduleReadPolicy.UserTaskPolling.TaskQueueExpire)
	default:
		panic("invalid read schedule task policy")
	}
}

// Create a new user based task queue.
func newUserBasedTaskQueue(username string) *userBasedTaskQueue {
	return &userBasedTaskQueue{
		cleanupTimestamp: time.Now(),
		queue:            list.New(),
		username:         username,
	}
}

// User based task queue.
type userBasedTaskQueue struct {
	cleanupTimestamp time.Time
	queue            *list.List
	username         string
}

// Get length of user task
func (q *userBasedTaskQueue) len() int {
	return q.queue.Len()
}

// Add a new task to end of queue.
func (q *userBasedTaskQueue) push(t readTask) {
	q.queue.PushBack(t)
}

// Get the first task from queue.
func (q *userBasedTaskQueue) front() readTask {
	element := q.queue.Front()
	if element == nil {
		return nil
	}
	return element.Value.(readTask)
}

// Remove the first task from queue.
func (q *userBasedTaskQueue) pop() {
	front := q.queue.Front()
	if front != nil {
		q.queue.Remove(front)
	}
	if q.queue.Len() == 0 {
		q.cleanupTimestamp = time.Now()
	}
}

// Return true if user based task is empty and empty for d time.
func (q *userBasedTaskQueue) expire(d time.Duration) bool {
	if q.queue.Len() != 0 {
		return false
	}
	if time.Since(q.cleanupTimestamp) > d {
		return true
	}
	return false
}

// Merge a new task to task in queue.
func (q *userBasedTaskQueue) mergeTask(rt readTask) bool {
	for element := q.queue.Back(); element != nil; element = element.Prev() {
		task := element.Value.(readTask)
		if task.CanMergeWith(rt) {
			task.Merge(rt)
			return true
		}
	}
	return false
}

// Implement user based schedule read policy.
type userTaskPollingScheduleReadPolicy struct {
	taskCount       int                   // task count in the policy.
	route           map[string]*ring.Ring // map username to node of task ring.
	checkpoint      *ring.Ring            // last not schedule ring node, ring.Ring[list.List]
	taskQueueExpire time.Duration
}

// Create a new user-based schedule read policy.
func newUserTaskPollingScheduleReadPolicy(taskQueueExpire time.Duration) scheduleReadPolicy {
	return &userTaskPollingScheduleReadPolicy{
		taskCount:       0,
		route:           make(map[string]*ring.Ring),
		checkpoint:      nil,
		taskQueueExpire: taskQueueExpire,
	}
}

// Get length of task queue.
func (p *userTaskPollingScheduleReadPolicy) len() int {
	return p.taskCount
}

// Add a new task into ready task queue.
func (p *userTaskPollingScheduleReadPolicy) addTask(rt readTask) {
	username := contextutil.GetUserFromGrpcMetadata(rt.Ctx()) // empty user will compete on single list.
	if r, ok := p.route[username]; ok {
		// Add new task to the back of queue if queue exist.
		r.Value.(*userBasedTaskQueue).push(rt)
	} else {
		// Create a new list, and add it to the route and queues.
		newQueue := newUserBasedTaskQueue(username)
		newQueue.push(rt)
		newRing := ring.New(1)
		newRing.Value = newQueue
		p.route[username] = newRing
		if p.checkpoint == nil {
			// Create new ring if not exist.
			p.checkpoint = newRing
		} else {
			// Add the new ring before the checkpoint.
			p.checkpoint.Prev().Link(newRing)
		}
	}
	p.taskCount++
}

// Merge a new task into exist ready task.
func (p *userTaskPollingScheduleReadPolicy) mergeTask(rt readTask) bool {
	if p.taskCount == 0 {
		return false
	}

	username := contextutil.GetUserFromGrpcMetadata(rt.Ctx()) // empty user will compete on single list.
	// Applied to task with same user first.
	if r, ok := p.route[username]; ok {
		// Try to merge task into queue.
		if r.Value.(*userBasedTaskQueue).mergeTask(rt) {
			return true
		}
	}

	// Try to merge task into other user queue before checkpoint.
	node := p.checkpoint.Prev()
	queuesLen := p.checkpoint.Len()
	for i := 0; i < queuesLen; i++ {
		prev := node.Prev()
		queue := node.Value.(*userBasedTaskQueue)
		if queue.len() == 0 || queue.username == username {
			continue
		}
		if queue.mergeTask(rt) {
			return true
		}
		node = prev
	}

	return false
}

func (p *userTaskPollingScheduleReadPolicy) schedule(targetCPUUsage int32, maxNum int32, minNum int32) (result []readTask, usage int32) {
	// ignoring minNum for userTaskPollingScheduleReadPolicy

	// Return directly if there's no task ready.
	if p.taskCount == 0 {
		return
	}

	queuesLen := p.checkpoint.Len()
	checkpoint := p.checkpoint
	// TODO: infinite loop.
L:
	for {
		readyCount := len(result)
		for i := 0; i < queuesLen; i++ {
			if len(result) >= int(maxNum) {
				break L
			}

			next := checkpoint.Next()
			// Find task in this queue.
			taskQueue := checkpoint.Value.(*userBasedTaskQueue)

			// empty task queue for this user.
			if taskQueue.len() == 0 {
				// expire the queue.
				if taskQueue.expire(p.taskQueueExpire) {
					delete(p.route, taskQueue.username)
					if checkpoint.Len() == 1 {
						checkpoint = nil
						break L
					} else {
						checkpoint.Prev().Unlink(1)
					}
				}
				checkpoint = next
				continue
			}

			// Read first read task of queue and check if cpu is enough.
			task := taskQueue.front()
			tUsage := task.CPUUsage()
			if usage+tUsage > targetCPUUsage {
				break L
			}

			// Pop the task and add to schedule list.
			usage += tUsage
			result = append(result, task)
			taskQueue.pop()
			p.taskCount--

			checkpoint = next
		}

		// Stop loop if no task is added.
		if readyCount == len(result) {
			break L
		}
	}

	// Update checkpoint.
	p.checkpoint = checkpoint
	return result, usage
}

// Implement default FIFO policy.
type fifoScheduleReadPolicy struct {
	ready *list.List // Save ready to run task.
}

// Create a new default schedule read policy
func newFIFOScheduleReadPolicy() scheduleReadPolicy {
	return &fifoScheduleReadPolicy{
		ready: list.New(),
	}
}

// Read task count inside.
func (p *fifoScheduleReadPolicy) len() int {
	return p.ready.Len()
}

// Add a new task into ready task.
func (p *fifoScheduleReadPolicy) addTask(rt readTask) {
	p.ready.PushBack(rt)
}

// Merge a new task into exist ready task.
func (p *fifoScheduleReadPolicy) mergeTask(rt readTask) bool {
	// Reverse Iterate the task in the queue
	for task := p.ready.Back(); task != nil; task = task.Prev() {
		taskExist := task.Value.(readTask)
		if taskExist.CanMergeWith(rt) {
			taskExist.Merge(rt)
			return true
		}
	}
	return false
}

// Schedule a new task.
func (p *fifoScheduleReadPolicy) schedule(targetCPUUsage int32, maxNum int32, minNum int32) (result []readTask, usage int32) {
	var ret []readTask
	var next *list.Element
	var added int32
	for e := p.ready.Front(); e != nil && maxNum > 0; e = next {
		next = e.Next()
		t, _ := e.Value.(readTask)
		tUsage := t.CPUUsage()
		if added >= minNum && usage+tUsage > targetCPUUsage {
			break
		}
		added++
		usage += tUsage
		p.ready.Remove(e)
		rateCol.rtCounter.sub(t, readyQueueType)
		ret = append(ret, t)
		maxNum--
	}
	return ret, usage
}
