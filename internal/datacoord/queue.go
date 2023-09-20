// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"container/ring"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

// schedulePolicy is the policy of scheduler.
type schedulePolicy interface {
	Push(task *indexBuildTask)
	// Pop get the task next ready to run.
	Pop() *indexBuildTask
	NumRequesters() int
	TaskCount() int
	Exist(task *indexBuildTask) bool
	Remove(collectionID, buildID int64)
}

var _ schedulePolicy = &fairQueuePolicy{}

type fairQueuePolicy struct {
	queue *fairPollingTaskQueue
}

func newFairQueuePolicy() *fairQueuePolicy {
	return &fairQueuePolicy{
		queue: newFairPollingTaskQueue(),
	}
}

func (fqp *fairQueuePolicy) Push(t *indexBuildTask) {
	fqp.queue.push(t)
}

func (fqp *fairQueuePolicy) Pop() *indexBuildTask {
	return fqp.queue.pop()
}

func (fqp *fairQueuePolicy) NumRequesters() int {
	return fqp.queue.len()
}

func (fqp *fairQueuePolicy) TaskCount() int {
	return fqp.queue.getTaskCount()
}

func (fqp *fairQueuePolicy) Exist(t *indexBuildTask) bool {
	return fqp.queue.exist(t)
}

func (fqp *fairQueuePolicy) Remove(collectionID, buildID int64) {
	fqp.queue.removeTask(collectionID, buildID)
}

type taskQueue struct {
	collectionID UniqueID
	tasks        []*indexBuildTask
}

func newTaskQueue(collectionID UniqueID) *taskQueue {
	return &taskQueue{
		collectionID: collectionID,
		tasks:        make([]*indexBuildTask, 0),
	}
}

func (q *taskQueue) len() int {
	return len(q.tasks)
}

func (q *taskQueue) push(t *indexBuildTask) {
	q.tasks = append(q.tasks, t)
}

func (q *taskQueue) pop() *indexBuildTask {
	if len(q.tasks) > 0 {
		task := q.tasks[0]
		q.tasks = q.tasks[1:]
		return task
	}
	return nil
}

func (q *taskQueue) remove(buildID int64) {
	for i := 0; i < len(q.tasks); i++ {
		if q.tasks[i].buildID == buildID {
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			i--
		}
	}
}

func (q *taskQueue) exist(t *indexBuildTask) bool {
	if t.collectionID != q.collectionID {
		return false
	}
	for _, task := range q.tasks {
		if task.buildID == t.buildID {
			return true
		}
	}
	return false
}

type fairPollingTaskQueue struct {
	taskCount  int
	checkpoint *ring.Ring
	route      map[UniqueID]*ring.Ring
	lock       sync.RWMutex
}

func newFairPollingTaskQueue() *fairPollingTaskQueue {
	return &fairPollingTaskQueue{
		taskCount:  0,
		checkpoint: nil,
		route:      make(map[UniqueID]*ring.Ring),
		lock:       sync.RWMutex{},
	}
}

func (fptq *fairPollingTaskQueue) push(t *indexBuildTask) {
	fptq.lock.Lock()
	defer fptq.lock.Unlock()
	if r, ok := fptq.route[t.collectionID]; ok {
		r.Value.(*taskQueue).push(t)
	} else {
		newQueue := newTaskQueue(t.collectionID)
		newQueue.push(t)
		newRing := ring.New(1)
		newRing.Value = newQueue
		fptq.route[t.collectionID] = newRing
		if fptq.checkpoint == nil {
			// Create new ring if not exist.
			fptq.checkpoint = newRing
		} else {
			// Add the new ring before the checkpoint.
			fptq.checkpoint.Prev().Link(newRing)
		}
	}
	fptq.taskCount++
}

func (fptq *fairPollingTaskQueue) pop() (task *indexBuildTask) {
	fptq.lock.Lock()
	defer fptq.lock.Unlock()
	if fptq.taskCount == 0 {
		return
	}
	checkpoint := fptq.checkpoint
	queuesLen := fptq.checkpoint.Len()

	for i := 0; i < queuesLen; i++ {
		next := checkpoint.Next()
		// Find task in this queue.
		queue := checkpoint.Value.(*taskQueue)

		// empty task queue for this user.
		if queue.len() == 0 {
			// expire the queue.
			delete(fptq.route, queue.collectionID)

			if checkpoint.Len() == 1 {
				checkpoint = nil
				break
			} else {
				checkpoint.Prev().Unlink(1)
			}
			checkpoint = next
			continue
		}
		task = queue.pop()
		fptq.taskCount--
		checkpoint = next
		break
	}

	// Update checkpoint.
	fptq.checkpoint = checkpoint
	return
}

func (fptq *fairPollingTaskQueue) len() int {
	fptq.lock.RLock()
	defer fptq.lock.RUnlock()
	return len(fptq.route)
}

func (fptq *fairPollingTaskQueue) getTaskCount() int {
	fptq.lock.RLock()
	defer fptq.lock.RUnlock()
	return fptq.taskCount
}

func (fptq *fairPollingTaskQueue) exist(t *indexBuildTask) bool {
	fptq.lock.RLock()
	defer fptq.lock.RUnlock()
	if r, ok := fptq.route[t.collectionID]; ok {
		return r.Value.(*taskQueue).exist(t)
	}
	return false
}

func (fptq *fairPollingTaskQueue) removeTask(collectionID, buildID int64) {
	fptq.lock.Lock()
	defer fptq.lock.Unlock()
	if fptq.taskCount == 0 {
		return
	}
	if r, ok := fptq.route[collectionID]; ok {
		queue := r.Value.(*taskQueue)
		queue.remove(buildID)
	}
}

type runningTasksQueue struct {
	tasks map[int64]map[int64]*indexBuildTask
	lock  sync.RWMutex
}

func newRunningTasksQueue() *runningTasksQueue {
	return &runningTasksQueue{
		tasks: make(map[UniqueID]map[UniqueID]*indexBuildTask),
		lock:  sync.RWMutex{},
	}
}

func (rtq *runningTasksQueue) push(t *indexBuildTask) {
	rtq.lock.Lock()
	defer rtq.lock.Unlock()
	if _, ok := rtq.tasks[t.collectionID]; !ok {
		rtq.tasks[t.collectionID] = make(map[int64]*indexBuildTask)
	}
	rtq.tasks[t.collectionID][t.buildID] = t
}

func (rtq *runningTasksQueue) pop(collectionID, buildID int64) {
	rtq.lock.Lock()
	defer rtq.lock.Unlock()
	if _, ok := rtq.tasks[collectionID]; !ok {
		return
	}
	delete(rtq.tasks[collectionID], buildID)
	if len(rtq.tasks[collectionID]) == 0 {
		delete(rtq.tasks, collectionID)
	}
}

func (rtq *runningTasksQueue) updateTaskState(collectionID, buildID int64, state indexTaskState) {
	rtq.lock.Lock()
	defer rtq.lock.Unlock()
	tasks, ok := rtq.tasks[collectionID]
	if !ok {
		log.Warn("task is not exist in running queue", zap.Int64("collectionID", collectionID),
			zap.Int64("buildID", buildID), zap.String("new state", state.String()))
		return
	}
	task, ok2 := tasks[buildID]
	if !ok2 {
		log.Warn("task is not exist in running queue", zap.Int64("instanceID", collectionID),
			zap.Int64("buildID", buildID), zap.String("new state", state.String()))
		return
	}
	oldState := task.state
	rtq.tasks[collectionID][buildID].state = state
	log.Info("update task state success", zap.Int64("instanceID", collectionID),
		zap.Int64("buildID", buildID), zap.String("old state", oldState.String()),
		zap.String("new state", state.String()))
}

func (rtq *runningTasksQueue) getTaskState(collectionID, buildID UniqueID) indexTaskState {
	rtq.lock.RLock()
	defer rtq.lock.RUnlock()
	tasks, ok := rtq.tasks[collectionID]
	if !ok {
		return indexTaskDone
	}
	task, ok2 := tasks[buildID]
	if !ok2 {
		return indexTaskDone
	}
	return task.state
}

func (rtq *runningTasksQueue) getAllTasks() []*indexBuildTask {
	rtq.lock.RLock()
	defer rtq.lock.RUnlock()

	tasks := make([]*indexBuildTask, 0)
	for _, instanceTasks := range rtq.tasks {
		for _, task := range instanceTasks {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (rtq *runningTasksQueue) exist(t *indexBuildTask) bool {
	rtq.lock.RLock()
	defer rtq.lock.RUnlock()

	if tasks, ok := rtq.tasks[t.collectionID]; ok {
		if _, ok2 := tasks[t.buildID]; ok2 {
			return true
		}
	}
	return false
}

func (rtq *runningTasksQueue) taskCount() int {
	rtq.lock.RLock()
	defer rtq.lock.RUnlock()
	taskCount := 0
	for _, instanceTasks := range rtq.tasks {
		taskCount += len(instanceTasks)
	}
	return taskCount
}
