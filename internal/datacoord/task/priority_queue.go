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

package task

import (
	"container/heap"
	"slices"
	"sync"
)

// PriorityQueue is the policy of scheduler.
type PriorityQueue interface {
	Push(task Task)
	// Pop get the task next ready to run.
	Pop() Task
	Get(taskID int64) Task
	Remove(taskID int64)
	TaskCount() int
	TaskCountBy(filter func(Task) bool) int
	TaskIDs() []int64
	// TaskIDsByPriority returns a snapshot in Pop order without removing tasks.
	TaskIDsByPriority() []int64
}

var _ PriorityQueue = &priorityQueuePolicy{}

// priorityQueuePolicy implements a priority queue that sorts tasks by taskID (smaller taskID has higher priority)
type priorityQueuePolicy struct {
	lock  sync.RWMutex
	tasks map[int64]Task
	heap  *taskHeap
}

// taskHeap implements a min-heap for Task objects, sorted by taskID.
type taskHeap struct {
	tasks []Task
	// positions is maintained only for the live heap, never for a snapshot.
	positions map[int64]int
}

func (h taskHeap) Len() int           { return len(h.tasks) }
func (h taskHeap) Less(i, j int) bool { return h.tasks[i].GetTaskID() < h.tasks[j].GetTaskID() }
func (h taskHeap) Swap(i, j int) {
	h.tasks[i], h.tasks[j] = h.tasks[j], h.tasks[i]
	if h.positions != nil {
		h.positions[h.tasks[i].GetTaskID()] = i
		h.positions[h.tasks[j].GetTaskID()] = j
	}
}

func (h *taskHeap) Push(x interface{}) {
	task := x.(Task)
	if h.positions != nil {
		h.positions[task.GetTaskID()] = len(h.tasks)
	}
	h.tasks = append(h.tasks, task)
}

func (h *taskHeap) Pop() interface{} {
	n := len(h.tasks)
	item := h.tasks[n-1]
	h.tasks[n-1] = nil
	h.tasks = h.tasks[:n-1]
	if h.positions != nil {
		delete(h.positions, item.GetTaskID())
	}
	return item
}

// NewPriorityQueuePolicy creates a new priority queue policy
func NewPriorityQueuePolicy() *priorityQueuePolicy {
	h := &taskHeap{positions: make(map[int64]int)}
	heap.Init(h)
	return &priorityQueuePolicy{
		tasks: make(map[int64]Task),
		heap:  h,
		lock:  sync.RWMutex{},
	}
}

func (pqp *priorityQueuePolicy) Push(task Task) {
	pqp.lock.Lock()
	defer pqp.lock.Unlock()

	taskID := task.GetTaskID()
	if _, exists := pqp.tasks[taskID]; !exists {
		pqp.tasks[taskID] = task
		heap.Push(pqp.heap, task)
	}
}

func (pqp *priorityQueuePolicy) Pop() Task {
	pqp.lock.Lock()
	defer pqp.lock.Unlock()

	if pqp.heap.Len() == 0 {
		return nil
	}

	task := heap.Pop(pqp.heap).(Task)
	delete(pqp.tasks, task.GetTaskID())
	return task
}

func (pqp *priorityQueuePolicy) Get(taskID int64) Task {
	pqp.lock.RLock()
	defer pqp.lock.RUnlock()

	return pqp.tasks[taskID]
}

func (pqp *priorityQueuePolicy) TaskCount() int {
	pqp.lock.RLock()
	defer pqp.lock.RUnlock()

	return len(pqp.tasks)
}

func (pqp *priorityQueuePolicy) TaskCountBy(filter func(Task) bool) int {
	pqp.lock.RLock()
	defer pqp.lock.RUnlock()

	count := 0
	for _, task := range pqp.tasks {
		if filter(task) {
			count++
		}
	}
	return count
}

func (pqp *priorityQueuePolicy) TaskIDs() []int64 {
	pqp.lock.RLock()
	defer pqp.lock.RUnlock()

	taskIDs := make([]int64, 0, len(pqp.tasks))
	for _, t := range pqp.heap.tasks {
		taskIDs = append(taskIDs, t.GetTaskID())
	}
	return taskIDs
}

func (pqp *priorityQueuePolicy) TaskIDsByPriority() []int64 {
	pqp.lock.RLock()
	snapshot := taskHeap{tasks: slices.Clone(pqp.heap.tasks)}
	pqp.lock.RUnlock()

	// Reuse the queue's comparator without holding its lock while ordering.
	taskIDs := make([]int64, 0, snapshot.Len())
	for snapshot.Len() > 0 {
		task := heap.Pop(&snapshot).(Task)
		taskIDs = append(taskIDs, task.GetTaskID())
	}
	return taskIDs
}

func (pqp *priorityQueuePolicy) Remove(taskID int64) {
	pqp.lock.Lock()
	defer pqp.lock.Unlock()

	index, exists := pqp.heap.positions[taskID]
	if !exists {
		return
	}

	heap.Remove(pqp.heap, index)
	delete(pqp.tasks, taskID)
}
