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
	"sync"
)

// PriorityQueue is the policy of scheduler.
type PriorityQueue interface {
	Push(task Task)
	// Pop get the task next ready to run.
	Pop() Task
	Get(taskID int64) Task
	Remove(taskID int64)
	TaskIDs() []int64
}

var _ PriorityQueue = &priorityQueuePolicy{}

// priorityQueuePolicy implements a priority queue that sorts tasks by taskID (smaller taskID has higher priority)
type priorityQueuePolicy struct {
	lock  sync.RWMutex
	tasks map[int64]Task
	heap  *taskHeap
}

// taskHeap implements a min-heap for Task objects, sorted by taskID
type taskHeap []Task

func (h taskHeap) Len() int           { return len(h) }
func (h taskHeap) Less(i, j int) bool { return h[i].GetTaskID() < h[j].GetTaskID() }
func (h taskHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *taskHeap) Push(x interface{}) {
	*h = append(*h, x.(Task))
}

func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// NewPriorityQueuePolicy creates a new priority queue policy
func NewPriorityQueuePolicy() *priorityQueuePolicy {
	h := &taskHeap{}
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

func (pqp *priorityQueuePolicy) TaskIDs() []int64 {
	pqp.lock.RLock()
	defer pqp.lock.RUnlock()

	taskIDs := make([]int64, 0, len(pqp.tasks))
	for _, t := range *pqp.heap {
		taskIDs = append(taskIDs, t.GetTaskID())
	}
	return taskIDs
}

func (pqp *priorityQueuePolicy) Remove(taskID int64) {
	pqp.lock.Lock()
	defer pqp.lock.Unlock()

	if _, exists := pqp.tasks[taskID]; !exists {
		return
	}

	delete(pqp.tasks, taskID)

	// Find and remove from heap
	for i, task := range *pqp.heap {
		if task.GetTaskID() == taskID {
			heap.Remove(pqp.heap, i)
			break
		}
	}
}
