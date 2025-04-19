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
	"sync"
)

type FIFOQueue interface {
	Push(task Task)
	Pop() Task
	Get(taskID int64) Task
	Remove(taskID int64)
	TaskIDs() []int64
}

type fifoQueue struct {
	lock    sync.RWMutex
	tasks   map[int64]Task
	taskIDs []int64
}

func NewFIFOQueue() FIFOQueue {
	return &fifoQueue{
		lock:    sync.RWMutex{},
		tasks:   make(map[int64]Task, 0),
		taskIDs: make([]int64, 0),
	}
}

func (f *fifoQueue) Push(t Task) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if _, ok := f.tasks[t.GetTaskID()]; !ok {
		f.tasks[t.GetTaskID()] = t
		f.taskIDs = append(f.taskIDs, t.GetTaskID())
	}
}

func (f *fifoQueue) Pop() Task {
	f.lock.Lock()
	defer f.lock.Unlock()
	if len(f.taskIDs) == 0 {
		return nil
	}
	taskID := f.taskIDs[0]
	f.taskIDs = f.taskIDs[1:]
	task := f.tasks[taskID]
	delete(f.tasks, taskID)
	return task
}

func (f *fifoQueue) Get(taskID int64) Task {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.tasks[taskID]
}

func (f *fifoQueue) Remove(taskID int64) {
	f.lock.Lock()
	defer f.lock.Unlock()

	index := -1
	for i := range f.taskIDs {
		if f.taskIDs[i] == taskID {
			index = i
			break
		}
	}
	if index != -1 {
		f.taskIDs = append(f.taskIDs[:index], f.taskIDs[index+1:]...)
		delete(f.tasks, taskID)
	}
}

func (f *fifoQueue) TaskIDs() []int64 {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.taskIDs
}
