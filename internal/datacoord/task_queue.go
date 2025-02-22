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
	"sync"
)

// schedulePolicy is the policy of scheduler.
type schedulePolicy interface {
	Push(task Task)
	// Pop get the task next ready to run.
	Pop() Task
	BatchPop(batch int) []Task
	Get(taskID UniqueID) Task
	Keys() []UniqueID
	TaskCount() int
	Exist(taskID UniqueID) bool
	Remove(taskID UniqueID)
}

var _ schedulePolicy = &fairQueuePolicy{}

type fairQueuePolicy struct {
	tasks   map[UniqueID]Task
	taskIDs []UniqueID
	lock    sync.RWMutex
}

func newFairQueuePolicy() *fairQueuePolicy {
	return &fairQueuePolicy{
		tasks:   make(map[UniqueID]Task, 0),
		taskIDs: make([]UniqueID, 0),
		lock:    sync.RWMutex{},
	}
}

func (fqp *fairQueuePolicy) Push(t Task) {
	fqp.lock.Lock()
	defer fqp.lock.Unlock()
	if _, ok := fqp.tasks[t.GetTaskID()]; !ok {
		fqp.tasks[t.GetTaskID()] = t
		fqp.taskIDs = append(fqp.taskIDs, t.GetTaskID())
	}
}

func (fqp *fairQueuePolicy) Pop() Task {
	fqp.lock.Lock()
	defer fqp.lock.Unlock()
	if len(fqp.taskIDs) == 0 {
		return nil
	}
	taskID := fqp.taskIDs[0]
	fqp.taskIDs = fqp.taskIDs[1:]
	task := fqp.tasks[taskID]
	delete(fqp.tasks, taskID)
	return task
}

func (fqp *fairQueuePolicy) BatchPop(batch int) []Task {
	fqp.lock.Lock()
	defer fqp.lock.Unlock()
	tasks := make([]Task, 0)
	if len(fqp.taskIDs) <= batch {
		for _, taskID := range fqp.taskIDs {
			task := fqp.tasks[taskID]
			delete(fqp.tasks, taskID)
			tasks = append(tasks, task)
		}
		fqp.taskIDs = make([]UniqueID, 0)
		return tasks
	}

	taskIDs := fqp.taskIDs[:batch]
	for _, taskID := range taskIDs {
		task := fqp.tasks[taskID]
		delete(fqp.tasks, taskID)
		tasks = append(tasks, task)
	}
	fqp.taskIDs = fqp.taskIDs[batch:]
	return tasks
}

func (fqp *fairQueuePolicy) Get(taskID UniqueID) Task {
	fqp.lock.RLock()
	defer fqp.lock.RUnlock()
	if len(fqp.taskIDs) == 0 {
		return nil
	}
	task := fqp.tasks[taskID]
	return task
}

func (fqp *fairQueuePolicy) TaskCount() int {
	fqp.lock.RLock()
	defer fqp.lock.RUnlock()
	return len(fqp.taskIDs)
}

func (fqp *fairQueuePolicy) Exist(taskID UniqueID) bool {
	fqp.lock.RLock()
	defer fqp.lock.RUnlock()
	_, ok := fqp.tasks[taskID]
	return ok
}

func (fqp *fairQueuePolicy) Remove(taskID UniqueID) {
	fqp.lock.Lock()
	defer fqp.lock.Unlock()

	taskIndex := -1
	for i := range fqp.taskIDs {
		if fqp.taskIDs[i] == taskID {
			taskIndex = i
			break
		}
	}
	if taskIndex != -1 {
		fqp.taskIDs = append(fqp.taskIDs[:taskIndex], fqp.taskIDs[taskIndex+1:]...)
		delete(fqp.tasks, taskID)
	}
}

func (fqp *fairQueuePolicy) Keys() []UniqueID {
	fqp.lock.RLock()
	defer fqp.lock.RUnlock()

	return fqp.taskIDs
}
