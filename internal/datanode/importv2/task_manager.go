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

package importv2

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/log"
)

type TaskManager interface {
	Add(task Task)
	Update(taskID int64, actions ...UpdateAction)
	Get(taskID int64) Task
	GetBy(filters ...TaskFilter) []Task
	Remove(taskID int64)
}

type taskManager struct {
	mu    sync.RWMutex // guards tasks
	tasks map[int64]Task
}

func NewTaskManager() TaskManager {
	return &taskManager{
		tasks: make(map[int64]Task),
	}
}

func (m *taskManager) Add(task Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[task.GetTaskID()]; ok {
		log.Warn("duplicated task", WrapLogFields(task)...)
		return
	}
	m.tasks[task.GetTaskID()] = task
}

func (m *taskManager) Update(taskID int64, actions ...UpdateAction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[taskID]; ok {
		updatedTask := m.tasks[taskID].Clone()
		for _, action := range actions {
			action(updatedTask)
		}
		m.tasks[taskID] = updatedTask
	}
}

func (m *taskManager) Get(taskID int64) Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks[taskID]
}

func (m *taskManager) GetBy(filters ...TaskFilter) []Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]Task, 0)
OUTER:
	for _, task := range m.tasks {
		for _, f := range filters {
			if !f(task) {
				continue OUTER
			}
		}
		ret = append(ret, task)
	}
	return ret
}

func (m *taskManager) Remove(taskID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[taskID]; ok {
		task.Cancel()
	}
	delete(m.tasks, taskID)
}
