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

	"github.com/milvus-io/milvus/internal/metastore"
)

type ImportTaskManager interface {
	Add(task ImportTask) error
	Update(taskID int64, actions ...UpdateAction) error
	Get(taskID int64) ImportTask
	GetBy(filters ...ImportTaskFilter) []ImportTask
	Remove(taskID int64) error
}

type importTaskManager struct {
	mu    sync.RWMutex // guards tasks
	tasks map[int64]ImportTask

	catalog metastore.DataCoordCatalog
}

func (m *importTaskManager) Add(task ImportTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.catalog.SaveImportTask(task.(*importTask).ImportTaskV2)
	if err != nil {
		return err
	}
	m.tasks[task.GetTaskID()] = task
	return nil
}

func (m *importTaskManager) Update(taskID int64, actions ...UpdateAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[taskID]; ok {
		updatedTask := task.Clone()
		for _, action := range actions {
			action(updatedTask)
		}
		err := m.catalog.SaveImportTask(updatedTask.(*importTask).ImportTaskV2)
		if err != nil {
			return err
		}
		m.tasks[taskID] = updatedTask
	}

	return nil
}

func (m *importTaskManager) Get(taskID int64) ImportTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks[taskID]
}

func (m *importTaskManager) GetBy(filters ...ImportTaskFilter) []ImportTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]ImportTask, 0)
	for _, task := range m.tasks {
		for _, f := range filters {
			if !f(task) {
				continue
			}
		}
		ret = append(ret, task)
	}
	return ret
}

func (m *importTaskManager) Remove(taskID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[taskID]; ok {
		err := m.catalog.DropImportTask(taskID)
		if err != nil {
			return err
		}
		delete(m.tasks, taskID)
	}
	return nil
}
