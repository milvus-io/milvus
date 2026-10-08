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
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type TaskManager interface {
	Add(task Task)
	Update(task Task, actions ...UpdateAction)
	UpdateIfState(task Task, expected datapb.ImportTaskStateV2, actions ...UpdateAction) bool
	Get(taskID int64) Task
	GetBy(filters ...TaskFilter) []Task
	Remove(taskID int64)
	RemoveExpiredTasks(ctx context.Context, cutoff time.Time) int
}

type taskManager struct {
	mu sync.RWMutex // guards tasks and owners

	tasks     map[int64]Task
	owners    map[int64]Task
	startedAt map[int64]time.Time
}

func NewTaskManager() TaskManager {
	return &taskManager{
		tasks:     make(map[int64]Task),
		owners:    make(map[int64]Task),
		startedAt: make(map[int64]time.Time),
	}
}

func (m *taskManager) Add(task Task) {
	m.mu.Lock()
	if owner, ok := m.owners[task.GetTaskID()]; ok {
		m.mu.Unlock()
		if owner != task {
			task.Cancel()
		}
		mlog.Warn(context.TODO(), "duplicated task", WrapLogFields(task)...)
		return
	}
	m.tasks[task.GetTaskID()] = task
	m.owners[task.GetTaskID()] = task
	m.startedAt[task.GetTaskID()] = time.Now()
	m.mu.Unlock()
}

// Update applies actions only while task is the currently registered attempt.
// A task ID may be reused after Drop; keeping the original task object as the
// owner fences late workers and scheduler finalizers from an older attempt.
func (m *taskManager) Update(task Task, actions ...UpdateAction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateLocked(task, nil, actions...)
}

// UpdateIfState applies actions only while task is the registered attempt and
// its latest snapshot is still in expected.  The owner and state checks share
// the same critical section as the update, so a scheduler finalizer cannot
// overwrite a failure published by Execute or a replacement with the same ID.
func (m *taskManager) UpdateIfState(task Task, expected datapb.ImportTaskStateV2, actions ...UpdateAction) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updateLocked(task, &expected, actions...)
}

func (m *taskManager) updateLocked(task Task, expected *datapb.ImportTaskStateV2, actions ...UpdateAction) bool {
	taskID := task.GetTaskID()
	owner, ok := m.owners[taskID]
	if !ok || owner != task {
		return false
	}
	current, ok := m.tasks[taskID]
	if !ok || expected != nil && current.GetState() != *expected {
		return false
	}

	updatedTask := current.Clone()
	for _, action := range actions {
		action(updatedTask)
	}
	m.tasks[taskID] = updatedTask
	return true
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
		if owner, owned := m.owners[taskID]; owned {
			owner.Cancel()
		} else {
			task.Cancel()
		}
	}
	delete(m.tasks, taskID)
	delete(m.owners, taskID)
	delete(m.startedAt, taskID)
}

// RemoveExpiredTasks cancels and removes tasks older than the DataNode-wide
// retention cutoff. Removing the owner fences any late worker update.
func (m *taskManager) RemoveExpiredTasks(ctx context.Context, cutoff time.Time) int {
	type expiredTask struct {
		id        int64
		owner     Task
		state     datapb.ImportTaskStateV2
		startedAt time.Time
	}

	m.mu.Lock()
	expired := make([]expiredTask, 0)
	for taskID, task := range m.tasks {
		startedAt, ok := m.startedAt[taskID]
		if !ok || startedAt.After(cutoff) {
			continue
		}
		owner := task
		if registered, ok := m.owners[taskID]; ok {
			owner = registered
		}
		expired = append(expired, expiredTask{
			id: taskID, owner: owner, state: task.GetState(), startedAt: startedAt,
		})
		delete(m.tasks, taskID)
		delete(m.owners, taskID)
		delete(m.startedAt, taskID)
	}
	m.mu.Unlock()

	for _, task := range expired {
		task.owner.Cancel()
		mlog.Info(ctx, "reclaiming expired import task",
			mlog.FieldTaskID(task.id),
			mlog.String("state", task.state.String()),
			mlog.Duration("age", time.Since(task.startedAt)))
	}
	return len(expired)
}
