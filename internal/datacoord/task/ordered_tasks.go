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

	"github.com/google/btree"
)

// orderedTasks keeps scheduler ownership ordered by task ID. Its lock protects
// only the container; callers inspect task state and invoke callbacks after
// Values has released the lock.
type orderedTasks struct {
	mu   sync.RWMutex
	tree *btree.BTreeG[orderedTask]
}

type orderedTask struct {
	id   int64
	task Task
}

func newOrderedTasks() *orderedTasks {
	return &orderedTasks{
		tree: btree.NewG(32, func(a, b orderedTask) bool { return a.id < b.id }),
	}
}

func (m *orderedTasks) GetOrInsert(id int64, task Task) (Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.tree.Get(orderedTask{id: id}); ok {
		return existing.task, true
	}
	m.tree.ReplaceOrInsert(orderedTask{id: id, task: task})
	return task, false
}

func (m *orderedTasks) GetAndRemove(id int64) (Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.tree.Delete(orderedTask{id: id})
	return entry.task, ok
}

func (m *orderedTasks) Get(id int64) (Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.tree.Get(orderedTask{id: id})
	return entry.task, ok
}

func (m *orderedTasks) Insert(id int64, task Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tree.ReplaceOrInsert(orderedTask{id: id, task: task})
}

func (m *orderedTasks) Contain(id int64) bool {
	_, ok := m.Get(id)
	return ok
}

func (m *orderedTasks) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tree.Len()
}

// Values returns a snapshot in ascending task-ID order. Copy references under
// the read lock so no task callback runs while holding the container lock.
func (m *orderedTasks) Values() []Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	values := make([]Task, 0, m.tree.Len())
	m.tree.Ascend(func(entry orderedTask) bool {
		values = append(values, entry.task)
		return true
	})
	return values
}
