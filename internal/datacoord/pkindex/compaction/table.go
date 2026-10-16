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

package compaction

import (
	"sync"

	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
)

// taskTable holds the tasks from their creation until they finish or fail.
// It is the source of the in-flight outputs that the GC keeps, and it keeps
// one task per vchannel. It lives in memory only: a coordinator restart
// forgets every task, which is safe because a forgotten task is never
// committed and its outputs are collected as orphans.
type taskTable struct {
	mu   sync.RWMutex
	byID map[int64]*Task
	byVC map[manifest.Target]*Task
}

func newTaskTable() *taskTable {
	return &taskTable{byID: map[int64]*Task{}, byVC: map[manifest.Target]*Task{}}
}

// add puts t in the table, or returns false when its vchannel has a task.
func (tb *taskTable) add(t *Task) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if _, busy := tb.byVC[t.target]; busy {
		return false
	}
	tb.byID[t.taskID] = t
	tb.byVC[t.target] = t
	return true
}

func (tb *taskTable) remove(taskID int64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	t, ok := tb.byID[taskID]
	if !ok {
		return
	}
	delete(tb.byID, taskID)
	delete(tb.byVC, t.target)
}

func (tb *taskTable) has(target manifest.Target) bool {
	tb.mu.RLock()
	defer tb.mu.RUnlock()
	_, ok := tb.byVC[target]
	return ok
}

func (tb *taskTable) len() int {
	tb.mu.RLock()
	defer tb.mu.RUnlock()
	return len(tb.byID)
}

// inFlightOutputs returns the IDs of the current output range of the task of
// target, or nil.
func (tb *taskTable) inFlightOutputs(target manifest.Target) []int64 {
	tb.mu.RLock()
	t, ok := tb.byVC[target]
	tb.mu.RUnlock()
	if !ok {
		return nil
	}
	begin, end := t.outputRange()
	if end <= begin {
		return nil
	}
	ids := make([]int64, 0, end-begin)
	for id := begin; id < end; id++ {
		ids = append(ids, id)
	}
	return ids
}
