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

package nodescheduler

import "container/heap"

// One oldest-ready dispatch after each burst bounds starvation in dispatch
// opportunities, not wall-clock time. Running tasks are never preempted.
const priorityBurst = 8

func (h *taskHandle) SetPriority(priority int32) bool {
	h.mu.Lock()
	update := h.setPriority
	h.mu.Unlock()
	return update != nil && update(priority)
}

func (s *nodeScheduler) setPriority(entry *taskEntry, priority int32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry.handle == nil {
		return false
	}
	entry.priority = priority
	if entry.queued != nil {
		heap.Fix(&s.ready, entry.readyIndex)
	}
	// Delayed tasks retain their deadline. Running tasks use the new value
	// only if Execute subsequently requests a retry.
	return true
}

func (s *nodeScheduler) enqueueLocked(entry *taskEntry) {
	s.sequence++
	entry.readyOrder = s.sequence
	entry.queued = s.queue.PushBack(entry)
	heap.Push(&s.ready, entry)
}

func (s *nodeScheduler) removeReadyLocked(entry *taskEntry) {
	s.queue.Remove(entry.queued)
	entry.queued = nil
	heap.Remove(&s.ready, entry.readyIndex)
	if len(s.ready) == 0 {
		s.ready = nil
		s.priorityDispatches = 0
	}
}

// The existing list retains ready age; the heap supplies priority selection.
// Reprioritization preserves age and ties use FIFO order.
type readyTasks []*taskEntry

func (h readyTasks) Len() int { return len(h) }

func (h readyTasks) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].readyOrder < h[j].readyOrder
}

func (h readyTasks) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].readyIndex, h[j].readyIndex = i, j
}

func (h *readyTasks) Push(value any) {
	entry := value.(*taskEntry)
	entry.readyIndex = len(*h)
	*h = append(*h, entry)
}

func (h *readyTasks) Pop() any {
	last := len(*h) - 1
	entry := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	entry.readyIndex = -1
	return entry
}
