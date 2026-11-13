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

// One oldest-eligible dispatch after each burst bounds starvation in dispatch
// opportunities while capacity permits, not wall-clock time. Running tasks are
// never preempted.
const priorityBurst = 8

func (h *taskHandle) SetPriority(priority int32) bool {
	validatePriority(priority)
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
	oldBand := priorityBand(entry.priority)
	entry.priority = priority
	if entry.readyIndex >= 0 {
		heap.Fix(&s.ready, entry.readyIndex)
		if band := priorityBand(priority); band != oldBand {
			heap.Remove(&s.oldest[oldBand], entry.ageIndex)
			heap.Push(&s.oldest[band], entry)
		}
		s.cond.Broadcast()
	}
	// Delayed tasks retain their deadline. Running tasks use the new value
	// only if Execute subsequently requests a retry.
	return true
}

func (s *nodeScheduler) enqueueLocked(entry *taskEntry) {
	s.sequence++
	entry.readyOrder = s.sequence
	heap.Push(&s.ready, entry)
	heap.Push(&s.oldest[priorityBand(entry.priority)], entry)
}

func (s *nodeScheduler) removeReadyLocked(entry *taskEntry) {
	heap.Remove(&s.oldest[priorityBand(entry.priority)], entry.ageIndex)
	heap.Remove(&s.ready, entry.readyIndex)
	if len(s.ready) == 0 {
		s.ready = nil
		s.priorityDispatches = 0
	}
}

// The priority heap and per-band age heaps retain two indexes per ready task.
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

// ageTasks selects the oldest eligible task without scanning a backlog whose
// band has exhausted its worker quota. Reprioritization retains readyOrder.
type ageTasks []*taskEntry

func (h ageTasks) Len() int           { return len(h) }
func (h ageTasks) Less(i, j int) bool { return h[i].readyOrder < h[j].readyOrder }
func (h ageTasks) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].ageIndex, h[j].ageIndex = i, j
}

func (h *ageTasks) Push(value any) {
	entry := value.(*taskEntry)
	entry.ageIndex = len(*h)
	*h = append(*h, entry)
}

func (h *ageTasks) Pop() any {
	last := len(*h) - 1
	entry := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	entry.ageIndex = -1
	if len(*h) == 0 {
		*h = nil
	}
	return entry
}

const priorityBands = 3

func priorityBand(priority int32) int {
	switch {
	case priority <= -10:
		return 0
	case priority < 0:
		return 1
	default:
		return 2
	}
}

func (s *nodeScheduler) canRunLocked(band int) bool {
	percentages := [priorityBands]int{50, 75, 100}
	for i := band; i < priorityBands; i++ {
		// Divide first to avoid overflowing at large concurrency values.
		limit := max(1, s.concurrency/100*percentages[i]+s.concurrency%100*percentages[i]/100)
		if s.running[i] >= limit {
			return false
		}
	}
	return true
}

func (s *nodeScheduler) nextReadyLocked() *taskEntry {
	if len(s.ready) == 0 {
		return nil
	}
	entry := s.ready[0]
	// Eligibility is monotone in priority: if the highest band is blocked,
	// every lower band is blocked by the same cumulative limit.
	if !s.canRunLocked(priorityBand(entry.priority)) {
		return nil
	}
	if s.priorityDispatches >= priorityBurst {
		for band, queue := range s.oldest {
			if len(queue) > 0 && queue[0].readyOrder < entry.readyOrder && s.canRunLocked(band) {
				entry = queue[0]
			}
		}
		s.priorityDispatches = 0
	} else {
		s.priorityDispatches++
	}
	return entry
}

func (s *nodeScheduler) releaseWorkerLocked(entry *taskEntry) {
	if entry.runningBand < 0 {
		return
	}
	for band := entry.runningBand; band < priorityBands; band++ {
		s.running[band]--
	}
	entry.runningBand = -1
	s.cond.Broadcast()
}
