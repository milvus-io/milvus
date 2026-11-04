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

// delayedTasks orders retries by deadline, then FIFO for equal deadlines.
// heapIndex allows cancellation without scanning the delayed queue.
type delayedTasks []*taskEntry

func (h delayedTasks) Len() int { return len(h) }
func (h delayedTasks) Less(i, j int) bool {
	if h[i].nextRun.Equal(h[j].nextRun) {
		return h[i].order < h[j].order
	}
	return h[i].nextRun.Before(h[j].nextRun)
}

func (h delayedTasks) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex, h[j].heapIndex = i, j
}

func (h *delayedTasks) Push(value any) {
	entry := value.(*taskEntry)
	entry.heapIndex = len(*h)
	*h = append(*h, entry)
}

func (h *delayedTasks) Pop() any {
	last := len(*h) - 1
	entry := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	entry.heapIndex = -1
	return entry
}
