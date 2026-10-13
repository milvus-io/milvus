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

package assign

import (
	"container/heap"
)

// Item has a comparable identity (normally a pointer). After changing the
// priority of a queued item, call Fix before changing another queued item.
type Item interface {
	getPriority() int
	setPriority(priority int)
}

type BaseItem struct {
	priority int
}

func (b *BaseItem) getPriority() int {
	return b.priority
}

func (b *BaseItem) setPriority(priority int) {
	b.priority = priority
}

// SetPriority lets items owned by another package update the embedded queue
// priority. A same-named unexported method in that package does not override
// assign.Item's method. Call the owning queue's Fix after changing this value.
func (b *BaseItem) SetPriority(priority int) {
	b.setPriority(priority)
}

type heapQueue []Item

func (hq heapQueue) Len() int {
	return len(hq)
}

func (hq heapQueue) Less(i, j int) bool {
	return hq[i].getPriority() < hq[j].getPriority()
}

func (hq heapQueue) Swap(i, j int) {
	hq[i], hq[j] = hq[j], hq[i]
}

func (hq *heapQueue) Push(x any) {
	i := x.(Item)
	*hq = append(*hq, i)
}

func (hq *heapQueue) Pop() any {
	arr := *hq
	l := len(arr)
	ret := arr[l-1]
	arr[l-1] = nil
	*hq = arr[0 : l-1]
	return ret
}

// indexedHeapQueue tracks membership per queue, since an item may be shared by
// multiple queues. Items must have comparable identities (normally pointers).
type indexedHeapQueue struct {
	heapQueue
	indices map[Item]int
}

func (hq *indexedHeapQueue) Swap(i, j int) {
	hq.heapQueue.Swap(i, j)
	hq.indices[hq.heapQueue[i]] = i
	hq.indices[hq.heapQueue[j]] = j
}

func (hq *indexedHeapQueue) Push(x any) {
	item := x.(Item)
	hq.indices[item] = hq.Len()
	hq.heapQueue.Push(item)
}

func (hq *indexedHeapQueue) Pop() any {
	item := hq.heapQueue.Pop().(Item)
	delete(hq.indices, item)
	return item
}

// PriorityQueue must not be copied after the first insertion.
type PriorityQueue struct {
	indexedHeapQueue
}

func NewPriorityQueue() PriorityQueue {
	return PriorityQueue{
		indexedHeapQueue: indexedHeapQueue{indices: make(map[Item]int)},
	}
}

func NewPriorityQueuePtr() *PriorityQueue {
	pq := NewPriorityQueue()
	return &pq
}

// Push inserts an item, or repairs its priority if it is already queued.
func (pq *PriorityQueue) Push(item Item) {
	if pq.Fix(item) {
		return
	}
	if pq.indices == nil {
		pq.indices = make(map[Item]int)
	}
	heap.Push(&pq.indexedHeapQueue, item)
}

func (pq *PriorityQueue) Pop() Item {
	return heap.Pop(&pq.indexedHeapQueue).(Item)
}

// Fix repairs the heap after changing one queued item's priority.
// It returns false if the item is not queued.
func (pq *PriorityQueue) Fix(item Item) bool {
	index, ok := pq.indices[item]
	if !ok {
		return false
	}
	heap.Fix(&pq.indexedHeapQueue, index)
	return true
}

// Remove removes an item by identity, returning false if it is not queued.
func (pq *PriorityQueue) Remove(item Item) bool {
	index, ok := pq.indices[item]
	if !ok {
		return false
	}
	heap.Remove(&pq.indexedHeapQueue, index)
	return true
}
