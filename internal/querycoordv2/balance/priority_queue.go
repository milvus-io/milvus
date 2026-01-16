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

package balance

import (
	"container/heap"
)

type Item interface {
	getPriority() int
	setPriority(priority int)
	getIndex() int
	setIndex(idx int)
}

type BaseItem struct {
	priority int
	index    int
}

func (b *BaseItem) getPriority() int {
	return b.priority
}

func (b *BaseItem) setPriority(priority int) {
	b.priority = priority
}

func (b *BaseItem) getIndex() int {
	return b.index
}

func (b *BaseItem) setIndex(idx int) {
	b.index = idx
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
	// update indices after swap
	hq[i].setIndex(i)
	hq[j].setIndex(j)
}

func (hq *heapQueue) Push(x any) {
	i := x.(Item)
	*hq = append(*hq, i)
	// set index for the newly pushed item
	i.setIndex(len(*hq) - 1)
}

func (hq *heapQueue) Pop() any {
	arr := *hq
	l := len(arr)
	ret := arr[l-1]
	*hq = arr[0 : l-1]
	// clear index for popped item
	ret.setIndex(-1)
	return ret
}

type PriorityQueue struct {
	heapQueue
}

func NewPriorityQueue() PriorityQueue {
	hq := make(heapQueue, 0)
	heap.Init(&hq)
	return PriorityQueue{
		heapQueue: hq,
	}
}

func NewPriorityQueuePtr() *PriorityQueue {
	hq := make(heapQueue, 0)
	heap.Init(&hq)
	return &PriorityQueue{
		heapQueue: hq,
	}
}

func (pq *PriorityQueue) Push(item Item) {
	heap.Push(&pq.heapQueue, item)
}

func (pq *PriorityQueue) Pop() Item {
	return heap.Pop(&pq.heapQueue).(Item)
}

func (pq *PriorityQueue) Fix(it Item) {
	heap.Fix(&pq.heapQueue, it.getIndex())
}
