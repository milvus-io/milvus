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

type item interface {
	getPriority() int
	setPriority(priority int)
}

type baseItem struct {
	priority int
}

func (b *baseItem) getPriority() int {
	return b.priority
}

func (b *baseItem) setPriority(priority int) {
	b.priority = priority
}

type heapQueue []item

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
	i := x.(item)
	*hq = append(*hq, i)
}

func (hq *heapQueue) Pop() any {
	arr := *hq
	l := len(arr)
	ret := arr[l-1]
	*hq = arr[0 : l-1]
	return ret
}

type priorityQueue struct {
	heapQueue
}

func newPriorityQueue() priorityQueue {
	hq := make(heapQueue, 0)
	heap.Init(&hq)
	return priorityQueue{
		heapQueue: hq,
	}
}

func (pq *priorityQueue) push(item item) {
	heap.Push(&pq.heapQueue, item)
}

func (pq *priorityQueue) pop() item {
	return heap.Pop(&pq.heapQueue).(item)
}
