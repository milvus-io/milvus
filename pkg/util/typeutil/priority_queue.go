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

package typeutil

import (
	"container/heap"
)

type Comparator[T any] func(a, b T) bool

type binaryHeap[T any] struct {
	inner []T
	cmp   Comparator[T]
}

func (h binaryHeap[T]) Len() int {
	return len(h.inner)
}

func (h binaryHeap[T]) Less(i, j int) bool {
	return h.cmp(h.inner[i], h.inner[j])
}

func (h binaryHeap[T]) Swap(i, j int) {
	h.inner[i], h.inner[j] = h.inner[j], h.inner[i]
}

func (h *binaryHeap[T]) Push(x any) {
	i := x.(T)
	h.inner = append(h.inner, i)
}

func (h *binaryHeap[T]) Pop() any {
	arr := h.inner
	l := len(arr)
	ret := arr[l-1]
	h.inner = arr[0 : l-1]
	return ret
}

type PriorityQueue[T any] struct {
	inner binaryHeap[T]
}

// NewPriorityQueue returns a priority queue,
// which compares items with the given comparator,
// cmp should returns true if a has less priority than b,
// which means a will be popped later than b.
func NewPriorityQueue[T any](cmp Comparator[T]) PriorityQueue[T] {
	inner := binaryHeap[T]{
		inner: make([]T, 0),
		cmp:   cmp,
	}
	return PriorityQueue[T]{
		inner: inner,
	}
}

func (pq *PriorityQueue[T]) Push(item T) {
	heap.Push(&pq.inner, item)
}

func (pq *PriorityQueue[T]) Pop() T {
	return heap.Pop(&pq.inner).(T)
}
