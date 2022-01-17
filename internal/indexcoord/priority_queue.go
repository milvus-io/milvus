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

package indexcoord

import (
	"container/heap"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

// PQItem is something we manage in a priority queue.
type PQItem struct {
	key UniqueID

	priority int // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	weight int // The weight of the item in the queue.
	// When the priority is the same, a smaller weight is more preferred.
	index int // The index of the item in the heap.

	totalMem uint64 // The total memory of the IndexNode.
}

// PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	items  []*PQItem
	lock   sync.RWMutex
	policy PeekClientPolicy
}

// Len is the length of the priority queue.
func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

// Less reports whether the element with index i
// must sort before the element with index j.
func (pq *PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return (pq.items[i].priority < pq.items[j].priority) ||
		(pq.items[i].priority == pq.items[j].priority && pq.items[i].weight < pq.items[j].weight)
}

// Swap swaps the elements with indexes i and j.
func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

// Push adds an element to the priority.
func (pq *PriorityQueue) Push(x interface{}) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	n := (*pq).Len()
	item := x.(*PQItem)
	item.index = n
	pq.items = append(pq.items, item)
}

// Pop do not call this directly.
func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}

// CheckExist checks whether the nodeID is already in the priority.
func (pq *PriorityQueue) CheckExist(nodeID UniqueID) bool {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	for _, item := range pq.items {
		if nodeID == item.key {
			return true
		}
	}
	return false
}

func (pq *PriorityQueue) getItemByKey(key UniqueID) interface{} {
	var ret interface{}
	for _, item := range pq.items {
		if item.key == key {
			ret = item
			break
		}
	}
	return ret
}

// IncPriority update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) IncPriority(key UniqueID, priority int) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	item := pq.getItemByKey(key)
	if item != nil {
		item.(*PQItem).priority += priority
		if priority > 0 {
			item.(*PQItem).weight += priority
		}
		heap.Fix(pq, item.(*PQItem).index)
	}
}

// UpdatePriority update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) UpdatePriority(key UniqueID, priority int) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	item := pq.getItemByKey(key)
	if item != nil {
		item.(*PQItem).priority = priority
		item.(*PQItem).weight = priority
		heap.Fix(pq, item.(*PQItem).index)
	}
}

// Remove deletes the corresponding item according to the key.
func (pq *PriorityQueue) Remove(key UniqueID) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	item := pq.getItemByKey(key)
	if item != nil {
		heap.Remove(pq, item.(*PQItem).index)
	}
}

// Peek picks an key with the lowest load.
func (pq *PriorityQueue) Peek(memorySize uint64, indexParams []*commonpb.KeyValuePair, typeParams []*commonpb.KeyValuePair) UniqueID {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	if pq.Len() == 0 {
		return UniqueID(-1)
	}
	return pq.policy(memorySize, indexParams, typeParams, pq)
}

// PeekAll return the key of all the items.
func (pq *PriorityQueue) PeekAll() []UniqueID {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	var ret []UniqueID
	for _, item := range pq.items {
		ret = append(ret, item.key)
	}

	return ret
}

// GetMemory get the memory info for the speicied key.
func (pq *PriorityQueue) GetMemory(key UniqueID) uint64 {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	for i := range pq.items {
		if pq.items[i].key == key {
			return pq.items[i].totalMem
		}
	}
	return 0
}

// SetMemory sets the memory info for IndexNode.
func (pq *PriorityQueue) SetMemory(key UniqueID, memorySize uint64) {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	for i := range pq.items {
		if pq.items[i].key == key {
			pq.items[i].totalMem = memorySize
			return
		}
	}
}
