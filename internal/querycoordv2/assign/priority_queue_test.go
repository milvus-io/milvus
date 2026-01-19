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
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBaseItem_GetSetPriority tests BaseItem priority methods
func TestBaseItem_GetSetPriority(t *testing.T) {
	item := &BaseItem{}

	// Initial priority should be 0
	assert.Equal(t, 0, item.getPriority())

	// Set priority
	item.setPriority(100)
	assert.Equal(t, 100, item.getPriority())
}

// TestNewPriorityQueue tests priority queue creation
func TestNewPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue()
	assert.NotNil(t, pq)
	assert.Equal(t, 0, pq.Len())
}

// TestNewPriorityQueuePtr tests priority queue pointer creation
func TestNewPriorityQueuePtr(t *testing.T) {
	pq := NewPriorityQueuePtr()
	assert.NotNil(t, pq)
	assert.Equal(t, 0, pq.Len())
}

// TestPriorityQueue_PushPop tests basic push and pop operations
func TestPriorityQueue_PushPop(t *testing.T) {
	pq := NewPriorityQueue()

	// Create items with different priorities
	item1 := NewNodeItem(100, 1)
	item2 := NewNodeItem(50, 2)
	item3 := NewNodeItem(200, 3)

	// Push items
	pq.Push(&item1)
	pq.Push(&item2)
	pq.Push(&item3)

	assert.Equal(t, 3, pq.Len())

	// Pop should return items in priority order (lowest first)
	// item2 has priority 50 (lowest)
	popped1 := pq.Pop().(*NodeItem)
	assert.Equal(t, int64(2), popped1.NodeID)

	// item1 has priority 100
	popped2 := pq.Pop().(*NodeItem)
	assert.Equal(t, int64(1), popped2.NodeID)

	// item3 has priority 200 (highest)
	popped3 := pq.Pop().(*NodeItem)
	assert.Equal(t, int64(3), popped3.NodeID)

	assert.Equal(t, 0, pq.Len())
}

// TestPriorityQueue_SingleItem tests with single item
func TestPriorityQueue_SingleItem(t *testing.T) {
	pq := NewPriorityQueue()

	item := NewNodeItem(100, 1)
	pq.Push(&item)

	assert.Equal(t, 1, pq.Len())

	popped := pq.Pop().(*NodeItem)
	assert.Equal(t, int64(1), popped.NodeID)
	assert.Equal(t, 0, pq.Len())
}

// TestPriorityQueue_SamePriority tests items with same priority
func TestPriorityQueue_SamePriority(t *testing.T) {
	pq := NewPriorityQueue()

	// Create items with same priority
	item1 := NewNodeItem(100, 1)
	item2 := NewNodeItem(100, 2)
	item3 := NewNodeItem(100, 3)

	pq.Push(&item1)
	pq.Push(&item2)
	pq.Push(&item3)

	assert.Equal(t, 3, pq.Len())

	// All have same priority, should still pop 3 items
	_ = pq.Pop()
	_ = pq.Pop()
	_ = pq.Pop()

	assert.Equal(t, 0, pq.Len())
}

// TestPriorityQueue_RePush tests pushing item back after popping
func TestPriorityQueue_RePush(t *testing.T) {
	pq := NewPriorityQueue()

	item1 := NewNodeItem(50, 1)
	item2 := NewNodeItem(100, 2)

	pq.Push(&item1)
	pq.Push(&item2)

	// Pop the lowest priority item
	popped := pq.Pop().(*NodeItem)
	assert.Equal(t, int64(1), popped.NodeID)
	assert.Equal(t, 1, pq.Len())

	// Modify and push back
	popped.AddCurrentScoreDelta(200)
	pq.Push(popped)

	assert.Equal(t, 2, pq.Len())

	// Now item2 should be popped first (lower priority)
	nextPopped := pq.Pop().(*NodeItem)
	assert.Equal(t, int64(2), nextPopped.NodeID)
}

// TestPriorityQueue_LargeDataset tests with many items
func TestPriorityQueue_LargeDataset(t *testing.T) {
	pq := NewPriorityQueue()

	// Push 100 items with random priorities
	for i := 0; i < 100; i++ {
		item := NewNodeItem(i*10, int64(i))
		pq.Push(&item)
	}

	assert.Equal(t, 100, pq.Len())

	// Pop all items and verify they come out in priority order
	lastPriority := -1
	for i := 0; i < 100; i++ {
		item := pq.Pop().(*NodeItem)
		currentPriority := item.getPriority()
		assert.GreaterOrEqual(t, currentPriority, lastPriority)
		lastPriority = currentPriority
	}

	assert.Equal(t, 0, pq.Len())
}

// TestPriorityQueue_WithAssignedScore tests priority ordering with assigned scores
func TestPriorityQueue_WithAssignedScore(t *testing.T) {
	pq := NewPriorityQueue()

	item1 := NewNodeItem(100, 1)
	item1.SetAssignedScore(50) // Priority: 100 - 50 = 50

	item2 := NewNodeItem(80, 2)
	item2.SetAssignedScore(50) // Priority: 80 - 50 = 30

	item3 := NewNodeItem(120, 3)
	item3.SetAssignedScore(50) // Priority: 120 - 50 = 70

	pq.Push(&item1)
	pq.Push(&item2)
	pq.Push(&item3)

	// Should pop in order: item2 (30), item1 (50), item3 (70)
	popped1 := pq.Pop().(*NodeItem)
	assert.Equal(t, int64(2), popped1.NodeID)

	popped2 := pq.Pop().(*NodeItem)
	assert.Equal(t, int64(1), popped2.NodeID)

	popped3 := pq.Pop().(*NodeItem)
	assert.Equal(t, int64(3), popped3.NodeID)
}

// TestHeapQueue_Len tests heap queue length
func TestHeapQueue_Len(t *testing.T) {
	hq := make(heapQueue, 0)
	assert.Equal(t, 0, hq.Len())

	item := &BaseItem{}
	hq = append(hq, item)
	assert.Equal(t, 1, hq.Len())
}

// TestHeapQueue_Less tests heap queue comparison
func TestHeapQueue_Less(t *testing.T) {
	item1 := &BaseItem{priority: 10}
	item2 := &BaseItem{priority: 20}

	hq := heapQueue{item1, item2}

	assert.True(t, hq.Less(0, 1))
	assert.False(t, hq.Less(1, 0))
}

// TestHeapQueue_Swap tests heap queue swap
func TestHeapQueue_Swap(t *testing.T) {
	item1 := &BaseItem{priority: 10}
	item2 := &BaseItem{priority: 20}

	hq := heapQueue{item1, item2}

	hq.Swap(0, 1)

	assert.Equal(t, 20, hq[0].getPriority())
	assert.Equal(t, 10, hq[1].getPriority())
}
