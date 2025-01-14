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

package datacoord

import (
	"container/heap"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/lock"
)

type Item[T any] struct {
	value    T
	priority int // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item[CompactionTask]

var _ heap.Interface = (*PriorityQueue)(nil)

func (pq PriorityQueue) Len() int { return len(pq) }

// Use planID as priority if they have same priority
func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].priority == pq[j].priority {
		return pq[i].value.GetPlanID() < pq[j].value.GetPlanID()
	}

	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item[CompactionTask])
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) Update(item *Item[CompactionTask], value CompactionTask, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

var (
	ErrFull          = errors.New("compaction queue is full")
	ErrNoSuchElement = errors.New("compaction queue has no element")
)

type Prioritizer func(t CompactionTask) int

type CompactionQueue struct {
	pq          PriorityQueue
	lock        lock.RWMutex
	prioritizer Prioritizer
	capacity    int
}

func NewCompactionQueue(capacity int, prioritizer Prioritizer) *CompactionQueue {
	return &CompactionQueue{
		pq:          make(PriorityQueue, 0),
		lock:        lock.RWMutex{},
		prioritizer: prioritizer,
		capacity:    capacity,
	}
}

func (q *CompactionQueue) Enqueue(t CompactionTask) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.capacity > 0 && len(q.pq) >= q.capacity {
		return ErrFull
	}

	heap.Push(&q.pq, &Item[CompactionTask]{value: t, priority: q.prioritizer(t)})
	return nil
}

func (q *CompactionQueue) Dequeue() (CompactionTask, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if len(q.pq) == 0 {
		return nil, ErrNoSuchElement
	}

	item := heap.Pop(&q.pq).(*Item[CompactionTask])
	return item.value, nil
}

func (q *CompactionQueue) UpdatePrioritizer(prioritizer Prioritizer) {
	q.prioritizer = prioritizer
	q.lock.Lock()
	defer q.lock.Unlock()
	for i := range q.pq {
		q.pq[i].priority = q.prioritizer(q.pq[i].value)
	}
	heap.Init(&q.pq)
}

func (q *CompactionQueue) RemoveAll(predicate func(CompactionTask) bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	f := lo.Filter[*Item[CompactionTask]](q.pq, func(i1 *Item[CompactionTask], _ int) bool {
		return !predicate(i1.value)
	})
	q.pq = f
	heap.Init(&q.pq)
}

// ForEach calls f on each item in the queue.
func (q *CompactionQueue) ForEach(f func(CompactionTask)) {
	q.lock.RLock()
	defer q.lock.RUnlock()
	lo.ForEach[*Item[CompactionTask]](q.pq, func(i *Item[CompactionTask], _ int) {
		f(i.value)
	})
}

func (q *CompactionQueue) Len() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.pq)
}

var (
	DefaultPrioritizer Prioritizer = func(task CompactionTask) int {
		return int(task.GetPlanID())
	}

	LevelPrioritizer Prioritizer = func(task CompactionTask) int {
		switch task.GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			return 1
		case datapb.CompactionType_MixCompaction:
			return 10
		case datapb.CompactionType_ClusteringCompaction:
			return 100
		default:
			return 1000
		}
	}

	MixFirstPrioritizer Prioritizer = func(task CompactionTask) int {
		switch task.GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			return 10
		case datapb.CompactionType_MixCompaction:
			return 1
		case datapb.CompactionType_ClusteringCompaction:
			return 100
		default:
			return 1000
		}
	}
)

func getPrioritizer() Prioritizer {
	p := Params.DataCoordCfg.CompactionTaskPrioritizer.GetValue()
	switch p {
	case "level":
		return LevelPrioritizer
	case "mix":
		return MixFirstPrioritizer
	default:
		return DefaultPrioritizer
	}
}
