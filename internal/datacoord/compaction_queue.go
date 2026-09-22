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

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

type Item[T any] struct {
	value    T
	priority int // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue[T any] []*Item[T]

var _ heap.Interface = (*PriorityQueue[any])(nil)

func (pq PriorityQueue[T]) Len() int { return len(pq) }

func (pq PriorityQueue[T]) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue[T]) Push(x any) {
	n := len(*pq)
	item := x.(*Item[T])
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue[T]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue[T]) Update(item *Item[T], value T, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

// errNoSuchElement is an INTERNAL sentinel: caught by errors.Is inside the
// compaction inspector / scheduler loop and never serialized across any gRPC
// boundary. See docs/dev/error_sentinel_convention.md.
var errNoSuchElement = errors.New("compaction queue has no element")

type Prioritizer func(t CompactionTask) int

type CompactionQueue struct {
	pq   PriorityQueue[CompactionTask]
	lock lock.RWMutex
	// prioritizer and prioritizerName are guarded by lock.
	// Prioritizer is a func value and therefore cannot be compared with ==,
	// so the configuration name is kept alongside it as its identity.
	//
	// prioritizerName is nil while prioritizer has not been resolved from the
	// configuration -- the constructor and UpdatePrioritizer both take a func
	// directly. It is a pointer rather than a string because "" is itself a
	// settable configuration value (the RESTful alterConfig contract treats a
	// present value, including the empty string, as a set and only null as a
	// reset, and dataCoord.compaction.taskPrioritizer has no Formatter), so a
	// string zero value cannot stand in for "unset" without colliding with it.
	prioritizer     Prioritizer
	prioritizerName *string
	// capacity is an advisory limit, not a gate. It is read before a producer
	// commits to creating work (IsFull), and never enforced at Enqueue: by the
	// time a task reaches Enqueue it is already persisted, and refusing it there
	// would leave durable work with no runtime owner. Concurrent producers and
	// the scheduler's own put-backs can therefore push the queue a handful of
	// items past the limit, which is the point -- the limit exists to stop
	// unbounded growth, not to be exact. Zero means unbounded.
	capacity int
}

func NewCompactionQueue(capacity int, prioritizer Prioritizer) *CompactionQueue {
	return &CompactionQueue{
		pq:          make(PriorityQueue[CompactionTask], 0),
		lock:        lock.RWMutex{},
		prioritizer: prioritizer,
		capacity:    capacity,
	}
}

// Enqueue always accepts. Every task that reaches it is already persisted, so
// the only thing a refusal could achieve is a durable record with nothing in
// memory driving it -- stuck until the next DataCoord restart, holding its input
// segments compacting the whole time. Backpressure belongs one step earlier, at
// IsFull, before a producer commits to creating the work at all.
func (q *CompactionQueue) Enqueue(t CompactionTask) {
	q.lock.Lock()
	defer q.lock.Unlock()
	heap.Push(&q.pq, &Item[CompactionTask]{value: t, priority: q.prioritizer(t)})
}

// IsFull reports whether the queue has reached its advisory limit. Producers
// consult it before they claim segments and persist a task; nothing enforces it
// afterwards, so the answer can be stale by the time the task is enqueued. That
// is deliberate -- see capacity.
func (q *CompactionQueue) IsFull() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.capacity > 0 && len(q.pq) >= q.capacity
}

func (q *CompactionQueue) Dequeue() (CompactionTask, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if len(q.pq) == 0 {
		return nil, errNoSuchElement
	}

	item := heap.Pop(&q.pq).(*Item[CompactionTask])
	return item.value, nil
}

// UpdatePrioritizer sets the prioritizer out of band, so the queue no longer
// knows which configuration value it corresponds to; the next SyncPrioritizer
// re-adopts from the configuration whatever name it is given.
func (q *CompactionQueue) UpdatePrioritizer(prioritizer Prioritizer) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.prioritizerName = nil
	q.updatePrioritizerLocked(prioritizer)
}

// SyncPrioritizer re-prioritizes the queue only when the configured prioritizer
// actually changed. It is safe to call on every scheduling tick.
func (q *CompactionQueue) SyncPrioritizer(name string) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.prioritizerName != nil && *q.prioritizerName == name {
		return
	}
	q.prioritizerName = &name
	q.updatePrioritizerLocked(getPrioritizerByName(name))
}

func (q *CompactionQueue) updatePrioritizerLocked(prioritizer Prioritizer) {
	q.prioritizer = prioritizer
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
		return int(task.GetTask().GetPlanID())
	}

	LevelPrioritizer Prioritizer = func(task CompactionTask) int {
		switch task.GetTask().GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			return 1
		case datapb.CompactionType_MixCompaction:
			return 10
		case datapb.CompactionType_BumpSchemaVersionCompaction:
			return 10
		case datapb.CompactionType_ClusteringCompaction:
			return 100
		default:
			return 1000
		}
	}

	MixFirstPrioritizer Prioritizer = func(task CompactionTask) int {
		switch task.GetTask().GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			return 10
		case datapb.CompactionType_MixCompaction:
			return 1
		case datapb.CompactionType_BumpSchemaVersionCompaction:
			return 1
		case datapb.CompactionType_ClusteringCompaction:
			return 100
		default:
			return 1000
		}
	}
)

func getPrioritizerName() string {
	return Params.DataCoordCfg.CompactionTaskPrioritizer.GetValue()
}

func getPrioritizerByName(name string) Prioritizer {
	switch name {
	case "level":
		return LevelPrioritizer
	case "mix":
		return MixFirstPrioritizer
	default:
		return DefaultPrioritizer
	}
}

func getPrioritizer() Prioritizer {
	return getPrioritizerByName(getPrioritizerName())
}
