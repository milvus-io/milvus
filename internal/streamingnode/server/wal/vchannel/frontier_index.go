package vchannel

import (
	"container/heap"
	"math"
	"sync"
)

type frontierHeapItem[K comparable] struct {
	key   K
	value uint64
}

type frontierHeap[K comparable] struct {
	values  []frontierHeapItem[K]
	indexes map[K]int
}

func (h frontierHeap[K]) Len() int           { return len(h.values) }
func (h frontierHeap[K]) Less(i, j int) bool { return h.values[i].value < h.values[j].value }
func (h frontierHeap[K]) Swap(i, j int) {
	h.values[i], h.values[j] = h.values[j], h.values[i]
	h.indexes[h.values[i].key] = i
	h.indexes[h.values[j].key] = j
}

func (h *frontierHeap[K]) Push(value any) {
	item := value.(frontierHeapItem[K])
	h.indexes[item.key] = len(h.values)
	h.values = append(h.values, item)
}

func (h *frontierHeap[K]) Pop() any {
	old := h.values
	last := old[len(old)-1]
	var zero frontierHeapItem[K]
	old[len(old)-1] = zero
	h.values = old[:len(old)-1]
	delete(h.indexes, last.key)
	return last
}

type minimumFrontierIndex[K comparable] struct {
	mu    sync.Mutex
	items map[K]int
	heap  frontierHeap[K]
}

func newMinimumFrontierIndex[K comparable]() *minimumFrontierIndex[K] {
	items := make(map[K]int)
	return &minimumFrontierIndex[K]{
		items: items,
		heap:  frontierHeap[K]{indexes: items},
	}
}

func (i *minimumFrontierIndex[K]) Update(key K, value uint64) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	index, ok := i.items[key]
	if ok && i.heap.values[index].value == value {
		return false
	}
	if ok {
		i.heap.values[index].value = value
		heap.Fix(&i.heap, index)
		return true
	}
	heap.Push(&i.heap, frontierHeapItem[K]{key: key, value: value})
	return true
}

func (i *minimumFrontierIndex[K]) Remove(key K) {
	i.mu.Lock()
	if index, ok := i.items[key]; ok {
		heap.Remove(&i.heap, index)
	}
	i.mu.Unlock()
}

func (i *minimumFrontierIndex[K]) Minimum() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.heap.Len() > 0 {
		return i.heap.values[0].value
	}
	return math.MaxUint64
}

func (i *minimumFrontierIndex[K]) Len() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.heap.Len()
}

type segmentFrontierScope struct {
	collectionID int64
	partitionID  int64
}

type segmentFrontierIndex struct {
	mu         sync.Mutex
	all        *minimumFrontierIndex[segmentFrontierScope]
	scopes     map[int64]segmentFrontierScope
	partitions map[segmentFrontierScope]*minimumFrontierIndex[int64]
}

func newSegmentFrontierIndex() *segmentFrontierIndex {
	return &segmentFrontierIndex{
		all:        newMinimumFrontierIndex[segmentFrontierScope](),
		scopes:     make(map[int64]segmentFrontierScope),
		partitions: make(map[segmentFrontierScope]*minimumFrontierIndex[int64]),
	}
}

func (i *segmentFrontierIndex) Update(segmentID, collectionID, partitionID int64, frontier uint64) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	scope := segmentFrontierScope{collectionID: collectionID, partitionID: partitionID}
	if previous, ok := i.scopes[segmentID]; ok && previous != scope {
		previousPartition := i.partitions[previous]
		previousPartition.Remove(segmentID)
		if previousPartition.Len() == 0 {
			delete(i.partitions, previous)
			i.all.Remove(previous)
		} else {
			i.all.Update(previous, previousPartition.Minimum())
		}
	}
	i.scopes[segmentID] = scope
	partition := i.partitions[scope]
	if partition == nil {
		partition = newMinimumFrontierIndex[int64]()
		i.partitions[scope] = partition
	}
	partitionChanged := partition.Update(segmentID, frontier)
	allChanged := i.all.Update(scope, partition.Minimum())
	return allChanged || partitionChanged
}

func (i *segmentFrontierIndex) All() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.all.Minimum()
}

func (i *segmentFrontierIndex) Partition(collectionID, partitionID int64) uint64 {
	i.mu.Lock()
	partition := i.partitions[segmentFrontierScope{collectionID: collectionID, partitionID: partitionID}]
	if partition == nil {
		i.mu.Unlock()
		return math.MaxUint64
	}
	minimum := partition.Minimum()
	i.mu.Unlock()
	return minimum
}

func (i *segmentFrontierIndex) Remove(segmentID int64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	scope, ok := i.scopes[segmentID]
	if !ok {
		return
	}
	delete(i.scopes, segmentID)
	partition := i.partitions[scope]
	partition.Remove(segmentID)
	if partition.Len() == 0 {
		delete(i.partitions, scope)
		i.all.Remove(scope)
		return
	}
	i.all.Update(scope, partition.Minimum())
}
