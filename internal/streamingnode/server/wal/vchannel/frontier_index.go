package vchannel

import (
	"container/heap"
	"math"
	"sync"
)

type frontierHeapItem[K comparable] struct {
	key   K
	value uint64
	index int
}

type frontierHeap[K comparable] []*frontierHeapItem[K]

func (h frontierHeap[K]) Len() int           { return len(h) }
func (h frontierHeap[K]) Less(i, j int) bool { return h[i].value < h[j].value }
func (h frontierHeap[K]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *frontierHeap[K]) Push(value any) {
	item := value.(*frontierHeapItem[K])
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *frontierHeap[K]) Pop() any {
	old := *h
	last := old[len(old)-1]
	old[len(old)-1] = nil
	last.index = -1
	*h = old[:len(old)-1]
	return last
}

type minimumFrontierIndex[K comparable] struct {
	mu    sync.Mutex
	items map[K]*frontierHeapItem[K]
	heap  frontierHeap[K]
}

func newMinimumFrontierIndex[K comparable]() *minimumFrontierIndex[K] {
	return &minimumFrontierIndex[K]{items: make(map[K]*frontierHeapItem[K])}
}

func (i *minimumFrontierIndex[K]) Update(key K, value uint64) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	item, ok := i.items[key]
	if ok && item.value == value {
		return false
	}
	if ok {
		item.value = value
		heap.Fix(&i.heap, item.index)
		return true
	}
	item = &frontierHeapItem[K]{key: key, value: value}
	i.items[key] = item
	heap.Push(&i.heap, item)
	return true
}

func (i *minimumFrontierIndex[K]) Remove(key K) {
	i.mu.Lock()
	if item, ok := i.items[key]; ok {
		heap.Remove(&i.heap, item.index)
		delete(i.items, key)
	}
	i.mu.Unlock()
}

func (i *minimumFrontierIndex[K]) Minimum() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.heap.Len() > 0 {
		return i.heap[0].value
	}
	return math.MaxUint64
}

type segmentFrontierScope struct {
	collectionID int64
	partitionID  int64
}

type segmentFrontierIndex struct {
	mu         sync.Mutex
	all        *minimumFrontierIndex[int64]
	scopes     map[int64]segmentFrontierScope
	partitions map[segmentFrontierScope]*minimumFrontierIndex[int64]
}

func newSegmentFrontierIndex() *segmentFrontierIndex {
	return &segmentFrontierIndex{
		all:        newMinimumFrontierIndex[int64](),
		scopes:     make(map[int64]segmentFrontierScope),
		partitions: make(map[segmentFrontierScope]*minimumFrontierIndex[int64]),
	}
}

func (i *segmentFrontierIndex) Update(segmentID, collectionID, partitionID int64, frontier uint64) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	scope := segmentFrontierScope{collectionID: collectionID, partitionID: partitionID}
	if previous, ok := i.scopes[segmentID]; ok && previous != scope {
		i.partitions[previous].Remove(segmentID)
	}
	i.scopes[segmentID] = scope
	partition := i.partitions[scope]
	if partition == nil {
		partition = newMinimumFrontierIndex[int64]()
		i.partitions[scope] = partition
	}
	allChanged := i.all.Update(segmentID, frontier)
	partitionChanged := partition.Update(segmentID, frontier)
	return allChanged || partitionChanged
}

func (i *segmentFrontierIndex) All() uint64 {
	return i.all.Minimum()
}

func (i *segmentFrontierIndex) Partition(collectionID, partitionID int64) uint64 {
	i.mu.Lock()
	partition := i.partitions[segmentFrontierScope{collectionID: collectionID, partitionID: partitionID}]
	i.mu.Unlock()
	if partition == nil {
		return math.MaxUint64
	}
	return partition.Minimum()
}
