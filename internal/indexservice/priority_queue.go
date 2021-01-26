package indexservice

import (
	"container/heap"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

// An Item is something we manage in a priority queue.
type PQItem struct {
	value    typeutil.IndexNodeInterface // The value of the item; arbitrary.
	key      UniqueID
	addr     *commonpb.Address
	priority int // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.

type PriorityQueue struct {
	items []*PQItem
	lock  sync.RWMutex
}

func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq.items[i].priority < pq.items[j].priority
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	n := (*pq).Len()
	item := x.(*PQItem)
	item.index = n
	pq.items = append(pq.items, item)
}

// do not call this directly.
func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) CheckAddressExist(addr *commonpb.Address) bool {
	pq.lock.RLock()
	defer pq.lock.RUnlock()

	for _, item := range pq.items {
		if CompareAddress(addr, item.addr) {
			return true
		}
	}
	return false
}

func (pq *PriorityQueue) getItemByKey(key UniqueID) interface{} {
	var ret interface{} = nil
	for _, item := range pq.items {
		if item.key == key {
			ret = item
			break
		}
	}
	return ret
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) IncPriority(key UniqueID, priority int) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	item := pq.getItemByKey(key)
	if item != nil {
		item.(*PQItem).priority += priority
		heap.Fix(pq, item.(*PQItem).index)
	}
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) UpdatePriority(key UniqueID, priority int) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	item := pq.getItemByKey(key)
	if item != nil {
		item.(*PQItem).priority = priority
		heap.Fix(pq, item.(*PQItem).index)
	}
}

func (pq *PriorityQueue) Remove(key UniqueID) {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	item := pq.getItemByKey(key)
	if item != nil {
		heap.Remove(pq, item.(*PQItem).index)
	}
}

func (pq *PriorityQueue) Peek() interface{} {
	pq.lock.RLock()
	defer pq.lock.RUnlock()
	if pq.Len() == 0 {
		return nil
	}
	return pq.items[0]
	//item := pq.items[0]
	//return item.value
}

func (pq *PriorityQueue) PeekClient() (UniqueID, typeutil.IndexNodeInterface) {
	item := pq.Peek()
	if item == nil {
		return UniqueID(-1), nil
	}
	return item.(*PQItem).key, item.(*PQItem).value
}
