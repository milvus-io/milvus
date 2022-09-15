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
