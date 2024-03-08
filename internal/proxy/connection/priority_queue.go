package connection

import (
	"container/heap"
	"time"
)

type queueItem struct {
	identifier     int64
	lastActiveTime time.Time
}

func newQueryItem(identifier int64, lastActiveTime time.Time) *queueItem {
	return &queueItem{
		identifier:     identifier,
		lastActiveTime: lastActiveTime,
	}
}

type priorityQueue []*queueItem

func (pq priorityQueue) Len() int {
	return len(pq)
}

func (pq priorityQueue) Less(i, j int) bool {
	// we should purge the oldest, so the newest should be on the root.
	return pq[i].lastActiveTime.After(pq[j].lastActiveTime)
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*queueItem)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[:n-1]
	return item
}

func newPriorityQueueWithCap(cap int) priorityQueue {
	q := make(priorityQueue, 0, cap)
	heap.Init(&q)
	return q
}

func newPriorityQueue() priorityQueue {
	return newPriorityQueueWithCap(0)
}
