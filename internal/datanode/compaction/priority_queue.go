package compaction

import "github.com/milvus-io/milvus/internal/storage"

type PQItem struct {
	Value *storage.Value
	Index int
	Pos   int
}

type PriorityQueue []*PQItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Value.PK.LT(pq[j].Value.PK)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Pos = i
	pq[j].Pos = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*PQItem)
	item.Pos = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.Pos = -1
	*pq = old[0 : n-1]
	return item
}
