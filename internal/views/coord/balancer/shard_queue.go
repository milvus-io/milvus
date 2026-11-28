package balancer

import (
	"container/heap"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

type scoredShard struct {
	id       qviews.ShardID
	priority int32
}

type queuedShard struct {
	id       qviews.ShardID
	priority int32
	sequence uint64
	revision uint64
	index    int
}

type shardHeap []*queuedShard

func (h shardHeap) Len() int { return len(h) }
func (h shardHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].sequence < h[j].sequence
}

func (h shardHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *shardHeap) Push(x any) {
	item := x.(*queuedShard)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *shardHeap) Pop() any {
	last := len(*h) - 1
	item := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return item
}

// Queue updates are ordered by the revision assigned before scoring.
type shardQueue struct {
	items    map[qviews.ShardID]*queuedShard
	heap     shardHeap
	sequence uint64
}

func (q *shardQueue) submit(shards []scoredShard, revision uint64) {
	for _, shard := range shards {
		id, score := shard.id, shard.priority
		item := q.items[id]
		if item == nil {
			item = &queuedShard{id: id, priority: score, sequence: q.sequence, revision: revision}
			q.sequence++
			q.items[id] = item
			heap.Push(&q.heap, item)
		} else if revision >= item.revision {
			item.revision = revision
			if item.priority != score {
				item.priority = score
				heap.Fix(&q.heap, item.index)
			}
		}
	}
}

func (q *shardQueue) take(limit int) []qviews.ShardID {
	count := min(limit, len(q.heap))
	ids := make([]qviews.ShardID, 0, count)
	for range count {
		item := heap.Pop(&q.heap).(*queuedShard)
		delete(q.items, item.id)
		ids = append(ids, item.id)
	}
	return ids
}
