package balancer

import (
	"container/heap"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// One release notification may span several priority batches.
type shardCompletion struct {
	mu        sync.Mutex
	future    *syncutil.Future[error]
	remaining int
	err       error
}

func (c *shardCompletion) finish(count int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = errors.CombineErrors(c.err, err)
	c.remaining -= count
	if c.remaining == 0 {
		c.future.Set(c.err)
	}
}

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
	waiters  []*shardCompletion
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

func (q *shardQueue) submit(shards []scoredShard, revision uint64, completion *shardCompletion) {
	if completion != nil {
		completion.mu.Lock()
		completion.remaining += len(shards)
		completion.mu.Unlock()
	}
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
		if completion != nil {
			item.waiters = append(item.waiters, completion)
		}
	}
}

func (q *shardQueue) take(limit int) ([]qviews.ShardID, map[*shardCompletion]int) {
	count := min(limit, len(q.heap))
	ids := make([]qviews.ShardID, 0, count)
	var waiters map[*shardCompletion]int
	for range count {
		item := heap.Pop(&q.heap).(*queuedShard)
		delete(q.items, item.id)
		ids = append(ids, item.id)
		for _, waiter := range item.waiters {
			if waiters == nil {
				waiters = make(map[*shardCompletion]int)
			}
			waiters[waiter]++
		}
	}
	return ids, waiters
}
