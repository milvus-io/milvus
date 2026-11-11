package snview

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// shardStats is owned by the PChannel handler, not by its View state machines.
// Entries survive View retirement and are reclaimed with the handler.
type shardStats struct {
	shards sync.Map // qviews.ShardID -> *shardAccessCounter
}

type shardAccessCounter struct {
	mu         sync.Mutex
	count      uint64
	lastAccess int64
}

func (s *shardStats) record(id qviews.ShardID, now int64) {
	value, ok := s.shards.Load(id)
	if !ok {
		value, _ = s.shards.LoadOrStore(id, &shardAccessCounter{})
	}
	counter := value.(*shardAccessCounter)
	counter.mu.Lock()
	counter.count++
	counter.lastAccess = max(counter.lastAccess, now)
	counter.mu.Unlock()
}

func (s *shardStats) collect() []qviews.ShardStats {
	var result []qviews.ShardStats
	s.shards.Range(func(key, value any) bool {
		counter := value.(*shardAccessCounter)
		counter.mu.Lock()
		sample := qviews.ShardStats{ShardID: key.(qviews.ShardID), RequestCount: counter.count, LastAccessUnixNanos: counter.lastAccess}
		counter.mu.Unlock()
		result = append(result, sample)
		return true
	})
	return result
}

func (h *SNQueryViewHandler) RecordShardAccess(id qviews.ShardID) {
	h.stats.record(id, time.Now().UnixNano())
}

func (h *SNQueryViewHandler) CollectShardStats() []qviews.ShardStats {
	return h.stats.collect()
}
