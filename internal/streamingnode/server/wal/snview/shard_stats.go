package snview

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// shardStats owns only the current window, independently of View state machines.
type shardStats struct {
	mu     sync.Mutex
	start  int64
	shards map[qviews.ShardID]qviews.ShardStats
	notify chan struct{}
}

func (s *shardStats) record(id qviews.ShardID, now int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shards == nil {
		s.shards = make(map[qviews.ShardID]qviews.ShardStats)
	}
	if s.start == 0 {
		s.start = now
	}
	sample := s.shards[id]
	sample.ShardID = id
	sample.RequestCount++
	sample.LastAccessUnixNanos = max(sample.LastAccessUnixNanos, now)
	s.shards[id] = sample
	if len(s.shards) >= qviews.ShardStatsMaxEntries {
		s.signalLocked()
	}
}

func (s *shardStats) signalLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *shardStats) ready() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.notify == nil {
		s.notify = make(chan struct{}, 1)
	}
	if len(s.shards) >= qviews.ShardStatsMaxEntries {
		s.signalLocked()
	}
	return s.notify
}

func (s *shardStats) take() qviews.ShardStatsWindow {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	window := qviews.ShardStatsWindow{StartUnixNanos: s.start, EndUnixNanos: now, Shards: s.shards}
	s.start, s.shards = now, nil
	select {
	case <-s.notify:
	default:
	}
	return window
}

func (h *SNQueryViewHandler) RecordShardAccess(id qviews.ShardID) {
	h.stats.record(id, time.Now().UnixNano())
}

func (h *SNQueryViewHandler) TakeShardStats() qviews.ShardStatsWindow {
	return h.stats.take()
}

func (h *SNQueryViewHandler) ShardStatsReady() <-chan struct{} {
	return h.stats.ready()
}
