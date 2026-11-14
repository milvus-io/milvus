package snview

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// viewFeedback owns only the current window, independently of View state machines.
type viewFeedback struct {
	mu     sync.Mutex
	start  int64
	shards map[qviews.ShardID]qviews.ViewFeedback
	notify chan struct{}
}

func (s *viewFeedback) record(id qviews.ShardID, now int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shards == nil {
		s.shards = make(map[qviews.ShardID]qviews.ViewFeedback)
	}
	if s.start == 0 {
		s.start = now
	}
	sample := s.shards[id]
	sample.ShardID = id
	sample.RequestCount++
	sample.LastAccessUnixNanos = max(sample.LastAccessUnixNanos, now)
	s.shards[id] = sample
	if len(s.shards) >= qviews.ViewFeedbackMaxEntries {
		s.signalLocked()
	}
}

func (s *viewFeedback) signalLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *viewFeedback) ready() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.notify == nil {
		s.notify = make(chan struct{}, 1)
	}
	if len(s.shards) >= qviews.ViewFeedbackMaxEntries {
		s.signalLocked()
	}
	return s.notify
}

func (s *viewFeedback) take() qviews.ViewFeedbackWindow {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	window := qviews.ViewFeedbackWindow{StartUnixNanos: s.start, EndUnixNanos: now, Shards: s.shards}
	// Even empty takes advance Start strictly past End so consecutive reports
	// satisfy Coord's non-overlapping window admission rule.
	s.start, s.shards = now+1, nil
	select {
	case <-s.notify:
	default:
	}
	return window
}

func (h *SNQueryViewHandler) RecordShardAccess(id qviews.ShardID) {
	h.feedback.record(id, time.Now().UnixNano())
}

func (h *SNQueryViewHandler) TakeViewFeedback() qviews.ViewFeedbackWindow {
	return h.feedback.take()
}

func (h *SNQueryViewHandler) ViewFeedbackReady() <-chan struct{} {
	return h.feedback.ready()
}
