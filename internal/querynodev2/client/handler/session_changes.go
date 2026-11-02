package handler

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
)

// sessionChanges broadcasts resolver revisions without copying QN topology or
// creating one watch per query. A missing session may be a discovery lag;
// queries can reconsider it on a revision, never by waiting on its dead RPC.
type sessionChanges struct {
	mu      sync.Mutex
	once    sync.Once
	changed chan struct{}
	cancel  context.CancelFunc
	done    chan struct{}
}

func newSessionChanges(r resolver.Resolver) *sessionChanges {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // Close owns cancellation.
	s := &sessionChanges{changed: make(chan struct{}), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		_ = r.Watch(ctx, func(resolver.VersionedState) error {
			s.mu.Lock()
			close(s.changed)
			s.changed = make(chan struct{})
			s.mu.Unlock()
			return nil
		})
	}()
	return s
}

func (s *sessionChanges) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

func (s *sessionChanges) Close() {
	s.once.Do(func() { s.cancel(); <-s.done; s.mu.Lock(); close(s.changed); s.mu.Unlock() })
}
