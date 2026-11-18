package handler

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
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
	members map[int64]struct{}
	gone    map[int64]time.Time
}

func newSessionChanges(r resolver.Resolver) *sessionChanges {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // Close owns cancellation.
	s := &sessionChanges{changed: make(chan struct{}), cancel: cancel, done: make(chan struct{}), members: make(map[int64]struct{}), gone: make(map[int64]time.Time)}
	go func() {
		defer close(s.done)
		_ = r.Watch(ctx, func(state resolver.VersionedState) error {
			s.update(state)
			return nil
		})
	}()
	return s
}

// Only a transition from present to absent confirms a vanished session.
// Initial discovery gaps and picker propagation lag are not tombstones.
func (s *sessionChanges) update(state resolver.VersionedState) {
	members := make(map[int64]struct{}, len(state.State.Addresses))
	for _, address := range state.State.Addresses {
		if id := attributes.GetServerID(address.Attributes); id != nil {
			members[*id] = struct{}{}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, until := range s.gone {
		_, present := members[id]
		if present || !until.After(now) {
			delete(s.gone, id)
		}
	}
	for id := range s.members {
		if _, present := members[id]; !present && len(s.gone) < 4096 {
			s.gone[id] = now.Add(5 * time.Minute)
		}
	}
	s.members = members
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *sessionChanges) Gone(nodeID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gone[nodeID].After(time.Now())
}

func (s *sessionChanges) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

func (s *sessionChanges) Close() {
	s.once.Do(func() { s.cancel(); <-s.done; s.mu.Lock(); close(s.changed); s.mu.Unlock() })
}
