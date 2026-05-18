package syncer

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// pendingSyncQueryViews tracks query views dispatched to a single work node
// that are still waiting for responses. Owned by a single resumableSyncer.
// Thread-safe.
type pendingSyncQueryViews struct {
	mu      sync.Mutex
	entries map[qviews.QueryViewKey]SyncView
	unsent  []*viewpb.QueryViewOfShard // protos accumulated by Upsert, drained by sendLoop
	notify  chan struct{}              // cap 1, signaled by Upsert
}

func newPendingSyncQueryViews() *pendingSyncQueryViews {
	return &pendingSyncQueryViews{
		entries: make(map[qviews.QueryViewKey]SyncView),
		notify:  make(chan struct{}, 1),
	}
}

// Upsert inserts or replaces a pending entry, accumulates the proto
// for incremental sending, and signals Ready().
func (p *pendingSyncQueryViews) Upsert(sv SyncView) {
	key := sv.View.QueryViewKey()

	p.mu.Lock()
	p.entries[key] = sv
	p.unsent = append(p.unsent, sv.View.IntoProto())
	p.mu.Unlock()

	// Non-blocking notify: if already signaled, sendLoop will drain all.
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// Ready returns a channel that is signaled when new unsent protos are available.
func (p *pendingSyncQueryViews) Ready() <-chan struct{} {
	return p.notify
}

// DrainUnsent atomically drains and returns protos accumulated by Upsert.
// Used by sendLoop for incremental sends.
func (p *pendingSyncQueryViews) DrainUnsent() []*viewpb.QueryViewOfShard {
	p.mu.Lock()
	protos := p.unsent
	p.unsent = nil
	p.mu.Unlock()
	return protos
}

// MatchResponse matches a received response proto to pending entries
// and invokes the callback. If callback returns true, the entry is removed.
//
// The callback is invoked while holding the lock to prevent a concurrent
// Upsert from replacing the entry between the read and delete.
// OnSyncResponse must not block for long or call back into pendingSyncQueryViews.
func (p *pendingSyncQueryViews) MatchResponse(pb *viewpb.QueryViewOfShard) {
	view := qviews.NewQueryViewAtWorkNodeFromProto(pb)
	key := view.QueryViewKey()

	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.entries[key]
	if !ok {
		return
	}

	if entry.OnSyncResponse(view) {
		delete(p.entries, key)
	}
}

// Drain removes all pending entries and invokes OnNodeLost() for each.
// Called when the node is declared lost.
func (p *pendingSyncQueryViews) Drain() {
	p.mu.Lock()
	drained := make([]SyncView, 0, len(p.entries))
	for _, sv := range p.entries {
		drained = append(drained, sv)
	}
	p.entries = make(map[qviews.QueryViewKey]SyncView)
	p.unsent = nil
	p.mu.Unlock()

	for _, entry := range drained {
		entry.OnNodeLost()
	}
}

// CollectProtos returns the protos of all pending entries.
// Used by resumableSyncer to re-push on stream reconnection.
func (p *pendingSyncQueryViews) CollectProtos() []*viewpb.QueryViewOfShard {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.entries) == 0 {
		return nil
	}

	protos := make([]*viewpb.QueryViewOfShard, 0, len(p.entries))
	for _, sv := range p.entries {
		protos = append(protos, sv.View.IntoProto())
	}
	return protos
}
