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
	mu       sync.Mutex
	entries  map[qviews.QueryViewKey]pendingSyncEntry
	revision uint64
	unsent   []*viewpb.QueryViewOfShard // protos accumulated by Upsert, drained by sendLoop
	notify   chan struct{}              // cap 1, signaled by Upsert
}

type pendingSyncEntry struct {
	revision uint64
	view     SyncView
}

func newPendingSyncQueryViews() *pendingSyncQueryViews {
	return &pendingSyncQueryViews{
		entries: make(map[qviews.QueryViewKey]pendingSyncEntry),
		notify:  make(chan struct{}, 1),
	}
}

// Upsert inserts or replaces a pending entry, accumulates the proto
// for incremental sending, and signals Ready().
func (p *pendingSyncQueryViews) Upsert(sv SyncView) {
	key := sv.View.QueryViewKey()

	p.mu.Lock()
	p.revision++
	p.entries[key] = pendingSyncEntry{
		revision: p.revision,
		view:     sv,
	}
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
// The callback is invoked outside p.mu so it may enqueue follow-up syncs.
// If the entry is replaced while the callback runs, a true return only deletes
// the entry when the revision still matches.
func (p *pendingSyncQueryViews) MatchResponse(pb *viewpb.QueryViewOfShard) {
	view := qviews.NewQueryViewAtWorkNodeFromProto(pb)
	key := view.QueryViewKey()

	p.mu.Lock()
	entry, ok := p.entries[key]
	p.mu.Unlock()
	if !ok {
		return
	}

	if !entry.view.OnSyncResponse(view) {
		return
	}

	p.mu.Lock()
	current, ok := p.entries[key]
	if ok && current.revision == entry.revision {
		delete(p.entries, key)
	}
	p.mu.Unlock()
}

// Drain removes all pending entries and invokes OnQueryNodeLost for each entry
// only when the lost node is a QueryNode.
func (p *pendingSyncQueryViews) Drain(node qviews.WorkNode) {
	p.mu.Lock()
	drained := make([]SyncView, 0, len(p.entries))
	for _, sv := range p.entries {
		drained = append(drained, sv.view)
	}
	p.entries = make(map[qviews.QueryViewKey]pendingSyncEntry)
	p.unsent = nil
	p.mu.Unlock()

	qn, ok := node.(qviews.QueryNode)
	if !ok {
		return
	}
	for _, entry := range drained {
		if _, ok := entry.View.WorkNode().(qviews.QueryNode); !ok {
			continue
		}
		if entry.OnQueryNodeLost != nil {
			entry.OnQueryNodeLost(qn)
		}
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
		protos = append(protos, sv.view.View.IntoProto())
	}
	return protos
}
