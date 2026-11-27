package syncer

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

// pendingSyncQueryViews tracks query views dispatched to a single work node
// that are still waiting for responses. Owned by a single resumableSyncer.
// Thread-safe.
type pendingSyncQueryViews struct {
	mu       sync.Mutex
	entries  map[qviews.QueryViewKey]pendingSyncEntry
	revision uint64
	unsent   map[qviews.QueryViewKey]*viewpb.QueryViewOfShard
	notify   chan struct{} // cap 1, signaled by Upsert
}

type pendingSyncEntry struct {
	timing   *syncTiming
	revision uint64
	view     SyncView
}

func newPendingSyncQueryViews() *pendingSyncQueryViews {
	return &pendingSyncQueryViews{
		entries: make(map[qviews.QueryViewKey]pendingSyncEntry),
		unsent:  make(map[qviews.QueryViewKey]*viewpb.QueryViewOfShard),
		notify:  make(chan struct{}, 1),
	}
}

// Upsert inserts or replaces a pending entry, accumulates the proto
// for incremental sending, and signals Ready().
func (p *pendingSyncQueryViews) Upsert(sv SyncView) {
	key := sv.View.QueryViewKey()

	p.mu.Lock()
	p.revision++
	var timing *syncTiming
	if old, ok := p.entries[key]; ok {
		if old.view.View.State() == sv.View.State() {
			timing = old.timing
		} else {
			old.timing.finish(stage.Superseded)
		}
	}
	if timing == nil {
		timing = newSyncTiming(sv.View.State())
	}
	p.entries[key] = pendingSyncEntry{
		revision: p.revision,
		timing:   timing,
		view:     sv,
	}
	p.unsent[key] = sv.View.IntoProto()
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
	protos := make([]*viewpb.QueryViewOfShard, 0, len(p.unsent))
	for key, view := range p.unsent {
		protos = append(protos, view)
		if entry, ok := p.entries[key]; ok && entry.view.View.State() == qviews.QueryViewState(view.GetMeta().GetState()) {
			entry.timing.queue.End(nil)
		}
	}
	clear(p.unsent)
	p.mu.Unlock()
	if len(protos) == 0 {
		return nil
	}
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
	if ok {
		entry.timing.ack(entry.view.View.State(), view.State())
	}
	p.mu.Unlock()
	if !ok {
		return
	}

	callbackTimer := syncCallback.Begin()
	complete := entry.view.OnSyncResponse(view)
	callbackTimer.End(nil)
	if !complete {
		return
	}

	p.mu.Lock()
	current, ok := p.entries[key]
	if ok && current.revision == entry.revision {
		current.timing.finish(stage.Superseded)
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
		sv.timing.finish(stage.Canceled)
		drained = append(drained, sv.view)
	}
	p.entries = make(map[qviews.QueryViewKey]pendingSyncEntry)
	clear(p.unsent)
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

// Timers share pending entries' synchronization and survive reconnects and
// same-target updates. The interval includes queueing and worker processing.
type syncTiming struct{ queue, roundtrip stage.Timer }

var (
	syncPreparing = stage.New("coord", "sync_preparing", "roundtrip")
	syncUp        = stage.New("coord", "sync_up", "roundtrip")
	syncCleanup   = stage.New("coord", "sync_cleanup", "roundtrip")
	syncQueue     = stage.New("coord", "sync", "send_queue")
	syncCallback  = stage.New("coord", "sync", "callback")
)

func newSyncTiming(state qviews.QueryViewState) *syncTiming {
	recorder := syncCleanup
	switch state {
	case qviews.QueryViewStatePreparing:
		recorder = syncPreparing
	case qviews.QueryViewStateUp:
		recorder = syncUp
	}
	return &syncTiming{queue: syncQueue.Begin(), roundtrip: recorder.Begin()}
}

func (t *syncTiming) finish(result stage.Result) {
	if t != nil {
		t.queue.EndResult(result)
		t.roundtrip.EndResult(result)
	}
}

func (p *pendingSyncQueryViews) closeTiming() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, entry := range p.entries {
		entry.timing.finish(stage.Canceled)
	}
}

func (t *syncTiming) ack(target, reported qviews.QueryViewState) {
	if reported == qviews.QueryViewStateUnrecoverable {
		t.finish(stage.Error)
		return
	}
	complete := target == qviews.QueryViewStatePreparing && (reported == qviews.QueryViewStateReady || reported == qviews.QueryViewStateUp) ||
		target == qviews.QueryViewStateUp && reported == qviews.QueryViewStateUp ||
		target == qviews.QueryViewStateDown && (reported == qviews.QueryViewStateDown || reported == qviews.QueryViewStateDropped) ||
		target == qviews.QueryViewStateDropped && reported == qviews.QueryViewStateDropped
	if complete {
		t.finish(stage.Success)
	}
}
