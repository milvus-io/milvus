package resource

import (
	"context"
	"sync"
)

// This is local coordination, independent of the broadcaster's resource key:
// the split callback may hold that key while waiting for g1 to become ready.
// Only schema handoff and g2 adoption/retirement use this guard. In particular,
// g1 publication and target admission must never acquire it.
var splitLifecycle = struct {
	sync.Mutex
	entries map[int64]*splitLifecycleEntry
}{entries: make(map[int64]*splitLifecycleEntry)}

type splitLifecycleEntry struct {
	token chan struct{}
	refs  int
}

// AcquireSplitLifecycle serializes schema handoff with adoption and retirement
// of a collection on this cluster. Waiters honor cancellation; an idle entry
// is removed after its final owner/waiter leaves.
func AcquireSplitLifecycle(ctx context.Context, collectionID int64) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	splitLifecycle.Lock()
	entry := splitLifecycle.entries[collectionID]
	if entry == nil {
		entry = &splitLifecycleEntry{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		splitLifecycle.entries[collectionID] = entry
	}
	entry.refs++
	splitLifecycle.Unlock()
	drop := func() {
		splitLifecycle.Lock()
		defer splitLifecycle.Unlock()
		entry.refs--
		if entry.refs == 0 {
			delete(splitLifecycle.entries, collectionID)
		}
	}
	select {
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	case <-entry.token:
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			entry.token <- struct{}{}
			drop()
		})
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
