package broadcaster

import (
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// newResourceKeyLocker creates a new resource key locker.
func newResourceKeyLocker(metrics *broadcasterMetrics) *resourceKeyLocker {
	return &resourceKeyLocker{
		cond:    syncutil.NewContextCond(&sync.Mutex{}),
		sem:     make(map[message.ResourceKey]uint64),
		metrics: metrics,
	}
}

// resourceKeyLocker is the locker for the resource keys.
// It's a low performance implementation, but the broadcaster is only used at low frequency of ddl.
// So it's acceptable to use this implementation.
type resourceKeyLocker struct {
	cond    *syncutil.ContextCond
	sem     map[message.ResourceKey]uint64
	metrics *broadcasterMetrics
}

// lockGuards is the guards for multiple resource keys.
type lockGuards struct {
	guards []*lockGuard
}

// append appends the guard to the guards.
func (l *lockGuards) append(guard *lockGuard) {
	l.guards = append(l.guards, guard)
}

// Unlock unlocks the resource keys.
func (l *lockGuards) Unlock() {
	// release the locks in reverse order to avoid deadlock.
	for i := len(l.guards) - 1; i >= 0; i-- {
		l.guards[i].Unlock()
	}
	l.guards = nil
}

// lockGuard is the guard for the resource key.
type lockGuard struct {
	locker      *resourceKeyLocker
	broadcastID uint64
	releaser    message.ResourceKey
}

// Unlock unlocks the resource key.
func (l *lockGuard) Unlock() {
	l.locker.unlockWithKey(l.broadcastID, l.releaser)
}

// FastLock locks the resource keys without waiting.
// return error if the resource key is already locked.
func (r *resourceKeyLocker) FastLock(broadcastID uint64, keys ...message.ResourceKey) (*lockGuards, error) {
	sortResourceKeys(keys)

	g := &lockGuards{}
	for _, key := range keys {
		if guard := r.fastLockWithKey(broadcastID, key); guard != nil {
			g.append(guard)
			continue
		}
		g.Unlock()
		return nil, errors.Errorf(
			"unreachable: dirty recovery info in metastore, broadcast ids: [%d, %d]",
			broadcastID,
			r.sem[key],
		)
	}
	return g, nil
}

// Lock locks the resource keys.
func (r *resourceKeyLocker) Lock(ctx context.Context, broadcastID uint64, keys ...message.ResourceKey) (*lockGuards, error) {
	// lock the keys in order to avoid deadlock.
	sortResourceKeys(keys)

	g := &lockGuards{}
	for _, key := range keys {
		guard, err := r.lockWithKey(ctx, broadcastID, key)
		if err != nil {
			g.Unlock()
			return nil, err
		}
		g.append(guard)
	}
	return g, nil
}

// lockWithKey locks the resource key.
func (r *resourceKeyLocker) lockWithKey(ctx context.Context, broadcastID uint64, key message.ResourceKey) (*lockGuard, error) {
	r.cond.L.Lock()
	for {
		if guard := r.fastLockWithKey(broadcastID, key); guard != nil {
			r.cond.L.Unlock()
			return guard, nil
		}
		if err := r.cond.Wait(ctx); err != nil {
			return nil, err
		}
	}
}

// fastLockWithKey locks the resource key without waiting.
// return nil if the resource key is already locked.
func (r *resourceKeyLocker) fastLockWithKey(broadcastID uint64, key message.ResourceKey) *lockGuard {
	if _, ok := r.sem[key]; !ok {
		r.sem[key] = broadcastID
		r.metrics.IncomingResourceKey(key.Domain)
		return &lockGuard{
			locker:      r,
			broadcastID: broadcastID,
			releaser:    key,
		}
	}
	return nil
}

// unlockWithKey unlocks the resource key.
func (r *resourceKeyLocker) unlockWithKey(broadcastID uint64, key message.ResourceKey) {
	r.cond.LockAndBroadcast()
	defer r.cond.L.Unlock()

	if bid, ok := r.sem[key]; ok && bid == broadcastID {
		delete(r.sem, key)
		r.metrics.GoneResourceKey(key.Domain)
	}
}

// sortResourceKeys sorts the resource keys.
func sortResourceKeys(keys []message.ResourceKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Domain != keys[j].Domain {
			return keys[i].Domain < keys[j].Domain
		}
		return keys[i].Key < keys[j].Key
	})
}
