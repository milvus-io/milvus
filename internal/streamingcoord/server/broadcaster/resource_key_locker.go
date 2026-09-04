package broadcaster

import (
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ErrFastLockFailed marks a fast-lock contention: some resource key is held by
// an in-flight broadcast. Exported so tests can construct it; classify with
// IsFastLockFailed.
var ErrFastLockFailed = errors.New("fast lock failed")

// IsFastLockFailed reports whether the error came from a FastLock contention,
// i.e. some resource key is held by an in-flight broadcast.
func IsFastLockFailed(err error) bool {
	return errors.Is(err, ErrFastLockFailed)
}

// newResourceKeyLocker creates a new resource key locker.
func newResourceKeyLocker() *resourceKeyLocker {
	return &resourceKeyLocker{
		inner: lock.NewKeyLock[resourceLockKey](),
	}
}

// newResourceLockKey creates a new resource lock key.
func newResourceLockKey(key message.ResourceKey) resourceLockKey {
	return resourceLockKey{
		Domain: key.Domain,
		Key:    key.Key,
	}
}

// resourceLockKey is the key for the resource lock.
type resourceLockKey struct {
	Domain messagespb.ResourceDomain
	Key    string
}

// resourceKeyLocker is the locker for the resource keys.
// It's a low performance implementation, but the broadcaster is only used at low frequency of ddl.
// So it's acceptable to use this implementation.
type resourceKeyLocker struct {
	inner *lock.KeyLock[resourceLockKey]
}

// lockGuards is the guards for multiple resource keys.
type lockGuards struct {
	guards []*lockGuard
}

// ResourceKeys returns the resource keys.
func (l *lockGuards) ResourceKeys() []message.ResourceKey {
	keys := make([]message.ResourceKey, 0, len(l.guards))
	for _, guard := range l.guards {
		keys = append(keys, guard.key)
	}
	return keys
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
	locker *resourceKeyLocker
	key    message.ResourceKey
}

// Unlock unlocks the resource key.
func (l *lockGuard) Unlock() {
	l.locker.unlockWithKey(l.key)
}

// FastLock locks the resource keys without waiting.
// return error if the resource key is already locked.
func (r *resourceKeyLocker) FastLock(keys ...message.ResourceKey) (*lockGuards, error) {
	keys = uniqueSortResourceKeys(keys)

	g := &lockGuards{}
	for _, key := range keys {
		var locked bool
		if key.Shared {
			locked = r.inner.TryRLock(newResourceLockKey(key))
		} else {
			locked = r.inner.TryLock(newResourceLockKey(key))
		}
		if locked {
			g.append(&lockGuard{locker: r, key: key})
			continue
		}
		g.Unlock()
		return nil, merr.Wrapf(ErrFastLockFailed, "fast lock failed at resource key %s", key.String())
	}
	return g, nil
}

// FastLockExclusive fails fast only on exclusive keys; shared keys are acquired
// blocking. Shared keys have no stuck-exclusive-holder hazard, while a shared
// TryRLock would also fail on a merely WAITING exclusive locker, so trying them
// only widens spurious contention. Keys are walked in the same global sorted
// order as Lock, so mixing blocking and try acquisitions cannot deadlock.
func (r *resourceKeyLocker) FastLockExclusive(keys ...message.ResourceKey) (*lockGuards, error) {
	keys = uniqueSortResourceKeys(keys)

	g := &lockGuards{}
	for _, key := range keys {
		if key.Shared {
			r.inner.RLock(newResourceLockKey(key))
		} else if !r.inner.TryLock(newResourceLockKey(key)) {
			g.Unlock()
			return nil, merr.Wrapf(ErrFastLockFailed, "fast lock failed at resource key %s", key.String())
		}
		g.append(&lockGuard{locker: r, key: key})
	}
	return g, nil
}

// Lock locks the resource keys.
func (r *resourceKeyLocker) Lock(keys ...message.ResourceKey) *lockGuards {
	// lock the keys in order to avoid deadlock.
	keys = uniqueSortResourceKeys(keys)

	g := &lockGuards{}
	for _, key := range keys {
		if key.Shared {
			r.inner.RLock(newResourceLockKey(key))
		} else {
			r.inner.Lock(newResourceLockKey(key))
		}
		g.append(&lockGuard{locker: r, key: key})
	}
	return g
}

// unlockWithKey unlocks the resource key.
func (r *resourceKeyLocker) unlockWithKey(key message.ResourceKey) {
	if key.Shared {
		r.inner.RUnlock(newResourceLockKey(key))
		return
	}
	r.inner.Unlock(newResourceLockKey(key))
}

// uniqueSortResourceKeys sorts the resource keys.
func uniqueSortResourceKeys(keys []message.ResourceKey) []message.ResourceKey {
	keys = typeutil.NewSet(keys...).Collect()
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Domain != keys[j].Domain {
			return keys[i].Domain < keys[j].Domain
		}
		return keys[i].Key < keys[j].Key
	})
	return keys
}
