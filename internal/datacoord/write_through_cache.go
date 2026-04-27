package datacoord

import (
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// CacheUpdateFunc modifies a cached value in place.
// Return false to abort the update (value unchanged).
type CacheUpdateFunc[V any] func(existing V) bool

type cacheEntry[V any] struct {
	value   V
	version int64
	deleted bool
}

// Cache is an in-memory cache with version tracking and tombstone support.
// All write methods (Insert/Update/Erase) are CAS-safe for concurrent access.
// Version ordering prevents stale writes: a write with version <= current is rejected.
// Erase creates a tombstone (retains version) to reject subsequent stale writes.
// Prune removes tombstone entries to free memory.
type Cache[K comparable, V any] struct {
	entries   *typeutil.ConcurrentMap[K, *cacheEntry[V]]
	cloneFn   func(V) V
	liveCount atomic.Int64
}

func NewCache[K comparable, V any](cloneFn func(V) V) *Cache[K, V] {
	return &Cache[K, V]{
		entries: typeutil.NewConcurrentMap[K, *cacheEntry[V]](),
		cloneFn: cloneFn,
	}
}

// Lookup returns the cached value. Returns (zero, false) if not found or tombstone.
func (c *Cache[K, V]) Lookup(key K) (V, bool) {
	var zero V
	entry, ok := c.entries.Get(key)
	if !ok || entry.deleted {
		return zero, false
	}
	return entry.value, true
}

// LookupWithVersion returns the live value alongside its current version.
// The version matches the one provided to Insert/Update at the time of write
// (typically the etcd ModRevision), so callers can use it as the
// expectedVersion argument to a subsequent CAS Update.
func (c *Cache[K, V]) LookupWithVersion(key K) (V, int64, bool) {
	var zero V
	entry, ok := c.entries.Get(key)
	if !ok || entry.deleted {
		return zero, 0, false
	}
	return entry.value, entry.version, true
}

// Insert creates or overwrites an entry with the given version.
// If version > 0, rejects if current version >= given version
// (stale write protection, including tombstones).
// If version == 0, treats the write as local/test seed data and overwrites.
// Returns the previous live value, whether it existed, and whether this write
// was applied.
func (c *Cache[K, V]) Insert(key K, value V, version int64) (old V, existed bool, applied bool) {
	var zero V
	newEntry := &cacheEntry[V]{value: value, version: version}
	for {
		entry, ok := c.entries.Get(key)
		if !ok {
			if _, loaded := c.entries.GetOrInsert(key, newEntry); !loaded {
				c.liveCount.Add(1)
				return zero, false, true
			}
			continue // concurrent insert, retry
		}
		if version > 0 && entry.version >= version {
			// Stale — current version is newer or equal
			if entry.deleted {
				return zero, false, false
			}
			return entry.value, true, false
		}
		if c.entries.CompareAndSwap(key, entry, newEntry) {
			if entry.deleted {
				c.liveCount.Add(1) // tombstone → live
				return zero, false, true
			}
			return entry.value, true, true
		}
		// CAS failed, retry
	}
}

// Update applies fn to the cached value. CAS-safe.
// fn returns false to abort (value unchanged).
// If version > 0, rejects when current version >= given version (stale protection).
// If version == 0, keeps current version (for local-only updates without persist).
// Returns the pre-update value and whether the key existed (non-tombstone).
func (c *Cache[K, V]) Update(key K, fn CacheUpdateFunc[V], version int64) (old V, existed bool) {
	var zero V
	for {
		entry, ok := c.entries.Get(key)
		if !ok || entry.deleted {
			return zero, false
		}
		if version > 0 && entry.version >= version {
			return entry.value, true // stale
		}
		old = entry.value
		cloned := c.cloneFn(old)
		if !fn(cloned) {
			return old, true // fn aborted
		}
		newVersion := version
		if newVersion == 0 {
			newVersion = entry.version
		}
		newEntry := &cacheEntry[V]{value: cloned, version: newVersion}
		if c.entries.CompareAndSwap(key, entry, newEntry) {
			return old, true
		}
		// CAS failed, retry
	}
}

// Erase marks the entry as a tombstone, retaining the given version
// to reject subsequent stale writes. Returns the previous live value.
func (c *Cache[K, V]) Erase(key K, version int64) (old V) {
	var zero V
	tombstone := &cacheEntry[V]{version: version, deleted: true}
	for {
		entry, ok := c.entries.Get(key)
		if !ok || entry.deleted {
			return zero
		}
		if version > 0 && entry.version >= version {
			return zero
		}
		if c.entries.CompareAndSwap(key, entry, tombstone) {
			c.liveCount.Add(-1)
			return entry.value
		}
	}
}

// Prune removes a tombstone entry, freeing memory.
// Only call after the deletion is fully settled (e.g., after GC).
func (c *Cache[K, V]) Prune(key K) {
	entry, ok := c.entries.Get(key)
	if ok && entry.deleted {
		c.entries.CompareAndDelete(key, entry)
	}
}

// Range iterates over all live (non-tombstone) entries. Return false to stop.
func (c *Cache[K, V]) Range(fn func(K, V) bool) {
	c.entries.Range(func(key K, entry *cacheEntry[V]) bool {
		if entry.deleted {
			return true // skip tombstones
		}
		return fn(key, entry.value)
	})
}

// Len returns the count of live (non-tombstone) entries.
func (c *Cache[K, V]) Len() int {
	return int(c.liveCount.Load())
}
