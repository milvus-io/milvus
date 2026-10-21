// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metacache

import (
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type versionedEntry[V any] struct {
	value   V
	version int64
	deleted bool
}

// versionedMap is a concurrent map whose entries carry the store version of
// their value. Reads are lock-free and every write is a compare-and-swap of an
// entry, so writers of different keys never wait for each other.
// A write with a version > 0 is rejected when the entry already holds an equal
// or newer version. Erase leaves a tombstone that keeps the version, so stale
// writes of a removed key are rejected too, until Prune removes it.
type versionedMap[K comparable, V any] struct {
	entries   *typeutil.ConcurrentMap[K, *versionedEntry[V]]
	liveCount atomic.Int64
}

func newVersionedMap[K comparable, V any]() *versionedMap[K, V] {
	return &versionedMap[K, V]{
		entries: typeutil.NewConcurrentMap[K, *versionedEntry[V]](),
	}
}

// Lookup returns the live value of key. Returns (zero, false) if not found or tombstone.
func (c *versionedMap[K, V]) Lookup(key K) (V, bool) {
	var zero V
	entry, ok := c.entries.Get(key)
	if !ok || entry.deleted {
		return zero, false
	}
	return entry.value, true
}

// LookupWithVersion returns the live value of key and its version, read
// together. Returns (zero, 0, false) if not found or tombstone.
func (c *versionedMap[K, V]) LookupWithVersion(key K) (V, int64, bool) {
	var zero V
	entry, ok := c.entries.Get(key)
	if !ok || entry.deleted {
		return zero, 0, false
	}
	return entry.value, entry.version, true
}

// Swap replaces the value of key with fn(current, existed) using a CAS loop.
// A version > 0 is stale-protected: the swap is skipped when the current
// version (live or tombstone) is >= version. A version <= 0 forces the swap and
// keeps the current version. fn may be called more than once and must not
// mutate current. Returns the previous live value, whether it existed, and
// whether the swap was applied.
func (c *versionedMap[K, V]) Swap(key K, version int64, fn func(current V, existed bool) V) (old V, existed bool, applied bool) {
	var zero V
	for {
		entry, ok := c.entries.Get(key)
		live := ok && !entry.deleted
		if live {
			old = entry.value
		} else {
			old = zero
		}
		if ok && version > 0 && entry.version >= version {
			return old, live, false
		}
		newVersion := version
		if version <= 0 {
			newVersion = 0
			if ok {
				newVersion = entry.version
			}
		}
		newEntry := &versionedEntry[V]{value: fn(old, live), version: newVersion}
		if !ok {
			if _, loaded := c.entries.GetOrInsert(key, newEntry); !loaded {
				c.liveCount.Add(1)
				return zero, false, true
			}
			continue // concurrent insert, retry
		}
		if c.entries.CompareAndSwap(key, entry, newEntry) {
			if !live {
				c.liveCount.Add(1) // tombstone -> live
			}
			return old, live, true
		}
		// CAS failed, retry
	}
}

// Transform applies fn to the live value of key without changing its version.
// fn returns the replacement (it may share unchanged parts of the current
// value) and false to abort. Returns whether a live entry was updated.
func (c *versionedMap[K, V]) Transform(key K, fn func(current V) (V, bool)) bool {
	for {
		entry, ok := c.entries.Get(key)
		if !ok || entry.deleted {
			return false
		}
		nv, apply := fn(entry.value)
		if !apply {
			return false
		}
		if c.entries.CompareAndSwap(key, entry, &versionedEntry[V]{value: nv, version: entry.version}) {
			return true
		}
	}
}

// Version returns the version of the live entry of key.
func (c *versionedMap[K, V]) Version(key K) (int64, bool) {
	entry, ok := c.entries.Get(key)
	if !ok || entry.deleted {
		return 0, false
	}
	return entry.version, true
}

// Erase marks the entry as a tombstone, retaining the given version
// to reject subsequent stale writes. Returns the previous live value.
func (c *versionedMap[K, V]) Erase(key K, version int64) (old V) {
	var zero V
	tombstone := &versionedEntry[V]{version: version, deleted: true}
	for {
		entry, ok := c.entries.Get(key)
		if !ok {
			return zero
		}
		if version > 0 && entry.version >= version {
			return zero
		}
		if c.entries.CompareAndSwap(key, entry, tombstone) {
			if entry.deleted {
				return zero
			}
			c.liveCount.Add(-1)
			return entry.value
		}
	}
}

// Prune removes a tombstone entry, freeing memory.
// Only call after the deletion is fully settled (e.g., after GC).
func (c *versionedMap[K, V]) Prune(key K) {
	entry, ok := c.entries.Get(key)
	if ok && entry.deleted {
		c.entries.CompareAndDelete(key, entry)
	}
}

// Range iterates over all live (non-tombstone) entries. Return false to stop.
func (c *versionedMap[K, V]) Range(fn func(K, V) bool) {
	c.entries.Range(func(key K, entry *versionedEntry[V]) bool {
		if entry.deleted {
			return true // skip tombstones
		}
		return fn(key, entry.value)
	})
}

// Len returns the count of live (non-tombstone) entries.
func (c *versionedMap[K, V]) Len() int {
	return int(c.liveCount.Load())
}

// clear removes every entry, tombstones included.
func (c *versionedMap[K, V]) clear() {
	c.entries.Range(func(key K, entry *versionedEntry[V]) bool {
		if c.entries.CompareAndDelete(key, entry) && !entry.deleted {
			c.liveCount.Add(-1)
		}
		return true
	})
}
