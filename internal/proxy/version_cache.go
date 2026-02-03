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

package proxy

import "sync"

// EntryState represents the state of a cache entry.
type EntryState int

const (
	EntryStateActive EntryState = iota
	EntryStateStale
)

type VersionTable[K comparable, V any] struct {
	entries map[K]*VersionEntry[K, V]
}

func NewVersionTable[K comparable, V any]() *VersionTable[K, V] {
	return &VersionTable[K, V]{
		entries: make(map[K]*VersionEntry[K, V]),
	}
}

type VersionEntry[key comparable, V any] struct {
	key     key
	value   V
	version uint64
	state   EntryState
}

func (t *VersionTable[K, V]) Lookup(key K) (*VersionEntry[K, V], bool) {
	entry, ok := t.entries[key]
	if !ok {
		return nil, false
	}
	return entry, true
}

func (t *VersionTable[K, V]) Insert(key K, value V, version uint64) *VersionEntry[K, V] {
	entry, ok := t.entries[key]
	if !ok || version > entry.version {
		newEntry := &VersionEntry[K, V]{
			key:     key,
			value:   value,
			version: version,
			state:   EntryStateActive,
		}
		t.entries[key] = newEntry
		return newEntry
	}
	return entry
}

func (t *VersionTable[K, V]) Erase(key K) {
	delete(t.entries, key)
}

func (t *VersionTable[K, V]) Stale(key K, version uint64) {
	var v V
	newEntry := &VersionEntry[K, V]{
		key:     key,
		value:   v,
		version: version,
		state:   EntryStateStale,
	}
	t.entries[key] = newEntry
}

type RefCount[K comparable] map[K]uint64

func (r RefCount[K]) Inc(key K) {
	r[key]++
}

func (r RefCount[K]) Dec(key K) {
	r[key]--
}

func (r RefCount[K]) Count(key K) uint64 {
	return r[key]
}

func (r RefCount[K]) Erase(key K) {
	delete(r, key)
}

type VersionCache[K comparable, V any] struct {
	sync.Mutex
	table *VersionTable[K, V]
	refs  RefCount[K]
}

// ReleaseFunc is a function that releases the entry.
type ReleaseFunc[K comparable, V any] func(entry *VersionEntry[K, V])

// Lookup returns the entry if it exists and increments the reference count.
// Caller must call Release to decrement the reference count after using the entry.
func (c *VersionCache[K, V]) Lookup(key K) (*VersionEntry[K, V], bool, ReleaseFunc[K, V]) {
	c.Lock()
	defer c.Unlock()

	entry, ok := c.table.Lookup(key)
	if ok {
		c.refs.Inc(key)
		return entry, true, c.Release
	}

	return nil, false, c.Release
}

// Insert inserts a new entry into the cache and increments the reference count.
// Caller must call Release to decrement the reference count after using the entry.
func (c *VersionCache[K, V]) Insert(key K, value V, version uint64) (*VersionEntry[K, V], ReleaseFunc[K, V]) {
	c.Lock()
	defer c.Unlock()
	entry := c.table.Insert(key, value, version)
	c.refs.Inc(key)
	return entry, c.Release
}

func (c *VersionCache[K, V]) InsertBatchWithoutRef(keys []K, values []V, versions []uint64) {
	c.Lock()
	defer c.Unlock()

	for i := range keys {
		c.table.Insert(keys[i], values[i], versions[i])
	}
}

// Release decrements the reference count for the entry
// Users should always call this function to decrement the reference count after using the entry.
func (c *VersionCache[K, V]) Release(entry *VersionEntry[K, V]) {
	if entry == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	c.refs.Dec(entry.key)
}

// Stale marks the entry as stale or erases it if the reference count is 0.
func (c *VersionCache[K, V]) Stale(key K, version uint64) {
	c.Lock()
	defer c.Unlock()

	if c.refs.Count(key) == 0 {
		c.table.Erase(key)
		c.refs.Erase(key)
	} else {
		c.table.Stale(key, version)
	}
}

// StaleIf marks entries as stale or erases them based on the predicate.
// For each entry where predicate returns true:
//   - if ref count is 0, erase it directly
//   - otherwise, mark it as Stale
func (c *VersionCache[K, V]) StaleIf(predicate func(K) bool, version uint64) {
	c.Lock()
	defer c.Unlock()

	for key := range c.table.entries {
		if predicate(key) {
			if c.refs.Count(key) == 0 {
				c.table.Erase(key)
				c.refs.Erase(key)
			} else {
				c.table.Stale(key, version)
			}
		}
	}
}

// Prune erases all entries that are stale and have reference count 0.
// Users should call this function if they care about memory usage.
func (c *VersionCache[K, V]) Prune() {
	c.Lock()
	defer c.Unlock()
	for key, entry := range c.table.entries {
		if c.refs.Count(key) == 0 && entry.state == EntryStateStale {
			c.table.Erase(key)
			c.refs.Erase(key)
		}
	}
}

func NewVersionCache[K comparable, V any]() *VersionCache[K, V] {
	return &VersionCache[K, V]{
		table: NewVersionTable[K, V](),
		refs:  make(RefCount[K]),
	}
}
