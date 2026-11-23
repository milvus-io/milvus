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

package milvusclient

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/conc"
)

// defaultCacheCapacity bounds the number of cached collection schemas and
// write timestamps, evicting the least-recently-accessed entry when full.
// Mirrors the pymilvus/java/cpp/rust/csharp SchemaCache and CollectionTsCache
// default (4096).
const defaultCacheCapacity = 4096

// collectionCacheKey identifies a collection cache entry by database and
// collection name. The Go client is per-connection (one connection, one
// current database, and the collection cache lives on the *Client), so the
// endpoint is implicit and does not need to be part of the key; the database
// name is still required to separate same-named collections in different
// databases.
//
// If the cache is ever refactored into a process-wide singleton shared by
// multiple connections (like the other Milvus SDKs), the key MUST be extended
// to (endpoint, database, collection) so entries from different connections do
// not collide.
type collectionCacheKey struct {
	db         string
	collection string
}

// newCollectionCacheKey builds a normalized cache key, mapping an empty
// database name to "default" like the other Milvus SDKs.
func newCollectionCacheKey(dbName, collName string) collectionCacheKey {
	if dbName == "" {
		dbName = "default"
	}
	return collectionCacheKey{db: dbName, collection: collName}
}

// lruCache is a concurrency-safe, capacity-bounded LRU map shared by the
// schema and write-timestamp caches. Every access refreshes a monotonic
// sequence; when the cache is full, the least-recently-accessed entry is
// evicted. Keeping the eviction/ordering logic in one place avoids the two
// caches drifting apart.
type lruCache[V any] struct {
	mu       sync.Mutex
	capacity int
	seq      uint64
	entries  map[collectionCacheKey]*lruEntry[V]
}

type lruEntry[V any] struct {
	value      V
	lastAccess uint64
}

func newLRUCache[V any](capacity int) *lruCache[V] {
	if capacity <= 0 {
		capacity = defaultCacheCapacity
	}
	return &lruCache[V]{
		capacity: capacity,
		entries:  make(map[collectionCacheKey]*lruEntry[V]),
	}
}

// get returns the value for key and refreshes its access time.
func (c *lruCache[V]) get(key collectionCacheKey) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		var zero V
		return zero, false
	}
	entry.lastAccess = c.nextAccess()
	return entry.value, true
}

// put stores value under key, evicting the least-recently-accessed entry when
// the cache is full.
func (c *lruCache[V]) put(key collectionCacheKey, value V) {
	c.putMerge(key, value, func(_, new V) V { return new })
}

// putMerge stores value under key, combining it with the existing value via
// merge when the key is already present, and evicts the least-recently-accessed
// entry when the cache is full.
func (c *lruCache[V]) putMerge(key collectionCacheKey, value V, merge func(old, new V) V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok {
		entry.value = merge(entry.value, value)
		entry.lastAccess = c.nextAccess()
		return
	}
	c.evictIfNeeded()
	c.entries[key] = &lruEntry[V]{value: value, lastAccess: c.nextAccess()}
}

func (c *lruCache[V]) remove(key collectionCacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// clear removes every entry. It replaces the map in place under the lock, so
// it is safe to call concurrently with other methods.
func (c *lruCache[V]) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[collectionCacheKey]*lruEntry[V])
}

// Range calls f for each cached key/value pair on a snapshot taken under the
// lock, so f may call back into the cache (e.g. remove) without deadlocking.
// Iteration stops when f returns false.
func (c *lruCache[V]) Range(f func(key collectionCacheKey, value V) bool) {
	c.mu.Lock()
	keys := make([]collectionCacheKey, 0, len(c.entries))
	for key := range c.entries {
		keys = append(keys, key)
	}
	c.mu.Unlock()
	for _, key := range keys {
		c.mu.Lock()
		entry, ok := c.entries[key]
		var value V
		if ok {
			// copy the value out under the lock so a concurrent put/putMerge
			// cannot race with the callback reading it after the unlock
			value = entry.value
		}
		c.mu.Unlock()
		if !ok {
			continue
		}
		if !f(key, value) {
			return
		}
	}
}

func (c *lruCache[V]) nextAccess() uint64 {
	c.seq++
	return c.seq
}

// move removes the value under src and stores it under dst, combining with the
// dst value via merge, in a single critical section so the two entries cannot
// diverge under concurrent updates.
func (c *lruCache[V]) move(src, dst collectionCacheKey, merge func(srcV, dstV V) V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	srcEntry, ok := c.entries[src]
	if !ok {
		return
	}
	value := srcEntry.value
	delete(c.entries, src)
	c.mergeLocked(dst, value, merge)
}

// mergeInto stores merge(srcValue, dstValue) under dst in a single critical
// section, retaining the src entry, so a concurrent update to either key cannot
// be lost between the read and the write.
func (c *lruCache[V]) mergeInto(src, dst collectionCacheKey, merge func(srcV, dstV V) V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	srcEntry, ok := c.entries[src]
	if !ok {
		return
	}
	c.mergeLocked(dst, srcEntry.value, merge)
}

// mergeLocked stores value under dst, combining it with the existing entry via
// merge. It must be called with c.mu held.
func (c *lruCache[V]) mergeLocked(dst collectionCacheKey, value V, merge func(srcV, dstV V) V) {
	if entry, ok := c.entries[dst]; ok {
		entry.value = merge(value, entry.value)
		entry.lastAccess = c.nextAccess()
		return
	}
	c.evictIfNeeded()
	c.entries[dst] = &lruEntry[V]{value: value, lastAccess: c.nextAccess()}
}

func (c *lruCache[V]) evictIfNeeded() {
	if len(c.entries) < c.capacity {
		return
	}
	var victim collectionCacheKey
	var victimAccess uint64
	first := true
	for key, entry := range c.entries {
		if first || entry.lastAccess < victimAccess {
			victim = key
			victimAccess = entry.lastAccess
			first = false
		}
	}
	delete(c.entries, victim)
}

// collectionSchemaCache is the schema LRU cache. It wraps the shared lruCache
// and exposes the schema-specific accessors.
type collectionSchemaCache struct {
	inner *lruCache[*entity.Collection]
}

func newCollectionSchemaCache(capacity int) *collectionSchemaCache {
	return &collectionSchemaCache{inner: newLRUCache[*entity.Collection](capacity)}
}

func (c *collectionSchemaCache) Get(key collectionCacheKey) (*entity.Collection, bool) {
	return c.inner.get(key)
}

// Insert stores coll under key. When the cache is at capacity, the
// least-recently-accessed entry is evicted.
func (c *collectionSchemaCache) Insert(key collectionCacheKey, coll *entity.Collection) {
	c.inner.put(key, coll)
}

func (c *collectionSchemaCache) Remove(key collectionCacheKey) {
	c.inner.remove(key)
}

// Clear removes every entry. It replaces the map in place under the lock, so
// it is safe to call concurrently with other methods.
func (c *collectionSchemaCache) Clear() {
	c.inner.clear()
}

// Range calls f for each cached key/value pair on a snapshot taken under the
// lock, so f may call back into the cache (e.g. Remove) without deadlocking.
// Iteration stops when f returns false.
func (c *collectionSchemaCache) Range(f func(key collectionCacheKey, coll *entity.Collection) bool) {
	c.inner.Range(f)
}

// collectionTsCache is a concurrency-safe, capacity-bounded cache of per
// collection write timestamps, mirroring pymilvus CollectionTsCache
// (LRUCache(maxsize=4096)). Updates are monotonic: only a newer timestamp
// overwrites the cached one.
type collectionTsCache struct {
	inner *lruCache[uint64]
}

func newCollectionTsCache(capacity int) *collectionTsCache {
	return &collectionTsCache{inner: newLRUCache[uint64](capacity)}
}

func (c *collectionTsCache) Get(key collectionCacheKey) uint64 {
	value, ok := c.inner.get(key)
	if !ok {
		return 0
	}
	return value
}

// Update records the latest mutation timestamp, keeping the monotonically
// largest value, and evicts the least-recently-accessed entry when full.
func (c *collectionTsCache) Update(key collectionCacheKey, ts uint64) {
	c.inner.putMerge(key, ts, func(old, new uint64) uint64 {
		if new > old {
			return new
		}
		return old
	})
}

// Rename atomically moves the timestamp from oldKey to newKey. The new name's
// previous value is replaced: the write timeline belongs to the renamed
// collection, not to whatever previously used the new name.
func (c *collectionTsCache) Rename(oldKey, newKey collectionCacheKey) {
	c.inner.move(oldKey, newKey, func(srcV, _ uint64) uint64 { return srcV })
}

// Copy atomically stores max(source, target) under target while retaining the
// source entry, so a concurrent update to either key cannot be lost between the
// read and the write.
func (c *collectionTsCache) Copy(sourceKey, targetKey collectionCacheKey) {
	c.inner.mergeInto(sourceKey, targetKey, func(srcV, dstV uint64) uint64 {
		if srcV > dstV {
			return srcV
		}
		return dstV
	})
}

func (c *collectionTsCache) Remove(key collectionCacheKey) {
	c.inner.remove(key)
}

// Clear removes every entry. It replaces the map in place under the lock, so
// it is safe to call concurrently with other methods.
func (c *collectionTsCache) Clear() {
	c.inner.clear()
}

// Range calls f for each cached key/value pair on a snapshot taken under the
// lock, so f may call back into the cache (e.g. Remove) without deadlocking.
// Iteration stops when f returns false.
func (c *collectionTsCache) Range(f func(key collectionCacheKey, ts uint64) bool) {
	c.inner.Range(f)
}

// collectionCache stores the cached collection schema information together with
// the latest mutation timestamp of each collection.
//
// DML operations (insert/upsert/delete) feed the write timestamp through
// UpdateWriteTs; search/query requests then construct a guarantee_timestamp
// from it so that session consistency (read-your-writes) and the
// default-consistency path can be honored without extra RPCs.
type collectionCache struct {
	sf          conc.Singleflight[*entity.Collection, collectionCacheKey]
	collections *collectionSchemaCache
	fetcher     func(context.Context, string, string) (*entity.Collection, error)

	writeTs *collectionTsCache
}

func (c *collectionCache) GetCollection(ctx context.Context, key collectionCacheKey, forceUpdate bool) (*entity.Collection, error) {
	if !forceUpdate {
		if coll, ok := c.collections.Get(key); ok {
			return coll, nil
		}
	}

	coll, err, _ := c.sf.Do(key, func() (*entity.Collection, error) {
		// fetch against the database recorded in the key, not the connection's
		// current database: the key was built from the database at request
		// time, and a concurrent UseDatabase must not move this fetch
		coll, err := c.fetcher(ctx, key.db, key.collection)
		if err != nil {
			return nil, err
		}
		c.collections.Insert(key, coll)
		return coll, nil
	})
	return coll, err
}

// Evict removes the cached collection schema related to the provided key.
// The cached write timestamp is kept so that session consistency
// (read-your-writes) is preserved across schema-mutating operations.
//
// Alias keys are not evicted here: a schema-mutating operation evicts only the
// collection key, so a read issued through an alias can keep parsing results
// with the stale cached schema until the alias entry is evicted by other means.
// This mirrors the write-timestamp alias snapshot limitation and is a known
// shared behavior across the Milvus SDKs.
func (c *collectionCache) Evict(key collectionCacheKey) {
	c.collections.Remove(key)
}

// EvictWriteTs removes the cached write timestamp of the provided key.
// Used when the write timeline of a collection no longer exists, e.g. after
// drop.
func (c *collectionCache) EvictWriteTs(key collectionCacheKey) {
	c.writeTs.Remove(key)
}

// RenameWriteTs migrates the cached write timestamp from the old key to the
// new one. Rename keeps the same write timeline (the same data lives under the
// new name), so the timestamp is preserved rather than lost. The migration is a
// single atomic operation, so a concurrent write cannot be dropped in the
// read-evict-seed window.
func (c *collectionCache) RenameWriteTs(oldKey, newKey collectionCacheKey) {
	c.writeTs.Rename(oldKey, newKey)
}

// CopyWriteTs copies the cached write timestamp to an alias key while
// retaining the source key. The target is updated with the monotonic max, so
// a newer timestamp recorded through the alias is not overwritten. Mirrors the
// java/cpp CollectionTsCache.copy (which also takes the max of source and
// target) used on create/alter alias.
//
// On AlterAlias that re-points the alias to a different collection, the max
// keeps the old alias timeline's (possibly higher) timestamp until the next
// write through the alias or collection; this is a known shared behavior
// across the Milvus SDKs.
func (c *collectionCache) CopyWriteTs(sourceKey, targetKey collectionCacheKey) {
	c.writeTs.Copy(sourceKey, targetKey)
}

// UpdateWriteTs records the latest mutation timestamp of a collection.
// Only a newer timestamp overwrites the cached one; the cache is bounded by
// the same capacity as the schema cache.
func (c *collectionCache) UpdateWriteTs(key collectionCacheKey, ts uint64) {
	if ts == 0 {
		return
	}
	c.writeTs.Update(key, ts)
}

// GetWriteTs returns the cached mutation timestamp of a collection,
// 0 is returned when no mutation has been observed yet.
func (c *collectionCache) GetWriteTs(key collectionCacheKey) uint64 {
	return c.writeTs.Get(key)
}

// InvalidateDb removes all cached schemas and write timestamps of the given
// database. Used when the database is dropped.
func (c *collectionCache) InvalidateDb(dbName string) {
	if dbName == "" {
		dbName = "default"
	}
	c.collections.Range(func(key collectionCacheKey, _ *entity.Collection) bool {
		if key.db == dbName {
			c.collections.Remove(key)
		}
		return true
	})
	c.writeTs.Range(func(key collectionCacheKey, _ uint64) bool {
		if key.db == dbName {
			c.writeTs.Remove(key)
		}
		return true
	})
}

// Reset clears all cached info, used when client switching env.
//
// It clears the caches in place rather than reassigning the fields, so it is
// safe to call concurrently with GetCollection/UpdateWriteTs/Evict.
func (c *collectionCache) Reset() {
	c.collections.Clear()
	c.writeTs.Clear()
}

func newCollectionCache(fetcher func(context.Context, string, string) (*entity.Collection, error)) *collectionCache {
	return &collectionCache{
		collections: newCollectionSchemaCache(defaultCacheCapacity),
		writeTs:     newCollectionTsCache(defaultCacheCapacity),
		fetcher:     fetcher,
	}
}

// CollectionCache stores the cached collection schema information of the
// default database, keyed by collection name.
//
// Deprecated: the collection cache now lives on the *Client and is keyed by
// (database, collection). This type is retained only for source compatibility
// with code that constructs it directly; new code should use the Client APIs.
type CollectionCache struct {
	inner *collectionCache
}

// NewCollectionCache creates a CollectionCache for the default database.
//
// Deprecated: use the Client APIs instead, which own an internal cache.
func NewCollectionCache(fetcher func(context.Context, string) (*entity.Collection, error)) *CollectionCache {
	return &CollectionCache{inner: newCollectionCache(func(ctx context.Context, _, collName string) (*entity.Collection, error) {
		return fetcher(ctx, collName)
	})}
}

// GetCollection returns the cached collection schema of the provided
// collection name in the default database, loading it through the fetcher on
// a cache miss.
func (c *CollectionCache) GetCollection(ctx context.Context, collName string) (*entity.Collection, error) {
	return c.inner.GetCollection(ctx, newCollectionCacheKey("", collName), false)
}

// Evict removes the cached collection schema related to the provided
// collection name in the default database.
func (c *CollectionCache) Evict(collName string) {
	c.inner.Evict(newCollectionCacheKey("", collName))
}

// Reset clears all cached info.
func (c *CollectionCache) Reset() {
	c.inner.Reset()
}
