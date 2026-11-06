package milvusclient

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/conc"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
	"github.com/milvus-io/milvus/client/v3/internal/retry"
	"github.com/milvus-io/milvus/client/v3/internal/typeutil"
)

// guarantee timestamp constants shared with the pymilvus client,
// see pymilvus/client/constants.py (EVENTUALLY_TS / BOUNDED_TS).
const (
	// eventuallyTs is the fallback guarantee timestamp when no cached
	// mutation timestamp is available for the collection.
	eventuallyTs = uint64(1)
	// boundedTs is the guarantee timestamp used by the bounded consistency level.
	boundedTs = uint64(2)
)

// defaultSchemaCacheCapacity bounds the number of cached collection schemas,
// evicting the least-recently-accessed entry when full. Mirrors the
// pymilvus/java/cpp/rust/csharp SchemaCache default (4096).
const defaultSchemaCacheCapacity = 4096

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

// collectionSchemaCache is a concurrency-safe, capacity-bounded collection
// schema cache that evicts the least-recently-accessed entry when full,
// mirroring the pymilvus/java/cpp/rust/csharp SchemaCache. Reads and inserts
// refresh the entry's access time; insertion beyond capacity evicts the entry
// with the oldest access time.
type collectionSchemaCache struct {
	mu       sync.Mutex
	capacity int
	seq      uint64
	entries  map[collectionCacheKey]*schemaCacheEntry
}

type schemaCacheEntry struct {
	coll       *entity.Collection
	lastAccess uint64
}

func newCollectionSchemaCache(capacity int) *collectionSchemaCache {
	if capacity <= 0 {
		capacity = defaultSchemaCacheCapacity
	}
	return &collectionSchemaCache{
		capacity: capacity,
		entries:  make(map[collectionCacheKey]*schemaCacheEntry),
	}
}

func (c *collectionSchemaCache) Get(key collectionCacheKey) (*entity.Collection, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	entry.lastAccess = c.nextAccess()
	return entry.coll, true
}

func (c *collectionSchemaCache) Insert(key collectionCacheKey, coll *entity.Collection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok {
		entry.coll = coll
		entry.lastAccess = c.nextAccess()
		return
	}
	c.evictIfNeeded()
	c.entries[key] = &schemaCacheEntry{coll: coll, lastAccess: c.nextAccess()}
}

func (c *collectionSchemaCache) Remove(key collectionCacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// Range calls f for each cached key/value pair on a snapshot taken under the
// lock, so f may call back into the cache (e.g. Remove) without deadlocking.
// Iteration stops when f returns false.
func (c *collectionSchemaCache) Range(f func(key collectionCacheKey, coll *entity.Collection) bool) {
	c.mu.Lock()
	keys := make([]collectionCacheKey, 0, len(c.entries))
	for key := range c.entries {
		keys = append(keys, key)
	}
	c.mu.Unlock()
	for _, key := range keys {
		c.mu.Lock()
		entry, ok := c.entries[key]
		c.mu.Unlock()
		if !ok {
			continue
		}
		if !f(key, entry.coll) {
			return
		}
	}
}

func (c *collectionSchemaCache) nextAccess() uint64 {
	c.seq++
	return c.seq
}

func (c *collectionSchemaCache) evictIfNeeded() {
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
	fetcher     func(context.Context, string) (*entity.Collection, error)

	writeTs *typeutil.ConcurrentMap[collectionCacheKey, *atomic.Uint64]
}

func (c *collectionCache) GetCollection(ctx context.Context, key collectionCacheKey, forceUpdate bool) (*entity.Collection, error) {
	if !forceUpdate {
		if coll, ok := c.collections.Get(key); ok {
			return coll, nil
		}
	}

	coll, err, _ := c.sf.Do(key, func() (*entity.Collection, error) {
		coll, err := c.fetcher(ctx, key.collection)
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
// new name), so the timestamp is preserved rather than lost.
func (c *collectionCache) RenameWriteTs(oldKey, newKey collectionCacheKey) {
	ts := c.GetWriteTs(oldKey)
	c.EvictWriteTs(oldKey)
	c.UpdateWriteTs(newKey, ts)
}

// CopyWriteTs copies the cached write timestamp to an alias key while
// retaining the source key. The target is updated monotonically so a newer
// timestamp recorded through the alias is not overwritten. Mirrors the
// java/cpp CollectionTsCache.copy used on create/alter alias.
func (c *collectionCache) CopyWriteTs(sourceKey, targetKey collectionCacheKey) {
	latest := c.GetWriteTs(sourceKey)
	if targetTs := c.GetWriteTs(targetKey); targetTs > latest {
		latest = targetTs
	}
	c.UpdateWriteTs(targetKey, latest)
}

// UpdateWriteTs records the latest mutation timestamp of a collection.
// Only a newer timestamp overwrites the cached one.
func (c *collectionCache) UpdateWriteTs(key collectionCacheKey, ts uint64) {
	if ts == 0 {
		return
	}
	entry, ok := c.writeTs.Get(key)
	if !ok {
		entry = &atomic.Uint64{}
		entry.Store(ts)
		existing, _ := c.writeTs.GetOrInsert(key, entry)
		entry = existing
	}
	for {
		old := entry.Load()
		if old >= ts {
			return
		}
		if entry.CompareAndSwap(old, ts) {
			return
		}
	}
}

// GetWriteTs returns the cached mutation timestamp of a collection,
// 0 is returned when no mutation has been observed yet.
func (c *collectionCache) GetWriteTs(key collectionCacheKey) uint64 {
	entry, ok := c.writeTs.Get(key)
	if !ok {
		return 0
	}
	return entry.Load()
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
	c.writeTs.Range(func(key collectionCacheKey, _ *atomic.Uint64) bool {
		if key.db == dbName {
			c.writeTs.Remove(key)
		}
		return true
	})
}

// Reset clears all cached info, used when client switching env.
func (c *collectionCache) Reset() {
	c.collections = newCollectionSchemaCache(defaultSchemaCacheCapacity)
	c.writeTs = typeutil.NewConcurrentMap[collectionCacheKey, *atomic.Uint64]()
}

func newCollectionCache(fetcher func(context.Context, string) (*entity.Collection, error)) *collectionCache {
	return &collectionCache{
		collections: newCollectionSchemaCache(defaultSchemaCacheCapacity),
		writeTs:     typeutil.NewConcurrentMap[collectionCacheKey, *atomic.Uint64](),
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
	return &CollectionCache{inner: newCollectionCache(fetcher)}
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

func (c *Client) getCollection(ctx context.Context, collName string) (*entity.Collection, error) {
	return c.collCache.GetCollection(ctx, newCollectionCacheKey(c.getCurrentDB(), collName), false)
}

func (c *Client) retryIfSchemaError(ctx context.Context, collName string, work func(ctx context.Context) (uint64, error)) error {
	var lastTs uint64 = math.MaxUint64
	return retry.Handle(ctx, func() (bool, error) {
		ts, err := work(ctx)
		if err != nil {
			// if schema error
			if errors.Is(err, merr.ErrCollectionSchemaMismatch) {
				sameTs := ts == lastTs
				lastTs = ts
				if !sameTs {
					c.collCache.Evict(newCollectionCacheKey(c.getCurrentDB(), collName))
				}
				// retry if not same ts
				return !sameTs, err
			}
			return false, err
		}
		return false, nil
	})
}

// guaranteeTimestamp computes the guarantee_timestamp for a search/query request
// according to the requested consistency level, mirroring pymilvus's
// `construct_guarantee_ts`:
//
//   - default consistency: last write ts of the collection, or eventually (1)
//   - Strong: 0 (server assigns the newest ts)
//   - Session: last write ts of the collection, or eventually (1)
//   - Bounded: 2
//   - Eventually: caller-provided timestamp if any, else eventually (1)
//   - Customized: caller-provided timestamp if any, else eventually (1)
func (c *Client) guaranteeTimestamp(key collectionCacheKey, consistencyLevel commonpb.ConsistencyLevel, useDefault bool, currentTs uint64) uint64 {
	ts := c.collCache.GetWriteTs(key)
	if useDefault {
		return firstNonZero(ts, eventuallyTs)
	}
	switch entity.ConsistencyLevel(consistencyLevel) {
	case entity.ClStrong:
		return 0
	case entity.ClSession:
		return firstNonZero(ts, eventuallyTs)
	case entity.ClBounded:
		return boundedTs
	case entity.ClEventually, entity.ClCustomized:
		// pymilvus does kwargs.setdefault(GUARANTEE_TIMESTAMP, EVENTUALLY_TS):
		// keep a caller-provided timestamp, default to eventually (1)
		return firstNonZero(currentTs, eventuallyTs)
	default:
		return 0
	}
}

func firstNonZero(vals ...uint64) uint64 {
	for _, v := range vals {
		if v != 0 {
			return v
		}
	}
	return 0
}
