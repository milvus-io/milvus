package milvusclient

import (
	"context"
	"math"
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

// String returns a stable representation used as the singleflight key.
func (k collectionCacheKey) String() string {
	return k.db + "/" + k.collection
}

// newCollectionCacheKey builds a normalized cache key, mapping an empty
// database name to "default" like the other Milvus SDKs.
func newCollectionCacheKey(dbName, collName string) collectionCacheKey {
	if dbName == "" {
		dbName = "default"
	}
	return collectionCacheKey{db: dbName, collection: collName}
}

// collectionCache stores the cached collection schema information together with
// the latest mutation timestamp of each collection.
//
// DML operations (insert/upsert/delete) feed the write timestamp through
// UpdateWriteTs; search/query requests then construct a guarantee_timestamp
// from it so that session consistency (read-your-writes) and the
// default-consistency path can be honored without extra RPCs.
type collectionCache struct {
	sf          conc.Singleflight[*entity.Collection]
	collections *typeutil.ConcurrentMap[collectionCacheKey, *entity.Collection]
	fetcher     func(context.Context, string) (*entity.Collection, error)

	writeTs *typeutil.ConcurrentMap[collectionCacheKey, *atomic.Uint64]
}

func (c *collectionCache) GetCollection(ctx context.Context, key collectionCacheKey) (*entity.Collection, error) {
	coll, ok := c.collections.Get(key)
	if ok {
		return coll, nil
	}

	coll, err, _ := c.sf.Do(key.String(), func() (*entity.Collection, error) {
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
	c.collections = typeutil.NewConcurrentMap[collectionCacheKey, *entity.Collection]()
	c.writeTs = typeutil.NewConcurrentMap[collectionCacheKey, *atomic.Uint64]()
}

func newCollectionCache(fetcher func(context.Context, string) (*entity.Collection, error)) *collectionCache {
	return &collectionCache{
		collections: typeutil.NewConcurrentMap[collectionCacheKey, *entity.Collection](),
		writeTs:     typeutil.NewConcurrentMap[collectionCacheKey, *atomic.Uint64](),
		fetcher:     fetcher,
	}
}

func (c *Client) getCollection(ctx context.Context, collName string) (*entity.Collection, error) {
	return c.collCache.GetCollection(ctx, newCollectionCacheKey(c.getCurrentDB(), collName))
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
