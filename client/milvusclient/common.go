package milvusclient

import (
	"context"
	"math"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

// CollectionCache stores the cached collection schema information together with
// the latest mutation timestamp of each collection.
//
// DML operations (insert/upsert/delete) feed the write timestamp through
// UpdateWriteTs; search/query requests then construct a guarantee_timestamp
// from it so that session consistency (read-your-writes) and the
// default-consistency path can be honored without extra RPCs.
type CollectionCache struct {
	sf          conc.Singleflight[*entity.Collection]
	collections *typeutil.ConcurrentMap[string, *entity.Collection]
	fetcher     func(context.Context, string) (*entity.Collection, error)

	writeTs *typeutil.ConcurrentMap[string, *atomic.Uint64]
}

func (c *CollectionCache) GetCollection(ctx context.Context, collName string) (*entity.Collection, error) {
	coll, ok := c.collections.Get(collName)
	if ok {
		return coll, nil
	}

	coll, err, _ := c.sf.Do(collName, func() (*entity.Collection, error) {
		coll, err := c.fetcher(ctx, collName)
		if err != nil {
			return nil, err
		}
		c.collections.Insert(collName, coll)
		return coll, nil
	})
	return coll, err
}

// Evict removes the cached collection schema related to the provided collection name.
// The cached write timestamp is kept so that session consistency (read-your-writes)
// is preserved across schema-mutating operations.
func (c *CollectionCache) Evict(collName string) {
	c.collections.Remove(collName)
}

// EvictWriteTs removes the cached write timestamp of the provided collection name.
// Used when the write timeline of a collection no longer exists, e.g. after
// drop or rename.
func (c *CollectionCache) EvictWriteTs(collName string) {
	c.writeTs.Remove(collName)
}

// UpdateWriteTs records the latest mutation timestamp of a collection.
// Only a newer timestamp overwrites the cached one.
func (c *CollectionCache) UpdateWriteTs(collName string, ts uint64) {
	if ts == 0 {
		return
	}
	entry, ok := c.writeTs.Get(collName)
	if !ok {
		entry = &atomic.Uint64{}
		entry.Store(ts)
		existing, _ := c.writeTs.GetOrInsert(collName, entry)
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
func (c *CollectionCache) GetWriteTs(collName string) uint64 {
	entry, ok := c.writeTs.Get(collName)
	if !ok {
		return 0
	}
	return entry.Load()
}

// Reset clears all cached info, used when client switching env.
func (c *CollectionCache) Reset() {
	c.collections = typeutil.NewConcurrentMap[string, *entity.Collection]()
	c.writeTs = typeutil.NewConcurrentMap[string, *atomic.Uint64]()
}

func NewCollectionCache(fetcher func(context.Context, string) (*entity.Collection, error)) *CollectionCache {
	return &CollectionCache{
		collections: typeutil.NewConcurrentMap[string, *entity.Collection](),
		writeTs:     typeutil.NewConcurrentMap[string, *atomic.Uint64](),
		fetcher:     fetcher,
	}
}

func (c *Client) getCollection(ctx context.Context, collName string) (*entity.Collection, error) {
	return c.collCache.GetCollection(ctx, collName)
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
					c.collCache.Evict(collName)
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
//   - Customized: caller-provided timestamp if any, else eventually (1)
func (c *Client) guaranteeTimestamp(collName string, consistencyLevel commonpb.ConsistencyLevel, useDefault bool, currentTs uint64) uint64 {
	ts := c.collCache.GetWriteTs(collName)
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
	case entity.ClCustomized:
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
