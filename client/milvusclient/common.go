package milvusclient

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// CollectionCache stores the cached collection schema information.
type CollectionCache struct {
	sf          conc.Singleflight[*entity.Collection]
	collections *typeutil.ConcurrentMap[string, *entity.Collection]
	fetcher     func(context.Context, string) (*entity.Collection, error)
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

// Evict removes the collection cache related to the provided collection name.
func (c *CollectionCache) Evict(collName string) {
	c.collections.Remove(collName)
}

// Reset clears all cached info, used when client switching env.
func (c *CollectionCache) Reset() {
	c.collections = typeutil.NewConcurrentMap[string, *entity.Collection]()
}

func NewCollectionCache(fetcher func(context.Context, string) (*entity.Collection, error)) *CollectionCache {
	return &CollectionCache{
		collections: typeutil.NewConcurrentMap[string, *entity.Collection](),
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
