package client

import (
	"context"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
