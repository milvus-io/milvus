package cache

import (
	"time"

	"github.com/karlseguin/ccache/v3"

	"github.com/milvus-io/milvus/pkg/util/generic"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type cacheOptions struct {
	Pin bool
}

type CacheOption func(*cacheOptions)

var Pin = func(opts *cacheOptions) {
	opts.Pin = true
}

type Cache[T any] interface {
	Get(key string) (T, bool)
	Set(key string, value T, options ...CacheOption)
	Fetch(key string, f func() (T, error)) (T, error)
	Remove(key string)
}

// lruCache extends the ccache library to provide pinning and unpinning of items.
type lruCache[T any] struct {
	pinned *typeutil.ConcurrentMap[string, T]
	cache  *ccache.Cache[T]
}

func NewLRUCache[T any](size int64) Cache[T] {
	return &lruCache[T]{
		pinned: typeutil.NewConcurrentMap[string, T](),
		cache:  ccache.New(ccache.Configure[T]().MaxSize(size).ItemsToPrune(1).GetsPerPromote(0)),
	}
}

func (c *lruCache[T]) Get(key string) (T, bool) {
	v, ok := c.pinned.Get(key)
	if ok {
		return v, true
	}

	item := c.cache.Get(key)
	if item == nil {
		return generic.Zero[T](), false
	}

	return item.Value(), true
}

const unexpired = 200 * 365 * 24 * time.Hour

// const unexpired = 0

func (c *lruCache[T]) Set(key string, value T, options ...CacheOption) {
	opts := cacheOptions{}
	for _, opt := range options {
		opt(&opts)
	}

	if opts.Pin {
		c.pinned.Insert(key, value)
	} else {
		// we don't want to expire the item, so we set the expiration to 250 years
		c.cache.Set(key, value, unexpired)
	}
}

func (c *lruCache[T]) Fetch(key string, f func() (T, error)) (T, error) {
	v, ok := c.pinned.Get(key)
	if ok {
		return v, nil
	}

	item, err := c.cache.Fetch(key, unexpired, f)
	if err != nil || item == nil {
		return generic.Zero[T](), err
	}

	return item.Value(), nil
}

func (c *lruCache[T]) Remove(key string) {
	c.pinned.Remove(key)
	c.cache.Delete(key)
}
