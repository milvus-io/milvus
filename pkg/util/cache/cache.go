package cache

import (
	"container/list"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
)

var (
	ErrNoSuchItem     = errors.New("no such item")
	ErrNotEnoughSpace = errors.New("not enough space")
)

type cacheItem[K comparable, V any] struct {
	key      K
	value    V
	pinCount atomic.Int32
}

func newCacheItem[K comparable, V any](key K, value V) *cacheItem[K, V] {
	return &cacheItem[K, V]{
		key:   key,
		value: value,
	}
}

func (item *cacheItem[K, V]) Unpin() {
	item.pinCount.Dec()
}

func (i *cacheItem[K, V]) Value() V {
	return i.value
}

type (
	Loader[K comparable, V any]    func(key K) (V, bool)
	Finalizer[K comparable, V any] func(key K, value V) error
)

// Scavenger records occupation of cache and decide whether to evict if necessary.
//
//	The scavenger makes decision based on keys only, and it is called before value loading,
//	because value loading could be very expensive.
type Scavenger[K comparable] interface {
	// Collect records entry additions, if there is room, return true, or else return false and a collector.
	//	The collector is a function which can be invoked repetedly, each invocation will test if there is enough
	//	room provided that all entries in the collector is evicted.
	Collect(key K) (bool, func(K) bool)
	// Throw records entry removals.
	Throw(key K)
}

type LazyScavenger[K comparable] struct {
	capacity int64
	size     int64
	weight   func(K) int64
}

func NewLazyScavenger[K comparable](weight func(K) int64, capacity int64) *LazyScavenger[K] {
	return &LazyScavenger[K]{
		capacity: capacity,
		weight:   weight,
	}
}

func (s *LazyScavenger[K]) Collect(key K) (bool, func(K) bool) {
	w := s.weight(key)
	if s.size+w > s.capacity {
		needCollect := s.size + w - s.capacity
		return false, func(key K) bool {
			needCollect -= s.weight(key)
			return needCollect <= 0
		}
	}
	s.size += w
	return true, nil
}

func (s *LazyScavenger[K]) Throw(key K) {
	s.size -= s.weight(key)
}

type Cache[K comparable, V any] interface {
	Do(key K, doer func(V) error) error
}

// lruCache extends the ccache library to provide pinning and unpinning of items.
type lruCache[K comparable, V any] struct {
	rwlock sync.RWMutex
	// the value is *cacheItem[V]
	items      map[K]*list.Element
	accessList *list.List

	loader    Loader[K, V]
	finalizer Finalizer[K, V]
	scavenger Scavenger[K]
}

type CacheBuilder[K comparable, V any] struct {
	loader    Loader[K, V]
	finalizer Finalizer[K, V]
	scavenger Scavenger[K]
}

func NewCacheBuilder[K comparable, V any]() *CacheBuilder[K, V] {
	return &CacheBuilder[K, V]{
		loader:    nil,
		finalizer: nil,
		scavenger: NewLazyScavenger(
			func(key K) int64 {
				return 1
			},
			64,
		),
	}
}

func (b *CacheBuilder[K, V]) WithLoader(loader Loader[K, V]) *CacheBuilder[K, V] {
	b.loader = loader
	return b
}

func (b *CacheBuilder[K, V]) WithFinalizer(finalizer Finalizer[K, V]) *CacheBuilder[K, V] {
	b.finalizer = finalizer
	return b
}

func (b *CacheBuilder[K, V]) WithLazyScavenger(weight func(K) int64, capacity int64) *CacheBuilder[K, V] {
	b.scavenger = NewLazyScavenger(weight, capacity)
	return b
}

func (b *CacheBuilder[K, V]) WithCapacityScavenger(capacity int64) *CacheBuilder[K, V] {
	b.scavenger = NewLazyScavenger(
		func(key K) int64 {
			return 1
		},
		capacity,
	)
	return b
}

func (b *CacheBuilder[K, V]) Build() Cache[K, V] {
	return newLRUCache(b.loader, b.finalizer, b.scavenger)
}

func newLRUCache[K comparable, V any](
	loader Loader[K, V],
	finalizer Finalizer[K, V],
	scavenger Scavenger[K],
) Cache[K, V] {
	return &lruCache[K, V]{
		items:      make(map[K]*list.Element),
		accessList: list.New(),
		loader:     loader,
		finalizer:  finalizer,
		scavenger:  scavenger,
	}
}

// Do picks up an item from cache and executes doer. The entry of interest is garented in the cache when doer is executing.
func (c *lruCache[K, V]) Do(key K, doer func(V) error) error {
	item, err := c.getAndPin(key)
	if err != nil {
		return err
	}
	defer item.Unpin()

	if err := doer(item.Value()); err != nil {
		return err
	}
	return nil
}

// GetAndPin gets and pins the given key if it exists,
// NOTE: remember to unpin this key or it will never be evicted
func (c *lruCache[K, V]) getAndPin(key K) (*cacheItem[K, V], error) {
	c.rwlock.Lock()

	iter, ok := c.items[key]
	if ok {
		item := iter.Value.(*cacheItem[K, V])
		c.accessList.Remove(iter)
		c.accessList.PushFront(item)
		item.pinCount.Inc()

		c.rwlock.Unlock()
		return item, nil
	}

	c.rwlock.Unlock()
	if c.loader != nil {
		// Try scavenge if there is room. If not, fail fast.
		//	Note that the test is not accurate since we are not holding the lock here.
		if _, ok := c.tryScavenge(key); !ok {
			return nil, ErrNotEnoughSpace
		}
		value, ok := c.loader(key)
		if ok {
			item, err := c.setAndPin(key, value)
			if err != nil {
				return nil, err
			}
			return item, nil
		}
	}

	return nil, ErrNoSuchItem
}

func (c *lruCache[K, V]) tryScavenge(key K) ([]K, bool) {
	ok, collector := c.scavenger.Collect(key)
	toEvict := make([]K, 0)
	if !ok {
		// Try evict to make room
		done := false
		for p := c.accessList.Back(); p != nil && !done; p = p.Prev() {
			evictItem := p.Value.(*cacheItem[K, V])
			if evictItem.pinCount.Load() > 0 {
				continue
			}
			toEvict = append(toEvict, evictItem.key)
			done = collector(evictItem.key)
		}
		if !done {
			return nil, false
		}
	} else {
		c.scavenger.Throw(key)
	}
	return toEvict, true
}

// for cache miss
func (c *lruCache[K, V]) setAndPin(key K, value V) (*cacheItem[K, V], error) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	item := newCacheItem[K, V](key, value)
	item.pinCount.Inc()

	// tryScavenge is done again since the previous call is not protected by lock,
	//	thus the result is not accurate.
	toEvict, ok := c.tryScavenge(key)
	if !ok {
		item.pinCount.Dec()
		return nil, ErrNotEnoughSpace
	}

	for _, key := range toEvict {
		c.remove(key)
	}

	c.add(item)
	return item, nil
}

func (c *lruCache[K, V]) add(item *cacheItem[K, V]) {
	c.scavenger.Collect(item.key)
	iter := c.accessList.PushFront(item)
	c.items[item.key] = iter
}

func (c *lruCache[K, V]) remove(key K) {
	iter, ok := c.items[key]
	if !ok {
		return
	}

	delete(c.items, key)
	c.accessList.Remove(iter)
	c.scavenger.Throw(key)

	if c.finalizer != nil {
		item := iter.Value.(*cacheItem[K, V])
		c.finalizer(key, item.value)
	}
}
