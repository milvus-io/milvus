package cache

import (
	"container/list"
	"errors"
	"sync"

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

type Scavenger[K comparable, V any] interface {
	Collect(key K, value V) (bool, func(K, V) bool)
	Throw(key K, value V)
}

type LazyScavenger[K comparable, V any] struct {
	capacity int64
	size     int64
	weight   func(K, V) int64
}

func NewLazyScavenger[K comparable, V any](weight func(K, V) int64, capacity int64) *LazyScavenger[K, V] {
	return &LazyScavenger[K, V]{
		capacity: capacity,
		weight:   weight,
	}
}

func (s *LazyScavenger[K, V]) Collect(key K, value V) (bool, func(K, V) bool) {
	w := s.weight(key, value)
	if s.size+w > s.capacity {
		needCollect := s.size + w - s.capacity
		return false, func(key K, value V) bool {
			needCollect -= s.weight(key, value)
			return needCollect <= 0
		}
	}
	s.size += w
	return true, nil
}

func (s *LazyScavenger[K, V]) Throw(key K, value V) {
	s.size -= s.weight(key, value)
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
	scavenger Scavenger[K, V]
}

type CacheBuilder[K comparable, V any] struct {
	loader    Loader[K, V]
	finalizer Finalizer[K, V]
	scavenger Scavenger[K, V]
}

func NewCacheBuilder[K comparable, V any]() *CacheBuilder[K, V] {
	return &CacheBuilder[K, V]{
		loader:    nil,
		finalizer: nil,
		scavenger: NewLazyScavenger(
			func(key K, value V) int64 {
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

func (b *CacheBuilder[K, V]) WithLazyScavenger(weight func(K, V) int64, capacity int64) *CacheBuilder[K, V] {
	b.scavenger = NewLazyScavenger(weight, capacity)
	return b
}

func (b *CacheBuilder[K, V]) WithCapacityScavenger(capacity int64) *CacheBuilder[K, V] {
	b.scavenger = NewLazyScavenger(
		func(key K, value V) int64 {
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
	scavenger Scavenger[K, V],
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
		value, ok := c.loader(key)
		if ok {
			if item, err := c.setAndPin(key, value); err != nil {
				return nil, err
			} else {
				return item, nil
			}
		}
	}

	return nil, ErrNoSuchItem
}

// for cache miss
func (c *lruCache[K, V]) setAndPin(key K, value V) (*cacheItem[K, V], error) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	item := newCacheItem[K, V](key, value)
	item.pinCount.Inc()

	ok, collector := c.scavenger.Collect(key, value)
	// Try evict to make room
	if !ok {
		toEvict := make([]*list.Element, 0)
		done := false
		for p := c.accessList.Back(); p != nil && !done; p = p.Prev() {
			evictItem := p.Value.(*cacheItem[K, V])
			if evictItem.pinCount.Load() > 0 {
				continue
			}
			toEvict = append(toEvict, p)
			done = collector(evictItem.key, evictItem.value)
		}
		if !done {
			return nil, ErrNotEnoughSpace
		} else {
			for _, p := range toEvict {
				c.accessList.Remove(p)
				evictItem := p.Value.(*cacheItem[K, V])
				delete(c.items, evictItem.key)
				c.scavenger.Throw(evictItem.key, evictItem.value)

				if c.finalizer != nil {
					c.finalizer(evictItem.key, evictItem.value)
				}
			}
			// Now we have enough space
			c.scavenger.Collect(item.key, item.value)
		}
	}

	c.add(item)
	return item, nil
}

func (c *lruCache[K, V]) add(item *cacheItem[K, V]) {
	iter := c.accessList.PushFront(item)
	c.items[item.key] = iter
}
