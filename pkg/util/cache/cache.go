package cache

import (
	"container/list"
	"sync"

	"go.uber.org/atomic"
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
	OnCacheMiss[K comparable, V any] func(key K) (V, bool)
	OnEvict[K comparable, V any]     func(key K, value V)
)

type Cache[K comparable, V any] interface {
	GetAndPin(key K) (item *cacheItem[K, V], missing bool, ok bool)
	Contain(key K) bool
	Set(key K, value V)
	Remove(key K)
}

// lruCache extends the ccache library to provide pinning and unpinning of items.
type lruCache[K comparable, V any] struct {
	rwlock sync.RWMutex
	// the value is *cacheItem[V]
	items      map[K]*list.Element
	accessList *list.List

	size int32
	cap  int32

	onCacheMiss OnCacheMiss[K, V]
	onEvict     OnEvict[K, V]
}

func NewLRUCache[K comparable, V any](
	cap int32,
	onCacheMiss OnCacheMiss[K, V],
	onEvict OnEvict[K, V],
) Cache[K, V] {
	return &lruCache[K, V]{
		items:       make(map[K]*list.Element),
		accessList:  list.New(),
		cap:         cap,
		onCacheMiss: onCacheMiss,
		onEvict:     onEvict,
	}
}

// GetAndPin gets and pins the given key if it exists,
// NOTE: remember to unpin this key or it will never be evicted
func (c *lruCache[K, V]) GetAndPin(key K) (*cacheItem[K, V], bool, bool) {
	c.rwlock.Lock()

	iter, ok := c.items[key]
	if ok {
		item := iter.Value.(*cacheItem[K, V])
		c.accessList.Remove(iter)
		c.accessList.PushFront(item)
		item.pinCount.Inc()

		c.rwlock.Unlock()
		return item, false, true
	}

	c.rwlock.Unlock()

	// cache missing.
	if c.onCacheMiss != nil {
		value, ok := c.onCacheMiss(key)
		if ok {
			c.rwlock.Lock()
			defer c.rwlock.Unlock()
			item := c.setAndPin(key, value)
			return item, true, true
		}
	}

	return nil, true, false
}

func (c *lruCache[K, V]) Contain(key K) bool {
	c.rwlock.RLock()
	defer c.rwlock.RUnlock()

	_, ok := c.items[key]
	return ok
}

func (c *lruCache[K, V]) Set(key K, value V) {
	item := newCacheItem[K, V](key, value)

	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	if c.size >= c.cap {
		c.evict(c.size - c.cap + 1)
	}

	c.add(item)
}

// for cache miss
func (c *lruCache[K, V]) setAndPin(key K, value V) *cacheItem[K, V] {
	item := newCacheItem[K, V](key, value)
	item.pinCount.Inc()

	if c.size >= c.cap {
		c.evict(c.size - c.cap + 1)
	}

	c.add(item)
	return item
}

func (c *lruCache[K, V]) add(item *cacheItem[K, V]) {
	iter := c.accessList.PushFront(item)
	c.items[item.key] = iter
	c.size++
}

func (c *lruCache[K, V]) evict(n int32) {
	if c.size < n {
		n = c.size
	}
	for ; n > 0; n-- {
		for {
			oldest := c.accessList.Back()
			item := oldest.Value.(*cacheItem[K, V])
			if item.pinCount.Load() > 0 {
				c.accessList.MoveToFront(oldest)
			} else {
				break
			}
		}

		// evict
		validOldest := c.accessList.Back()
		item := validOldest.Value.(*cacheItem[K, V])
		c.accessList.Remove(validOldest)
		delete(c.items, item.key)
		c.size--

		// TODO(yah01): this could be optimized as it doesn't need to acquire the lock
		if c.onEvict != nil {
			c.onEvict(item.key, item.value)
		}
	}
}

func (c *lruCache[K, V]) Remove(key K) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	iter, ok := c.items[key]
	if ok {
		c.accessList.Remove(iter)
		delete(c.items, key)
		c.size--
	}
}
