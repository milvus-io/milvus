package cache

import (
	"container/list"

	"github.com/cockroachdb/errors"
)

var (
	ErrNoEvictionPossible = errors.New("no eviction possible")
	ErrKeyAlreadyExists   = errors.New("key already exists")
)

// PinnableEvictor is an interface for cache eviction.
type PinnableEvictor[K comparable] interface {
	// Evict removes one used key from the cache and returns the evicted key.
	// If the key is pinned, Evict will not remove the key.
	// If no key can be evicted, Evict will return an error.
	Evict() (K, error)
	// Pin adds a pin to the key in the cache.
	Pin(k K)
	// Unpin removes the pin from the key in the cache.
	Unpin(k K)
	// Remove removes the key from the cache.
	// If the key is pinned, Remove will return false.
	Remove(k K) bool
}

// Measure is an interface for measuring the size of a key-value pair.
type Measure[K any, V any] interface {
	measure(k K, v V) uint64
	get() uint64
	add(c uint64)
	sub(c uint64)
}

type defaultMeasure[K comparable, V any] struct {
	size uint64
}

func (m *defaultMeasure[K, V]) measure(k K, v V) uint64 {
	return 1
}

func (m *defaultMeasure[K, V]) get() uint64 {
	return m.size
}

func (m *defaultMeasure[K, V]) add(c uint64) {
	m.size += c
}

func (m *defaultMeasure[K, V]) sub(c uint64) {
	m.size -= c
}

// PinnableCache is an interface for a cache that supports pinning keys.
type PinnableCache[K comparable, V any, M Measure[K, V]] interface {
	// Get returns the value associated with the key and a boolean indicating if the key exists.
	// Get will also pin the key in the cache.
	Get(k K) (V, bool)
	// Put inserts a key-value pair into the cache.
	// If the key already exists, Put will return an error.
	// Put will also pin the key in the cache.
	Put(k K, v V) error
	// Unpin removes the pin from the key in the cache.
	Unpin(k K)
	// Evict removes one used key-value pair from the cache and returns the evicted key-value pair.
	Evict() (K, V, error)
	// Capacity returns the capacity of the cache.
	Capacity() uint64
	// Size returns the size of the cache.
	Size() uint64
	// Remove removes the key-value pair from the cache.
	// If a key is pinned, Remove will return false.
	// If a key is not found, Remove will do nothing and return true.
	Remove(k K) bool
}

type refItem[K comparable] struct {
	ref uint
	k   K
}

// LRUEvictor is an implementation of PinnableEvictor using LRU eviction.
type LRUEvictor[K comparable] struct {
	list *list.List // back is the least recently used
	emap map[K]*list.Element
}

func (e *LRUEvictor[K]) Evict() (K, error) {
	for i := e.list.Back(); i != nil; i = i.Prev() {
		k := i.Value.(*refItem[K])
		if k.ref > 0 {
			continue
		}
		if _, ok := e.emap[k.k]; ok {
			e.list.Remove(i)
			delete(e.emap, k.k)
			return k.k, nil
		}
	}

	var r K
	return r, ErrNoEvictionPossible
}

func (e *LRUEvictor[K]) Pin(k K) {
	if v, ok := e.emap[k]; ok {
		e.list.MoveToFront(v)
		v.Value.(*refItem[K]).ref++
		return
	}
	e.emap[k] = e.list.PushFront(&refItem[K]{k: k, ref: 1})
}

func (e *LRUEvictor[K]) Unpin(k K) {
	if v, ok := e.emap[k]; ok {
		v.Value.(*refItem[K]).ref--
	}
}

func (e *LRUEvictor[K]) Remove(k K) bool {
	if v, ok := e.emap[k]; ok {
		if v.Value.(*refItem[K]).ref > 0 {
			return false
		}
		e.list.Remove(v)
		delete(e.emap, k)
	}
	return true
}

func NewLRUEvictor[K comparable]() *LRUEvictor[K] {
	return &LRUEvictor[K]{list: list.New(), emap: make(map[K]*list.Element)}
}

type CacheImpl[K comparable, V any, E PinnableEvictor[K], M Measure[K, V]] struct {
	capacity uint64
	items    map[K]V

	evictor E
	measure M
}

func (c *CacheImpl[K, V, E, M]) Get(k K) (V, bool) {
	if v, ok := c.items[k]; ok {
		c.evictor.Pin(k)
		return v, true
	}

	var r V
	return r, false
}

func (c *CacheImpl[K, V, E, M]) Put(k K, v V) error {
	if _, ok := c.items[k]; ok {
		return ErrKeyAlreadyExists
	}

	c.measure.add(c.measure.measure(k, v))
	c.items[k] = v
	c.evictor.Pin(k)

	for c.measure.get() > c.capacity {
		k, err := c.evictor.Evict()
		if err != nil {
			return err
		}

		c.measure.sub(c.measure.measure(k, c.items[k]))
		delete(c.items, k)
	}

	return nil
}

func (c *CacheImpl[K, V, E, M]) Unpin(k K) {
	c.evictor.Unpin(k)
}

func (c *CacheImpl[K, V, E, M]) Evict() (K, V, error) {
	var k K
	var v V
	var err error
	k, err = c.evictor.Evict()
	if err != nil {
		return k, v, err
	}

	v = c.items[k]
	c.measure.sub(c.measure.measure(k, v))
	delete(c.items, k)

	return k, v, nil
}

func (c *CacheImpl[K, V, E, M]) Capacity() uint64 {
	return c.capacity
}

func (c *CacheImpl[K, V, E, M]) Size() uint64 {
	return c.measure.get()
}

func (c *CacheImpl[K, V, E, M]) Remove(k K) bool {
	return c.evictor.Remove(k)
}

func NewCacheImpl[K comparable, V any, E PinnableEvictor[K], M Measure[K, V]](capacity uint64, evictor E, measure M) *CacheImpl[K, V, E, M] {
	return &CacheImpl[K, V, E, M]{capacity: capacity, items: make(map[K]V), evictor: evictor, measure: measure}
}

// type SegmentCache[C PinnableCache[int64, any, *defaultMeasure[int64, any]]] struct {
// }

// type LRUSegmentCache = SegmentCache[*CacheImpl[int64, any, *LRUEvictor[int64], *defaultMeasure[int64, any]]]
