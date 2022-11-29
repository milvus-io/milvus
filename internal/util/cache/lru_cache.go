// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
)

// LRU generic utility for lru cache.
type LRU[K comparable, V any] struct {
	evictList *list.List
	items     map[K]*list.Element
	capacity  int
	onEvicted func(k K, v V)
	m         sync.RWMutex
	evictedCh chan *entry[K, V]
	closeCh   chan struct{}
	closeOnce sync.Once
	stats     *Stats
}

// Stats is the model for cache statistics.
type Stats struct {
	hitCount     float32
	evictedCount float32
	readCount    float32
	writeCount   float32
}

// String implement stringer for printing.
func (s *Stats) String() string {
	var hitRatio float32
	var evictedRatio float32
	if s.readCount != 0 {
		hitRatio = s.hitCount / s.readCount
		evictedRatio = s.evictedCount / s.writeCount
	}

	return fmt.Sprintf("lru cache hit ratio = %f, evictedRatio = %f", hitRatio, evictedRatio)
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRU creates a LRU cache with provided capacity and `onEvicted` function.
// `onEvicted` will be executed when an item is chosed to be evicted.
func NewLRU[K comparable, V any](capacity int, onEvicted func(k K, v V)) (*LRU[K, V], error) {
	if capacity <= 0 {
		return nil, errors.New("cache size must be positive")
	}
	c := &LRU[K, V]{
		capacity:  capacity,
		evictList: list.New(),
		items:     make(map[K]*list.Element),
		onEvicted: onEvicted,
		evictedCh: make(chan *entry[K, V], 16),
		closeCh:   make(chan struct{}),
		stats:     &Stats{},
	}
	go c.evictedWorker()
	return c, nil
}

// evictedWorker executes onEvicted function for each evicted items.
func (c *LRU[K, V]) evictedWorker() {
	for {
		select {
		case <-c.closeCh:
			return
		case e, ok := <-c.evictedCh:
			if ok {
				if c.onEvicted != nil {
					c.onEvicted(e.key, e.value)
				}
			}
		}
	}
}

// closed returns whether cache is closed.
func (c *LRU[K, V]) closed() bool {
	select {
	case <-c.closeCh:
		return true
	default:
		return false
	}
}

// Add puts an item into cache.
func (c *LRU[K, V]) Add(key K, value V) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.closed() {
		// evict since cache closed
		c.onEvicted(key, value)
		return
	}

	c.stats.writeCount++
	if e, ok := c.items[key]; ok {
		c.evictList.MoveToFront(e)
		e.Value.(*entry[K, V]).value = value
		return
	}
	e := &entry[K, V]{key: key, value: value}
	listE := c.evictList.PushFront(e)
	c.items[key] = listE

	if c.evictList.Len() > c.capacity {
		c.stats.evictedCount++
		oldestE := c.evictList.Back()
		if oldestE != nil {
			c.evictList.Remove(oldestE)
			kv := oldestE.Value.(*entry[K, V])
			delete(c.items, kv.key)
			if c.onEvicted != nil {
				c.evictedCh <- kv
			}
		}
	}
}

// Get returns value for provided key.
func (c *LRU[K, V]) Get(key K) (value V, ok bool) {
	c.m.RLock()
	defer c.m.RUnlock()

	var zeroV V
	if c.closed() {
		// cache closed, returns nothing
		return zeroV, false
	}

	c.stats.readCount++
	if e, ok := c.items[key]; ok {
		c.stats.hitCount++
		c.evictList.MoveToFront(e)
		kv := e.Value.(*entry[K, V])
		return kv.value, true
	}

	return zeroV, false
}

// Remove removes item associated with provided key.
func (c *LRU[K, V]) Remove(key K) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.closed() {
		return
	}

	if e, ok := c.items[key]; ok {
		c.evictList.Remove(e)
		kv := e.Value.(*entry[K, V])
		delete(c.items, kv.key)
		if c.onEvicted != nil {
			c.evictedCh <- kv
		}
	}
}

// Contains returns whether items with provided key exists in cache.
func (c *LRU[K, V]) Contains(key K) bool {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.closed() {
		return false
	}
	_, ok := c.items[key]
	return ok
}

// Keys returns all the keys exist in cache.
func (c *LRU[K, V]) Keys() []K {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.closed() {
		return nil
	}
	keys := make([]K, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*entry[K, V]).key
		i++
	}
	return keys
}

// Len returns items count in cache.
func (c *LRU[K, V]) Len() int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.closed() {
		return 0
	}
	return c.evictList.Len()
}

// Capacity returns cache capacity.
func (c *LRU[K, V]) Capacity() int {
	return c.capacity
}

// Purge removes all items and put them into evictedCh.
func (c *LRU[K, V]) Purge() {
	c.m.Lock()
	defer c.m.Unlock()
	for k, v := range c.items {
		if c.onEvicted != nil {
			c.evictedCh <- v.Value.(*entry[K, V])
		}
		delete(c.items, k)
	}
	c.evictList.Init()
}

// Resize changes the capacity of cache.
func (c *LRU[K, V]) Resize(capacity int) int {
	c.m.Lock()
	defer c.m.Unlock()
	if c.closed() {
		return 0
	}

	c.capacity = capacity
	if capacity >= c.evictList.Len() {
		return 0
	}
	diff := c.evictList.Len() - c.capacity
	for i := 0; i < diff; i++ {
		oldestE := c.evictList.Back()
		if oldestE != nil {
			c.evictList.Remove(oldestE)
			kv := oldestE.Value.(*entry[K, V])
			delete(c.items, kv.key)
			if c.onEvicted != nil {
				c.evictedCh <- kv
			}
		}
	}
	return diff
}

// GetOldest returns the oldest item in cache.
func (c *LRU[K, V]) GetOldest() (K, V, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	var (
		zeroK K
		zeroV V
	)
	if c.closed() {
		return zeroK, zeroV, false
	}
	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*entry[K, V])
		return kv.key, kv.value, true
	}

	return zeroK, zeroV, false
}

// Close cleans up the cache resources.
func (c *LRU[K, V]) Close() {
	c.closeOnce.Do(func() {
		// fetch lock to
		// - wait on-going operations done
		// - block incoming operations
		c.m.Lock()
		close(c.closeCh)
		c.m.Unlock()

		// execute purge in a goroutine, otherwise Purge may block forever putting evictedCh
		go func() {
			c.Purge()
			close(c.evictedCh)
		}()
		for e := range c.evictedCh {
			c.onEvicted(e.key, e.value)
		}
	})
}

// Stats returns cache statistics.
func (c *LRU[K, V]) Stats() *Stats {
	return c.stats
}
