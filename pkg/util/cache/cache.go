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
	"context"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

var (
	ErrNoSuchItem     = merr.WrapErrServiceInternal("no such item")
	ErrNotEnoughSpace = merr.WrapErrServiceInternal("not enough space")
	ErrTimeOut        = merr.WrapErrServiceInternal("time out")
)

type cacheItem[K comparable, V any] struct {
	key        K
	value      V
	pinCount   atomic.Int32
	needReload bool
}

type (
	Loader[K comparable, V any]    func(ctx context.Context, key K) (V, error)
	Finalizer[K comparable, V any] func(ctx context.Context, key K, value V) error
)

// Scavenger records occupation of cache and decide whether to evict if necessary.
//
//	The scavenger makes decision based on keys only, and it is called before value loading,
//	because value loading could be very expensive.
type Scavenger[K comparable] interface {
	// Collect records entry additions, if there is room, return true, or else return false and a collector.
	//	The collector is a function which can be invoked repetedly, each invocation will test if there is enough
	//	room provided that all entries in the collector is evicted. Typically, the collector will get multiple false
	//	before it gets a true.
	Collect(key K) (bool, func(K) bool)
	// Throw records entry removals.
	Throw(key K)
	// Spare returns a collector function based on given key.
	//	The collector is a function which can be invoked repetedly, each invocation will test if there is enough
	//	room for all the pending entries if the thrown entry is evicted. Typically, the collector will get multiple true
	//	before it gets a false.
	Spare(key K) func(K) bool
	Replace(key K) (bool, func(K) bool, func())
}

type LazyScavenger[K comparable] struct {
	capacity int64
	size     int64
	weight   func(K) int64
	weights  map[K]int64
}

func NewLazyScavenger[K comparable](weight func(K) int64, capacity int64) *LazyScavenger[K] {
	return &LazyScavenger[K]{
		capacity: capacity,
		weight:   weight,
		weights:  make(map[K]int64),
	}
}

func (s *LazyScavenger[K]) Collect(key K) (bool, func(K) bool) {
	w := s.weight(key)
	if s.size+w > s.capacity {
		needCollect := s.size + w - s.capacity
		return false, func(key K) bool {
			needCollect -= s.weights[key]
			return needCollect <= 0
		}
	}
	s.size += w
	s.weights[key] = w
	return true, nil
}

func (s *LazyScavenger[K]) Replace(key K) (bool, func(K) bool, func()) {
	pw := s.weights[key]
	w := s.weight(key)
	if s.size-pw+w > s.capacity {
		needCollect := s.size - pw + w - s.capacity
		return false, func(key K) bool {
			needCollect -= s.weights[key]
			return needCollect <= 0
		}, nil
	}
	s.size += w - pw
	s.weights[key] = w
	return true, nil, func() {
		s.size -= w - pw
		s.weights[key] = pw
	}
}

func (s *LazyScavenger[K]) Throw(key K) {
	if w, ok := s.weights[key]; ok {
		s.size -= w
		delete(s.weights, key)
	}
}

func (s *LazyScavenger[K]) Spare(key K) func(K) bool {
	w := s.weight(key)
	available := s.capacity - s.size + w
	return func(k K) bool {
		available -= s.weight(k)
		return available >= 0
	}
}

type Stats struct {
	HitCount            atomic.Uint64
	MissCount           atomic.Uint64
	LoadSuccessCount    atomic.Uint64
	LoadFailCount       atomic.Uint64
	TotalLoadTimeMs     atomic.Uint64
	TotalFinalizeTimeMs atomic.Uint64
	EvictionCount       atomic.Uint64
}

type Cache[K comparable, V any] interface {
	// Do the operation `doer` on the given key `key`. The key is kept in the cache until the operation
	// completes.
	// Throws `ErrNoSuchItem` if the key is not found or not able to be loaded from given loader.
	// Throws `ErrNotEnoughSpace` if there is no room for the operation.
	Do(ctx context.Context, key K, doer func(V) error) (missing bool, err error)
	// Do the operation `doer` on the given key `key`. The key is kept in the cache until the operation
	// completes. The function waits for `timeout` if there is not enough space for the given key.
	// Throws `ErrNoSuchItem` if the key is not found or not able to be loaded from given loader.
	// Throws `ErrTimeOut` if timed out.
	DoWait(ctx context.Context, key K, timeout time.Duration, doer func(context.Context, V) error) (missing bool, err error)
	// Get stats
	Stats() *Stats

	MarkItemNeedReload(ctx context.Context, key K) bool

	// Expire removes the item from the cache.
	// Return true if the item is not in used and removed immediately or the item is not in cache now.
	// Return false if the item is in used, it will be marked as need to be reloaded, a lazy expire is applied.
	Expire(ctx context.Context, key K) (evicted bool)
}

type Waiter[K comparable] struct {
	key K
	c   *sync.Cond
}

func newWaiter[K comparable](key K) Waiter[K] {
	return Waiter[K]{
		key: key,
		c:   sync.NewCond(&sync.Mutex{}),
	}
}

// lruCache extends the ccache library to provide pinning and unpinning of items.
type lruCache[K comparable, V any] struct {
	rwlock sync.RWMutex
	// the value is *cacheItem[V]
	items          map[K]*list.Element
	accessList     *list.List
	loaderKeyLocks *lock.KeyLock[K]
	stats          *Stats
	waitQueue      *list.List

	loader    Loader[K, V]
	finalizer Finalizer[K, V]
	scavenger Scavenger[K]
	reloader  Loader[K, V]
}

type CacheBuilder[K comparable, V any] struct {
	loader    Loader[K, V]
	finalizer Finalizer[K, V]
	scavenger Scavenger[K]
	reloader  Loader[K, V]
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

func (b *CacheBuilder[K, V]) WithCapacity(capacity int64) *CacheBuilder[K, V] {
	b.scavenger = NewLazyScavenger(
		func(key K) int64 {
			return 1
		},
		capacity,
	)
	return b
}

func (b *CacheBuilder[K, V]) WithReloader(reloader Loader[K, V]) *CacheBuilder[K, V] {
	b.reloader = reloader
	return b
}

func (b *CacheBuilder[K, V]) Build() Cache[K, V] {
	return newLRUCache(b.loader, b.finalizer, b.scavenger, b.reloader)
}

func newLRUCache[K comparable, V any](
	loader Loader[K, V],
	finalizer Finalizer[K, V],
	scavenger Scavenger[K],
	reloader Loader[K, V],
) Cache[K, V] {
	return &lruCache[K, V]{
		items:          make(map[K]*list.Element),
		accessList:     list.New(),
		waitQueue:      list.New(),
		loaderKeyLocks: lock.NewKeyLock[K](),
		stats:          new(Stats),
		loader:         loader,
		finalizer:      finalizer,
		scavenger:      scavenger,
		reloader:       reloader,
	}
}

// Do picks up an item from cache and executes doer. The entry of interest is garented in the cache when doer is executing.
func (c *lruCache[K, V]) Do(ctx context.Context, key K, doer func(V) error) (bool, error) {
	item, missing, err := c.getAndPin(ctx, key)
	if err != nil {
		return missing, err
	}
	defer c.Unpin(key)
	return missing, doer(item.value)
}

func (c *lruCache[K, V]) DoWait(ctx context.Context, key K, timeout time.Duration, doer func(context.Context, V) error) (bool, error) {
	timedWait := func(cond *sync.Cond, timeout time.Duration) error {
		if timeout <= 0 {
			return ErrTimeOut // timed out
		}
		c := make(chan struct{})
		go func() {
			cond.L.Lock()
			defer cond.L.Unlock()
			defer close(c)
			cond.Wait()
		}()
		select {
		case <-c:
			return nil // completed normally
		case <-time.After(timeout):
			return ErrTimeOut // timed out
		case <-ctx.Done():
			return ctx.Err() // context timeout
		}
	}

	var ele *list.Element
	defer func() {
		if ele != nil {
			c.rwlock.Lock()
			c.waitQueue.Remove(ele)
			c.rwlock.Unlock()
		}
	}()
	start := time.Now()
	log := log.Ctx(ctx).With(zap.Any("key", key))
	for {
		item, missing, err := c.getAndPin(ctx, key)
		if err == nil {
			defer c.Unpin(key)
			return missing, doer(ctx, item.value)
		} else if err == ErrNotEnoughSpace {
			log.Warn("Failed to get disk cache for segment, wait and try again")
		} else if err == merr.ErrServiceResourceInsufficient {
			log.Warn("Failed to load segment for insufficient resource, wait and try later")
		} else if err != nil {
			return true, err
		}
		if ele == nil {
			// If no enough space, enqueue the key
			c.rwlock.Lock()
			waiter := newWaiter(key)
			ele = c.waitQueue.PushBack(&waiter)
			log.Info("push waiter into waiter queue", zap.Any("key", key))
			c.rwlock.Unlock()
		}
		// Wait for the key to be available
		timeLeft := time.Until(start.Add(timeout))
		if err = timedWait(ele.Value.(*Waiter[K]).c, timeLeft); err != nil {
			log.Warn("failed to get item for key",
				zap.Any("key", key),
				zap.Int("wait_len", c.waitQueue.Len()),
				zap.Error(err))
			return true, err
		}
	}
}

func (c *lruCache[K, V]) Stats() *Stats {
	return c.stats
}

func (c *lruCache[K, V]) Unpin(key K) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()
	e, ok := c.items[key]
	if !ok {
		return
	}
	item := e.Value.(*cacheItem[K, V])
	item.pinCount.Dec()

	log := log.With(zap.Any("UnPinedKey", key))
	if item.pinCount.Load() == 0 && c.waitQueue.Len() > 0 {
		log.Debug("Unpin item to zero ref, trigger activating waiters")
		// Notify waiters
		// collector := c.scavenger.Spare(key)
		for e := c.waitQueue.Front(); e != nil; e = e.Next() {
			w := e.Value.(*Waiter[K])
			log.Info("try to activate waiter", zap.Any("activated_waiter_key", w.key))
			w.c.Broadcast()
			// we try best to activate as many waiters as possible every time
		}
	} else {
		log.Debug("Miss to trigger activating waiters",
			zap.Int32("PinCount", item.pinCount.Load()),
			zap.Int("wait_len", c.waitQueue.Len()))
	}
}

func (c *lruCache[K, V]) peekAndPin(ctx context.Context, key K) *cacheItem[K, V] {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()
	e, ok := c.items[key]
	log := log.Ctx(ctx)
	if ok {
		item := e.Value.(*cacheItem[K, V])
		if item.needReload && item.pinCount.Load() == 0 {
			ok, _, retback := c.scavenger.Replace(key)
			if ok {
				// there is room for reload and no one is using the item
				if c.reloader != nil {
					reloaded, err := c.reloader(ctx, key)
					if err == nil {
						item.value = reloaded
					} else if retback != nil {
						retback()
					}
				}
				item.needReload = false
			}
		}
		c.accessList.MoveToFront(e)
		item.pinCount.Inc()
		log.Debug("peeked item success",
			zap.Int32("PinCount", item.pinCount.Load()),
			zap.Any("key", key))
		return item
	}
	log.Debug("failed to peek item", zap.Any("key", key))
	return nil
}

// GetAndPin gets and pins the given key if it exists
func (c *lruCache[K, V]) getAndPin(ctx context.Context, key K) (*cacheItem[K, V], bool, error) {
	if item := c.peekAndPin(ctx, key); item != nil {
		c.stats.HitCount.Inc()
		return item, false, nil
	}
	log := log.Ctx(ctx)
	c.stats.MissCount.Inc()
	if c.loader != nil {
		// Try scavenge if there is room. If not, fail fast.
		//	Note that the test is not accurate since we are not locking `loader` here.
		if _, ok := c.tryScavenge(key); !ok {
			log.Warn("getAndPin ran into scavenge failure, return", zap.Any("key", key))
			return nil, true, ErrNotEnoughSpace
		}
		c.loaderKeyLocks.Lock(key)
		defer c.loaderKeyLocks.Unlock(key)
		if item := c.peekAndPin(ctx, key); item != nil {
			return item, false, nil
		}
		timer := time.Now()
		value, err := c.loader(ctx, key)
		c.stats.TotalLoadTimeMs.Add(uint64(time.Since(timer).Milliseconds()))
		if err != nil {
			c.stats.LoadFailCount.Inc()
			log.Debug("loader failed for key", zap.Any("key", key))
			return nil, true, err
		}

		c.stats.LoadSuccessCount.Inc()
		item, err := c.setAndPin(ctx, key, value)
		if err != nil {
			log.Debug("setAndPin failed for key", zap.Any("key", key), zap.Error(err))
			return nil, true, err
		}
		return item, true, nil
	}
	return nil, true, ErrNoSuchItem
}

func (c *lruCache[K, V]) tryScavenge(key K) ([]K, bool) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()
	return c.lockfreeTryScavenge(key)
}

func (c *lruCache[K, V]) lockfreeTryScavenge(key K) ([]K, bool) {
	ok, collector := c.scavenger.Collect(key)
	toEvict := make([]K, 0)
	if !ok {
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
		// If no collection needed, give back the space.
		c.scavenger.Throw(key)
	}
	return toEvict, true
}

// for cache miss
func (c *lruCache[K, V]) setAndPin(ctx context.Context, key K, value V) (*cacheItem[K, V], error) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	item := &cacheItem[K, V]{key: key, value: value}
	item.pinCount.Inc()

	// tryScavenge is done again since the load call is lock free.
	toEvict, ok := c.lockfreeTryScavenge(key)
	log := log.Ctx(ctx)
	if !ok {
		if c.finalizer != nil {
			log.Warn("setAndPin ran into scavenge failure, release data for", zap.Any("key", key))
			c.finalizer(ctx, key, value)
		}
		return nil, ErrNotEnoughSpace
	}

	for _, ek := range toEvict {
		c.evict(ctx, ek)
		log.Debug("cache evicting", zap.Any("key", ek), zap.Any("by", key))
	}

	c.scavenger.Collect(key)
	e := c.accessList.PushFront(item)
	c.items[item.key] = e
	log.Debug("setAndPin set up item", zap.Any("item.key", item.key),
		zap.Int32("pinCount", item.pinCount.Load()))
	return item, nil
}

func (c *lruCache[K, V]) Expire(ctx context.Context, key K) (evicted bool) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	e, ok := c.items[key]
	if !ok {
		return true
	}

	item := e.Value.(*cacheItem[K, V])
	if item.pinCount.Load() == 0 {
		c.evict(ctx, key)
		return true
	}

	item.needReload = true
	return false
}

func (c *lruCache[K, V]) evict(ctx context.Context, key K) {
	c.stats.EvictionCount.Inc()
	e := c.items[key]
	delete(c.items, key)
	c.accessList.Remove(e)
	c.scavenger.Throw(key)

	if c.finalizer != nil {
		item := e.Value.(*cacheItem[K, V])
		c.finalizer(ctx, key, item.value)
	}
}

func (c *lruCache[K, V]) MarkItemNeedReload(ctx context.Context, key K) bool {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	if e, ok := c.items[key]; ok {
		item := e.Value.(*cacheItem[K, V])
		item.needReload = true
		return true
	}

	return false
}
