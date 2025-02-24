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

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var (
	ErrNoSuchItem     = merr.WrapErrServiceInternal("no such item")
	ErrNotEnoughSpace = merr.WrapErrServiceInternal("not enough space")
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
	Do(ctx context.Context, key K, doer func(context.Context, V) error) (missing bool, err error)

	// Get stats
	Stats() *Stats

	MarkItemNeedReload(ctx context.Context, key K) bool

	// Remove removes the item from the cache.
	// Return nil if the item is removed.
	// Return error if the Remove operation is canceled.
	Remove(ctx context.Context, key K) error
}

// lruCache extends the ccache library to provide pinning and unpinning of items.
type lruCache[K comparable, V any] struct {
	rwlock sync.RWMutex
	// the value is *cacheItem[V]
	items          map[K]*list.Element
	accessList     *list.List
	loaderKeyLocks *lock.KeyLock[K]
	stats          *Stats
	waitNotifier   *syncutil.VersionedNotifier

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
		waitNotifier:   syncutil.NewVersionedNotifier(),
		loaderKeyLocks: lock.NewKeyLock[K](),
		stats:          new(Stats),
		loader:         loader,
		finalizer:      finalizer,
		scavenger:      scavenger,
		reloader:       reloader,
	}
}

func (c *lruCache[K, V]) Do(ctx context.Context, key K, doer func(context.Context, V) error) (bool, error) {
	log := log.Ctx(ctx).With(zap.Any("key", key))
	for {
		// Get a listener before getAndPin to avoid missing the notification.
		listener := c.waitNotifier.Listen(syncutil.VersionedListenAtLatest)

		item, missing, err := c.getAndPin(ctx, key)
		if err == nil {
			defer c.Unpin(key)
			return missing, doer(ctx, item.value)
		} else if err != ErrNotEnoughSpace {
			return true, err
		}
		log.Warn("Failed to get disk cache for segment, wait and try again", zap.Error(err))

		// wait for the listener to be notified.
		if err := listener.Wait(ctx); err != nil {
			log.Warn("failed to get item for key with timeout", zap.Error(context.Cause(ctx)))
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
	if item.pinCount.Load() == 0 {
		log.Debug("Unpin item to zero ref, trigger activating waiters")
		c.waitNotifier.NotifyAll()
	} else {
		log.Debug("Miss to trigger activating waiters", zap.Int32("PinCount", item.pinCount.Load()))
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

		for retryAttempt := 0; merr.ErrServiceDiskLimitExceeded.Is(err) && retryAttempt < paramtable.Get().QueryNodeCfg.LazyLoadMaxRetryTimes.GetAsInt(); retryAttempt++ {
			// Try to evict one item if there is not enough disk space, then retry.
			c.evictItems(ctx, paramtable.Get().QueryNodeCfg.LazyLoadMaxEvictPerRetry.GetAsInt())
			value, err = c.loader(ctx, key)
		}

		if err != nil {
			c.stats.LoadFailCount.Inc()
			log.Debug("loader failed for key", zap.Any("key", key))
			return nil, true, err
		}

		c.stats.TotalLoadTimeMs.Add(uint64(time.Since(timer).Milliseconds()))
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

func (c *lruCache[K, V]) Remove(ctx context.Context, key K) error {
	for {
		listener := c.waitNotifier.Listen(syncutil.VersionedListenAtLatest)

		if c.tryToRemoveKey(ctx, key) {
			return nil
		}

		if err := listener.Wait(ctx); err != nil {
			log.Warn("failed to remove item for key with timeout", zap.Error(err))
			return err
		}
	}
}

func (c *lruCache[K, V]) tryToRemoveKey(ctx context.Context, key K) (removed bool) {
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

func (c *lruCache[K, V]) evictItems(ctx context.Context, n int) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	toEvict := make([]K, 0)
	for p := c.accessList.Back(); p != nil && n > 0; p = p.Prev() {
		evictItem := p.Value.(*cacheItem[K, V])
		if evictItem.pinCount.Load() > 0 {
			continue
		}
		toEvict = append(toEvict, evictItem.key)
		n--
	}

	for _, key := range toEvict {
		c.evict(ctx, key)
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
