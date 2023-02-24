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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/milvus-io/milvus/internal/log"
)

const (
	// Default maximum number of cache entries.
	maximumCapacity = 1 << 30
	// Buffer size of entry channels
	chanBufSize = 64
	// Maximum number of entries to be drained in a single clean up.
	drainMax = 16
	// Number of cache access operations that will trigger clean up.
	drainThreshold = 64
)

// currentTime is an alias for time.Now, used for testing.
var currentTime = time.Now

// localCache is an asynchronous LRU cache.
type localCache[K comparable, V any] struct {
	// internal data structure
	cache cache // Must be aligned on 32-bit

	// user configurations
	expireAfterAccess time.Duration
	expireAfterWrite  time.Duration
	refreshAfterWrite time.Duration
	policyName        string

	onInsertion Func[K, V]
	onRemoval   Func[K, V]

	singleflight   singleflight.Group
	loader         LoaderFunc[K, V]
	getPreLoadData GetPreLoadDataFunc[K, V]

	stats StatsCounter

	// cap is the cache capacity.
	cap int64

	// accessQueue is the cache retention policy, which manages entries by access time.
	accessQueue policy
	// writeQueue is for managing entries by write time.
	// It is only fulfilled when expireAfterWrite or refreshAfterWrite is set.
	writeQueue policy
	// events is the cache event queue for processEntries
	events chan entryEvent

	// readCount is a counter of the number of reads since the last write.
	readCount int32

	// for closing routines created by this cache.
	closing int32
	closeWG sync.WaitGroup
}

// newLocalCache returns a default localCache.
// init must be called before this cache can be used.
func newLocalCache[K comparable, V any]() *localCache[K, V] {
	return &localCache[K, V]{
		cap:   maximumCapacity,
		cache: cache{},
		stats: &statsCounter{},
	}
}

// init initializes cache replacement policy after all user configuration properties are set.
func (c *localCache[K, V]) init() {
	c.accessQueue = newPolicy()
	c.accessQueue.init(&c.cache, c.cap)
	if c.expireAfterWrite > 0 || c.refreshAfterWrite > 0 {
		c.writeQueue = &recencyQueue{}
	} else {
		c.writeQueue = discardingQueue{}
	}
	c.writeQueue.init(&c.cache, c.cap)
	c.events = make(chan entryEvent, chanBufSize)

	c.closeWG.Add(1)
	go c.processEntries()

	if c.getPreLoadData != nil {
		c.asyncPreload()
	}
}

// Close implements io.Closer and always returns a nil error.
// Caller would ensure the cache is not being used (reading and writing) before closing.
func (c *localCache[K, V]) Close() error {
	if atomic.CompareAndSwapInt32(&c.closing, 0, 1) {
		// Do not close events channel to avoid panic when cache is still being used.
		c.events <- entryEvent{nil, eventClose, make(chan struct{})}
		// Wait for the goroutine to close this channel
		c.closeWG.Wait()
	}
	return nil
}

// GetIfPresent gets cached value from entries list and updates
// last access time for the entry if it is found.
func (c *localCache[K, V]) GetIfPresent(k K) (v V, exist bool) {
	en := c.cache.get(k, sum(k))
	if en == nil {
		c.stats.RecordMisses(1)
		return v, false
	}
	now := currentTime()
	if c.isExpired(en, now) {
		c.stats.RecordMisses(1)
		c.sendEvent(eventDelete, en)
		return v, false
	}
	c.stats.RecordHits(1)
	c.setEntryAccessTime(en, now)
	c.sendEvent(eventAccess, en)
	return en.getValue().(V), true
}

// Put adds new entry to entries list.
func (c *localCache[K, V]) Put(k K, v V) {
	h := sum(k)
	en := c.cache.get(k, h)
	now := currentTime()
	if en == nil {
		en = newEntry(k, v, h)
		c.setEntryWriteTime(en, now)
		c.setEntryAccessTime(en, now)
		// Add to the cache directly so the new value is available immediately.
		// However, only do this within the cache capacity (approximately).
		if c.cap == 0 || int64(c.cache.len()) < c.cap {
			cen := c.cache.getOrSet(en)
			if cen != nil {
				cen.setValue(v)
				en = cen
			}
		}
	} else {
		// Update value and send notice
		en.setValue(v)
		en.setWriteTime(now.UnixNano())
	}
	<-c.sendEvent(eventWrite, en)
}

// Invalidate removes the entry associated with key k.
func (c *localCache[K, V]) Invalidate(k K) {
	en := c.cache.get(k, sum(k))
	if en != nil {
		en.setInvalidated(true)
		c.sendEvent(eventDelete, en)
	}
}

// InvalidateAll resets entries list.
func (c *localCache[K, V]) InvalidateAll() {
	c.cache.walk(func(en *entry) {
		en.setInvalidated(true)
	})
	c.sendEvent(eventDelete, nil)
}

// Scan entries list with a filter function
func (c *localCache[K, V]) Scan(filter func(K, V) bool) map[K]V {
	ret := make(map[K]V)
	c.cache.walk(func(en *entry) {
		k := en.key.(K)
		v := en.getValue().(V)
		if filter(k, v) {
			ret[k] = v
		}
	})
	return ret
}

// Get returns value associated with k or call underlying loader to retrieve value
// if it is not in the cache. The returned value is only cached when loader returns
// nil error.
func (c *localCache[K, V]) Get(k K) (V, error) {
	val, err, _ := c.singleflight.Do(fmt.Sprintf("%v", k), func() (any, error) {
		en := c.cache.get(k, sum(k))
		if en == nil {
			c.stats.RecordMisses(1)
			return c.load(k)
		}
		// Check if this entry needs to be refreshed
		now := currentTime()
		if c.isExpired(en, now) {
			c.stats.RecordMisses(1)
			if c.loader == nil {
				c.sendEvent(eventDelete, en)
			} else {
				// Update value if expired
				c.setEntryAccessTime(en, now)
				c.refresh(en)
			}
		} else {
			c.stats.RecordHits(1)
			c.setEntryAccessTime(en, now)
			c.sendEvent(eventAccess, en)
		}
		return en.getValue(), nil
	})
	var v V
	if err != nil {
		return v, err
	}
	v = val.(V)
	return v, nil
}

// Refresh synchronously load and block until it value is loaded.
func (c *localCache[K, V]) Refresh(k K) error {
	if c.loader == nil {
		return errors.New("cache loader should be set")
	}
	en := c.cache.get(k, sum(k))
	var err error
	if en == nil {
		_, err = c.load(k)
	} else {
		err = c.refresh(en)
	}
	return err
}

// Stats copies cache stats to t.
func (c *localCache[K, V]) Stats() *Stats {
	t := &Stats{}
	c.stats.Snapshot(t)
	return t
}

// asyncPreload async preload cache by Put
func (c *localCache[K, V]) asyncPreload() error {
	var err error
	go func() {
		var data map[K]V
		data, err = c.getPreLoadData()
		if err != nil {
			return
		}

		for k, v := range data {
			c.Put(k, v)
		}
	}()

	return nil
}

func (c *localCache[K, V]) processEntries() {
	defer c.closeWG.Done()
	for e := range c.events {
		switch e.event {
		case eventWrite:
			c.write(e.entry)
			e.Done()
			c.postWriteCleanup()
		case eventAccess:
			c.access(e.entry)
			e.Done()
			c.postReadCleanup()
		case eventDelete:
			if e.entry == nil {
				c.removeAll()
			} else {
				c.remove(e.entry)
			}
			e.Done()
			c.postReadCleanup()
		case eventClose:
			c.removeAll()
			return
		}
	}
}

// sendEvent sends event only when the cache is not closing/closed.
func (c *localCache[K, V]) sendEvent(typ event, en *entry) chan struct{} {
	ch := make(chan struct{})
	if atomic.LoadInt32(&c.closing) == 0 {
		c.events <- entryEvent{en, typ, ch}
		return ch
	}
	close(ch)
	return ch
}

// This function must only be called from processEntries goroutine.
func (c *localCache[K, V]) write(en *entry) {
	ren := c.accessQueue.write(en)
	c.writeQueue.write(en)
	if c.onInsertion != nil {
		c.onInsertion(en.key.(K), en.getValue().(V))
	}
	if ren != nil {
		c.writeQueue.remove(ren)
		// An entry has been evicted
		c.stats.RecordEviction()
		if c.onRemoval != nil {
			c.onRemoval(ren.key.(K), ren.getValue().(V))
		}
	}
}

// removeAll remove all entries in the cache.
// This function must only be called from processEntries goroutine.
func (c *localCache[K, V]) removeAll() {
	c.accessQueue.iterate(func(en *entry) bool {
		c.remove(en)
		return true
	})
}

// remove removes the given element from the cache and entries list.
// It also calls onRemoval callback if it is set.
func (c *localCache[K, V]) remove(en *entry) {
	ren := c.accessQueue.remove(en)
	c.writeQueue.remove(en)
	if ren != nil && c.onRemoval != nil {
		c.onRemoval(ren.key.(K), ren.getValue().(V))
	}
}

// access moves the given element to the top of the entries list.
// This function must only be called from processEntries goroutine.
func (c *localCache[K, V]) access(en *entry) {
	c.accessQueue.access(en)
}

// load uses current loader to synchronously retrieve value for k and adds new
// entry to the cache only if loader returns a nil error.
func (c *localCache[K, V]) load(k K) (v V, err error) {
	if c.loader == nil {
		var ret V
		return ret, errors.New("cache loader function must be set")
	}

	start := currentTime()
	v, err = c.loader(k)
	now := currentTime()
	loadTime := now.Sub(start)
	if err != nil {
		c.stats.RecordLoadError(loadTime)
		return v, err
	}
	c.stats.RecordLoadSuccess(loadTime)
	en := newEntry(k, v, sum(k))
	c.setEntryWriteTime(en, now)
	c.setEntryAccessTime(en, now)
	// wait event processed
	<-c.sendEvent(eventWrite, en)

	return v, err
}

// refresh reloads value for the given key. If loader returns an error,
// that error will be omitted. Otherwise, the entry value will be updated.
func (c *localCache[K, V]) refresh(en *entry) error {
	defer en.setLoading(false)

	start := currentTime()
	v, err := c.loader(en.key.(K))
	now := currentTime()
	loadTime := now.Sub(start)
	if err == nil {
		c.stats.RecordLoadSuccess(loadTime)
		en.setValue(v)
		en.setWriteTime(now.UnixNano())
		c.sendEvent(eventWrite, en)
	} else {
		log.Warn("refresh cache fail", zap.Any("key", en.key), zap.Error(err))
		c.stats.RecordLoadError(loadTime)
	}
	return err
}

// postReadCleanup is run after entry access/delete event.
// This function must only be called from processEntries goroutine.
func (c *localCache[K, V]) postReadCleanup() {
	if atomic.AddInt32(&c.readCount, 1) > drainThreshold {
		atomic.StoreInt32(&c.readCount, 0)
		c.expireEntries()
	}
}

// postWriteCleanup is run after entry add event.
// This function must only be called from processEntries goroutine.
func (c *localCache[K, V]) postWriteCleanup() {
	atomic.StoreInt32(&c.readCount, 0)
	c.expireEntries()
}

// expireEntries removes expired entries.
func (c *localCache[K, V]) expireEntries() {
	remain := drainMax
	now := currentTime()
	if c.expireAfterAccess > 0 {
		expiry := now.Add(-c.expireAfterAccess).UnixNano()
		c.accessQueue.iterate(func(en *entry) bool {
			if remain == 0 || en.getAccessTime() >= expiry {
				// Can stop as the entries are sorted by access time.
				// (the next entry is accessed more recently.)
				return false
			}
			// accessTime + expiry passed
			c.remove(en)
			c.stats.RecordEviction()
			remain--
			return remain > 0
		})
	}
	if remain > 0 && c.expireAfterWrite > 0 {
		expiry := now.Add(-c.expireAfterWrite).UnixNano()
		c.writeQueue.iterate(func(en *entry) bool {
			if remain == 0 || en.getWriteTime() >= expiry {
				return false
			}
			// writeTime + expiry passed
			c.remove(en)
			c.stats.RecordEviction()
			remain--
			return remain > 0
		})
	}
	if remain > 0 && c.loader != nil && c.refreshAfterWrite > 0 {
		expiry := now.Add(-c.refreshAfterWrite).UnixNano()
		c.writeQueue.iterate(func(en *entry) bool {
			if remain == 0 || en.getWriteTime() >= expiry {
				return false
			}
			err := c.refresh(en)
			if err == nil {
				remain--
			}
			return remain > 0
		})
	}
}

func (c *localCache[K, V]) isExpired(en *entry, now time.Time) bool {
	if en.getInvalidated() {
		return true
	}
	if c.expireAfterAccess > 0 && en.getAccessTime() < now.Add(-c.expireAfterAccess).UnixNano() {
		// accessTime + expiry passed
		return true
	}
	if c.expireAfterWrite > 0 && en.getWriteTime() < now.Add(-c.expireAfterWrite).UnixNano() {
		// writeTime + expiry passed
		return true
	}
	return false
}

func (c *localCache[K, V]) needRefresh(en *entry, now time.Time) bool {
	if en.getLoading() {
		return false
	}
	if c.refreshAfterWrite > 0 {
		tm := en.getWriteTime()
		if tm > 0 && tm < now.Add(-c.refreshAfterWrite).UnixNano() {
			// writeTime + refresh passed
			return true
		}
	}
	return false
}

// setEntryAccessTime sets access time if needed.
func (c *localCache[K, V]) setEntryAccessTime(en *entry, now time.Time) {
	if c.expireAfterAccess > 0 {
		en.setAccessTime(now.UnixNano())
	}
}

// setEntryWriteTime sets write time if needed.
func (c *localCache[K, V]) setEntryWriteTime(en *entry, now time.Time) {
	if c.expireAfterWrite > 0 || c.refreshAfterWrite > 0 {
		en.setWriteTime(now.UnixNano())
	}
}

// NewCache returns a local in-memory Cache.
func NewCache[K comparable, V any](options ...Option[K, V]) Cache[K, V] {
	c := newLocalCache[K, V]()
	for _, opt := range options {
		opt(c)
	}
	c.init()
	return c
}

// NewLoadingCache returns a new LoadingCache with given loader function
// and cache options.
func NewLoadingCache[K comparable, V any](loader LoaderFunc[K, V], options ...Option[K, V]) LoadingCache[K, V] {
	c := newLocalCache[K, V]()
	c.loader = loader
	for _, opt := range options {
		opt(c)
	}
	c.init()
	return c
}

// Option add options for default Cache.
type Option[K comparable, V any] func(c *localCache[K, V])

// WithMaximumSize returns an Option which sets maximum size for the cache.
// Any non-positive numbers is considered as unlimited.
func WithMaximumSize[K comparable, V any](size int64) Option[K, V] {
	if size < 0 {
		size = 0
	}
	if size > maximumCapacity {
		size = maximumCapacity
	}
	return func(c *localCache[K, V]) {
		c.cap = size
	}
}

// WithRemovalListener returns an Option to set cache to call onRemoval for each
// entry evicted from the cache.
func WithRemovalListener[K comparable, V any](onRemoval Func[K, V]) Option[K, V] {
	return func(c *localCache[K, V]) {
		c.onRemoval = onRemoval
	}
}

// WithExpireAfterAccess returns an option to expire a cache entry after the
// given duration without being accessed.
func WithExpireAfterAccess[K comparable, V any](d time.Duration) Option[K, V] {
	return func(c *localCache[K, V]) {
		c.expireAfterAccess = d
	}
}

// WithExpireAfterWrite returns an option to expire a cache entry after the
// given duration from creation.
func WithExpireAfterWrite[K comparable, V any](d time.Duration) Option[K, V] {
	return func(c *localCache[K, V]) {
		c.expireAfterWrite = d
	}
}

// WithRefreshAfterWrite returns an option to refresh a cache entry after the
// given duration. This option is only applicable for LoadingCache.
func WithRefreshAfterWrite[K comparable, V any](d time.Duration) Option[K, V] {
	return func(c *localCache[K, V]) {
		c.refreshAfterWrite = d
	}
}

// WithStatsCounter returns an option which overrides default cache stats counter.
func WithStatsCounter[K comparable, V any](st StatsCounter) Option[K, V] {
	return func(c *localCache[K, V]) {
		c.stats = st
	}
}

// WithPolicy returns an option which sets cache policy associated to the given name.
// Supported policies are: lru, slru.
func WithPolicy[K comparable, V any](name string) Option[K, V] {
	return func(c *localCache[K, V]) {
		c.policyName = name
	}
}

// WithAsyncInitPreLoader return an option which to async loading data during initialization.
func WithAsyncInitPreLoader[K comparable, V any](fn GetPreLoadDataFunc[K, V]) Option[K, V] {
	return func(c *localCache[K, V]) {
		c.getPreLoadData = fn
	}
}

func WithInsertionListener[K comparable, V any](onInsertion Func[K, V]) Option[K, V] {
	return func(c *localCache[K, V]) {
		c.onInsertion = onInsertion
	}
}
