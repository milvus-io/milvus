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
	"sync"
	"sync/atomic"
)

const (
	// Number of cache data store will be 2 ^ concurrencyLevel.
	concurrencyLevel = 2
	segmentCount     = 1 << concurrencyLevel
	segmentMask      = segmentCount - 1
)

// entry stores cached entry key and value.
type entry struct {
	// Structs with first field align to 64 bits will also be aligned to 64.
	// https://golang.org/pkg/sync/atomic/#pkg-note-BUG

	// hash is the hash value of this entry key
	hash uint64
	// accessTime is the last time this entry was accessed.
	accessTime int64 // Access atomically - must be aligned on 32-bit
	// writeTime is the last time this entry was updated.
	writeTime int64 // Access atomically - must be aligned on 32-bit

	// FIXME: More efficient way to store boolean flags
	invalidated int32
	loading     int32

	key   interface{}
	value atomic.Value // Store value

	// These properties are managed by only cache policy so do not need atomic access.

	// accessList is the list (ordered by access time) this entry is currently in.
	accessList *list.Element
	// writeList is the list (ordered by write time) this entry is currently in.
	writeList *list.Element
	// listID is ID of the list which this entry is currently in.
	listID uint8
}

func newEntry(k interface{}, v interface{}, h uint64) *entry {
	en := &entry{
		key:  k,
		hash: h,
	}
	en.setValue(v)
	return en
}

func (e *entry) getValue() interface{} {
	return e.value.Load()
}

func (e *entry) setValue(v interface{}) {
	e.value.Store(v)
}

func (e *entry) getAccessTime() int64 {
	return atomic.LoadInt64(&e.accessTime)
}

func (e *entry) setAccessTime(v int64) {
	atomic.StoreInt64(&e.accessTime, v)
}

func (e *entry) getWriteTime() int64 {
	return atomic.LoadInt64(&e.writeTime)
}

func (e *entry) setWriteTime(v int64) {
	atomic.StoreInt64(&e.writeTime, v)
}

func (e *entry) getLoading() bool {
	return atomic.LoadInt32(&e.loading) != 0
}

func (e *entry) setLoading(v bool) bool {
	if v {
		return atomic.CompareAndSwapInt32(&e.loading, 0, 1)
	}
	return atomic.CompareAndSwapInt32(&e.loading, 1, 0)
}

func (e *entry) getInvalidated() bool {
	return atomic.LoadInt32(&e.invalidated) != 0
}

func (e *entry) setInvalidated(v bool) {
	if v {
		atomic.StoreInt32(&e.invalidated, 1)
	} else {
		atomic.StoreInt32(&e.invalidated, 0)
	}
}

// getEntry returns the entry attached to the given list element.
func getEntry(el *list.Element) *entry {
	return el.Value.(*entry)
}

// event is the cache event (add, hit or delete).
type event uint8

const (
	eventWrite event = iota
	eventAccess
	eventDelete
	eventClose
)

type entryEvent struct {
	entry *entry
	event event
}

// cache is a data structure for cache entries.
type cache struct {
	size int64                  // Access atomically - must be aligned on 32-bit
	segs [segmentCount]sync.Map // map[Key]*entry
}

func (c *cache) get(k interface{}, h uint64) *entry {
	seg := c.segment(h)
	v, ok := seg.Load(k)
	if ok {
		return v.(*entry)
	}
	return nil
}

func (c *cache) getOrSet(v *entry) *entry {
	seg := c.segment(v.hash)
	en, ok := seg.LoadOrStore(v.key, v)
	if ok {
		return en.(*entry)
	}
	atomic.AddInt64(&c.size, 1)
	return nil
}

func (c *cache) delete(v *entry) {
	seg := c.segment(v.hash)
	seg.Delete(v.key)
	atomic.AddInt64(&c.size, -1)
}

func (c *cache) len() int {
	return int(atomic.LoadInt64(&c.size))
}

func (c *cache) walk(fn func(*entry)) {
	for i := range c.segs {
		c.segs[i].Range(func(k, v interface{}) bool {
			fn(v.(*entry))
			return true
		})
	}
}

func (c *cache) segment(h uint64) *sync.Map {
	return &c.segs[h&segmentMask]
}

// policy is a cache policy.
type policy interface {
	// init initializes the policy.
	init(cache *cache, maximumSize int64)
	// write handles Write event for the entry.
	// It adds new entry and returns evicted entry if needed.
	write(entry *entry) *entry
	// access handles Access event for the entry.
	// It marks then entry recently accessed.
	access(entry *entry)
	// remove the entry.
	remove(entry *entry) *entry
	// iterate all entries by their access time.
	iterate(func(entry *entry) bool)
}

func newPolicy() policy {
	return &lruCache{}
}

// recencyQueue manages cache entries by write time.
type recencyQueue struct {
	ls list.List
}

func (w *recencyQueue) init(cache *cache, maximumSize int64) {
	w.ls.Init()
}

func (w *recencyQueue) write(en *entry) *entry {
	if en.writeList == nil {
		en.writeList = w.ls.PushFront(en)
	} else {
		w.ls.MoveToFront(en.writeList)
	}
	return nil
}

func (w *recencyQueue) access(en *entry) {
}

func (w *recencyQueue) remove(en *entry) *entry {
	if en.writeList == nil {
		return en
	}
	w.ls.Remove(en.writeList)
	en.writeList = nil
	return en
}

func (w *recencyQueue) iterate(fn func(en *entry) bool) {
	iterateListFromBack(&w.ls, fn)
}

type discardingQueue struct{}

func (discardingQueue) init(cache *cache, maximumSize int64) {
}

func (discardingQueue) write(en *entry) *entry {
	return nil
}

func (discardingQueue) access(en *entry) {
}

func (discardingQueue) remove(en *entry) *entry {
	return en
}

func (discardingQueue) iterate(fn func(en *entry) bool) {
}

func iterateListFromBack(ls *list.List, fn func(en *entry) bool) {
	for el := ls.Back(); el != nil; {
		en := getEntry(el)
		prev := el.Prev() // Get Prev as fn can delete the entry.
		if !fn(en) {
			return
		}
		el = prev
	}
}
