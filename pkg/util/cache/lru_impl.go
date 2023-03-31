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
)

// lruCache is a LRU cache.
type lruCache struct {
	cache *cache
	cap   int64
	ls    list.List
}

// init initializes cache list.
func (l *lruCache) init(c *cache, cap int64) {
	l.cache = c
	l.cap = cap
	l.ls.Init()
}

// write adds new entry to the cache and returns evicted entry if necessary.
func (l *lruCache) write(en *entry) *entry {
	// Fast path
	if en.accessList != nil {
		// Entry existed, update its status instead.
		l.markAccess(en)
		return nil
	}

	// Try to add new entry to the list
	cen := l.cache.getOrSet(en)
	if cen == nil {
		// Brand new entry, add to the LRU list.
		en.accessList = l.ls.PushFront(en)
	} else {
		// Entry has already been added, update its value instead.
		cen.setValue(en.getValue())
		cen.setWriteTime(en.getWriteTime())
		if cen.accessList == nil {
			// Entry is loaded to the cache but not yet registered.
			cen.accessList = l.ls.PushFront(cen)
		} else {
			l.markAccess(cen)
		}
	}
	if l.cap > 0 && int64(l.ls.Len()) > l.cap {
		// Remove the last element when capacity exceeded.
		en = getEntry(l.ls.Back())
		return l.remove(en)
	}
	return nil
}

// access updates cache entry for a get.
func (l *lruCache) access(en *entry) {
	if en.accessList != nil {
		l.markAccess(en)
	}
}

// markAccess marks the element has just been accessed.
// en.accessList must not be null.
func (l *lruCache) markAccess(en *entry) {
	l.ls.MoveToFront(en.accessList)
}

// remove an entry from the cache.
func (l *lruCache) remove(en *entry) *entry {
	if en.accessList == nil {
		// Already deleted
		return nil
	}
	l.cache.delete(en)
	l.ls.Remove(en.accessList)
	en.accessList = nil
	return en
}

// iterate walks through all lists by access time.
func (l *lruCache) iterate(fn func(en *entry) bool) {
	iterateListFromBack(&l.ls, fn)
}
