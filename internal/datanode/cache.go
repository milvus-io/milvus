// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"sync"
)

// Cache stores flushing segments' ids to prevent flushing the same segment again and again.
//  Once a segment is flushed, its id will be removed from the cache.
//
//  A segment not in cache will be added into the cache when `FlushSegments` is called.
//   After the flush procedure, whether the segment successfully flushed or not,
//   it'll be removed from the cache. So if flush failed, the secondary flush can be triggered.
type Cache struct {
	cacheMu  sync.RWMutex
	cacheMap map[UniqueID]bool // TODO GOOSE: change into sync.map
}

func newCache() *Cache {
	return &Cache{
		cacheMap: make(map[UniqueID]bool),
	}
}

func (c *Cache) checkIfCached(key UniqueID) bool {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	_, ok := c.cacheMap[key]
	return ok
}

// Cache caches a specific segment ID into the cache
func (c *Cache) Cache(segID UniqueID) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	c.cacheMap[segID] = true
}

// Remove removes a set of segment IDs from the cache
func (c *Cache) Remove(segIDs ...UniqueID) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	for _, id := range segIDs {
		delete(c.cacheMap, id)
	}
}
