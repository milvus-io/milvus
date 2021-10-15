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
	cacheMap sync.Map
}

// newCache returns a new Cache
func newCache() *Cache {
	return &Cache{
		cacheMap: sync.Map{},
	}
}

// checkIfCached returns whether unique id is in cache
func (c *Cache) checkIfCached(key UniqueID) bool {
	_, ok := c.cacheMap.Load(key)
	return ok
}

// Cache caches a specific segment ID into the cache
func (c *Cache) Cache(segID UniqueID) {
	c.cacheMap.Store(segID, struct{}{})
}

// Remove removes a set of segment IDs from the cache
func (c *Cache) Remove(segIDs ...UniqueID) {
	for _, id := range segIDs {
		c.cacheMap.Delete(id)
	}
}
