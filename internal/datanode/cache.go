package datanode

import (
	"sync"
)

// Cache stores flusing segments' ids to prevent flushing the same segment again and again.
//  Once a segment is flushed, its id will be removed from the cache.
//
//  A segment not in cache will be added into the cache when `FlushSegments` is called.
//   After the flush procedure, wether the segment successfully flushed or not,
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
