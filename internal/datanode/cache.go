package datanode

import (
	"sync"
)

// Cache stores flusing segments' ids to prevent flushing the same segment again and again.
//  Once the segment is flushed, its id will be removed from the cache.
type Cache struct {
	cacheMu  sync.RWMutex
	cacheMap map[UniqueID]bool
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
