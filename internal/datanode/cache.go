package datanode

import (
	"sync"
)

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

func (c *Cache) Cache(segID UniqueID) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	c.cacheMap[segID] = true
}

func (c *Cache) Remove(segIDs ...UniqueID) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	for _, id := range segIDs {
		delete(c.cacheMap, id)
	}
}
