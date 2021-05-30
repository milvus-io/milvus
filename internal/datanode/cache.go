package datanode

import (
	"sync"
)

type segmentCache struct {
	cacheMu sync.RWMutex
	cache   map[UniqueID]bool // segmentID
}

func newSegmentCache() *segmentCache {
	return &segmentCache{
		cache: make(map[UniqueID]bool),
	}
}

func (c *segmentCache) checkIfFlushingOrFlushed(segID UniqueID) bool {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	if _, ok := c.cache[segID]; !ok {
		return true
	}

	return false

}

func (c *segmentCache) setIsFlushing(segID UniqueID) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	c.cache[segID] = true
}
