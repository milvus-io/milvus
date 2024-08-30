package impls

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/blist"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

const (
	// TODO: configurable
	defaultCapacity = 16 * 1024 * 1024
)

// NewCache creates a new cache.
func NewCache(capacity int) *Cache {
	if capacity == 0 {
		capacity = defaultCapacity
	}
	return &Cache{
		mu:         sync.Mutex{},
		capacity:   capacity,
		immutables: make([]*blist.ImmutableContinousBlockList, 0),
		mutable:    nil,
	}
}

// Cache is a cache to store messages.
// TODO: clear cache after the Cache is closed.
type Cache struct {
	mu         sync.Mutex
	capacity   int
	immutables []*blist.ImmutableContinousBlockList
	mutable    *blist.MutableCountinousBlockList
}

// Append appends messages to cache.
// The Append operation is not concurrent-safe with other Append and RecordIncontinuous operation
func (c *Cache) Append(msgs []message.ImmutableMessage) {
	if len(msgs) == 0 {
		return
	}
	mutable, ok := c.getOrCreateMutableCountinousBlockList(msgs[0])
	if ok {
		mutable.Append(msgs)
		return
	}
	if len(msgs) > 1 {
		mutable.Append(msgs[1:])
		return
	}
}

// getOrCreateMutableCountinousBlockList gets or creates the mutable block list.
func (c *Cache) getOrCreateMutableCountinousBlockList(msg message.ImmutableMessage) (bl *blist.MutableCountinousBlockList, loaded bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mutable == nil {
		c.mutable = blist.NewMutableContinousBlockList(c.capacity, msg, c)
		return c.mutable, false
	}
	return c.mutable, true
}

// OnEvict is called when a block is evicted.
func (c *Cache) OnEvict(blockID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.evictFromMutable(blockID) {
		return
	}
	c.mustEvictFromImmutable(blockID)
}

// mustEvictFromImmutable evicts the block from immutable block list.
func (c *Cache) mustEvictFromImmutable(blockID int64) {
	for idx, b := range c.immutables {
		if b2, ok := b.Evict(blockID); ok {
			newList := make([]*blist.ImmutableContinousBlockList, 0, len(c.immutables)+len(b2)-1)
			newList = append(newList, c.immutables[:idx]...)
			newList = append(newList, b2...)
			newList = append(newList, c.immutables[idx+1:]...)
			c.immutables = newList
			return
		}
	}
	panic("the block id is not found in the cache")
}

// evictFromMutable evicts the block from mutable block list.
func (c *Cache) evictFromMutable(blockID int64) bool {
	if c.mutable == nil {
		return false
	}
	newImmutables, ok := c.mutable.Evict(blockID)
	if ok {
		c.immutables = append(c.immutables, newImmutables...)
	}
	return ok
}

// RecordIncontinuous records make the cache to record the incontinuous event.
// It will generate a new countinous block list to mark the incontinuous event.
func (c *Cache) RecordIncontinuous() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mutable != nil {
		c.immutables = append(c.immutables, c.mutable.IntoImmutable())
		c.mutable = nil
	}
}

// Read creates a scanner to read messages from cache.
func (c *Cache) Read(started message.MessageID) (walcache.MessageScanner, error) {
	readFrom := c.findReadFrom(started)
	if readFrom == nil {
		return nil, walcache.ErrNotFound
	}
	return readFrom.Read(started)
}

// findReadFrom finds the ReadFrom that contains the started message.
func (c *Cache) findReadFrom(started message.MessageID) walcache.MessageReader {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, immutable := range c.immutables {
		if immutable.Range().In(started) {
			return immutable
		}
	}

	if c.mutable != nil && c.mutable.Range().In(started) {
		return c.mutable
	}
	return nil
}
