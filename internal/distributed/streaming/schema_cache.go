package streaming

import (
	"container/list"
	"time"
)

const schemaExpireBatchSize = 64

type schemaCacheEntry[K comparable, V any] struct {
	key      K
	value    V
	accessed time.Time
	idle     *list.Element
}

// schemaCache is an idle-expiring KV ordered from least to most recently used.
// Its owner holds the shard lock. Expiry checks only the idle prefix and removes
// at most one batch, so the owner can release the lock between batches.
type schemaCache[K comparable, V any] struct {
	entries map[K]*schemaCacheEntry[K, V]
	idle    list.List
	ttl     time.Duration
	// onEvict runs under the owner lock when an entry is removed.
	onEvict func(K, V)
}

func newSchemaCache[K comparable, V any](ttl time.Duration) *schemaCache[K, V] {
	return &schemaCache[K, V]{entries: make(map[K]*schemaCacheEntry[K, V]), ttl: ttl}
}

func (c *schemaCache[K, V]) load(key K) (V, bool) {
	entry, ok := c.entries[key]
	now := time.Now()
	if !ok || now.Sub(entry.accessed) >= c.ttl {
		c.erase(key)
		var zero V
		return zero, false
	}
	entry.accessed = now
	c.idle.MoveToBack(entry.idle)
	return entry.value, true
}

func (c *schemaCache[K, V]) store(key K, value V) {
	if c.ttl <= 0 {
		return
	}
	if entry, ok := c.entries[key]; ok {
		entry.value, entry.accessed = value, time.Now()
		c.idle.MoveToBack(entry.idle)
		return
	}
	entry := &schemaCacheEntry[K, V]{key: key, value: value, accessed: time.Now()}
	entry.idle = c.idle.PushBack(entry)
	c.entries[key] = entry
}

func (c *schemaCache[K, V]) erase(key K) {
	if entry, ok := c.entries[key]; ok {
		c.idle.Remove(entry.idle)
		delete(c.entries, key)
		if c.onEvict != nil {
			c.onEvict(key, entry.value)
		}
	}
}

// expire returns whether another batch is needed at the same cutoff.
func (c *schemaCache[K, V]) expire(now time.Time) bool {
	for i := 0; i < schemaExpireBatchSize; i++ {
		first := c.idle.Front()
		if first == nil {
			return false
		}
		entry := first.Value.(*schemaCacheEntry[K, V])
		if now.Sub(entry.accessed) < c.ttl {
			return false
		}
		c.erase(entry.key)
	}
	first := c.idle.Front()
	return first != nil && now.Sub(first.Value.(*schemaCacheEntry[K, V]).accessed) >= c.ttl
}

func (c *schemaCache[K, V]) clear() {
	clear(c.entries)
	c.idle.Init()
}
