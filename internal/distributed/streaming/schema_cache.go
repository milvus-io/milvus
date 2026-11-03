package streaming

import (
	"sync"
	"time"
)

type schemaCacheEntry[V any] struct {
	value    V
	accessed time.Time
}

// schemaCache is an idle-expiring KV. Fetching, version selection and RPC
// coordination belong to its owner, not to the cache.
type schemaCache[K comparable, V any] struct {
	mu      sync.Mutex
	entries map[K]schemaCacheEntry[V]
	ttl     time.Duration
}

func newSchemaCache[K comparable, V any](ttl time.Duration) *schemaCache[K, V] {
	return &schemaCache[K, V]{entries: make(map[K]schemaCacheEntry[V]), ttl: ttl}
}

func (c *schemaCache[K, V]) load(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	now := time.Now()
	if !ok || now.Sub(entry.accessed) >= c.ttl {
		delete(c.entries, key)
		var zero V
		return zero, false
	}
	entry.accessed = now
	c.entries[key] = entry
	return entry.value, true
}

func (c *schemaCache[K, V]) store(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ttl > 0 {
		c.entries[key] = schemaCacheEntry[V]{value, time.Now()}
	}
}

func (c *schemaCache[K, V]) erase(key K) { c.mu.Lock(); defer c.mu.Unlock(); delete(c.entries, key) }
func (c *schemaCache[K, V]) expire(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, entry := range c.entries {
		if now.Sub(entry.accessed) >= c.ttl {
			delete(c.entries, key)
		}
	}
}
func (c *schemaCache[K, V]) clear() { c.mu.Lock(); defer c.mu.Unlock(); clear(c.entries) }
